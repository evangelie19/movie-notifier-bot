use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use movie_notifier_bot::telegram::{
    SendMessageRequest, TelegramDispatcher, TelegramTransport, TelegramTransportResponse,
};
use reqwest::StatusCode;
use tokio::time::timeout;

#[derive(Clone)]
struct MockTransport {
    responses: Arc<Mutex<VecDeque<TelegramTransportResponse>>>,
    calls: Arc<AtomicUsize>,
}

impl MockTransport {
    fn new(responses: Vec<TelegramTransportResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TelegramTransport for MockTransport {
    async fn post_json(
        &self,
        _url: &str,
        _payload: &SendMessageRequest,
    ) -> Result<TelegramTransportResponse, reqwest::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut responses = self.responses.lock().expect("очередь ответов доступна");
        Ok(responses
            .pop_front()
            .expect("ответы должны быть подготовлены заранее"))
    }
}

fn dispatcher_for(transport: Arc<MockTransport>) -> TelegramDispatcher {
    TelegramDispatcher::builder("TOKEN", vec![1])
        .transport(transport)
        .retry_delays(vec![Duration::from_millis(10)])
        .max_retries(3)
        .build()
}

#[tokio::test]
async fn send_batch_delivers_all_messages() {
    let transport = Arc::new(MockTransport::new(vec![
        TelegramTransportResponse {
            status: StatusCode::OK,
            body: String::new(),
        },
        TelegramTransportResponse {
            status: StatusCode::OK,
            body: String::new(),
        },
    ]));

    let dispatcher = dispatcher_for(transport.clone());
    dispatcher
        .send_batch(1, vec!["Первое сообщение", "Второе сообщение"])
        .await
        .expect("отправка должна завершиться успешно");

    assert_eq!(transport.call_count(), 2);
}

#[tokio::test]
async fn rate_limit_response_is_retried_with_retry_after() {
    let transport = Arc::new(MockTransport::new(vec![
        TelegramTransportResponse {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"ok":false,"parameters":{"retry_after":0}}"#.to_string(),
        },
        TelegramTransportResponse {
            status: StatusCode::OK,
            body: String::new(),
        },
    ]));

    let dispatcher = dispatcher_for(transport.clone());
    dispatcher
        .send_batch(1, vec!["тестовое сообщение"])
        .await
        .expect("повтор должен завершиться успехом");

    assert_eq!(transport.call_count(), 2);
}

#[tokio::test]
async fn server_errors_are_retried_before_erroring() {
    let transport = Arc::new(MockTransport::new(vec![
        TelegramTransportResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: String::new(),
        },
        TelegramTransportResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: String::new(),
        },
        TelegramTransportResponse {
            status: StatusCode::OK,
            body: String::new(),
        },
    ]));

    let dispatcher = dispatcher_for(transport.clone());
    dispatcher
        .send_batch(1, vec!["запрос"])
        .await
        .expect("должно пройти после повторов");

    assert_eq!(transport.call_count(), 3);
}

#[tokio::test]
async fn unknown_chat_returns_error_without_calling_api() {
    let transport = Arc::new(MockTransport::new(Vec::new()));
    let dispatcher = TelegramDispatcher::builder("TOKEN", vec![1])
        .transport(transport.clone())
        .build();

    let send = dispatcher.send_batch(999, vec!["сообщение"]);
    let result = timeout(Duration::from_secs(1), send)
        .await
        .expect("future должна завершиться");

    assert!(result.is_err());
    assert_eq!(transport.call_count(), 0);
}
