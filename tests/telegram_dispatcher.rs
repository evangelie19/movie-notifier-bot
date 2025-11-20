use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use movie_notifier_bot::telegram::TelegramDispatcher;
use serde_json::json;
use tokio::time::timeout;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn dispatcher_for(server: &MockServer) -> TelegramDispatcher {
    TelegramDispatcher::builder("TOKEN", vec![1])
        .base_url(server.uri())
        .retry_delays(vec![Duration::from_millis(10)])
        .max_retries(3)
        .build()
}

#[tokio::test]
async fn send_batch_delivers_all_messages() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/botTOKEN/sendMessage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .expect(2)
        .mount(&server)
        .await;

    let dispatcher = dispatcher_for(&server);
    dispatcher
        .send_batch(1, vec!["Первое сообщение", "Второе сообщение"])
        .await
        .expect("отправка должна завершиться успешно");
}

#[tokio::test]
async fn rate_limit_response_is_retried_with_retry_after() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();

    Mock::given(method("POST"))
        .and(path("/botTOKEN/sendMessage"))
        .respond_with(move |_req: &wiremock::Request| {
            let call = counter.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                ResponseTemplate::new(429).set_body_json(json!({
                    "ok": false,
                    "parameters": { "retry_after": 0 }
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({ "ok": true }))
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    let dispatcher = dispatcher_for(&server);
    dispatcher
        .send_batch(1, vec!["тестовое сообщение"])
        .await
        .expect("повтор должен завершиться успехом");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn server_errors_are_retried_before_erroring() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();

    Mock::given(method("POST"))
        .and(path("/botTOKEN/sendMessage"))
        .respond_with(move |_req: &wiremock::Request| {
            let call = counter.fetch_add(1, Ordering::SeqCst);
            if call < 2 {
                ResponseTemplate::new(500).set_body_json(json!({ "ok": false }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({ "ok": true }))
            }
        })
        .expect(3)
        .mount(&server)
        .await;

    let dispatcher = dispatcher_for(&server);
    dispatcher
        .send_batch(1, vec!["запрос"])
        .await
        .expect("должно пройти после повторов");

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unknown_chat_returns_error_without_calling_api() {
    let server = MockServer::start().await;
    let dispatcher = TelegramDispatcher::builder("TOKEN", vec![1])
        .base_url(server.uri())
        .build();

    let send = dispatcher.send_batch(999, vec!["сообщение"]);
    let result = timeout(Duration::from_secs(1), send)
        .await
        .expect("future должна завершиться");

    assert!(result.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}
