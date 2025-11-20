pub use tokio_macros::main;

pub mod runtime {
    use std::future::Future;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    pub struct Runtime;

    impl Runtime {
        pub fn new() -> Result<Self, std::io::Error> {
            Ok(Self)
        }

        pub fn block_on<F: Future>(&self, future: F) -> F::Output {
            block_on(future)
        }
    }

    pub fn block_on<F: Future>(future: F) -> F::Output {
        executor::block_on(future)
    }

    mod executor {
        use super::*;
        pub fn block_on<F: Future>(future: F) -> F::Output {
            let mut future = Box::pin(future);
            let waker = dummy_waker();
            let mut cx = Context::from_waker(&waker);
            loop {
                match future.as_mut().poll(&mut cx) {
                    Poll::Ready(value) => return value,
                    Poll::Pending => std::thread::yield_now(),
                }
            }
        }

        fn dummy_waker() -> Waker {
            unsafe { Waker::from_raw(dummy_raw_waker()) }
        }

        unsafe fn dummy_raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                unsafe { dummy_raw_waker() }
            }
            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}
            RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, wake, wake_by_ref, drop))
        }
    }
}

pub mod fs {
    use std::io;
    use std::path::Path;

    pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
        std::fs::write(path, contents)
    }

    pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }
}
