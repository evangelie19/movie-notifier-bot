#![allow(dead_code)]

use movie_notifier_bot::app::{
    AppError, NoopDispatcher, dispatch_and_persist, fetch_releases, restore_history,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let mut history = restore_history()?;
    let releases = fetch_releases(&mut history).await?;

    let dispatcher = NoopDispatcher;
    dispatch_and_persist(&dispatcher, &mut history, &releases).await?;

    Ok(())
}
