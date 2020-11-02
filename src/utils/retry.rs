use super::download_to_file;
use futures::lock::Mutex;
use futures::Future;
use futures_retry::{ErrorHandler, FutureFactory, FutureRetry, RetryPolicy};
use reqwest::StatusCode;
use slog_scope::warn;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use overlay::{OverlayDirectory, OverlayFile};

use crate::error::{Error, Result};

pub struct IoHandler<D> {
    max_attempts: usize,
    current_attempt: usize,
    display_name: D,
}

impl<D> IoHandler<D> {
    pub fn new(max_attempts: usize, display_name: D) -> Self {
        IoHandler {
            max_attempts,
            current_attempt: 0,
            display_name,
        }
    }

    /// Calculates a duration to wait before a retry based on the current attempt number.
    ///
    /// I'm using the `atan` function to increase the duration from 5 to 1000 milliseconds (or
    /// actually close to 1000) rather fast, but never actually exceed the upper value.
    ///
    /// On the first attempt the duration will be 5 msec, on the second — 299 ms, then 503 ms,
    /// 628 ms, 706 ms and by the tenth attempt it will be about 861 ms.
    fn calculate_wait_duration(&self) -> Duration {
        const MIN_WAIT_MSEC: f32 = 1000_f32;
        const MAX_WAIT_MSEC: f32 = 10000_f32;
        const FRAC_DIFF: f32 = 1_f32 / (MAX_WAIT_MSEC - MIN_WAIT_MSEC);
        let duration_msec = MIN_WAIT_MSEC
            + (self.current_attempt as f32 - 1_f32).atan()
                * ::std::f32::consts::FRAC_2_PI
                * FRAC_DIFF;
        Duration::from_millis(duration_msec.round() as u64)
    }
}

impl<D> ErrorHandler<Error> for IoHandler<D>
where
    D: ::std::fmt::Display,
{
    type OutError = Error;

    fn handle(&mut self, current_attempt: usize, e: Error) -> RetryPolicy<Error> {
        if matches!(e, Error::Reqwest(_) | Error::MockError(_) | Error::ChecksumError { .. } | Error::LengthMismatch { .. })
        {
            if current_attempt > self.max_attempts {
                warn!(
                    "[{}] all attempts ({}) have been used up",
                    self.display_name, self.max_attempts
                );
                return RetryPolicy::ForwardError(e);
            }

            warn!(
                "[{}] attempt {}/{} has failed: {}",
                self.display_name, current_attempt, self.max_attempts, e
            );

            return RetryPolicy::WaitRetry(self.calculate_wait_duration());
        }

        if let Error::HTTPError(status) = e {
            if matches!(status, StatusCode::TOO_MANY_REQUESTS) {
                // wait for 30 secs if too many requests
                warn!("{}, retry after 30s", e);
                return RetryPolicy::WaitRetry(Duration::from_millis(30000));
            }
            if status.is_server_error() {
                warn!("{}, retry after 1min", e);
                return RetryPolicy::WaitRetry(Duration::from_millis(60000));
            }
        }

        RetryPolicy::ForwardError(e)
    }
}

pub async fn retry<FF: FutureFactory<FutureItem = F>, F: Future<Output = Result<T>>, T>(
    f: FF,
    times: usize,
    msg: String,
) -> Result<T> {
    let result = FutureRetry::new(f, IoHandler::new(times, msg)).await;
    match result {
        Ok((x, _)) => Ok(x),
        Err((err, x)) => {
            warn!("{} after {} trials", err, x);
            Err(err)
        }
    }
}

pub async fn retry_download(
    client: reqwest::Client,
    base: Arc<Mutex<OverlayDirectory>>,
    url: String,
    path: impl AsRef<Path>,
    times: usize,
    msg: impl AsRef<str>,
) -> Result<OverlayFile> {
    let path = path.as_ref().to_owned();
    Ok(retry(
        || async {
            let mut repo_file = base
                .lock()
                .await
                .create_file_for_write(path.clone())
                .await?;
            download_to_file(client.clone(), url.clone(), &mut repo_file).await?;
            Ok(repo_file)
        },
        times,
        msg.as_ref().to_string(),
    )
    .await?)
}
