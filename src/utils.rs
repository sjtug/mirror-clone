use crate::error::{Error, Result};
use futures::Future;
use futures_retry::{ErrorHandler, FutureFactory, FutureRetry, RetryPolicy};
use futures_util::StreamExt;
use log::warn;
use overlay::OverlayFile;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn download_to_file(url: String, file: &mut OverlayFile) -> Result<usize> {
    use rand::prelude::*;
    if thread_rng().gen::<f64>() > 0.8 {
        // return Err(Error::MockError("lost connection".to_string()));
    }
    let response = reqwest::get(url.as_str()).await?;
    let mut stream = response.bytes_stream();
    let mut size = 0;
    while let Some(v) = stream.next().await {
        let v = v?;
        file.file().write_all(&v).await?;
        size += v.len();
    }
    Ok(size)
}

pub async fn content_of(url: String, file: &mut OverlayFile) -> Result<Vec<u8>> {
    let size = download_to_file(url, file).await?;
    let mut buf = Vec::with_capacity(size);
    let file = file.file();
    file.seek(std::io::SeekFrom::Start(0)).await?;
    file.read_to_end(&mut buf).await?;
    assert_eq!(buf.len(), size);
    Ok(buf)
}

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
        const MIN_WAIT_MSEC: f32 = 5_f32;
        const MAX_WAIT_MSEC: f32 = 1000_f32;
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
        if current_attempt > self.max_attempts {
            warn!(
                "[{}] All attempts ({}) have been used up",
                self.display_name, self.max_attempts
            );
            return RetryPolicy::ForwardError(e);
        }
        warn!(
            "[{}] Attempt {}/{} has failed",
            self.display_name, current_attempt, self.max_attempts
        );

        RetryPolicy::WaitRetry(self.calculate_wait_duration())
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
        Err((err, _)) => Err(err),
    }
}
