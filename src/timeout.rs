use crate::error::{Error, Result};
use std::future::Future;
use std::time::Duration;
use tokio::time::{Elapsed, Timeout};

type StdResult<T, E> = std::result::Result<T, E>;

pub trait TryTimeoutExt<T> {
    fn into_result(self) -> Result<T>;
}

impl<T, E> TryTimeoutExt<T> for StdResult<StdResult<T, E>, Elapsed>
where
    E: Into<Error>,
{
    fn into_result(self) -> Result<T> {
        match self {
            Ok(x) => x.map_err(|e| e.into()),
            Err(_) => Err(Error::TimeoutError(())),
        }
    }
}

pub trait TryTimeoutFutureExt: Future {
    fn timeout(self, duration: Duration) -> Timeout<Self>
    where
        Self: Sized,
    {
        tokio::time::timeout(duration, self)
    }
}

impl<T: ?Sized> TryTimeoutFutureExt for T where T: Future {}
