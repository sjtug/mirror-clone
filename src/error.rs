use std::result;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reqwest Error {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Process Error {0}")]
    ProcessError(String),
    #[error("IO Error {0}")]
    IoError(#[from] std::io::Error),
    #[error("None Error")]
    NoneError,
    #[error("Zip Error {0}")]
    ZipError(#[from] zip::result::ZipError),
    #[error("Timeout Error")]
    TimeoutError(()),
    #[error("Storage Error {0}")]
    StorageError(String),
    #[error("Rusoto Error {0}")]
    RusotoError(String),
    #[error("Configure Error {0}")]
    ConfigureError(String),
    #[error("HTTP Error {0}")]
    HTTPError(reqwest::StatusCode),
    #[error("Pipe Error {0}")]
    PipeError(String),
}

impl<T: std::fmt::Debug> From<rusoto_core::RusotoError<T>> for Error {
    fn from(error: rusoto_core::RusotoError<T>) -> Self {
        Error::RusotoError(format!("Rusoto Error: {:?}", error))
    }
}

pub type Result<T> = result::Result<T, Error>;
