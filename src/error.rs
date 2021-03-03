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
}

pub type Result<T> = result::Result<T, Error>;
