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
    #[error("S3 Error {0}")]
    S3Error(String),
    #[error("GCP Error {0}")]
    GCPError(String),
    #[error("Configure Error {0}")]
    ConfigureError(String),
    #[error("HTTP Error {0}")]
    HTTPError(reqwest::StatusCode),
    #[error("Pipe Error {0}")]
    PipeError(String),
    #[error("Json Decode Error {0}")]
    JsonDecodeError(#[from] serde_json::Error),
    #[error("Yaml Decode Error {0}")]
    YamlDecodeError(#[from] serde_yaml::Error),
    #[error("Datetime Parse Error {0}")]
    DatetimeParseError(#[from] chrono::ParseError),
    #[error("Checksum mismatch ({method}). Expect {expected}, got {got}")]
    ChecksumError {
        method: String,
        expected: String,
        got: String,
    },
}

impl From<aws_sdk_s3::primitives::ByteStreamError> for Error {
    fn from(error: aws_sdk_s3::primitives::ByteStreamError) -> Self {
        Error::S3Error(format!("S3 ByteStream Error: {:?}", error))
    }
}

impl From<google_bigquery2::Error> for Error {
    fn from(error: google_bigquery2::Error) -> Self {
        Error::GCPError(format!("GCP Error: {:?}", error))
    }
}

pub type Result<T> = result::Result<T, Error>;
