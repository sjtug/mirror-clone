use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error {0}")]
    Io(#[from] std::io::Error),
    #[error("Reqwest Error {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("UTF8 Error {0}")]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error("Regex Error {0}")]
    RegexError(#[from] regex::Error),
    #[error("Mock Error {0}")]
    #[allow(unused)]
    MockError(String),
    #[error("Checksum Error {checksum_type} expected {expected}, get {checksum}")]
    ChecksumError {
        checksum_type: String,
        expected: String,
        checksum: String,
    },
    #[error("Length Mismatch expected {expected}, get {size}")]
    LengthMismatch { expected: usize, size: usize },
    #[error("HTTP Error {0}")]
    HTTPError(reqwest::StatusCode),
    #[error("JSON Decode Error {0}")]
    JsonDecodeError(#[from] serde_json::Error),
    #[error("Overlay Filesystem Error {0}")]
    OverlayFSError(#[from] overlay::OverlayFSError),
}

pub type Result<T> = std::result::Result<T, Error>;
