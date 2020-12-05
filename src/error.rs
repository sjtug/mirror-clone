use std::result;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reqwest Error {0}")]
    Reqwest(#[from] reqwest::Error),
}

pub type Result<T> = result::Result<T, Error>;
