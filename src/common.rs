use indicatif::ProgressBar;
use reqwest::Client;
use slog::Logger;

#[derive(Clone)]
pub struct Mission {
    pub progress: ProgressBar,
    pub client: Client,
    pub logger: Logger,
}

#[derive(Debug, Copy, Clone)]
pub struct SnapshotConfig {
    pub concurrent_resolve: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotPath(pub String, pub bool);

impl SnapshotPath {
    pub fn new(key: String) -> Self {
        Self(key, false)
    }

    pub fn force(key: String) -> Self {
        Self(key, true)
    }
}

#[derive(Debug)]
pub struct TransferURL(pub String);
