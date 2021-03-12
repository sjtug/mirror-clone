use indicatif::ProgressBar;
use reqwest::Client;
use slog::Logger;

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
pub struct SnapshotPath(pub String);

#[derive(Debug)]
pub struct TransferPath(pub String);

#[derive(Debug)]
pub struct TransferURL(pub String);
