use indicatif::ProgressBar;
use reqwest::Client;
use slog::Logger;

pub struct Mission {
    pub progress: ProgressBar,
    pub client: Client,
    pub logger: Logger,
}

#[derive(Debug)]
pub struct SnapshotConfig {
    pub concurrent_resolve: usize,
}
