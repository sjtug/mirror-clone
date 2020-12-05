use indicatif::ProgressBar;
use reqwest::Client;
use slog::Logger;

pub struct Mission {
    pub progress: ProgressBar,
    pub client: Client,
    pub logger: Logger,
}
