pub struct Oracle {
    pub client: reqwest::Client,
    pub progress: indicatif::ProgressBar,
}
