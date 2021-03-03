use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::Result;
use crate::traits::SnapshotStorage;

use async_trait::async_trait;
use regex::Regex;
use slog::info;

#[derive(Debug)]
pub struct HtmlScanner {
    pub url: String,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for HtmlScanner {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "downloading web content...");
        let index = client.get(&self.url).send().await?.text().await?;
        let matcher = Regex::new(r#"<a.*href="(.*?)".*"#).unwrap();

        let snapshot: Vec<String> = matcher
            .captures_iter(&index)
            .map(|cap| cap[1].to_string())
            .collect();

        progress.finish_with_message("done");

        Ok(crate::utils::snapshot_string_to_path(snapshot))
    }

    fn info(&self) -> String {
        format!("html_scanner, {:?}", self)
    }
}
