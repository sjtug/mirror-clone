use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferPath};
use crate::error::Result;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use slog::info;

#[derive(Debug)]
pub struct GitHubRelease {
    pub repo: String,
    pub version_to_retain: usize,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for GitHubRelease {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "fetching GitHub json...");
        let data = client
            .get(&format!(
                "https://api.github.com/repos/{}/releases",
                self.repo
            ))
            .send()
            .timeout(Duration::from_secs(60))
            .await
            .into_result()?
            .text()
            .timeout(Duration::from_secs(60))
            .await
            .into_result()?;

        info!(logger, "parsing...");
        let json: Value = serde_json::from_str(&data).unwrap();
        let releases = json.as_array().unwrap();
        let snapshot: Vec<String> = releases
            .into_iter()
            .filter_map(|releases| releases.as_object())
            .filter_map(|releases| {
                progress.set_message(
                    releases
                        .get("tag_name")
                        .and_then(|tag_name| tag_name.as_str())
                        .unwrap_or(""),
                );
                releases.get("assets")
            })
            .take(self.version_to_retain)
            .filter_map(|assets| assets.as_array())
            .flatten()
            .filter_map(|asset| asset.get("browser_download_url"))
            .filter_map(|url| url.as_str())
            .map(|url| url.replace("https://github.com/", ""))
            .collect();

        progress.finish_with_message("done");

        Ok(crate::utils::snapshot_string_to_path(snapshot))
    }

    fn info(&self) -> String {
        format!("github releases, {:?}", self)
    }
}
