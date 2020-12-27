use crate::common::{Mission, SnapshotConfig};
use crate::error::Result;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use slog::info;

#[derive(Debug)]
pub struct Homebrew {
    pub api_base: String,
    pub arch: String,
}

#[async_trait]
impl SnapshotStorage<String> for Homebrew {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "fetching API json...");
        let data = client
            .get(&self.api_base)
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
        let packages = json.as_array().unwrap();
        let snapshot: Vec<String> = packages
            .into_iter()
            .filter_map(|package| package.as_object())
            .filter_map(|package| {
                progress.set_message(
                    package
                        .get("name")
                        .and_then(|name| name.as_str())
                        .unwrap_or(""),
                );
                package.get("bottle")
            })
            .filter_map(|bottles| bottles.as_object())
            .filter_map(|bottles| bottles.get("stable"))
            .filter_map(|bottles| bottles.as_object())
            .filter_map(|bottles| bottles.get("files"))
            .filter_map(|files| files.as_object())
            .flat_map(|files| files.values())
            .filter_map(|bottle_urls| bottle_urls.get("url"))
            .filter_map(|url| url.as_str())
            .filter(|url| {
                if self.arch.is_empty() {
                    true
                } else {
                    url.contains(&self.arch)
                }
            })
            .map(|url| url.to_string())
            .map(|url| url.replace("https://homebrew.bintray.com/", ""))
            .map(|url| url.replace("https://linuxbrew.bintray.com/", ""))
            .collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("homebrew, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for Homebrew {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
