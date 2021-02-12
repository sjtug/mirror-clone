use crate::common::{Mission, SnapshotConfig};
use crate::error::Result;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use slog::info;

#[derive(Debug)]
pub struct Gradle {
    pub api_base: String,
    pub distribution_base: String,
}

#[async_trait]
impl SnapshotStorage<String> for Gradle {
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
                        .get("version")
                        .and_then(|version| version.as_str())
                        .unwrap_or(""),
                );
                if let Some(rc_for) = package.get("rcFor") {
                    if let Some(rc_for) = rc_for.as_str() {
                        if rc_for != "" {
                            return None;
                        }
                    }
                }
                package.get("downloadUrl")
            })
            .filter_map(|url| url.as_str())
            .filter(|url| url.starts_with(&self.distribution_base))
            .map(|url| url.to_string())
            .map(|url| url.replace(&self.distribution_base, ""))
            .collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("gradle, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for Gradle {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
