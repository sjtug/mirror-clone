use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};
use async_trait::async_trait;
use serde_json::Value;
use slog::info;
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Gradle {
    #[structopt(long, default_value = "https://services.gradle.org/versions/all")]
    pub api_base: String,
    #[structopt(long, default_value = "https://services.gradle.org/distributions/")]
    pub distribution_base: String,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for Gradle {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
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
        let snapshot: Vec<SnapshotMeta> = packages
            .iter()
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
                        if !rc_for.is_empty() {
                            return None;
                        }
                    }
                }
                package.get("downloadUrl")
            })
            .filter_map(|url| url.as_str())
            .filter(|url| url.starts_with(&self.distribution_base))
            .map(|url| url.to_string())
            .map(|url| {
                if url.starts_with(&self.distribution_base) {
                    url[self.distribution_base.len()..].to_string()
                } else {
                    panic!("package doesn't lay at its base {}", url)
                }
            })
            .map(|key| SnapshotMeta::new(key))
            .collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("gradle, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for Gradle {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "{}/{}",
            self.distribution_base, snapshot.key
        )))
    }
}
