//! Homebrew source
//!
//! Homebrew source will use brew.sh API to fetch all available bottles.
//! It will generate a list of URLs.

use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::Result;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use slog::info;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Homebrew {
    #[structopt(long, default_value = "https://formulae.brew.sh/api/formula.json")]
    pub api_base: String,
    #[structopt(long, default_value = "https://homebrew.bintray.com")]
    pub bottles_base: String,
    #[structopt(long, default_value = "all")]
    pub arch: String,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for Homebrew {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;
        let gen_map = crate::utils::generate_s3_url_reverse_encode_map();

        info!(logger, "fetching API json...");
        progress.set_message("fetching API json...");
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
        let bottles_base = if self.bottles_base.ends_with("/") {
            self.bottles_base.clone()
        } else {
            format!("{}/", self.bottles_base)
        };
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
            .filter(|url| self.arch.is_empty() || self.arch == "all" || url.contains(&self.arch))
            .map(|url| url.to_string())
            .filter_map(|url| {
                if url.starts_with(&bottles_base) {
                    Some(url[bottles_base.len()..].to_string())
                } else {
                    None
                }
            })
            .map(|url| crate::utils::rewrite_url_string(&gen_map, &url))
            .collect();

        progress.finish_with_message("done");

        Ok(crate::utils::snapshot_string_to_path(snapshot))
    }

    fn info(&self) -> String {
        format!("homebrew, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, TransferURL> for Homebrew {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.bottles_base, snapshot.0)))
    }
}
