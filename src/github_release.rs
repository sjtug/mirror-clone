//! GitHub Release source
//!
//! GitHubRelease source will fetch the GitHub API when taking snapshots.
//! Then, it will construct a list of downloadable URLs.

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use slog::info;
use std::time::Duration;
use structopt::StructOpt;

#[derive(Deserialize, Debug)]
pub struct GitHubReleaseAsset {
    url: String,
    id: u64,
    name: String,
    content_type: String,
    size: u64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    browser_download_url: String,
}

#[derive(Deserialize, Debug)]
pub struct GitHubReleaseItem {
    tag_name: String,
    name: String,
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Clone, StructOpt)]
pub struct GitHubRelease {
    #[structopt(long, help = "GitHub Repo")]
    pub repo: String,
    #[structopt(long, help = "Version numbers to retain")]
    pub version_to_retain: usize,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for GitHubRelease {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
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
        let releases = serde_json::from_str::<Vec<GitHubReleaseItem>>(&data)?;
        let replace_string = format!("https://github.com/{}/", self.repo);
        let snapshot: Vec<SnapshotMeta> = releases
            .into_iter()
            .map(|release| {
                progress.set_message(&release.tag_name);
                release.assets
            })
            .take(self.version_to_retain)
            .flatten()
            .map(|asset| SnapshotMeta {
                key: if asset.browser_download_url.starts_with(&replace_string) {
                    asset.browser_download_url[replace_string.len()..].to_string()
                } else {
                    panic!("Unmatched base URL: {:?}", asset)
                },
                size: Some(asset.size),
                last_modified: Some(asset.updated_at.timestamp() as u64),
                ..Default::default()
            })
            .collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("github releases, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for GitHubRelease {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "https://github.com/{}/{}",
            self.repo, snapshot.key
        )))
    }
}
