use async_trait::async_trait;
use slog::{info, warn};

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::ghcup::utils::get_raw_blob_url;
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

use super::parser::{GhcupYamlParser, EXPECTED_CONFIG_VERSION};
use super::utils::{filter_map_file_objs, list_files};
use super::GhcupRepoConfig;

#[derive(Debug, Clone)]
pub struct GhcupPackages {
    pub ghcup_repo_config: GhcupRepoConfig,
    pub include_old_versions: bool,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for GhcupPackages {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;
        let repo_config = &self.ghcup_repo_config;

        info!(logger, "fetching ghcup config...");
        progress.set_message("querying version files");
        let latest_yaml_obj = filter_map_file_objs(
            list_files(&client, repo_config, &self.ghcup_repo_config.branch).await?,
        )
        .max_by(|x, y| x.version.cmp(&y.version))
        .ok_or_else(|| Error::ProcessError(String::from("no config file found")))?;

        if latest_yaml_obj.version != EXPECTED_CONFIG_VERSION {
            warn!(
                logger,
                "unmatched ghcup config yaml. expected: {}, got: {}",
                EXPECTED_CONFIG_VERSION,
                latest_yaml_obj.version
            )
        }

        progress.set_message("downloading yaml config");
        let latest_yaml_blob_url = get_raw_blob_url(&client, repo_config, latest_yaml_obj).await?;
        let yaml_data = client
            .get(latest_yaml_blob_url.url)
            .send()
            .await?
            .bytes()
            .await?;
        let ghcup_config: GhcupYamlParser = serde_yaml::from_slice(&yaml_data)?;

        let fetch_uris: Vec<_> = ghcup_config
            .ghcup_downloads
            .uris(self.include_old_versions)
            .into_iter()
            .filter_map(|s| s.strip_prefix("https://downloads.haskell.org/"))
            .map(String::from)
            .collect();

        progress.finish_with_message("done");
        Ok(crate::utils::snapshot_string_to_meta(fetch_uris))
    }

    fn info(&self) -> String {
        format!("ghcup_packages, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for GhcupPackages {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "{}/{}",
            "https://downloads.haskell.org", snapshot.key
        )))
    }
}
