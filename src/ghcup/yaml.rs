use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use itertools::Itertools;
use slog::info;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::{SnapshotMeta, SnapshotMetaFlag};
use crate::traits::{SnapshotStorage, SourceStorage};

use super::utils::{fetch_last_tag, filter_map_file_objs, list_files};
use super::GhcupRepoConfig;

#[derive(Debug, Clone)]
pub struct GhcupYaml {
    pub ghcup_repo_config: GhcupRepoConfig,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for GhcupYaml {
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
        let yaml_objs = filter_map_file_objs(
            list_files(
                &client,
                repo_config,
                fetch_last_tag(&client, repo_config).await?,
            )
            .await?,
        )
        .collect_vec();

        progress.finish_with_message("done");

        Ok(yaml_objs
            .into_iter()
            .map(|obj| format!("ghcup/data/{}", obj.name()))
            .map(|key| SnapshotMeta {
                key,
                last_modified: Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                flags: SnapshotMetaFlag {
                    force: true,
                    force_last: true,
                },
                ..Default::default()
            })
            .collect())
    }

    fn info(&self) -> String {
        format!("ghcup_config, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for GhcupYaml {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "{}/{}",
            "https://www.haskell.org", snapshot.key
        )))
    }
}
