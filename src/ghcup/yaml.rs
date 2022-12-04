use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::future::join_all;
use itertools::Itertools;
use slog::info;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::{SnapshotMeta, SnapshotMetaFlag};
use crate::traits::{SnapshotStorage, SourceStorage};

use super::utils::{filter_map_file_objs, get_raw_blob_url, list_files};
use super::GhcupRepoConfig;

#[derive(Debug, Clone)]
pub struct GhcupYaml {
    pub ghcup_repo_config: GhcupRepoConfig,
    path_prefix: &'static str,
    name_url_map: HashMap<String, String>,
}

impl GhcupYaml {
    pub fn new(ghcup_repo_config: GhcupRepoConfig, legacy: bool) -> Self {
        let path_prefix = if legacy {
            "ghcup/data"
        } else {
            "haskell/ghcup-metadata/master"
        };
        GhcupYaml {
            ghcup_repo_config,
            path_prefix,
            name_url_map: HashMap::new(),
        }
    }
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
        let yaml_objs: Vec<_> = join_all(
            filter_map_file_objs(list_files(&client, repo_config, &repo_config.branch).await?)
                .map(|obj| get_raw_blob_url(&client, repo_config, obj)),
        )
        .await
        .into_iter()
        .try_collect()?;

        progress.finish_with_message("done");

        Ok(yaml_objs
            .into_iter()
            .map(|obj| {
                let key = format!("{}/{}", self.path_prefix, obj.name);
                self.name_url_map.insert(key.clone(), obj.url);
                key
            })
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
        Ok(TransferURL(
            self.name_url_map.get(&snapshot.key).unwrap().to_string(),
        ))
    }
}
