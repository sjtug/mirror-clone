use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, SourceStorage};

use async_trait::async_trait;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use slog::{info, warn};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct CondaConfig {
    pub repo_config: String,
}

#[derive(Deserialize)]
pub struct CondaRepos {
    pub base: String,
    pub repos: Vec<String>,
}

pub struct Conda {
    config: CondaConfig,
    repos: CondaRepos,
}

fn parse_index(data: &[u8]) -> Result<Vec<String>> {
    let v: JsonValue = serde_json::from_slice(data)?;
    let mut result = vec![];

    let package_mapper = |(key, _value): (&String, &JsonValue)| key.clone();

    if let Some(JsonValue::Object(map)) = v.get("packages") {
        result.append(&mut map.iter().map(package_mapper).collect());
    }
    if let Some(JsonValue::Object(map)) = v.get("packages.conda") {
        result.append(&mut map.iter().map(package_mapper).collect());
    }

    Ok(result)
}

impl Conda {
    pub fn new(config: CondaConfig) -> Self {
        let content = std::fs::read(&config.repo_config).unwrap();
        let repos = serde_yaml::from_str(std::str::from_utf8(&content).unwrap()).unwrap();
        Self { config, repos }
    }
}

impl std::fmt::Debug for Conda {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.config.fmt(f)
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for Conda {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        let fetch = |repo: String| {
            info!(logger, "fetching {}", repo);
            let progress = progress.clone();
            let base = self.repos.base.clone();
            let client = client.clone();
            let logger = logger.clone();
            let repo_ = repo.clone();

            let future = async move {
                let mut snapshot = vec![];
                let repodata = format!("{}/{}/repodata.json", base, repo);
                let index_data = client.get(&repodata).send().await?.bytes().await?;
                let packages = parse_index(&index_data)?;
                snapshot.extend(packages.into_iter().map(|pkg| format!("{}/{}", repo, pkg)));
                progress.set_message(&repo);
                snapshot.append(&mut vec![
                    "repodata.json".to_string(),
                    "repodata.json.bz2".to_string(),
                    "current_repodata.json".to_string(),
                ]);
                Ok::<_, Error>(snapshot)
            };

            async move {
                let result = future.await;
                if let Err(err) = result.as_ref() {
                    warn!(logger, "failed to fetch {}: {:?}", repo_, err);
                }
                result
            }
        };

        let snapshots = stream::iter(self.repos.repos.clone())
            .map(fetch)
            .buffer_unordered(4)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .map(SnapshotPath)
            .collect::<Vec<_>>();

        Ok(snapshots)
    }

    fn info(&self) -> String {
        format!("conda, {:?}", self.config)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, TransferURL> for Conda {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.repos.base, snapshot.0)))
    }
}
