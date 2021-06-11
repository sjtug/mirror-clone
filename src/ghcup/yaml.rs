use async_trait::async_trait;
use slog::info;
use structopt::StructOpt;
use url::Url;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

use super::utils::get_yaml_url;

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupYaml {
    #[structopt(
        long,
        default_value = "https://gitlab.haskell.org/haskell/ghcup-hs/-/raw/master/"
    )]
    pub ghcup_base: String,
    #[structopt(long, default_value = "ghcup-0.0.4.yaml")]
    pub additional_yaml: Vec<String>,
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

        let base_url = self.ghcup_base.trim_end_matches('/');

        info!(logger, "fetching ghcup config...");
        progress.set_message("downloading version file");
        let yaml_url = Url::parse(get_yaml_url(base_url, &client).await?.as_str())
            .map_err(|_| Error::ProcessError(String::from("invalid ghcup yaml url")))?;

        // additional yaml paths
        let mut yaml_paths = self
            .additional_yaml
            .iter()
            .map(|filename| {
                let mut new_yaml_url = yaml_url.clone();
                {
                    let mut segments = new_yaml_url
                        .path_segments_mut()
                        .map_err(|_| Error::ProcessError(String::from("invalid ghcup yaml url")))?;
                    segments.pop().push(filename);
                }
                Ok(new_yaml_url.path()[1..].to_string())
            })
            .collect::<Result<Vec<_>>>()?;

        yaml_paths.push(yaml_url.path()[1..].to_string()); // base yaml path

        progress.finish_with_message("done");

        Ok(yaml_paths.into_iter().map(SnapshotMeta::force).collect())
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
