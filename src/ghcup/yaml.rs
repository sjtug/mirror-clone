use async_trait::async_trait;
use slog::info;
use structopt::StructOpt;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

use super::utils::{get_last_modified, get_yaml_url};

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupYaml {
    #[structopt(
        long,
        default_value = "https://gitlab.haskell.org/haskell/ghcup-hs/-/raw/master/"
    )]
    pub ghcup_base: String,
    #[structopt(long, help = "mirror url for packages")]
    pub target_mirror: String,
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
        let yaml_url = get_yaml_url(base_url, &client).await?;
        let last_modified = get_last_modified(&client, &yaml_url).await?;

        let yaml_url = yaml_url.trim_start_matches("https://www.haskell.org/");
        progress.finish_with_message("done");
        Ok(vec![SnapshotMeta {
            key: String::from(yaml_url),
            last_modified,
            ..Default::default()
        }])
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
