use async_trait::async_trait;
use slog::info;
use structopt::StructOpt;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupScript {
    #[structopt(long, default_value = "https://get-ghcup.haskell.org/")]
    pub script_url: String,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for GhcupScript {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;

        info!(logger, "fetching metadata of ghcup install script...");
        progress.set_message("fetching head of url");

        progress.finish_with_message("done");
        Ok(vec![SnapshotMeta::force(String::from("install.sh"))])
    }

    fn info(&self) -> String {
        format!("ghcup_install_script, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for GhcupScript {
    async fn get_object(
        &self,
        _snapshot: &SnapshotMeta,
        _mission: &Mission,
    ) -> Result<TransferURL> {
        Ok(TransferURL(self.script_url.clone()))
    }
}
