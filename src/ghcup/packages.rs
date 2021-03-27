use super::parser::GhcupYamlParser;
use super::utils::get_yaml_url;
use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};
use async_trait::async_trait;
use slog::info;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupPackages {
    #[structopt(
        long,
        help = "Ghcup upstream",
        default_value = "https://gitlab.haskell.org/haskell/ghcup-hs/-/raw/master/"
    )]
    pub ghcup_base: String,
    #[structopt(long, help = "Include legacy versions of packages")]
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

        let base_url = self.ghcup_base.trim_end_matches('/');

        info!(logger, "fetching ghcup config...");
        progress.set_message("downloading version file");
        let yaml_url = get_yaml_url(base_url, &client).await?;
        progress.set_message("downloading yaml config");
        let yaml_data = client.get(&yaml_url).send().await?.bytes().await?;
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
