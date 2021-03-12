use std::collections::{HashMap, HashSet};

use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, SourceStorage};

use crate::metadata::SnapshotMeta;
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use slog::info;
use structopt::StructOpt;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DownloadSource {
    pub dl_uri: String,
    pub dl_subdir: Option<String>,
    pub dl_hash: String,
}

type DistributionRelease = HashMap<String, BinarySource>;
type BinarySource = HashMap<String, DownloadSource>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Release {
    pub vi_tags: Vec<String>,
    pub vi_source_dl: Option<DownloadSource>,
    pub vi_arch: HashMap<String, DistributionRelease>,
}

impl Release {
    pub fn uris(&self) -> HashSet<&str> {
        let mut binary_uris: HashSet<&str> = self
            .vi_arch
            .values()
            .into_iter()
            .flat_map(|dist| {
                dist.values()
                    .into_iter()
                    .flat_map(|bin_src| bin_src.values().into_iter().map(|src| src.dl_uri.as_str()))
            })
            .collect();
        if let Some(src) = self.vi_source_dl.as_ref() {
            binary_uris.insert(src.dl_uri.as_str());
        }
        binary_uris
    }
}

impl Release {
    pub fn is_old(&self) -> bool {
        self.vi_tags.contains(&"old".to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Components {
    #[serde(rename = "Cabal")]
    pub cabal: HashMap<String, Release>,
    #[serde(rename = "HLS")]
    pub hls: HashMap<String, Release>,
    #[serde(rename = "GHCup")]
    pub ghcup: HashMap<String, Release>,
    #[serde(rename = "GHC")]
    pub ghc: HashMap<String, Release>,
}

impl Components {
    pub fn uris(&self, include_old_versions: bool) -> HashSet<&str> {
        let fields: [&HashMap<String, Release>; 4] =
            [&self.cabal, &self.hls, &self.ghcup, &self.ghc];
        fields
            .iter()
            .flat_map(|field| {
                field.values().into_iter().flat_map(|release| {
                    if !include_old_versions && release.is_old() {
                        HashSet::new()
                    } else {
                        release.uris()
                    }
                })
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GhcupYamlParser {
    pub ghcup_downloads: Components,
}

async fn get_yaml_url<'a>(base_url: &'a str, client: &'a Client) -> Result<String> {
    let version_matcher = regex::Regex::new("ghcupURL.*(?P<url>https://.*yaml)").unwrap();

    let ghcup_version_module = client
        .get(&format!("{}/lib/GHCup/Version.hs", base_url))
        .send()
        .await?
        .text()
        .await?;

    version_matcher
        .captures(ghcup_version_module.as_str())
        .and_then(|capture| capture.name("url"))
        .map(|group| String::from(group.as_str()))
        .ok_or_else(|| {
            Error::ProcessError(String::from(
                "unable to parse ghcup version from haskell src",
            ))
        })
}

async fn get_last_modified<'a>(client: &'a Client, url: &'a str) -> Result<Option<u64>> {
    Ok(client
        .head(url)
        .send()
        .await?
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|value| std::str::from_utf8(value.as_bytes()).ok())
        .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
        .map(|dt| dt.timestamp() as u64))
}

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupScript {
    #[structopt(long, default_value = "https://get-ghcup.haskell.org/")]
    pub script_url: String,
    #[structopt(long, help = "mirror url for packages")]
    pub target_mirror: String,
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
        let client = mission.client;

        info!(logger, "fetching metadata of ghcup install script...");
        progress.set_message("fetching head of url");
        let last_modified = get_last_modified(&client, self.script_url.as_str()).await?;

        progress.finish_with_message("done");
        Ok(vec![SnapshotMeta {
            key: String::from("install.sh"),
            last_modified,
            ..Default::default()
        }])
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

#[derive(Debug, Clone, StructOpt)]
pub struct Ghcup {
    #[structopt(
        long,
        default_value = "https://gitlab.haskell.org/haskell/ghcup-hs/-/raw/master/"
    )]
    pub ghcup_base: String,
    #[structopt(long)]
    pub include_old_versions: bool,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for Ghcup {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
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
            .map(|s| s.trim_start_matches("https://downloads.haskell.org/"))
            .map(String::from)
            .collect();

        progress.finish_with_message("done");
        Ok(crate::utils::snapshot_string_to_path(fetch_uris))
    }

    fn info(&self) -> String {
        format!("ghcup, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, TransferURL> for Ghcup {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "{}/{}",
            "https://downloads.haskell.org", snapshot.0
        )))
    }
}
