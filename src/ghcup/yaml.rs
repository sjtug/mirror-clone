use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use slog::info;
use structopt::StructOpt;
use url::Url;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

use super::utils::get_yaml_url;
use super::version::Version;

#[derive(Debug, Clone)]
struct GhcupRepoConfig {
    host: String,
    repo: String,
    pagination: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileInfo {
    id: String,
    name: String,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TagInfo {
    name: String,
    commit: CommitInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitInfo {
    id: String,
}

impl TagInfo {
    pub fn id(&self) -> &str {
        &self.commit.id
    }
}

#[derive(Debug, Clone)]
struct ObjectInfo {
    id: String,
    name: String,
    version: Version,
}

async fn fetch_last_tag(client: &Client, config: &GhcupRepoConfig) -> Result<String> {
    let req = client.get(format!(
        "https://{}/api/v4/projects/{}/repository/tags",
        config.host,
        urlencoding::encode(&*config.repo)
    ));
    let res: Vec<TagInfo> = req
        .send()
        .await
        .map_err(|_| Error::ProcessError(String::from("unable to fetch last tag")))?
        .json()
        .await
        .map_err(|_| Error::ProcessError(String::from("unable to parse tag meta")))?;
    Ok(res
        .first()
        .ok_or_else(|| Error::ProcessError(String::from("no tag found")))?
        .id()
        .to_string())
}

async fn list_files(
    client: &Client,
    config: &GhcupRepoConfig,
    commit: String,
) -> Result<Vec<FileInfo>> {
    let mut output = Vec::new();
    for page in 1.. {
        let res: Vec<FileInfo> = client
            .get(format!(
                "https://{}/api/v4/projects/{}/repository/tree",
                config.host,
                urlencoding::encode(&*config.repo)
            ))
            .query(&[("per_page", config.pagination), ("page", page)])
            .query(&[("ref", commit.clone())])
            .send()
            .await
            .map_err(|_| Error::ProcessError(String::from("unable to list files")))?
            .json()
            .await
            .map_err(|_| Error::ProcessError(String::from("unable to parse files meta")))?;
        if !res.is_empty() {
            output.extend(res);
        } else {
            break;
        }
    }
    Ok(output)
}

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupYaml {
    #[structopt(
        long,
        default_value = "https://gitlab.haskell.org/haskell/ghcup-hs/-/raw/master/"
    )]
    pub ghcup_base: String,
    #[structopt(
        long,
        default_value = "ghcup-0.0.4.yaml,ghcup-0.0.5.yaml,ghcup-0.0.6.yaml"
    )]
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
                Ok(new_yaml_url.path()[1..].to_string()) // remove first char (slash)
            })
            .collect::<Result<Vec<_>>>()?;

        yaml_paths.push(yaml_url.path()[1..].to_string()); // append base yaml path (slash ditto)

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
