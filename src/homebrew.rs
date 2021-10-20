//! Homebrew source
//!
//! Homebrew source will use brew.sh API to fetch all available bottles.
//! It will generate a list of URLs.
//!
//! Reference: https://github.com/ustclug/ustcmirror-images/blob/master/homebrew-bottles/bottles-json/src/main.rs
//! MIT License, Copyright (c) 2017 Jian Zeng

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage};

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::metadata::SnapshotMeta;
use async_trait::async_trait;
use serde::Deserialize;
use slog::info;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
pub struct HomebrewConfig {
    #[structopt(long, default_value = "https://formulae.brew.sh/api/formula.json")]
    pub api_base: String,
    #[structopt(long, default_value = "all")]
    pub arch: String,
}

pub struct Homebrew {
    pub config: HomebrewConfig,
    url_mapping: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Versions {
    stable: Option<String>,
    bottle: bool,
}

#[derive(Deserialize)]
struct Bottle {
    stable: Option<BottleStable>,
}

#[derive(Deserialize)]
struct BottleStable {
    rebuild: u64,
    files: HashMap<String, BottleInfo>,
}

#[derive(Deserialize)]
struct Formula {
    name: String,
    versions: Versions,
    bottle: Bottle,
    revision: u64,
}

#[derive(Deserialize)]
struct Formulae(Vec<Formula>);

#[derive(Deserialize)]
struct BottleInfo {
    url: String,
    #[allow(dead_code)]
    sha256: String,
}

impl Homebrew {
    pub fn new(config: HomebrewConfig) -> Self {
        Self {
            config,
            url_mapping: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for Homebrew {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;
        let gen_map = crate::utils::generate_s3_url_reverse_encode_map();

        info!(logger, "fetching API json...");
        progress.set_message("fetching API json...");
        let data = client
            .get(&self.config.api_base)
            .send()
            .timeout(Duration::from_secs(60))
            .await
            .into_result()?
            .text()
            .timeout(Duration::from_secs(60))
            .await
            .into_result()?;

        info!(logger, "parsing...");
        let formulae: Formulae = serde_json::from_str(&data).unwrap();
        let mut snapshots = vec![];
        for f in formulae.0 {
            progress.set_message(&f.name);

            if f.versions.bottle {
                if let Some(versions_stable) = f.versions.stable {
                    if let Some(bs) = f.bottle.stable {
                        for (platform, v) in bs.files {
                            if self.config.arch.is_empty()
                                || self.config.arch == "all"
                                || platform == self.config.arch
                            {
                                let key = format!(
                                    "{name}-{version}{revision}.{platform}.bottle{rebuild}.tar.gz",
                                    name = f.name,
                                    version = versions_stable,
                                    revision = if f.revision == 0 {
                                        "".to_owned()
                                    } else {
                                        format!("_{}", f.revision)
                                    },
                                    platform = platform,
                                    rebuild = if bs.rebuild == 0 {
                                        "".to_owned()
                                    } else {
                                        format!(".{}", bs.rebuild)
                                    },
                                );
                                let key = crate::utils::rewrite_url_string(&gen_map, &key);
                                self.url_mapping.insert(key.clone(), v.url);
                                snapshots.push(SnapshotMeta {
                                    key,
                                    checksum_method: Some(String::from("sha256")),
                                    checksum: Some(v.sha256),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
            }
        }

        progress.finish_with_message("done");

        Ok(snapshots)
    }

    fn info(&self) -> String {
        format!("homebrew, {:?}", self.config)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for Homebrew {
    async fn get_object(&self, snapshot: &SnapshotMeta, mission: &Mission) -> Result<TransferURL> {
        let url = self
            .url_mapping
            .get(&snapshot.key)
            .expect("no URL for bottle");
        let resp = mission
            .client
            .get(url)
            .header(reqwest::header::AUTHORIZATION, "Bearer QQ==")
            .header(
                reqwest::header::ACCEPT,
                "application/vnd.oci.image.index.v1+json",
            )
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(Error::HTTPError(resp.status()));
        }
        Ok(TransferURL(resp.url().as_str().to_string()))
    }
}
