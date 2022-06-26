//! Conda source
//!
//! Conda is a source storage that scans an conda repository.
//! This source yields a snapshot with size and checksum metadata.
//! To ensure consistency, repository data is always transferred
//! at the end. This is done by setting priority in snapshot metadata.

use std::io;
use std::io::ErrorKind;

use async_trait::async_trait;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::de::DeserializeSeed;
use serde::Deserialize;
use slog::{info, warn};
use structopt::StructOpt;
use tokio_util::io::{StreamReader, SyncIoBridge};

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

#[derive(Debug, Clone, StructOpt)]
pub struct CondaConfig {
    pub repo_config: String,
}

#[derive(Deserialize)]
pub struct CondaRepos {
    pub base: String,
    pub repos: Vec<String>,
}

pub struct Conda {
    /// conda config path
    config: CondaConfig,
    /// parsed conda repos
    repos: CondaRepos,
}

mod de {
    use std::fmt::Formatter;

    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
    use serde::Deserializer;

    use crate::metadata::SnapshotMeta;

    pub struct Snapshot<'a> {
        pub(crate) repo: &'a str,
    }

    impl<'de> DeserializeSeed<'de> for Snapshot<'de> {
        type Value = Vec<SnapshotMeta>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct MetadataVisitor<'a> {
                repo: &'a str,
            }

            impl<'de> Visitor<'de> for MetadataVisitor<'de> {
                type Value = Vec<SnapshotMeta>;

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    formatter.write_str("a map of conda metadata")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut packages = vec![];

                    while let Some(key) = map.next_key::<String>()? {
                        if key == "packages" || key == "packages.conda" {
                            packages
                                .append(&mut map.next_value_seed(Packages { repo: self.repo })?);
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }

                    Ok(packages)
                }
            }

            deserializer.deserialize_map(MetadataVisitor { repo: self.repo })
        }
    }

    struct Packages<'a> {
        repo: &'a str,
    }

    impl<'de> DeserializeSeed<'de> for Packages<'de> {
        type Value = Vec<SnapshotMeta>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct PackagesVisitor<'a> {
                repo: &'a str,
            }

            impl<'de> Visitor<'de> for PackagesVisitor<'de> {
                type Value = Vec<SnapshotMeta>;

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    formatter.write_str("a map of conda packages")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut packages = vec![];

                    while let Some(key) = map.next_key::<String>()? {
                        packages.push(map.next_value_seed(Package {
                            repo: self.repo,
                            name: key,
                        })?);
                    }

                    Ok(packages)
                }
            }

            deserializer.deserialize_map(PackagesVisitor { repo: self.repo })
        }
    }

    struct Package<'a> {
        repo: &'a str,
        name: String,
    }

    impl<'de> DeserializeSeed<'de> for Package<'de> {
        type Value = SnapshotMeta;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct PackageVisitor<'a> {
                repo: &'a str,
                name: String,
            }

            impl<'de> Visitor<'de> for PackageVisitor<'de> {
                type Value = SnapshotMeta;

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    formatter.write_str("a map representing a single conda package")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut size = None;
                    let mut sha256 = None;
                    while let Some(key) = map.next_key::<String>()? {
                        if key == "size" {
                            size = Some(map.next_value::<u64>()?);
                        } else if key == "sha256" {
                            sha256 = Some(map.next_value::<String>()?);
                        } else {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }

                    Ok(SnapshotMeta {
                        key: format!("{}/{}", self.repo, self.name),
                        size,
                        last_modified: None,
                        checksum_method: sha256.as_ref().map(|_| "sha256".to_string()),
                        checksum: sha256,
                        ..Default::default()
                    })
                }
            }

            deserializer.deserialize_map(PackageVisitor {
                repo: self.repo,
                name: self.name,
            })
        }
    }
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
impl SnapshotStorage<SnapshotMeta> for Conda {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
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
                let stream = client
                    .get(&repodata)
                    .send()
                    .await?
                    .bytes_stream()
                    .map_err(|e| io::Error::new(ErrorKind::Other, e));
                let reader = SyncIoBridge::new(StreamReader::new(stream));
                let mut packages = {
                    let repo = repo.clone();
                    tokio::task::spawn_blocking(move || {
                        let mut deserializer = serde_json::de::Deserializer::from_reader(reader);
                        de::Snapshot { repo: &repo }.deserialize(&mut deserializer)
                    })
                    .await
                    .expect("task panicked")?
                };
                snapshot.append(&mut packages);
                progress.set_message(&repo);
                snapshot.append(&mut vec![
                    SnapshotMeta::force(format!("{}/repodata.json", repo)),
                    SnapshotMeta::force(format!("{}/repodata.json.bz2", repo)),
                    SnapshotMeta::force(format!("{}/current_repodata.json", repo)),
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
            .collect::<Vec<_>>();

        Ok(snapshots)
    }

    fn info(&self) -> String {
        format!("conda, {:?}", self.config)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for Conda {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.repos.base, snapshot.key)))
    }
}
