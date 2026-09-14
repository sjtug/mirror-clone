//! Conda source
//!
//! Conda is a source storage that scans an conda repository.
//! This source yields a snapshot with size and checksum metadata.
//! To ensure consistency, repository data is always transferred
//! at the end. This is done by setting priority in snapshot metadata.

use std::io;
// use std::io::ErrorKind;

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Deserialize;
use serde::de::DeserializeSeed;
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

    use serde::Deserializer;
    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};

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
                    .map_err(io::Error::other);
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

                // current_repodata.json is an optional reduced index; retired channels and
                // platforms may omit it while still publishing canonical repodata.json.
                let current_repodata = format!("{}/{}/current_repodata.json", base, repo);
                let current_repodata_status = client.head(&current_repodata).send().await?.status();

                progress.set_message(&repo);
                snapshot.append(&mut vec![
                    SnapshotMeta::force(format!("{}/repodata.json", repo)),
                    SnapshotMeta::force(format!("{}/repodata.json.bz2", repo)),
                ]);
                match current_repodata_status {
                    reqwest::StatusCode::NOT_FOUND => {}
                    status if status.is_success() => snapshot.push(SnapshotMeta::force(format!(
                        "{}/current_repodata.json",
                        repo
                    ))),
                    status => return Err(Error::HTTPError(status)),
                }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use indicatif::ProgressBar;
    use reqwest::Client;
    use slog::{Discard, Logger, o};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn test_conda(base: String) -> Conda {
        Conda {
            config: CondaConfig {
                repo_config: "unused-in-test".to_string(),
            },
            repos: CondaRepos {
                base,
                repos: vec!["channel/linux-64".to_string()],
            },
        }
    }

    fn test_mission() -> Mission {
        Mission {
            progress: ProgressBar::hidden(),
            client: Client::new(),
            logger: Logger::root(Discard, o!()),
        }
    }

    async fn snapshot_with_current_status(
        current_status: u16,
    ) -> (Result<Vec<SnapshotMeta>>, Vec<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 2048];
                while !request.ends_with(b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let request_line = String::from_utf8(request)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string();
                server_requests.lock().unwrap().push(request_line.clone());

                let (status, body) = match request_line.as_str() {
                    "GET /channel/linux-64/repodata.json HTTP/1.1" => (200, r#"{"packages":{}}"#),
                    "HEAD /channel/linux-64/current_repodata.json HTTP/1.1" => (current_status, ""),
                    request => panic!("unexpected fixture request: {request}"),
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });

        let result = test_conda(format!("http://{address}"))
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 1,
                },
            )
            .await;
        server.abort();
        let requests = requests.lock().unwrap().clone();
        (result, requests)
    }

    #[tokio::test]
    async fn snapshot_omits_current_repodata_on_not_found() {
        let (snapshot, requests) = snapshot_with_current_status(404).await;
        let keys = snapshot
            .unwrap()
            .into_iter()
            .map(|item| item.key)
            .collect::<Vec<_>>();

        assert!(!keys.contains(&"channel/linux-64/current_repodata.json".to_string()));
        assert!(
            requests
                .iter()
                .any(|request| request.starts_with("HEAD /channel/linux-64/current_repodata.json "))
        );
    }

    #[tokio::test]
    async fn snapshot_includes_current_repodata_when_available() {
        let (snapshot, _) = snapshot_with_current_status(200).await;
        let keys = snapshot
            .unwrap()
            .into_iter()
            .map(|item| item.key)
            .collect::<Vec<_>>();

        assert!(keys.contains(&"channel/linux-64/current_repodata.json".to_string()));
    }

    #[tokio::test]
    async fn snapshot_propagates_current_repodata_server_errors() {
        let (snapshot, _) = snapshot_with_current_status(500).await;

        assert!(matches!(
            snapshot,
            Err(Error::HTTPError(reqwest::StatusCode::INTERNAL_SERVER_ERROR))
        ));
    }

    #[tokio::test]
    async fn snapshot_propagates_current_repodata_transport_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 2048];
            while !request.ends_with(b"\r\n\r\n") {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            assert!(
                String::from_utf8(request)
                    .unwrap()
                    .starts_with("GET /channel/linux-64/repodata.json ")
            );
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"packages\":{}}",
                )
                .await
                .unwrap();
        });

        let snapshot = test_conda(format!("http://{address}"))
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 1,
                },
            )
            .await;
        server.await.unwrap();

        assert!(matches!(snapshot, Err(Error::Reqwest(_))));
    }
}
