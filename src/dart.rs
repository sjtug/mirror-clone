use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde_json::Value;
use slog::{info, warn};
use structopt::StructOpt;

const PUB_DEV: &str = "https://pub.dev";

fn archive_key(package_name: &str, version_name: &str, archive_url: &str) -> String {
    let base_prefix = format!("{}/", PUB_DEV);
    let archive_prefix = format!("{}/api/packages/", PUB_DEV);

    if archive_url.starts_with(&base_prefix) {
        return format!("packages/{}/versions/{}.tar.gz", package_name, version_name);
    }

    if let Some(key) = archive_url.strip_prefix(&archive_prefix) {
        return key.to_string();
    }

    panic!("Unexpected archive URL format: {}", archive_url);
}

#[derive(Debug, Clone, StructOpt)]
pub struct Dart {
    #[structopt(long, default_value = "https://mirrors.tuna.tsinghua.edu.cn/dart-pub")]
    pub base: String,
    #[structopt(long)]
    pub debug: bool,
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for Dart {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        let api_base = format!("{}/api/packages", PUB_DEV);

        info!(logger, "fetching packages...");
        let mut next_url = api_base.clone();
        let mut package_name = vec![];
        let mut page: usize = 1;

        loop {
            let data = client.get(&next_url).send().await?.text().await?;
            let data: Value = serde_json::from_str(&data).unwrap();
            let data = data.as_object().unwrap();

            let packages = data.get("packages").unwrap().as_array().unwrap();

            for package in packages {
                package_name.push(package.get("name").unwrap().as_str().unwrap().to_string());
            }

            let next_url_str = data.get("next_url");
            if let Some(next_url_str) = next_url_str {
                if !next_url_str.is_null() {
                    next_url = next_url_str.as_str().unwrap().to_string();
                } else {
                    break;
                }
            } else {
                break;
            }
            progress.set_message(&format!(
                "fetching page {}, total packages = {}",
                page,
                package_name.len()
            ));
            page += 1;
        }

        if self.debug {
            package_name.truncate(100);
        }

        progress.inc_length(package_name.len() as u64);

        let snapshots: Result<Vec<Vec<SnapshotMeta>>> =
            stream::iter(package_name.into_iter().map(|name| {
                let client = client.clone();
                let progress = progress.clone();
                let logger = logger.clone();

                let func = async move {
                    progress.set_message(&name);
                    let package = client
                        .get(format!("{}/api/packages/{}", PUB_DEV, name))
                        .send()
                        .await?
                        .text()
                        .await?;

                    let data: Value = serde_json::from_str(&package).unwrap();
                    let versions = data.get("versions").unwrap().as_array().unwrap();
                    let archives: Vec<SnapshotMeta> = versions
                        .iter()
                        .filter_map(|version| {
                            let version_name = version.get("version")?.as_str()?;
                            let archive_url = version.get("archive_url")?.as_str()?;

                            Some(SnapshotMeta {
                                key: archive_key(&name, version_name, archive_url),
                                ..Default::default()
                            })
                        })
                        .collect();

                    progress.inc(1);
                    Ok::<Vec<SnapshotMeta>, Error>(archives)
                };
                async move {
                    match func.await {
                        Ok(x) => Ok(x),
                        Err(err) => {
                            warn!(logger, "failed to fetch package meta {:?}", err);
                            Ok(vec![])
                        }
                    }
                }
            }))
            .buffer_unordered(config.concurrent_resolve)
            .try_collect()
            .await;

        let snapshot: Vec<_> = snapshots?.into_iter().flatten().collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("dart, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for Dart {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.base, snapshot.key)))
    }
}
