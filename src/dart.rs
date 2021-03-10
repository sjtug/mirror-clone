use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::traits::SnapshotStorage;

use async_trait::async_trait;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::Value;
use slog::{info, warn};

#[derive(Debug)]
pub struct Dart {
    pub base: String,
    pub debug: bool,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for Dart {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        let api_base = format!("{}/api/packages", self.base);

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

        let snapshots: Result<Vec<Vec<String>>> =
            stream::iter(package_name.into_iter().map(|name| {
                let client = client.clone();
                let base = format!("{}/", self.base);
                let progress = progress.clone();
                let logger = logger.clone();

                let func = async move {
                    progress.set_message(&name);
                    let package = client
                        .get(&format!("{}/api/packages/{}", base, name))
                        .send()
                        .await?
                        .text()
                        .await?;

                    let data: Value = serde_json::from_str(&package).unwrap();
                    let versions = data.get("versions").unwrap().as_array().unwrap();
                    let archives: Vec<String> = versions
                        .iter()
                        .filter_map(|version| version.get("archive_url"))
                        .filter_map(|archive_url| archive_url.as_str())
                        .map(|archive_url| archive_url.replace(&base, ""))
                        .collect();

                    progress.inc(1);
                    Ok::<Vec<String>, Error>(archives)
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

        Ok(crate::utils::snapshot_string_to_path(snapshot))
    }

    fn info(&self) -> String {
        format!("dart, {:?}", self)
    }
}
