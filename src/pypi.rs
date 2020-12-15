use crate::common::{Mission, SnapshotConfig};
use crate::error::Error;
use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::bar;

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use regex::Regex;
use slog::{info, warn};

#[derive(Debug)]
pub struct Pypi {
    pub simple_base: String,
    pub package_base: String,
    pub debug: bool,
}

#[async_trait]
impl SnapshotStorage<String> for Pypi {
    async fn snapshot(&mut self, mission: Mission, config: &SnapshotConfig) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "downloading pypi index...");
        let mut index = client
            .get(&format!("{}/", self.simple_base))
            .send()
            .await?
            .text()
            .await?;
        let matcher = Regex::new(r#"<a.*href="(.*?)".*>(.*?)</a>"#).unwrap();

        info!(logger, "parsing index...");
        if self.debug {
            index = index[..100000].to_string();
        }
        let caps: Vec<(String, String)> = matcher
            .captures_iter(&index)
            .map(|cap| (cap[1].to_string(), cap[2].to_string()))
            .collect();

        info!(logger, "downloading package index...");
        progress.set_length(caps.len() as u64);
        progress.set_style(bar());

        let packages: Result<Vec<Vec<(String, String)>>> =
            futures::stream::iter(caps.into_iter().map(|(url, name)| {
                let client = client.clone();
                let simple_base = self.simple_base.clone();
                let progress = progress.clone();
                let matcher = matcher.clone();
                let logger = logger.clone();
                let func = async move {
                    progress.set_message(&name);
                    let package = client
                        .get(&format!("{}/{}", simple_base, url))
                        .send()
                        .await?
                        .text()
                        .await?;
                    let caps: Vec<(String, String)> = matcher
                        .captures_iter(&package)
                        .map(|cap| (cap[1].to_string(), cap[2].to_string()))
                        .collect();
                    progress.inc(1);
                    Ok::<Vec<(String, String)>, Error>(caps)
                };
                async move {
                    match func.await {
                        Ok(x) => Ok(x),
                        Err(err) => {
                            warn!(logger, "failed to fetch index {:?}", err);
                            Ok(vec![])
                        }
                    }
                }
            }))
            .buffer_unordered(config.concurrent_resolve)
            .try_collect()
            .await;

        let snapshot = packages?
            .into_iter()
            .flatten()
            .map(|(url, _)| url.replace("../../packages/", "").to_string())
            .collect();

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("pypi, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for Pypi {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
