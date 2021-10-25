//! rustup source
//!
//! Rustup source provides a file list of recent rustup toolchains.
//! It is recommended to use it with `--no-delete` flag. This source
//! yields path snapshots.

use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, SourceStorage};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures_util::{stream, StreamExt, TryStreamExt};
use regex::Regex;
use slog::{info, warn};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Rustup {
    #[structopt(long, default_value = "https://static.rust-lang.org")]
    pub base: String,
    #[structopt(long, default_value = "120")]
    pub days_to_retain: usize,
}

fn day_earlier(date_time: DateTime<Utc>, days: i64) -> Option<DateTime<Utc>> {
    date_time.checked_sub_signed(Duration::days(days))
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for Rustup {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        let channels = ["beta", "stable", "nightly"];

        info!(logger, "fetching channels...");

        let matcher = Regex::new(r#"url = "(.*)""#).unwrap();

        let mut targets = vec![];
        for day_back in 0..self.days_to_retain {
            let now = Utc::now();
            let day = day_earlier(now, day_back as i64).unwrap();
            let day_string = day.format("%Y-%m-%d");
            for channel in &channels {
                targets.push((day_string.to_string(), channel.to_string()));
            }
        }

        // cache 4x more stable toolchains
        for day_back in self.days_to_retain..self.days_to_retain * 4 {
            let now = Utc::now();
            let day = day_earlier(now, day_back as i64).unwrap();
            let day_string = day.format("%Y-%m-%d");
            targets.push((day_string.to_string(), "stable".to_string()));
        }

        let packages: Result<Vec<Vec<SnapshotPath>>> =
            stream::iter(targets.into_iter().map(|(day_string, channel)| {
                let client = client.clone();
                let base = self.base.clone();
                let progress = progress.clone();
                let matcher = matcher.clone();
                let logger = logger.clone();
                let func = async move {
                    let mut caps = vec![];
                    let target = format!("dist/{}/channel-rust-{}.toml", day_string, channel);
                    progress.set_message(&target);
                    let data = client
                        .get(&format!("{}/{}", base, target))
                        .send()
                        .await?
                        .text()
                        .await?;

                    for capture in matcher.captures_iter(&data) {
                        let url = &capture[1];
                        let url = url.replace("https://static.rust-lang.org/", "");
                        caps.push(SnapshotPath::new(url));
                    }

                    caps.push(SnapshotPath::force(target));
                    progress.inc(1);
                    Ok::<_, Error>(caps)
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

        let mut snapshot: Vec<SnapshotPath> = packages?.into_iter().flatten().collect();

        for channel in channels {
            snapshot.push(SnapshotPath::force(format!(
                "dist/channel-rust-{}.toml",
                channel
            )));
            snapshot.push(SnapshotPath::force(format!(
                "dist/channel-rust-{}.toml.sha256",
                channel
            )));
        }

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("rustup, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, TransferURL> for Rustup {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.base, snapshot.0)))
    }
}
