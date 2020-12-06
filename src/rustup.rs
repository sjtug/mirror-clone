use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::bar;
use crate::{common::Mission, error::Error};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, TryStreamExt};
use regex::Regex;
use slog::{info, warn};

#[derive(Debug)]
pub struct Rustup {
    pub base: String,
    pub days_to_retain: usize,
}

fn day_earlier(date_time: DateTime<Utc>, days: i64) -> Option<DateTime<Utc>> {
    date_time.checked_sub_signed(Duration::days(days))
}

#[async_trait]
impl SnapshotStorage<String> for Rustup {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        let mut snapshot = vec![];
        let channels = ["beta", "stable", "nightly"];

        info!(logger, "fetching channels...");

        let matcher = Regex::new(r#"url = "(.*)""#).unwrap();

        for day_back in 1..self.days_to_retain {
            let now = Utc::now();
            let day = day_earlier(now, day_back as i64).unwrap();
            let day_string = day.format("%Y-%m-%d");
            for channel in &channels {
                let target = format!("dist/{}/channel-rust-{}.toml", day_string, channel);
                progress.set_message(&target);
                let data = client
                    .get(&format!("{}/{}", self.base, target))
                    .send()
                    .await?
                    .text()
                    .await?;

                for capture in matcher.captures_iter(&data) {
                    let url = &capture[1];
                    let url = url.replace("https://static.rust-lang.org/", "");
                    snapshot.push(url);
                }
                progress.inc(1);
            }
        }

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("pypi, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for Rustup {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
