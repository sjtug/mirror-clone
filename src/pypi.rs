use crate::common::Mission;
use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};
use async_trait::async_trait;
use slog::{info, warn};

pub struct Pypi;

impl Pypi {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SnapshotStorage<String> for Pypi {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "downloading pypi index...");
        tokio::time::delay_for(std::time::Duration::from_secs(3)).await;

        info!(logger, "downloading pypi package index...");
        for i in 0..100 {
            progress.inc(1);
            progress.set_message(&i.to_string());
            tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
        }

        progress.finish_with_message("done");

        Ok(vec![])
    }

    fn info(&self) -> String {
        format!("pypi from pypi.org")
    }
}

#[async_trait]
impl SourceStorage<String> for Pypi {
    async fn get_object(&self) -> Result<String> {
        Ok("".to_string())
    }
}
