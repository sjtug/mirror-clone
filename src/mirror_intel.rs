use crate::common::Mission;
use crate::error::Result;
use crate::traits::{SnapshotStorage, TargetStorage};
use async_trait::async_trait;
use slog::{info, warn};

#[derive(Debug)]
pub struct MirrorIntel {
    base: String,
}

impl MirrorIntel {
    pub fn new(base: String) -> Self {
        Self { base }
    }
}

#[async_trait]
impl SnapshotStorage<String> for MirrorIntel {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "checking intel connection...");
        client.get(&self.base).send().await?;
        progress.finish_with_message("done");

        // We always return empty file list, and diff transfer will transfer
        // all objects.
        Ok(vec![])
    }

    fn info(&self) -> String {
        format!("mirror_intel, {:?}", self)
    }
}

#[async_trait]
impl TargetStorage<String> for MirrorIntel {
    async fn put_object(&self, item: String) -> Result<()> {
        Ok(())
    }
}
