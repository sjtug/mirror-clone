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
        client.head(&self.base).send().await?;
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
    async fn put_object(&self, item: String, mission: &Mission) -> Result<()> {
        let target_url = format!("{}/{}", self.base, item);
        let response = mission.client.get(&target_url).send().await?;
        if let Some(content_length) = response.content_length() {
            if !response.url().as_str().contains("jcloud") {
                tokio::time::delay_for(std::time::Duration::from_millis(
                    content_length / 1000000 / 32,
                ))
                .await;
            }
        }
        if let Some(queue_length) = response.headers().get("X-Intel-Queue-Length") {
            let queue_length: u64 = queue_length.to_str().unwrap().parse().unwrap();
            if queue_length > 16384 {
                warn!(mission.logger, "queue full, length={}", queue_length);
                tokio::time::delay_for(std::time::Duration::from_secs((queue_length - 16384) * 10))
                    .await;
            }
        }
        Ok(())
    }
}
