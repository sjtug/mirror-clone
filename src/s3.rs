use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, TargetStorage};
use async_trait::async_trait;
use reqwest::{redirect::Policy, Client, ClientBuilder};
use slog::info;

#[derive(Debug)]
pub struct MirrorIntelConfig {
    base: String,
}

pub struct MirrorIntel {
    config: MirrorIntelConfig,
    client: Client,
}

impl MirrorIntel {
    pub fn new(base: String) -> Self {
        Self {
            config: MirrorIntelConfig { base },
            client: ClientBuilder::new()
                .user_agent("mirror-clone / 0.1 (siyuan.internal.sjtug.org)")
                .redirect(Policy::none())
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for MirrorIntel {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "checking intel connection...");
        client.head(&self.config.base).send().await?;
        progress.finish_with_message("done");

        // We always return empty file list, and diff transfer will transfer
        // all objects.
        Ok(vec![])
    }

    fn info(&self) -> String {
        format!("s3, {:?}", self.config)
    }
}

type ByteStream = std::fs::File;

#[async_trait]
impl TargetStorage<String, ByteStream> for MirrorIntel {
    async fn put_object(
        &self,
        _snapshot: &String,
        _item: ByteStream,
        _mission: &Mission,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_object(&self, _snapshot: &String, _mission: &Mission) -> Result<()> {
        Err(Error::StorageError(
            "delete is not supported for mirror-intel backend".to_string(),
        ))
    }
}
