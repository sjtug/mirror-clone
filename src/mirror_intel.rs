//! MirrorIntel backend
//!
//! MirrorIntel backend interacts with mirror-intel. This backend will
//! yield an empty snapshot. When using with simple diff transfer, all
//! objects will be fed into MirrorIntel backend. Given a source URL,
//! this backend will send HEAD request to corresponding mirror-intel
//! endpoint. Therefore, if the object exists in S3 backend, mirror-intel
//! will just ignore this request. Otherwise, the object will be downloaded
//! in the background by mirror-intel, and this backend will acknowledge its
//! absence and slow down sending HEAD.

use crate::error::{Error, Result};
use crate::traits::{Key, SnapshotStorage, TargetStorage};
use crate::{
    common::{Mission, SnapshotConfig, SnapshotPath, TransferPath},
    metadata::SnapshotMeta,
};
use async_trait::async_trait;
use reqwest::{redirect::Policy, Client, ClientBuilder};
use slog::{info, warn};

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
                .user_agent(crate::utils::user_agent())
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
        format!("mirror_intel, {:?}", self.config)
    }
}

#[async_trait]
impl<Snapshot: Key> TargetStorage<Snapshot, TransferPath> for MirrorIntel {
    async fn put_object(
        &self,
        _snapshot: &Snapshot,
        item: TransferPath,
        mission: &Mission,
    ) -> Result<()> {
        let item = item.0;
        let target_url = format!("{}/{}", self.config.base, item);
        let response = self.client.head(&target_url).send().await?;
        let headers = response.headers().clone();
        drop(response);

        if let Some(location) = headers.get("Location") {
            if !location.to_str().unwrap().contains("jcloud") {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        if let Some(queue_length) = headers.get("X-Intel-Queue-Length") {
            let queue_length: u64 = queue_length.to_str().unwrap().parse().unwrap();
            if queue_length > 16384 {
                warn!(mission.logger, "queue full, length={}", queue_length);
                tokio::time::sleep(std::time::Duration::from_secs(queue_length - 16384)).await;
            }
        }
        Ok(())
    }

    async fn delete_object(&self, _snapshot: &Snapshot, _mission: &Mission) -> Result<()> {
        Err(Error::StorageError(
            "delete is not supported for mirror-intel backend".to_string(),
        ))
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for MirrorIntel {
    async fn snapshot(
        &mut self,
        _mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        panic!("not supported");
    }

    fn info(&self) -> String {
        format!("mirror_intel, {:?}", self.config)
    }
}
