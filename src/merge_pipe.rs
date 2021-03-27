use async_trait::async_trait;
use slog::info;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::traits::{SnapshotStorage, SourceStorage};

pub struct MergePipe<Source1, Source2> {
    s1: Source1,
    s2: Source2,
    s1_prefix: String,
    s2_prefix: Option<String>,
}

impl<Source1, Source2> MergePipe<Source1, Source2> {
    pub fn new(s1: Source1, s2: Source2, s1_prefix: String, s2_prefix: Option<String>) -> Self {
        Self {
            s1,
            s2,
            s1_prefix,
            s2_prefix,
        }
    }
}

#[async_trait]
impl<Source1, Source2> SnapshotStorage<SnapshotPath> for MergePipe<Source1, Source2>
where
    Source1: SnapshotStorage<SnapshotPath> + Send + 'static,
    Source2: SnapshotStorage<SnapshotPath> + Send + 'static,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger.clone();

        info!(logger, "iterating the first source");
        let mut snapshot1 = self.s1.snapshot(mission.clone(), config).await?;
        snapshot1.iter_mut().for_each(|item| {
            item.0 = format!("{}/{}", self.s1_prefix, item.0);
        });
        info!(logger, "iterating the second source");
        let mut snapshot2 = self.s2.snapshot(mission.clone(), config).await?;
        if let Some(prefix) = &self.s2_prefix {
            snapshot2.iter_mut().for_each(|item| {
                item.0 = format!("{}/{}", prefix, item.0);
            });
        }

        snapshot1.append(&mut snapshot2);

        Ok(snapshot1)
    }

    fn info(&self) -> String {
        format!("MergePipe (<{}>, <{}>)", self.s1.info(), self.s2.info())
    }
}

#[async_trait]
impl<Source1, Source2> SnapshotStorage<SnapshotMeta> for MergePipe<Source1, Source2>
where
    Source1: SnapshotStorage<SnapshotMeta> + Send + 'static,
    Source2: SnapshotStorage<SnapshotMeta> + Send + 'static,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger.clone();

        info!(logger, "iterating the first source");
        let mut snapshot1 = self.s1.snapshot(mission.clone(), config).await?;
        snapshot1.iter_mut().for_each(|item| {
            item.key = format!("{}/{}", self.s1_prefix, item.key);
        });
        info!(logger, "iterating the second source");
        let mut snapshot2 = self.s2.snapshot(mission.clone(), config).await?;
        if let Some(prefix) = &self.s2_prefix {
            snapshot2.iter_mut().for_each(|item| {
                item.key = format!("{}/{}", prefix, item.key);
            });
        }

        snapshot1.append(&mut snapshot2);

        Ok(snapshot1)
    }

    fn info(&self) -> String {
        format!("MergePipe (<{}>, <{}>)", self.s1.info(), self.s2.info())
    }
}

#[async_trait]
impl<Source1, Source2, Source> SourceStorage<SnapshotPath, Source> for MergePipe<Source1, Source2>
where
    Source: Send + Sync + 'static,
    Source1: SourceStorage<SnapshotPath, Source> + Send + 'static,
    Source2: SourceStorage<SnapshotPath, Source> + Send + 'static,
{
    async fn get_object(&self, snapshot: &SnapshotPath, mission: &Mission) -> Result<Source> {
        let SnapshotPath(path, force) = snapshot;

        if let Some(key) = path.strip_prefix(format!("{}/", self.s1_prefix).as_str()) {
            self.s1
                .get_object(&SnapshotPath(String::from(key), *force), mission)
                .await
        } else if let Some(prefix) = &self.s2_prefix {
            if let Some(key) = path.strip_prefix(format!("{}/", prefix).as_str()) {
                self.s2
                    .get_object(&SnapshotPath(String::from(key), *force), mission)
                    .await
            } else {
                Err(Error::PipeError(String::from("unexpected prefix")))
            }
        } else {
            self.s2.get_object(snapshot, mission).await
        }
    }
}

#[async_trait]
impl<Source1, Source2, Source> SourceStorage<SnapshotMeta, Source> for MergePipe<Source1, Source2>
where
    Source: Send + Sync + 'static,
    Source1: SourceStorage<SnapshotMeta, Source> + Send + 'static,
    Source2: SourceStorage<SnapshotMeta, Source> + Send + 'static,
{
    async fn get_object(&self, snapshot: &SnapshotMeta, mission: &Mission) -> Result<Source> {
        let path = &snapshot.key;

        if let Some(key) = path.strip_prefix(format!("{}/", self.s1_prefix).as_str()) {
            let mut snapshot = snapshot.clone();
            snapshot.key = String::from(key);
            self.s1.get_object(&snapshot, mission).await
        } else if let Some(prefix) = &self.s2_prefix {
            if let Some(key) = path.strip_prefix(format!("{}/", prefix).as_str()) {
                let mut snapshot = snapshot.clone();
                snapshot.key = String::from(key);
                self.s2.get_object(&snapshot, mission).await
            } else {
                Err(Error::PipeError(String::from("unexpected prefix")))
            }
        } else {
            self.s2.get_object(snapshot, mission).await
        }
    }
}
