use async_trait::async_trait;
use slog::info;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, SourceStorage, Key};

pub struct MergePipe<Source1, Source2> {
    s1: Source1,
    s2: Source2,
    s1_prefix: String,
    s2_prefix: Option<String>,
}

impl<Source1, Source2> MergePipe<Source1, Source2> {
    pub fn new(s1: Source1, s2: Source2, s1_prefix: String, s2_prefix: Option<String>) -> Self {
        let s1_prefix = if s1_prefix.ends_with('/') {s1_prefix} else {format!("{}/", s1_prefix)};
        let s2_prefix = s2_prefix.map(|prefix| if prefix.ends_with('/') {prefix} else {format!("{}/", prefix)});
        Self {
            s1,
            s2,
            s1_prefix,
            s2_prefix,
        }
    }
}

#[async_trait]
impl<Source1, Source2, SnapshotItem> SnapshotStorage<SnapshotItem> for MergePipe<Source1, Source2>
where
    SnapshotItem: Key + Clone,
    Source1: SnapshotStorage<SnapshotItem> + Send + 'static,
    Source2: SnapshotStorage<SnapshotItem> + Send + 'static,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotItem>> {
        let logger = mission.logger.clone();

        info!(logger, "merge_pipe: snapshotting {}", self.s1_prefix);
        let mut snapshot1 = self.s1.snapshot(mission.clone(), config).await?;
        snapshot1.iter_mut().for_each(|item| {
            *item.key_mut() = format!("{}{}", self.s1_prefix, item.key());
        });

        if let Some(prefix) = &self.s2_prefix {
            info!(logger, "merge_pipe: snapshotting {}", prefix);
        } else {
            info!(logger, "merge_pipe: snapshotting remaining source(s)");
        }
        let mut snapshot2 = self.s2.snapshot(mission.clone(), config).await?;
        if let Some(prefix) = &self.s2_prefix {
            snapshot2.iter_mut().for_each(|item| {
                *item.key_mut() = format!("{}{}", prefix, item.key());
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
impl<Source1, Source2, Source, SnapshotItem> SourceStorage<SnapshotItem, Source> for MergePipe<Source1, Source2>
where
    SnapshotItem: Key + Clone,
    Source: Send + Sync + 'static,
    Source1: SourceStorage<SnapshotItem, Source> + Send + 'static,
    Source2: SourceStorage<SnapshotItem, Source> + Send + 'static,
{
    async fn get_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<Source> {
        let path = snapshot.key();

        if let Some(key) = path.strip_prefix(&self.s1_prefix) {
            let mut snapshot = snapshot.clone();
            *snapshot.key_mut() = String::from(key);
            self.s1.get_object(&snapshot, mission).await
        } else if let Some(prefix) = &self.s2_prefix {
            if let Some(key) = path.strip_prefix(prefix) {
                let mut snapshot = snapshot.clone();
                *snapshot.key_mut() = String::from(key);
                self.s2.get_object(&snapshot, mission).await
            } else {
                Err(Error::PipeError(String::from("unexpected prefix")))
            }
        } else {
            self.s2.get_object(snapshot, mission).await
        }
    }
}
