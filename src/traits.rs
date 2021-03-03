use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferPath};
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait SnapshotStorage<SnapshotItem> {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotItem>>;
    fn info(&self) -> String;
}

#[async_trait]
pub trait SourceStorage<SnapshotItem, SourceItem> {
    async fn get_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<SourceItem>;
}

#[async_trait]
pub trait TargetStorage<SnapshotItem, TargetItem> {
    async fn put_object(
        &self,
        snapshot: &SnapshotItem,
        item: TargetItem,
        mission: &Mission,
    ) -> Result<()>;
    async fn delete_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<()>;
}

#[async_trait]
impl<Source> SourceStorage<SnapshotPath, TransferPath> for Source
where
    Source: SnapshotStorage<SnapshotPath> + Send + Sync,
{
    async fn get_object(
        &self,
        snapshot: &SnapshotPath,
        _mission: &Mission,
    ) -> Result<TransferPath> {
        Ok(TransferPath(snapshot.0.clone()))
    }
}
