use crate::common::{Mission, SnapshotConfig};
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
    async fn get_object(&self, snapshot: SnapshotItem, mission: &Mission) -> Result<SourceItem>;
}

#[async_trait]
pub trait TargetStorage<TargetItem> {
    async fn put_object(&self, item: TargetItem, mission: &Mission) -> Result<()>;
}
