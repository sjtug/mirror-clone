use crate::common::Mission;
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait SnapshotStorage<SnapshotItem> {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<SnapshotItem>>;
    fn info(&self) -> String;
}

#[async_trait]
pub trait SourceStorage<SnapshotItem, SourceItem> {
    async fn get_object(&self, snapshot: SnapshotItem) -> Result<SourceItem>;
}

#[async_trait]
pub trait TargetStorage<TargetItem> {
    async fn put_object(&self, item: TargetItem) -> Result<()>;
}
