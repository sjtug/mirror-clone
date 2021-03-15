use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferPath};
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait SnapshotStorage<SnapshotItem>: Send + Sync + 'static {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotItem>>;
    fn info(&self) -> String;
}

#[async_trait]
pub trait SourceStorage<SnapshotItem, SourceItem>: Send + Sync + 'static {
    async fn get_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<SourceItem>;
}

#[async_trait]
pub trait TargetStorage<SnapshotItem, TargetItem>: Send + Sync + 'static {
    async fn put_object(
        &self,
        snapshot: &SnapshotItem,
        item: TargetItem,
        mission: &Mission,
    ) -> Result<()>;
    async fn delete_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<()>;
}

#[async_trait]
impl<Source, Snapshot> SourceStorage<Snapshot, TransferPath> for Source
where
    Source: SnapshotStorage<Snapshot>,
    Snapshot: Key,
{
    async fn get_object(&self, snapshot: &Snapshot, _mission: &Mission) -> Result<TransferPath> {
        Ok(TransferPath(snapshot.key().to_string()))
    }
}

pub trait Key: Send + Sync + 'static {
    fn key(&self) -> &str;
}

pub trait Metadata {
    fn priority(&self) -> isize {
        0
    }

    fn last_modified(&self) -> Option<u64> {
        None
    }
}

pub trait Diff {
    fn diff(&self, other: &Self) -> bool;
}

impl Key for SnapshotPath {
    fn key(&self) -> &str {
        &self.0
    }
}

impl Diff for SnapshotPath {
    fn diff(&self, other: &Self) -> bool {
        self.1 || other.1
    }
}

impl Metadata for SnapshotPath {}
