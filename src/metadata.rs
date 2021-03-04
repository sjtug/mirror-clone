use async_trait::async_trait;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::Result;
use crate::traits::{Diff, Key, SnapshotStorage};

#[derive(Clone, Debug)]
pub struct SnapshotMeta {
    pub key: String,
    pub size: Option<u64>,
    pub last_modified: Option<u64>,
    pub checksum_method: Option<String>,
    pub checksum: Option<String>,
}

pub struct MetaAsPath<Source: SnapshotStorage<SnapshotMeta> + std::fmt::Debug + std::marker::Send> {
    source: Source,
}

#[async_trait]
impl<Source: SnapshotStorage<SnapshotMeta> + std::fmt::Debug + std::marker::Send>
    SnapshotStorage<SnapshotPath> for MetaAsPath<Source>
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let snapshot = self.source.snapshot(mission, config).await?;
        Ok(snapshot
            .into_iter()
            .map(|item| SnapshotPath(item.key))
            .collect())
    }

    fn info(&self) -> String {
        format!("as snapshot path, {:?}", self.source)
    }
}

impl Key for SnapshotMeta {
    fn key(&self) -> &str {
        &self.key
    }
}

fn compare_option<T: Eq>(a: &Option<T>, b: &Option<T>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

impl Diff for SnapshotMeta {
    fn diff(&self, other: &Self) -> bool {
        if !compare_option(&self.size, &other.size) {
            return true;
        }
        if !compare_option(&self.last_modified, &other.last_modified) {
            return true;
        }
        if !compare_option(&self.checksum_method, &other.checksum_method) {
            return true;
        }
        if !compare_option(&self.checksum, &other.checksum) {
            return true;
        }
        false
    }
}
