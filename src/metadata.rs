use async_trait::async_trait;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::Result;
use crate::traits::{Diff, Key, Metadata, SnapshotStorage};

#[derive(Clone, Debug, Default)]
pub struct SnapshotMetaFlag {
    pub force: bool,
    pub force_last: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SnapshotMeta {
    pub key: String,
    pub size: Option<u64>,
    pub last_modified: Option<u64>,
    pub checksum_method: Option<String>,
    pub checksum: Option<String>,
    pub flags: SnapshotMetaFlag,
}

impl SnapshotMeta {
    pub fn force(key: String) -> Self {
        Self {
            key,
            flags: SnapshotMetaFlag {
                force: true,
                force_last: true,
            },
            ..Default::default()
        }
    }
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
        if self.flags.force || other.flags.force {
            return true;
        }
        false
    }
}

impl Metadata for SnapshotMeta {
    fn priority(&self) -> isize {
        if self.flags.force_last {
            -1
        } else {
            0
        }
    }

    fn last_modified(&self) -> Option<u64> {
        self.last_modified
    }
}
