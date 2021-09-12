//! FilterPipe excludes source items by regex pattern.

use async_trait::async_trait;
use regex::RegexSet;

use crate::common::{Mission, SnapshotConfig};
use crate::error::Result;
use crate::traits::{Key, SnapshotStorage, SourceStorage};

pub struct FilterPipe<Source> {
    pub source: Source,
    pub exclude_patterns: RegexSet,
}

impl<Source> FilterPipe<Source> {
    pub fn new(source: Source, exclude_patterns: RegexSet) -> Self {
        FilterPipe {
            source,
            exclude_patterns,
        }
    }
}

#[async_trait]
impl<Snapshot, Source> SnapshotStorage<Snapshot> for FilterPipe<Source>
where
    Snapshot: Key + Send + 'static,
    Source: SnapshotStorage<Snapshot> + Send,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<Snapshot>> {
        self.source
            .snapshot(mission, config)
            .await
            .map(|snapshots| {
                snapshots
                    .into_iter()
                    .filter(|snapshot| !self.exclude_patterns.is_match(snapshot.key()))
                    .collect()
            })
    }

    fn info(&self) -> String {
        format!(
            "Filter by exclude patterns {:?} <{}>",
            self.exclude_patterns,
            self.source.info()
        )
    }
}

#[async_trait]
impl<Snapshot, Source, SourceItem> SourceStorage<Snapshot, SourceItem> for FilterPipe<Source>
where
    Snapshot: Send + Sync + 'static,
    Source: SourceStorage<Snapshot, SourceItem>,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<SourceItem> {
        self.source.get_object(snapshot, mission).await
    }
}
