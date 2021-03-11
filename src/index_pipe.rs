//! IndexPipe adds Index to every directory of source.

use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::stream_pipe::ByteStream;
use crate::traits::{Key, SnapshotStorage, SourceStorage};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};

static LIST_URL: &'static str = "mirror_intel_list.html";
pub struct IndexPipe<Source> {
    source: Source,
    index: Index,
}

pub struct Index {
    prefixes: BTreeMap<String, Index>,
    objects: BTreeSet<String>,
}

impl Index {
    fn new() -> Self {
        Self {
            prefixes: BTreeMap::new(),
            objects: BTreeSet::new(),
        }
    }

    fn insert(&mut self, path: &str) {
        match path.split_once('/') {
            Some((parent, rest)) => {
                self.prefixes
                    .entry(parent.to_string())
                    .or_insert(Index::new())
                    .insert(rest);
            }
            None => {
                self.objects.insert(path.to_string());
            }
        }
    }

    fn snapshot(&self, prefix: &str, list_key: &str) -> Vec<String> {
        let mut result = vec![];
        result.push(format!("{}{}", prefix, list_key));
        for (key, index) in &self.prefixes {
            let new_prefix = format!("{}{}/", prefix, key);
            result.extend(index.snapshot(&new_prefix, list_key));
        }
        result
    }
}

fn generate_index(objects: &[String]) -> Index {
    let mut index = Index::new();
    for object in objects {
        index.insert(object);
    }
    index
}

impl<Source> IndexPipe<Source> {
    fn snapshot_index_keys(&mut self, mut snapshot: Vec<String>) -> Vec<String> {
        snapshot.sort();
        // If duplicated keys are found, there should be a warning.
        // This warning will be handled on transfer.
        snapshot.dedup();
        self.index = generate_index(&snapshot);
        self.index.snapshot("", LIST_URL)
    }
}

#[async_trait]
impl<Source> SnapshotStorage<SnapshotPath> for IndexPipe<Source>
where
    Source: SnapshotStorage<SnapshotPath> + std::fmt::Debug,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let mut snapshot = self.source.snapshot(mission, config).await?;
        let index_keys =
            self.snapshot_index_keys(snapshot.iter().map(|x| x.key().to_owned()).collect());
        snapshot.extend(index_keys.into_iter().map(|x| SnapshotPath(x)));
        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("IndexPipe (path) <{:?}>", self.source)
    }
}

#[async_trait]
impl<Source> SnapshotStorage<SnapshotMeta> for IndexPipe<Source>
where
    Source: SnapshotStorage<SnapshotMeta> + std::fmt::Debug,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let mut snapshot = self.source.snapshot(mission, config).await?;
        let index_keys =
            self.snapshot_index_keys(snapshot.iter().map(|x| x.key().to_owned()).collect());
        snapshot.extend(index_keys.into_iter().map(|x| SnapshotMeta::force(x)));
        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("IndexPipe (meta) <{:?}>", self.source)
    }
}
#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for IndexPipe<Source>
where
    Snapshot: Key,
    Source: SourceStorage<Snapshot, ByteStream> + std::fmt::Debug,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        if snapshot.key().ends_with(LIST_URL) {
            todo!();
        } else {
            self.source.get_object(snapshot, mission).await
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_simple() {
        let mut source = ["a", "b", "c"].iter().map(|x| x.to_string()).collect_vec();
        source.sort();
        assert_eq!(
            generate_index(&source).snapshot("", "list.html"),
            vec!["list.html"]
        );
    }

    #[test]
    fn test_dir() {
        let mut source = ["a", "b", "c/a", "c/b", "c/c", "d"]
            .iter()
            .map(|x| x.to_string())
            .collect_vec();
        source.sort();
        assert_eq!(
            generate_index(&source).snapshot("", "list.html"),
            vec!["list.html", "c/list.html"]
        );
    }

    #[test]
    fn test_dir_more() {
        let mut source = ["a", "b", "c/a/b/c/d/e"]
            .iter()
            .map(|x| x.to_string())
            .collect_vec();
        source.sort();
        assert_eq!(
            generate_index(&source).snapshot("", "list.html"),
            vec![
                "list.html",
                "c/list.html",
                "c/a/list.html",
                "c/a/b/list.html",
                "c/a/b/c/list.html",
                "c/a/b/c/d/list.html"
            ]
        );
    }
}
