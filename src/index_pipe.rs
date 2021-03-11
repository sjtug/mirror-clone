//! IndexPipe adds Index to every directory of source.

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::Result;
use crate::metadata::SnapshotMeta;
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{Key, SnapshotStorage, SourceStorage};
use crate::utils::{hash_string, unix_time};

use async_trait::async_trait;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

static LIST_URL: &'static str = "mirror_clone_list.html";
pub struct IndexPipe<Source> {
    source: Source,
    index: Index,
    buffer_path: String,
}

#[derive(Debug)]
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

    fn index_for(&self, prefix: &str, current_directory: &str, list_key: &str) -> String {
        if prefix == "" {
            let mut data = String::new();
            // TODO: need escape HTML and URL
            data += &format!("<p>{}</p>", html_escape::encode_text(current_directory));
            data += &self
                .prefixes
                .iter()
                .map(|(key, _)| {
                    format!(
                        r#"<a href="{}/{}">{}/</a>"#,
                        urlencoding::encode(key),
                        list_key,
                        html_escape::encode_text(key)
                    )
                })
                .collect_vec()
                .join("\n<br>\n");
            data += "\n<br>\n";
            data += &self
                .objects
                .iter()
                .map(|key| {
                    format!(
                        r#"<a href="{}">{}</a>"#,
                        urlencoding::encode(key),
                        html_escape::encode_text(key)
                    )
                })
                .collect_vec()
                .join("\n<br>\n");
            data
        } else {
            if let Some((parent, rest)) = prefix.split_once('/') {
                self.prefixes
                    .get(parent)
                    .unwrap()
                    .index_for(rest, parent, list_key)
            } else {
                panic!("unsupported prefix {}", prefix);
            }
        }
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
    pub fn new(source: Source, buffer_path: String) -> Self {
        Self {
            source,
            index: Index::new(),
            buffer_path,
        }
    }

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
    Source: SnapshotStorage<SnapshotPath>,
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
        format!("IndexPipe (path) <{}>", self.source.info())
    }
}

#[async_trait]
impl<Source> SnapshotStorage<SnapshotMeta> for IndexPipe<Source>
where
    Source: SnapshotStorage<SnapshotMeta>,
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
        format!("IndexPipe (meta) <{}>", self.source.info())
    }
}

#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for IndexPipe<Source>
where
    Snapshot: Key,
    Source: SourceStorage<Snapshot, ByteStream>,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let key = snapshot.key();
        if key.ends_with(LIST_URL) {
            let content = self
                .index
                .index_for(&key[..key.len() - LIST_URL.len()], "", LIST_URL)
                .into_bytes();
            let pipe_file = format!("{}.{}.buffer", hash_string(key), unix_time());
            let path = Path::new(&self.buffer_path).join(pipe_file);
            let mut f = BufWriter::new(
                tokio::fs::OpenOptions::default()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(&path)
                    .await?,
            );
            f.write_all(&content).await?;
            f.flush().await?;
            let mut f = f.into_inner();
            f.seek(std::io::SeekFrom::Start(0)).await?;
            Ok(ByteStream {
                object: ByteObject::LocalFile {
                    file: Some(f),
                    path: Some(path.into()),
                },
                length: content.len() as u64,
                modified_at: unix_time(),
            })
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
