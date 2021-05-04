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

static LIST_URL: &str = "mirror_clone_list.html";
pub struct IndexPipe<Source> {
    source: Source,
    index: Index,
    buffer_path: String,
    base_path: String,
    max_depth: usize,
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

    fn insert(&mut self, path: &str, remaining_depth: usize) {
        if remaining_depth == 0 {
            self.objects.insert(path.to_string());
        } else {
            match path.split_once('/') {
                Some((parent, rest)) => {
                    self.prefixes
                        .entry(parent.to_string())
                        .or_insert_with(Index::new)
                        .insert(rest, remaining_depth - 1);
                }
                None => {
                    self.objects.insert(path.to_string());
                }
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

    fn generate_navbar(&self, breadcrumb: &[&str], list_key: &str) -> String {
        let mut parent = "".to_string();
        let mut items = vec![];
        let mut is_first = true;
        for item in breadcrumb.iter().rev() {
            let item = html_escape::encode_text(item);
            if is_first {
                items.push(format!(
                    r#"<li class="breadcrumb-item active" aria-current="page">{}</li>"#,
                    item
                ));
                is_first = false;
            } else {
                items.push(format!(
                    r#"<li class="breadcrumb-item"><a href="{}{}">{}</a></li>"#,
                    parent, list_key, item
                ));
            }
            parent += "../";
        }
        items.reverse();
        format!(
            r#"
<nav aria-label="breadcrumb">
    <ol class="breadcrumb">
        {}
    </ol>
</nav>
        "#,
            items.join("\n")
        )
    }

    fn index_for(&self, prefix: &str, breadcrumb: &[&str], list_key: &str) -> String {
        if prefix.is_empty() {
            let mut data = String::new();

            let title = breadcrumb
                .last()
                .map(|x| html_escape::encode_text(x).to_string())
                .unwrap_or_else(|| String::from("Root"));
            let navbar = self.generate_navbar(breadcrumb, list_key);

            data += &format!(r#"<tr><td><a href="../{}">..</a></td></tr>"#, list_key);
            data += &self
                .prefixes
                .iter()
                .map(|(key, _)| {
                    format!(
                        r#"<tr><td><a href="{}/{}">{}/</a></td></tr>"#,
                        urlencoding::encode(key),
                        list_key,
                        html_escape::encode_text(key)
                    )
                })
                .collect_vec()
                .join("\n");
            data += "\n";
            data += &self
                .objects
                .iter()
                .map(|key| {
                    format!(
                        r#"<tr><td><a href="{}">{}</a></td></tr>"#,
                        urlencoding::encode(key),
                        html_escape::encode_text(key)
                    )
                })
                .collect_vec()
                .join("\n");
            format!(
                r#"
<!doctype html>
<html>

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/4.5.3/css/bootstrap.min.css" rel="stylesheet">

    <title>{} - SJTUG Mirror Index</title>
</head>

<body>
    <div class="container mt-3">
        {}
        <table class="table table-sm table-borderless">
            <tbody>
                {}
            </tbody>
        </table>
        <p class="small text-muted">该页面由 mirror-clone 自动生成。<a href="https://github.com/sjtug/mirror-clone">mirror-clone</a> 是 SJTUG 用于将软件源同步到对象存储的工具。</p>
        <p class="small text-muted">生成于 {}</p>
    </div>
</body>

</html>"#,
                title,
                navbar,
                data,
                chrono::Local::now().to_rfc2822()
            )
        } else if let Some((parent, rest)) = prefix.split_once('/') {
            let mut breadcrumb = breadcrumb.to_vec();
            breadcrumb.push(parent);
            self.prefixes
                .get(parent)
                .unwrap()
                .index_for(rest, &breadcrumb, list_key)
        } else {
            panic!("unsupported prefix {}", prefix);
        }
    }
}

fn generate_index(objects: &[String], max_depth: usize) -> Index {
    let mut index = Index::new();
    for object in objects {
        index.insert(object, max_depth);
    }
    index
}

impl<Source> IndexPipe<Source> {
    pub fn new(source: Source, buffer_path: String, base_path: String, max_depth: usize) -> Self {
        Self {
            source,
            index: Index::new(),
            buffer_path,
            base_path,
            max_depth,
        }
    }

    fn snapshot_index_keys(&mut self, mut snapshot: Vec<String>) -> Vec<String> {
        snapshot.sort();
        // If duplicated keys are found, there should be a warning.
        // This warning will be handled on transfer.
        snapshot.dedup();
        self.index = generate_index(&snapshot, self.max_depth);
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
        snapshot.extend(index_keys.into_iter().map(SnapshotPath::force));
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
        snapshot.extend(index_keys.into_iter().map(SnapshotMeta::force));
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
        if let Some(prefix) = key.strip_suffix(LIST_URL) {
            let content = self
                .index
                .index_for(prefix, &[&self.base_path], LIST_URL)
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
                    path: Some(path),
                },
                length: content.len() as u64,
                modified_at: unix_time(),
                content_type: None, // use `text/html` by default
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
            generate_index(&source, 999).snapshot("", "list.html"),
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
            generate_index(&source, 999).snapshot("", "list.html"),
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
            generate_index(&source, 999).snapshot("", "list.html"),
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

    #[test]
    fn test_dir_more_depth() {
        let mut source = ["a", "b", "c/a/b/c/d/e"]
            .iter()
            .map(|x| x.to_string())
            .collect_vec();
        source.sort();
        let index = generate_index(&source, 2);
        assert_eq!(
            index.snapshot("", "list.html"),
            vec!["list.html", "c/list.html", "c/a/list.html"]
        );
    }
}
