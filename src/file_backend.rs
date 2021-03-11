//! Local filesystem backend
//!
//! File backend is a target storage. This enables taking a snapshot of a
//! local file system, and transferring data to a local folder.
//!
//! File backend snapshots contains metadata (size + last modified).
//! It only accepts ByteStream.

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::stream_pipe::ByteStream;
use crate::traits::{Key, Metadata, SnapshotStorage, TargetStorage};

use async_trait::async_trait;
use filetime::FileTime;
use slog::info;
use structopt::StructOpt;
use walkdir::WalkDir;

#[derive(StructOpt, Debug)]
pub struct FileBackend {
    #[structopt(long)]
    pub base_path: String,
}

impl FileBackend {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for FileBackend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;

        info!(logger, "scanning local storage...");

        let base_path = self.base_path.clone();
        tokio::task::spawn_blocking(move || {
            let mut snapshot = vec![];
            let base_path = std::path::PathBuf::from(base_path).canonicalize().unwrap();
            for entry in WalkDir::new(&base_path) {
                let entry = entry.map_err(|err| {
                    Error::StorageError(format!("error while scanning file: {:?}", err))
                })?;
                let path = entry.path().to_path_buf();
                if path.is_file() {
                    let path = path.strip_prefix(&base_path).unwrap();
                    let path = path.to_str().unwrap().to_string();
                    let metadata = entry.metadata().map_err(|err| {
                        Error::StorageError(format!("file backend fails to get metadata {:?}", err))
                    })?;

                    let mtime = FileTime::from_last_modification_time(&metadata);

                    progress.set_message(&path);
                    snapshot.push(SnapshotMeta {
                        key: path,
                        size: Some(metadata.len()),
                        last_modified: Some(mtime.unix_seconds() as u64),
                        ..Default::default()
                    });
                }
            }
            Ok::<_, Error>(snapshot)
        })
        .await
        .map_err(|err| Error::ProcessError(format!("error while scanning: {:?}", err)))?
    }

    fn info(&self) -> String {
        format!("file (meta), {:?}", self)
    }
}

#[async_trait]
impl<Snapshot: Key + Metadata> TargetStorage<Snapshot, ByteStream> for FileBackend {
    async fn put_object(
        &self,
        snapshot: &Snapshot,
        byte_stream: ByteStream,
        _mission: &Mission,
    ) -> Result<()> {
        let path = byte_stream.object.use_file();
        let target: std::path::PathBuf = format!("{}/{}", self.base_path, snapshot.key()).into();
        let parent = target.parent().unwrap();
        tokio::fs::create_dir_all(parent).await?;
        tokio::fs::rename(&path, &target).await?;
        if let Some(last_modified) = snapshot.last_modified() {
            filetime::set_file_mtime(&target, FileTime::from_unix_time(last_modified as i64, 0))?;
        }
        Ok(())
    }

    async fn delete_object(&self, snapshot: &Snapshot, _mission: &Mission) -> Result<()> {
        let target = format!("{}/{}", self.base_path, snapshot.key());
        tokio::fs::remove_file(target).await?;
        Ok(())
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for FileBackend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        Ok(
            <Self as SnapshotStorage<SnapshotMeta>>::snapshot(self, mission, config)
                .await?
                .into_iter()
                .map(|x| SnapshotPath(x.key))
                .collect(),
        )
    }

    fn info(&self) -> String {
        format!("file (path), {:?}", self)
    }
}
