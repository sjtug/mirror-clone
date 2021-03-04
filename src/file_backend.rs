

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::stream_pipe::ByteStream;
use crate::traits::{SnapshotStorage, TargetStorage};

use async_trait::async_trait;
use slog::{debug, info, warn};
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
impl SnapshotStorage<SnapshotPath> for FileBackend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
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
                let path = entry.into_path();
                if path.is_file() {
                    let path = path.strip_prefix(&base_path).unwrap();
                    let path = path.to_str().unwrap().to_string();
                    progress.set_message(&path);
                    snapshot.push(SnapshotPath(path));
                }
            }
            Ok::<_, Error>(snapshot)
        })
        .await
        .map_err(|err| Error::ProcessError(format!("error while scanning: {:?}", err)))?
    }

    fn info(&self) -> String {
        format!("file, {:?}", self)
    }
}

#[async_trait]
impl TargetStorage<SnapshotPath, ByteStream> for FileBackend {
    async fn put_object(
        &self,
        snapshot: &SnapshotPath,
        byte_stream: ByteStream,
        _mission: &Mission,
    ) -> Result<()> {
        let path = byte_stream.object.use_file();
        let target: std::path::PathBuf = format!("{}/{}", self.base_path, snapshot.0).into();
        let parent = target.parent().unwrap();
        tokio::fs::create_dir_all(parent).await?;
        tokio::fs::rename(path, target).await?;
        Ok(())
    }

    async fn delete_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<()> {
        let target = format!("{}/{}", self.base_path, snapshot.0);
        tokio::fs::remove_file(target).await?;
        Ok(())
    }
}
