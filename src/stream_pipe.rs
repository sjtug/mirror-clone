use async_trait::async_trait;

use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::{Error, Result};
use crate::traits::{SnapshotStorage, SourceStorage};
use futures_util::StreamExt;
use slog::{debug, info, warn};
use std::sync::atomic::AtomicUsize;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

pub type ByteStream = (tokio::fs::File, u64);

pub struct ByteStreamPipe<Source: std::fmt::Debug> {
    pub source: Source,
    pub buffer_path: String,
}

#[async_trait]
impl<Snapshot, Source> SnapshotStorage<Snapshot> for ByteStreamPipe<Source>
where
    Snapshot: Send + 'static,
    Source: SnapshotStorage<Snapshot> + std::fmt::Debug + Send,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<Snapshot>> {
        self.source.snapshot(mission, config).await
    }

    fn info(&self) -> String {
        format!(
            "pipe <{:?}> to bytestream, buffered to {}",
            self.source, self.buffer_path
        )
    }
}

static FILE_ID: AtomicUsize = AtomicUsize::new(0);

#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for ByteStreamPipe<Source>
where
    Snapshot: Send + Sync + 'static,
    Source: SourceStorage<Snapshot, TransferURL> + std::fmt::Debug + Send + Sync,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let transfer_url = self.source.get_object(snapshot, mission).await?;

        let path = format!(
            "{}/{}.buffer",
            self.buffer_path,
            FILE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        let logger = &mission.logger;
        let mut f = BufWriter::new(
            OpenOptions::default()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&path)
                .await?,
        );

        let response = mission.client.get(&transfer_url.0).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::HTTPError(status));
        }

        let mut total_bytes: u64 = 0;
        let content_length = response.content_length();

        debug!(logger, "download: {} {:?}", transfer_url.0, content_length);

        let mut stream = response.bytes_stream();
        while let Some(content) = stream.next().await {
            let content = content?;
            f.write_all(&content).await?;
            total_bytes += content.len() as u64;
        }

        if let Some(content_length) = content_length {
            if total_bytes != content_length {
                return Err(Error::PipeError(format!(
                    "content length mismatch: {}/{}",
                    total_bytes, content_length
                )));
            }
        }

        f.flush().await?;
        let mut f = f.into_inner();

        // TODO: find a safer way to handle this. Currently, we remove the file
        // before dropping the file object.
        if let Err(err) = tokio::fs::remove_file(&path).await {
            warn!(logger, "failed to remove cache file: {:?} {:?}", err, path);
        }

        f.seek(std::io::SeekFrom::Start(0)).await?;

        Ok((f, total_bytes))
    }
}
