//! ByteStreamPipe pipes TransferURL to ByteObject.
//!
//! A `ByteStreamPipe` is a wrapper on sources which yields `TransferURL`.
//! After piping a source through `ByteStreamPipe`, it will become a source
//! storage which yields `ByteStream`.
//!
//! Currently, this is done by downloading files to local file system,
//! provide it to target storage, and delete it on dropping file object.
//! We may later refactor it to use in-memory stream or direct reqwest stream.

use async_trait::async_trait;
use chrono::DateTime;

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::{Error, Result};
use crate::traits::{Key, Metadata, SnapshotStorage, SourceStorage};
use crate::utils::{hash_string, unix_time};
use futures_core::Stream;
use futures_util::{StreamExt, TryStreamExt};
use slog::debug;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::codec;

pub enum ByteObject {
    LocalFile {
        file: Option<tokio::fs::File>,
        path: Option<std::path::PathBuf>,
    },
}

impl ByteObject {
    pub fn as_stream(&mut self) -> impl Stream<Item = std::io::Result<bytes::Bytes>> {
        match self {
            ByteObject::LocalFile { file, .. } => codec::FramedRead::new(
                BufReader::new(file.take().unwrap()),
                codec::BytesCodec::new(),
            )
            .map_ok(|bytes| bytes.freeze()),
        }
    }

    pub fn use_file(mut self) -> std::path::PathBuf {
        match &mut self {
            ByteObject::LocalFile { file, path } => {
                drop(file.take().unwrap());
                path.take().unwrap()
            }
        }
    }
}

impl Drop for ByteObject {
    fn drop(&mut self) {
        match self {
            ByteObject::LocalFile { path, .. } => {
                if let Some(path) = path {
                    // TODO: find a safer way to handle this. Currently, we remove the file
                    // before dropping the file object.
                    if let Err(err) = std::fs::remove_file(&path) {
                        eprintln!("failed to remove cache file: {:?} {:?}", err, path);
                    }
                }
            }
        }
    }
}

pub struct ByteStream {
    pub object: ByteObject,
    pub length: u64,
    pub modified_at: u64,
}

pub struct ByteStreamPipe<Source> {
    pub source: Source,
    pub buffer_path: String,
    pub use_snapshot_last_modified: bool,
}

impl<Source> ByteStreamPipe<Source> {
    pub fn new(source: Source, buffer_path: String, use_snapshot_last_modified: bool) -> Self {
        Self {
            source,
            buffer_path,
            use_snapshot_last_modified,
        }
    }
}

#[async_trait]
impl<Snapshot, Source> SnapshotStorage<Snapshot> for ByteStreamPipe<Source>
where
    Snapshot: Send + 'static,
    Source: SnapshotStorage<Snapshot> + Send,
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
            "StreamPipe buffered to {} <{}>",
            self.buffer_path,
            self.source.info()
        )
    }
}

#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for ByteStreamPipe<Source>
where
    Snapshot: Key + Metadata,
    Source: SourceStorage<Snapshot, TransferURL>,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let transfer_url = self.source.get_object(snapshot, mission).await?;

        let path = format!(
            "{}/{}.{}.buffer",
            self.buffer_path,
            hash_string(&transfer_url.0),
            unix_time()
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
        let snapshot_modified_at = snapshot.last_modified();
        let http_modified_at = std::str::from_utf8(
            response
                .headers()
                .get(reqwest::header::LAST_MODIFIED)
                .unwrap()
                .as_bytes(),
        )
        .ok()
        .and_then(|header| DateTime::parse_from_rfc2822(&header).ok())
        .map(|x| x.timestamp() as u64);

        let modified_at = if self.use_snapshot_last_modified {
            snapshot_modified_at
        } else {
            http_modified_at
        };

        let modified_at =
            modified_at.ok_or_else(|| Error::PipeError("no modified time".to_string()))?;

        if let Some(snapshot_modified_at) = snapshot_modified_at {
            if let Some(http_modified_at) = http_modified_at {
                if snapshot_modified_at != http_modified_at {
                    return Err(Error::PipeError(format!(
                        "mismatch modified time: http={}, snapshot={}",
                        http_modified_at, snapshot_modified_at
                    )));
                }
            }
        }

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

        f.seek(std::io::SeekFrom::Start(0)).await?;

        // TODO: check snapshot http modified_at consistency
        Ok(ByteStream {
            object: ByteObject::LocalFile {
                file: Some(f),
                path: Some(path.into()),
            },
            length: total_bytes,
            modified_at,
        })
    }
}
