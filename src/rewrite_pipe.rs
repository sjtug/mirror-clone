//! RewritePipe rewrites content of `ByteStream`.
//!
//! A `RewritePipe` is a wrapper on `ByteStream`, which may be provided by
//! a `ByteStreamPipe` or a `SourceStorage` directly.
//! It rewrites the content of the input by applying user-defined functions,
//! and yields the modified `ByteStream`.
//!
//! The rewriting process relies on `ByteStream` which only supports
//! `LocalFile` currently.
//! So a new file will be created when rewriting and deleted when dropped.

use async_trait::async_trait;

use slog::warn;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{SnapshotStorage, SourceStorage};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct RewritePipe<Source, RewriteItem, F>
where
    F: Fn(RewriteItem) -> Result<RewriteItem> + Send + Sync,
{
    pub source: Source,
    pub buffer_path: String,
    pub rewrite_fn: F,
    pub max_length: u64,
    _phantom: std::marker::PhantomData<RewriteItem>,
}

impl<Source, RewriteItem, F> RewritePipe<Source, RewriteItem, F>
where
    F: Fn(RewriteItem) -> Result<RewriteItem> + Send + Sync,
{
    pub fn new(source: Source, buffer_path: String, rewrite_fn: F, max_length: u64) -> Self {
        Self {
            source,
            buffer_path,
            rewrite_fn,
            max_length,
            _phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<Snapshot, Source, RewriteItem, F> SnapshotStorage<Snapshot>
    for RewritePipe<Source, RewriteItem, F>
where
    Snapshot: Send + 'static,
    Source: SnapshotStorage<Snapshot> + Send,
    RewriteItem: Send + Sync + 'static,
    F: Fn(RewriteItem) -> Result<RewriteItem> + Send + Sync + 'static,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<Snapshot>> {
        self.source.snapshot(mission, config).await
    }

    fn info(&self) -> String {
        format!("rewrite <{}>", self.source.info())
    }
}

// TODO support rewrite functions with `RewriteItem` other than String (eg. Vec<u8>)
#[async_trait]
impl<Snapshot, Source, F> SourceStorage<Snapshot, ByteStream> for RewritePipe<Source, String, F>
where
    Snapshot: Send + Sync + 'static,
    Source: SourceStorage<Snapshot, ByteStream>,
    F: Fn(String) -> Result<String> + Send + Sync + 'static,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let logger = &mission.logger;

        let mut byte_stream = self.source.get_object(snapshot, mission).await?;

        if byte_stream.length > self.max_length {
            Ok(byte_stream)
        } else {
            match byte_stream.object {
                ByteObject::LocalFile {
                    ref mut file,
                    path: _,
                } => {
                    if let Some(ref mut file) = file {
                        let mut buffer = String::new();
                        if file.read_to_string(&mut buffer).await.is_err() {
                            warn!(logger, "rewrite_pipe: not a valid UTF-8 file, ignored");
                            Ok(byte_stream)
                        } else {
                            match (self.rewrite_fn)(buffer) {
                                Err(e) => {
                                    warn!(logger, "rewrite_pipe: {:?}, ignored", e);
                                    Ok(byte_stream)
                                }
                                Ok(content) => {
                                    let content = content.into_bytes();
                                    let content_length = content.len() as u64;

                                    file.seek(std::io::SeekFrom::Start(0)).await?;
                                    file.set_len(0).await?;
                                    file.write_all(&content).await?;
                                    file.flush().await?;
                                    file.seek(std::io::SeekFrom::Start(0)).await?;

                                    byte_stream.length = content_length;
                                    Ok(byte_stream)
                                }
                            }
                        }
                    } else {
                        Err(Error::ProcessError(String::from(
                            "missing file when rewriting",
                        )))
                    }
                }
            }
        }
    }
}
