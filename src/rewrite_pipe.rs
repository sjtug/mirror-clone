use async_trait::async_trait;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::{hash_string, unix_time};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

pub struct RewritePipe<Source, RewriteItem> {
    pub source: Source,
    pub buffer_path: String,
    pub rewrite_fn: Box<dyn Fn(RewriteItem) -> Result<RewriteItem> + Send + Sync>,
    pub max_length: u64,
}

impl<Source, RewriteItem> RewritePipe<Source, RewriteItem> {
    pub fn new(
        source: Source,
        buffer_path: String,
        rewrite_fn: Box<dyn Fn(RewriteItem) -> Result<RewriteItem> + Send + Sync>,
        max_length: u64,
    ) -> Self {
        Self {
            source,
            buffer_path,
            rewrite_fn,
            max_length,
        }
    }
}

#[async_trait]
impl<Snapshot, Source, RewriteItem> SnapshotStorage<Snapshot> for RewritePipe<Source, RewriteItem>
where
    Snapshot: Send + 'static,
    Source: SnapshotStorage<Snapshot> + Send,
    RewriteItem: 'static,
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

#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for RewritePipe<Source, String>
where
    Snapshot: Send + Sync + 'static,
    Source: SourceStorage<Snapshot, ByteStream>,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let mut byte_stream = self.source.get_object(snapshot, mission).await?;

        if byte_stream.length > self.max_length {
            Ok(byte_stream)
        } else {
            match byte_stream.object {
                ByteObject::LocalFile {
                    ref mut file,
                    ref path,
                } => {
                    if let (Some(file), Some(path)) = (file, path) {
                        let mut buffer = String::new();
                        file.read_to_string(&mut buffer).await?;
                        let content = (*self.rewrite_fn)(buffer)?.into_bytes();
                        let content_length = content.len() as u64;

                        let original_filename = path
                            .file_name()
                            .ok_or_else(|| {
                                Error::ProcessError(String::from("given path is not a file"))
                            })?
                            .to_str()
                            .ok_or_else(|| {
                                Error::ProcessError(String::from("corrupted filename"))
                            })?;
                        let path = format!(
                            "{}/{}.{}.rewrite.buffer",
                            self.buffer_path,
                            hash_string(original_filename),
                            unix_time()
                        );
                        let mut f = BufWriter::new(
                            OpenOptions::default()
                                .create(true)
                                .truncate(true)
                                .write(true)
                                .read(true)
                                .open(&path)
                                .await?,
                        );
                        f.write_all(content.as_ref()).await?;
                        f.flush().await?;

                        let mut f = f.into_inner();
                        f.seek(std::io::SeekFrom::Start(0)).await?;

                        Ok(ByteStream {
                            object: ByteObject::LocalFile {
                                file: Some(f),
                                path: Some(path.into()),
                            },
                            length: content_length,
                            modified_at: byte_stream.modified_at,
                        })
                    } else {
                        Err(Error::ProcessError(String::from(
                            "missing file or path when rewriting",
                        )))
                    }
                }
            }
        }
    }
}
