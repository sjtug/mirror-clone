//! `ChecksumPipe` verifies the checksum of source items.
//!
//! A `ChecksumPipe` is a wrapper on source storages which yields `ByteStream`.
//! It reads the snapshot checksum meta, and calculates the corresponding checksum of `ByteStream`.
//! In case of a checksum mismatch, the pipe yields an `ChecksumError`.

use std::io::{Error as IOError, ErrorKind, Result as IOResult, SeekFrom};

use async_trait::async_trait;
use sha2::Digest;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio_io_compat::CompatHelperTrait;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{Key, Metadata, SnapshotStorage, SourceStorage};

async fn sha256(source: &mut (impl AsyncRead + Unpin)) -> IOResult<String> {
    let mut hasher = sha2::Sha256::new();
    tokio::io::copy(source, &mut hasher.tokio_io_mut()).await?;
    Ok(format!("{:x}", hasher.finalize()))
}

pub async fn calc_checksum(
    source: &mut (impl AsyncRead + AsyncSeek + Unpin),
    method: &str,
) -> IOResult<String> {
    let orig_pos = source.seek(SeekFrom::Current(0)).await?;

    let result = match method {
        "sha256" => sha256(source).await,
        _ => Err(IOError::new(
            ErrorKind::Unsupported,
            "unsupported checksum method",
        )),
    };

    source.seek(SeekFrom::Start(orig_pos)).await?;
    result
}

pub struct ChecksumPipe<Source> {
    pub source: Source,
}

impl<Source> ChecksumPipe<Source> {
    pub fn new(source: Source) -> Self {
        ChecksumPipe { source }
    }
}

#[async_trait]
impl<Snapshot, Source> SnapshotStorage<Snapshot> for ChecksumPipe<Source>
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
        format!("ChecksumPipe <{}>", self.source.info())
    }
}

#[async_trait]
impl<Snapshot, Source> SourceStorage<Snapshot, ByteStream> for ChecksumPipe<Source>
where
    Snapshot: Key + Metadata,
    Source: SourceStorage<Snapshot, ByteStream>,
{
    async fn get_object(&self, snapshot: &Snapshot, mission: &Mission) -> Result<ByteStream> {
        let mut source = self.source.get_object(snapshot, mission).await?;
        if let (Some(method), Some(expected_chksum)) =
            (snapshot.checksum_method(), snapshot.checksum())
        {
            let got_chksum = match &mut source.object {
                ByteObject::LocalFile { file: Some(f), .. } => calc_checksum(f, method).await?,
                ByteObject::LocalFile {
                    file: None,
                    path: Some(path),
                } => {
                    let mut f = File::open(path).await?;
                    calc_checksum(&mut f, method).await?
                }
                ByteObject::LocalFile {
                    file: None,
                    path: None,
                } => {
                    return Err(Error::IoError(IOError::new(
                        ErrorKind::NotFound,
                        "data missing",
                    )));
                }
            };

            if expected_chksum != got_chksum.as_str() {
                return Err(Error::ChecksumError {
                    method: method.to_string(),
                    expected: expected_chksum.to_string(),
                    got: got_chksum,
                });
            }
        };
        Ok(source)
    }
}
