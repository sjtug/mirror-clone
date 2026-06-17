//! S3 backend
//!
//! S3 backend is a target storage, which enables taking snapshot of an S3
//! storage, and uploading objects to it. For snapshot, this storage by default
//! only has size and path. We could enable modify time and other metadata
//! in snapshot later. This storage only accepts `ByteStream`.
//!
//! This backend has only been tested with SJTU S3 service, which is
//! (possibly) set up with Ceph. Unlike official S3 protocol, SJTU
//! S3 service supports special characters in key. For example, if
//! we put `go@1.10-1.10.8.catalina.bottle.2.tar.gz` into SJTU S3,
//! the `@` character won't be ignored. You may access it either at
//! `go@...` or `go%40...` on HTTP.
//!
//! This backend will automatically add a MIME type for object, based on
//! suffix.

use std::{collections::HashMap, sync::atomic::AtomicU64};

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::stream_pipe::ByteStream;
use crate::traits::{Key, SnapshotStorage, TargetStorage};

use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client, config::Region, primitives::ByteStream as S3ByteStream};
use futures_util::{StreamExt, stream};
use slog::{debug, info, warn};

#[derive(Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub prefix_hint_mode: Option<String>,
    pub scan_metadata: bool,
    pub max_keys: u64,
}

impl S3Config {
    pub fn new_jcloud(prefix: String, scan_metadata: bool) -> Self {
        Self {
            endpoint: "https://s3.jcloud.sjtu.edu.cn".to_string(),
            bucket: "899a892efef34b1b944a19981040f55b-oss01".to_string(),
            prefix,
            max_keys: 1000,
            prefix_hint_mode: None,
            scan_metadata,
        }
    }
}

pub struct S3Backend {
    config: S3Config,
    client: S3Client,
}

impl S3Backend {
    pub async fn new(config: S3Config) -> Self {
        let sdk_config = aws_config::from_env().load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .region(Region::new("jCloud S3"))
            .endpoint_url(config.endpoint.clone())
            .force_path_style(true)
            .build();
        let client = S3Client::from_conf(s3_config);
        Self { config, client }
    }

    pub fn gen_metadata(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("clone-backend".to_string(), "s3-v1".to_string());
        map
    }
}

fn s3_error(action: &str, error: impl std::fmt::Debug) -> Error {
    Error::StorageError(format!("S3 {} error: {:?}", action, error))
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for S3Backend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;

        info!(logger, "fetching data from S3 storage...");

        let s3_prefix_base = format!("{}/", self.config.prefix);
        let total_size = std::sync::Arc::new(AtomicU64::new(0));
        let max_keys = i32::try_from(self.config.max_keys)
            .map_err(|_| Error::ConfigureError("s3 max keys does not fit i32".to_string()))?;

        let prefix = match self.config.prefix_hint_mode.as_deref() {
            Some("pypi") => {
                let mut prefix = vec![];
                for i in 0..256 {
                    prefix.push(format!("/{:02x}", i));
                }
                prefix
            }
            None => vec!["".to_string()],
            Some(other) => {
                panic!("unsupported prefix hint mode {}", other);
            }
        };

        // List bucket
        let mut futures = stream::iter(prefix)
            .map(|additional_prefix| {
                let bucket = self.config.bucket.clone();
                let prefix = Some(format!("{}{}", self.config.prefix, additional_prefix));
                let client = self.client.clone();
                let total_size = total_size.clone();
                let progress = progress.clone();
                let logger = logger.clone();
                let s3_prefix_base = s3_prefix_base.clone();

                async move {
                    let mut snapshot = vec![];
                    let mut continuation_token = None;

                    loop {
                        let mut req = client
                            .list_objects_v2()
                            .bucket(bucket.clone())
                            .max_keys(max_keys as i32);
                        if let Some(prefix) = &prefix {
                            req = req.prefix(prefix);
                        }
                        if let Some(continuation_token) = continuation_token.take() {
                            req = req.continuation_token(continuation_token);
                        }

                        let resp = req
                            .send()
                            .await
                            .map_err(|err| s3_error("list objects", err))?;

                        let mut first_key = true;

                        for item in resp.contents() {
                            if let Some(size) = item.size() {
                                if size >= 0 {
                                    total_size.fetch_add(
                                        size as u64,
                                        std::sync::atomic::Ordering::SeqCst,
                                    );
                                }
                            }
                            if let Some(key) = item.key() {
                                if key.starts_with(&s3_prefix_base) {
                                    let key = key[s3_prefix_base.len()..].to_string();
                                    // let key = crate::utils::rewrite_url_string(&gen_map, &key);
                                    if first_key {
                                        first_key = false;
                                        progress.set_message(&key);
                                    }
                                    snapshot.push(SnapshotMeta {
                                        key,
                                        size: item.size().and_then(|x| u64::try_from(x).ok()),
                                        ..Default::default()
                                    });
                                } else {
                                    warn!(logger, "prefix not match {}", key);
                                }
                            }
                        }

                        if let Some(next_continuation_token) = resp.next_continuation_token() {
                            continuation_token = Some(next_continuation_token.to_string());
                        } else {
                            break;
                        }
                    }
                    Ok::<_, Error>(snapshot)
                }
            })
            .buffer_unordered(256);

        let mut snapshots = vec![];

        while let Some(snapshot) = futures.next().await {
            snapshots.append(&mut snapshot?);
        }

        // Get metadata
        let snapshots = if self.config.scan_metadata {
            let mut futures = stream::iter(snapshots)
                .map(|snapshot| {
                    let bucket = self.config.bucket.clone();
                    let client = self.client.clone();
                    let progress = progress.clone();
                    let prefix = self.config.prefix.clone();

                    async move {
                        progress.set_message(&snapshot.key);
                        let resp = client
                            .head_object()
                            .bucket(bucket)
                            .key(format!("{}/{}", prefix, snapshot.key))
                            .send()
                            .await
                            .map_err(|err| s3_error("head object", err))?;
                        let last_modified = resp
                            .metadata()
                            .and_then(|metadata| metadata.get("clone-last-modified"))
                            .and_then(|x| x.parse::<u64>().ok());
                        Ok::<_, Error>(SnapshotMeta {
                            last_modified,
                            ..snapshot
                        })
                    }
                })
                .buffer_unordered(64);

            let mut snapshots = vec![];

            while let Some(snapshot) = futures.next().await {
                snapshots.push(snapshot?);
            }

            snapshots
        } else {
            snapshots
        };

        progress.finish_with_message("done");

        let total_size = total_size.load(std::sync::atomic::Ordering::SeqCst);
        info!(
            logger,
            "total size: {}B or {}G",
            total_size,
            total_size as f64 / 1000.0 / 1000.0 / 1000.0
        );

        Ok(snapshots)
    }

    fn info(&self) -> String {
        format!("s3 (meta), {:?}", self.config)
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for S3Backend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        Ok(
            <Self as SnapshotStorage<SnapshotMeta>>::snapshot(self, mission, config)
                .await?
                .into_iter()
                .map(|x| SnapshotPath::new(x.key))
                .collect(),
        )
    }

    fn info(&self) -> String {
        format!("s3 (path), {:?}", self.config)
    }
}
pub trait S3Metadata {
    fn s3_meta(&self) -> HashMap<String, String>;
}

impl S3Metadata for SnapshotPath {
    fn s3_meta(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

impl S3Metadata for SnapshotMeta {
    fn s3_meta(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(checksum_method) = &self.checksum_method {
            map.insert(
                "clone-checksum-method".to_string(),
                checksum_method.to_string(),
            );
        }
        if let Some(checksum) = &self.checksum {
            map.insert("clone-checksum".to_string(), checksum.to_string());
        }
        map
    }
}

fn get_mime(key: &str) -> Option<String> {
    // TODO: add more types from https://github.com/nginx/nginx/blob/master/conf/mime.types
    // TODO: the correct way is to mirror content-type from remote as-is, or to read MIME type
    if key.ends_with(".htm") || key.ends_with(".html") || key.ends_with(".shtml") {
        Some("text/html; charset=utf-8".to_string())
    } else {
        None
    }
}

#[async_trait]
impl<Snapshot> TargetStorage<Snapshot, ByteStream> for S3Backend
where
    Snapshot: Key + S3Metadata,
{
    async fn put_object(
        &self,
        snapshot: &Snapshot,
        byte_stream: ByteStream,
        mission: &Mission,
    ) -> Result<()> {
        let logger = &mission.logger;
        debug!(logger, "upload: {}", snapshot.key());

        let ByteStream {
            object,
            length,
            modified_at,
            content_type,
        } = byte_stream;

        let body = S3ByteStream::read_from()
            .path(
                object
                    .path()
                    .ok_or_else(|| Error::PipeError("missing local object path".to_string()))?,
            )
            .build()
            .await
            .map_err(|err| s3_error("open upload body", err))?;

        let mut metadata = self.gen_metadata();
        metadata.insert("clone-last-modified".to_string(), modified_at.to_string());
        metadata.extend(snapshot.s3_meta());

        let req = self
            .client
            .put_object()
            .bucket(self.config.bucket.clone())
            .key(format!("{}/{}", self.config.prefix, snapshot.key()))
            .body(body)
            .set_metadata(Some(metadata))
            .content_length(length as i64)
            .set_content_type(content_type.or_else(|| get_mime(snapshot.key())));

        req.send()
            .await
            .map_err(|err| s3_error("put object", err))?;
        drop(object);

        Ok(())
    }

    async fn delete_object(&self, snapshot: &Snapshot, _mission: &Mission) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.config.bucket.clone())
            .key(format!("{}/{}", self.config.prefix, snapshot.key()))
            .send()
            .await
            .map_err(|err| s3_error("delete object", err))?;
        Ok(())
    }
}
