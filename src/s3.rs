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
use aws_sdk_s3::{
    Client as S3Client,
    config::{Region, RequestChecksumCalculation},
    primitives::ByteStream as S3ByteStream,
};
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
            // Avoid AWS-chunked trailers unsupported by jCloud S3.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build();
        let client = S3Client::from_conf(s3_config);
        Self { config, client }
    }

    pub fn gen_metadata(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("clone-backend".to_string(), "s3-v1".to_string());
        map
    }

    /// List objects directly under one directory and return its child directories.
    /// Child prefixes are formatted as `/channel/platform/`, matching the shard
    /// format used by the regular parallel listing path.
    async fn list_directory(
        &self,
        additional_prefix: &str,
        max_keys: i32,
    ) -> Result<(Vec<SnapshotMeta>, Vec<String>)> {
        let prefix_base = format!("{}/", self.config.prefix);
        let prefix = format!("{}{}", self.config.prefix, additional_prefix);
        let mut objects = vec![];
        let mut children = vec![];
        let mut continuation_token = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(self.config.bucket.clone())
                .prefix(prefix.clone())
                .delimiter('/')
                .max_keys(max_keys);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }
            let resp = req
                .send()
                .await
                .map_err(|err| s3_error("list objects (delimiter)", err))?;

            for item in resp.contents() {
                if let Some(key) = item.key()
                    && let Some(key) = key.strip_prefix(&prefix_base)
                {
                    objects.push(SnapshotMeta {
                        key: key.to_string(),
                        size: item.size().and_then(|size| u64::try_from(size).ok()),
                        ..Default::default()
                    });
                }
            }
            for common_prefix in resp.common_prefixes() {
                if let Some(prefix) = common_prefix.prefix()
                    && let Some(prefix) = prefix.strip_prefix(&self.config.prefix)
                {
                    children.push(prefix.to_string());
                }
            }

            if let Some(token) = resp.next_continuation_token() {
                continuation_token = Some(token.to_string());
            } else {
                break;
            }
        }

        Ok((objects, children))
    }

    /// Discover Conda's `<channel>/<platform>/` directory layout. Objects directly
    /// under the root or a channel are collected during delimiter discovery; each
    /// platform is split into independent file-name-prefix listing shards.
    async fn list_conda_shards(&self, max_keys: i32) -> Result<(Vec<SnapshotMeta>, Vec<String>)> {
        let (mut objects, channels) = self.list_directory("/", max_keys).await?;
        let mut futures = stream::iter(channels)
            .map(|channel| async move { self.list_directory(&channel, max_keys).await })
            .buffer_unordered(256);
        let mut platform_shards = vec![];

        while let Some(result) = futures.next().await {
            let (mut direct_objects, mut channel_platforms) = result?;
            objects.append(&mut direct_objects);
            platform_shards.append(&mut channel_platforms);
        }

        // Package files within a platform directory are flat and use ASCII file
        // names. Split once more by the first file-name byte so a large directory
        // such as conda-forge/linux-64 does not remain a million-key sequential
        // shard. Include every printable ASCII byte except '/', rather than only
        // currently valid Conda package-name characters, to preserve legacy files.
        let mut shards = Vec::with_capacity(platform_shards.len() * 94);
        for platform in platform_shards {
            for byte in b' '..=b'~' {
                if byte != b'/' {
                    shards.push(format!("{}{}", platform, char::from(byte)));
                }
            }
        }

        Ok((objects, shards))
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

        let (mut snapshots, prefix) = match self.config.prefix_hint_mode.as_deref() {
            Some("pypi") => {
                let mut prefix = vec![];
                for i in 0..256 {
                    prefix.push(format!("/{:02x}", i));
                }
                (vec![], prefix)
            }
            // Conda mirrors are laid out as <prefix>/<channel>/<platform>/...
            // (e.g. anaconda/cloud/conda-forge/noarch). Listing the whole prefix
            // in one request stream is sequential and takes tens of minutes for
            // millions of objects. Discover those directories using delimiter
            // listings, then scan the independent platform shards in parallel.
            Some("conda") => {
                let (objects, shards) = self.list_conda_shards(max_keys).await?;
                if objects.is_empty() && shards.is_empty() {
                    warn!(
                        logger,
                        "conda prefix hint found no objects or directories; falling back to single-prefix listing"
                    );
                    (vec![], vec!["".to_string()])
                } else {
                    (objects, shards)
                }
            }
            None => (vec![], vec!["".to_string()]),
            Some(other) => {
                return Err(Error::ConfigureError(format!(
                    "unsupported prefix hint mode {}",
                    other
                )));
            }
        };

        for snapshot in &snapshots {
            if let Some(size) = snapshot.size {
                total_size.fetch_add(size, std::sync::atomic::Ordering::SeqCst);
            }
        }

        // PyPI's 256 hash-prefix shards are intentionally scanned at full fan-out.
        // Conda creates several thousand channel/platform/file-prefix shards; cap
        // those at 64 to avoid tripping jCloud's response-throughput guard.
        let listing_concurrency = if self.config.prefix_hint_mode.as_deref() == Some("conda") {
            64
        } else {
            256
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
                            .max_keys(max_keys);
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
                            if let Some(size) = item.size()
                                && size >= 0
                            {
                                total_size
                                    .fetch_add(size as u64, std::sync::atomic::Ordering::SeqCst);
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
            .buffer_unordered(listing_concurrency);

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
            mut object,
            length,
            modified_at,
            content_type,
        } = byte_stream;

        let body = if let Some(path) = object.path() {
            S3ByteStream::read_from()
                .path(path)
                .build()
                .await
                .map_err(|err| s3_error("open upload body", err))?
        } else {
            let bytes = object
                .take_bytes()
                .ok_or_else(|| Error::PipeError("non-file object has no bytes".to_string()))?;
            S3ByteStream::from(bytes)
        };

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
