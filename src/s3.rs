use std::{collections::HashMap, sync::atomic::AtomicU64};

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::metadata::SnapshotMeta;
use crate::stream_pipe::ByteStream;
use crate::traits::{Key, SnapshotStorage, TargetStorage};

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use rusoto_core::Region;
use rusoto_s3::{DeleteObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use slog::{debug, info, warn};

#[derive(Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub prefix_hint_mode: Option<String>,
    pub max_keys: u64,
}

impl S3Config {
    pub fn new_jcloud(prefix: String) -> Self {
        Self {
            endpoint: "https://s3.jcloud.sjtu.edu.cn".to_string(),
            bucket: "899a892efef34b1b944a19981040f55b-oss01".to_string(),
            prefix,
            max_keys: 1000,
            prefix_hint_mode: None,
        }
    }
}

/// This backend has only been tested with SJTU S3 service, which is
/// (possibly) set up with Ceph. Unlike official S3 protocol, SJTU
/// S3 service supports special characters in key. For example, if
/// we put `go@1.10-1.10.8.catalina.bottle.2.tar.gz` into SJTU S3,
/// the `@` character won't be ignored. You may access it either at
/// `go@...` or `go%40...` on HTTP.
pub struct S3Backend {
    config: S3Config,
    client: S3Client,
}

fn jcloud_region(name: String, endpoint: String) -> Region {
    Region::Custom {
        name: name,
        endpoint,
    }
}

fn get_s3_client(name: String, endpoint: String) -> S3Client {
    S3Client::new(jcloud_region(name, endpoint))
}

impl S3Backend {
    pub fn new(config: S3Config) -> Self {
        let client = get_s3_client("jCloud S3".to_string(), config.endpoint.clone());
        Self { config, client }
    }

    pub fn gen_metadata(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("clone-backend".to_string(), "s3-v1".to_string());
        map
    }
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

        let prefix = match self.config.prefix_hint_mode.as_ref().map(|x| x.as_str()) {
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

        let mut futures = stream::iter(prefix)
            .map(|additional_prefix| {
                let bucket = self.config.bucket.clone();
                let prefix = Some(format!("{}{}", self.config.prefix, additional_prefix));
                let client = self.client.clone();
                let total_size = total_size.clone();
                let progress = progress.clone();
                let logger = logger.clone();
                let s3_prefix_base = s3_prefix_base.clone();
                let max_keys = self.config.max_keys;

                let scan_future = async move {
                    let mut snapshot = vec![];
                    let mut continuation_token = None;

                    loop {
                        let req = ListObjectsV2Request {
                            bucket: bucket.clone(),
                            prefix: prefix.clone(),
                            max_keys: Some(max_keys as i64),
                            continuation_token,
                            ..Default::default()
                        };

                        let resp = client.list_objects_v2(req).await?;

                        let mut first_key = true;

                        if let Some(contents) = resp.contents {
                            for item in contents {
                                if let Some(size) = item.size {
                                    total_size.fetch_add(
                                        size as u64,
                                        std::sync::atomic::Ordering::SeqCst,
                                    );
                                }
                                let key = item.key.unwrap();
                                if key.starts_with(&s3_prefix_base) {
                                    let key = key[s3_prefix_base.len()..].to_string();
                                    // let key = crate::utils::rewrite_url_string(&gen_map, &key);
                                    if first_key {
                                        first_key = false;
                                        progress.set_message(&key);
                                    }
                                    snapshot.push(SnapshotMeta {
                                        key,
                                        size: item.size.map(|x| x as u64),
                                        ..Default::default()
                                    });
                                } else {
                                    warn!(logger, "prefix not match {}", key);
                                }
                            }
                        }

                        if let Some(next_continuation_token) = resp.next_continuation_token {
                            continuation_token = Some(next_continuation_token);
                        } else {
                            break;
                        }
                    }
                    Ok::<_, Error>(snapshot)
                };

                scan_future
            })
            .buffer_unordered(256);

        let mut snapshots = vec![];

        while let Some(snapshot) = futures.next().await {
            snapshots.append(&mut snapshot?);
        }

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
        format!("s3 with meta, {:?}", self.config)
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
                .map(|x| SnapshotPath(x.key))
                .collect(),
        )
    }

    fn info(&self) -> String {
        format!("s3, {:?}", self.config)
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
        } = byte_stream;

        let body = object.as_stream();

        let mut metadata = self.gen_metadata();
        metadata.insert("clone-last-modified".to_string(), modified_at.to_string());
        metadata.extend(snapshot.s3_meta());

        let req = PutObjectRequest {
            bucket: self.config.bucket.clone(),
            key: format!("{}/{}", self.config.prefix, snapshot.key()),
            body: Some(rusoto_s3::StreamingBody::new(body)),
            metadata: Some(metadata),
            content_length: Some(length as i64),
            ..Default::default()
        };

        self.client.put_object(req).await?;

        Ok(())
    }

    async fn delete_object(&self, snapshot: &Snapshot, _mission: &Mission) -> Result<()> {
        let req = DeleteObjectRequest {
            bucket: self.config.bucket.clone(),
            key: format!("{}/{}", self.config.prefix, snapshot.key()),
            ..Default::default()
        };
        self.client.delete_object(req).await?;
        Ok(())
    }
}
