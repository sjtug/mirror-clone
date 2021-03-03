use std::collections::HashMap;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::Result;
use crate::stream_pipe::ByteStream;
use crate::traits::{SnapshotStorage, TargetStorage};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use rusoto_core::Region;
use rusoto_s3::{DeleteObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use slog::{debug, info, warn};
use structopt::StructOpt;
use tokio::io::BufReader;
use tokio_util::codec;

#[derive(StructOpt, Debug)]
pub struct S3Config {
    #[structopt(long, default_value = "https://s3.jcloud.sjtu.edu.cn")]
    pub endpoint: String,
    #[structopt(long, default_value = "899a892efef34b1b944a19981040f55b-oss01")]
    pub bucket: String,
    #[structopt(long)]
    pub prefix: String,
}

impl S3Config {
    pub fn new_jcloud(prefix: String) -> Self {
        Self {
            endpoint: "https://s3.jcloud.sjtu.edu.cn".to_string(),
            bucket: "899a892efef34b1b944a19981040f55b-oss01".to_string(),
            prefix,
        }
    }
}

pub struct S3Backend {
    config: S3Config,
    client: S3Client,
    mapper: Vec<(&'static str, &'static str)>,
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
        Self {
            config,
            client,
            mapper: crate::utils::generate_s3_url_encode_map(),
        }
    }

    pub fn gen_metadata(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("clone-backend".to_string(), "s3-v1".to_string());
        map
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for S3Backend {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;

        info!(logger, "fetching data from S3 storage...");

        let mut continuation_token = None;
        let mut snapshot = vec![];

        let s3_prefix_base = format!("{}/", self.config.prefix);
        let mut total_size: u64 = 0;
        let gen_map = crate::utils::generate_s3_url_reverse_encode_map();

        loop {
            let req = ListObjectsV2Request {
                bucket: self.config.bucket.clone(),
                prefix: Some(self.config.prefix.clone()),
                continuation_token,
                ..Default::default()
            };

            let resp = self.client.list_objects_v2(req).await?;

            let mut first_key = true;
            for item in resp.contents.unwrap() {
                if let Some(size) = item.size {
                    total_size += size as u64;
                }
                let key = item.key.unwrap();
                if key.starts_with(&s3_prefix_base) {
                    let key = key[s3_prefix_base.len()..].to_string();
                    let key = crate::utils::rewrite_url_string(&gen_map, &key);
                    if first_key {
                        first_key = false;
                        progress.set_message(&key);
                    }
                    snapshot.push(SnapshotPath(key));
                } else {
                    warn!(logger, "prefix not match {}", key);
                }
            }

            if let Some(next_continuation_token) = resp.next_continuation_token {
                continuation_token = Some(next_continuation_token);
            } else {
                break;
            }
        }

        progress.finish_with_message("done");

        info!(
            logger,
            "total size: {}B or {}G",
            total_size,
            total_size as f64 / 1000.0 / 1000.0 / 1000.0
        );

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("s3, {:?}", self.config)
    }
}

#[async_trait]
impl TargetStorage<SnapshotPath, ByteStream> for S3Backend {
    async fn put_object(
        &self,
        snapshot: &SnapshotPath,
        (file, content_length): ByteStream,
        mission: &Mission,
    ) -> Result<()> {
        let logger = &mission.logger;
        debug!(logger, "upload: {}", snapshot.0);

        let body = codec::FramedRead::new(BufReader::new(file), codec::BytesCodec::new())
            .map_ok(|bytes| bytes.freeze());
        let req = PutObjectRequest {
            bucket: self.config.bucket.clone(),
            key: format!(
                "{}/{}",
                self.config.prefix,
                crate::utils::rewrite_url_string(&self.mapper, &snapshot.0)
            ),
            body: Some(rusoto_s3::StreamingBody::new(body)),
            metadata: Some(self.gen_metadata()),
            content_length: Some(content_length as i64),
            ..Default::default()
        };
        self.client.put_object(req).await?;
        Ok(())
    }

    async fn delete_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<()> {
        let req = DeleteObjectRequest {
            bucket: self.config.bucket.clone(),
            key: format!("{}/{}", self.config.prefix, snapshot.0),
            ..Default::default()
        };
        self.client.delete_object(req).await?;
        Ok(())
    }
}
