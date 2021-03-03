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
    url_encode_map: Vec<(&'static str, &'static str)>,
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
    fn generate_url_encode_map() -> Vec<(&'static str, &'static str)> {
        // reference: https://github.com/GeorgePhillips/node-s3-url-encode/blob/master/index.js
        let mut map = vec![];
        map.push(("+", "%2B"));
        map.push(("!", "%21"));
        map.push(("\"", "%22"));
        map.push(("#", "%23"));
        map.push(("$", "%24"));
        map.push(("&", "%26"));
        map.push(("'", "%27"));
        map.push(("(", "%28"));
        map.push((")", "%29"));
        map.push(("*", "%2A"));
        map.push((",", "%2C"));
        map.push((":", "%3A"));
        map.push((";", "%3B"));
        map.push(("=", "%3D"));
        map.push(("?", "%3F"));
        map.push(("@", "%40"));
        map
    }

    pub fn new(config: S3Config) -> Self {
        let client = get_s3_client("jCloud S3".to_string(), config.endpoint.clone());
        Self {
            config,
            client,
            url_encode_map: Self::generate_url_encode_map(),
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
                let key = item.key.unwrap();
                if key.starts_with(&s3_prefix_base) {
                    let mut key = key[s3_prefix_base.len()..].to_string();
                    for (ch, seq) in &self.url_encode_map {
                        key = key.replace(ch, seq);
                    }
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
            key: format!("{}/{}", self.config.prefix, snapshot.0),
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
