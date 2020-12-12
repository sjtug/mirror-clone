use crate::common::Mission;
use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};

use async_trait::async_trait;
use serde::Deserialize;
use slog::info;
use std::io::Read;

#[derive(Deserialize, Debug)]
pub struct CratesIoPackage {
    name: String,
    vers: String,
    cksum: String,
}

#[derive(Debug)]
pub struct CratesIo {
    pub zip_master: String,
    pub debug: bool,
}

#[async_trait]
impl SnapshotStorage<String> for CratesIo {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "fetching crates.io-index zip...");
        let data = client.get(&self.zip_master).send().await?.bytes().await?;
        let mut data = std::io::Cursor::new(data);
        let mut buf = vec![];
        let mut snapshot = vec![];
        info!(logger, "parsing...");

        let mut idx = 0;
        loop {
            match zip::read::read_zipfile_from_stream(&mut data) {
                Ok(Some(mut file)) => {
                    progress.set_message(file.name());
                    buf.clear();
                    file.read_to_end(&mut buf)?;
                    let mut de = serde_json::Deserializer::from_reader(&buf[..]);
                    while let Ok(package) = CratesIoPackage::deserialize(&mut de) {
                        let url = format!(
                            "{crate}/{crate}-{version}.crate",
                            crate = package.name,
                            version = package.vers
                        );
                        idx += 1;
                        progress.inc(1);
                        snapshot.push(url);
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            }
            if self.debug && idx >= 100 {
                break;
            }
        }

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("crates.io, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for CratesIo {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
