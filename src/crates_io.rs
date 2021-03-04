use crate::common::{Mission, SnapshotConfig, SnapshotPath, TransferURL};
use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};

use async_trait::async_trait;
use serde::Deserialize;
use slog::info;
use std::io::Read;
use structopt::StructOpt;

#[derive(Deserialize, Debug)]
pub struct CratesIoPackage {
    name: String,
    vers: String,
    cksum: String,
}

#[derive(Debug, StructOpt)]
pub struct CratesIo {
    #[structopt(
        long,
        default_value = "https://github.com/rust-lang/crates.io-index/archive/master.zip"
    )]
    pub zip_master: String,
    #[structopt(long, default_value = "https://static.crates.io/crates")]
    pub crates_base: String,
    #[structopt(long)]
    pub debug: bool,
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for CratesIo {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "fetching crates.io-index zip...");
        progress.set_message("fetching crates.io-index zip...");
        let data = client.get(&self.zip_master).send().await?.bytes().await?;
        let mut data = std::io::Cursor::new(data);
        let mut buf = vec![];
        let mut snapshot = vec![];
        info!(logger, "parsing...");

        let mut idx = 0;
        loop {
            match zip::read::read_zipfile_from_stream(&mut data) {
                Ok(Some(mut file)) => {
                    let mut is_first = true;
                    buf.clear();
                    file.read_to_end(&mut buf)?;

                    let mut de = serde_json::Deserializer::from_reader(&buf[..]);
                    while let Ok(package) = CratesIoPackage::deserialize(&mut de) {
                        let url = format!(
                            "{crate}/{crate}-{version}.crate",
                            crate = package.name,
                            version = package.vers
                        );
                        if is_first {
                            progress.set_message(&url);
                            is_first = false;
                        }
                        idx += 1;
                        progress.inc(1);
                        snapshot.push(url);
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            }
            if self.debug && idx >= 10000 {
                break;
            }
            tokio::task::yield_now().await;
        }

        progress.finish_with_message("done");

        Ok(crate::utils::snapshot_string_to_path(snapshot))
    }

    fn info(&self) -> String {
        format!("crates.io, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, TransferURL> for CratesIo {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.crates_base, snapshot.0)))
    }
}
