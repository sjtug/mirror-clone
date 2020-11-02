use futures::lock::Mutex;
use regex::Regex;
use serde::Deserialize;
use slog_scope::{info, warn};
use std::collections::HashSet;
use std::io::Read;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use walkdir::WalkDir;

use overlay::OverlayDirectory;

use crate::error::Result;
use crate::oracle::Oracle;
use crate::tar::tar_gz_entries;
use crate::utils::{content_of, parallel_download_files, retry_download, DownloadTask};

pub struct CratesIo {
    pub repo_path: PathBuf,
    pub base_path: PathBuf,
    pub crates_io_url: String,
    pub debug_mode: bool,
    pub concurrent_downloads: usize,
}

#[derive(Deserialize, Debug)]
pub struct CratesIoPackage {
    name: String,
    vers: String,
    cksum: String,
}

async fn parse_registery_file<P: AsRef<Path>>(
    path: P,
    packages: &mut Vec<CratesIoPackage>,
) -> Result<()> {
    let path = path.as_ref();
    let buf = fs::read(path).await?;
    let mut de = serde_json::Deserializer::from_reader(&buf[..]);
    while let Ok(package) = CratesIoPackage::deserialize(&mut de) {
        packages.push(package);
    }
    Ok(())
}

impl CratesIo {
    pub async fn run(&self, oracle: Oracle) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));

        let client = &oracle.client;
        let progress = &oracle.progress;

        info!("scanning repo file");

        let mut packages: Vec<CratesIoPackage> = vec![];
        let mut cnt: usize = 0;

        for entry in WalkDir::new(&self.repo_path) {
            if self.debug_mode && cnt >= 100 {
                break;
            }
            let entry = entry?;
            let meta = fs::metadata(entry.path()).await?;
            if meta.is_file() {
                cnt += 1;
                parse_registery_file(entry.path(), &mut packages).await?;
            }
        }

        info!(
            "{} packages with {} version variants parsed",
            cnt,
            packages.len()
        );

        progress.set_length(packages.len() as u64);

        let file_list = packages
            .into_iter()
            .map(|CratesIoPackage { name, vers, cksum }| {
                let url = format!(
                    "{crate}/{crate}-{version}.crate",
                    crate = name,
                    version = vers
                )
                .to_string();
                DownloadTask {
                    name,
                    url: format!("{}/{}", self.crates_io_url, url),
                    path: PathBuf::from(url),
                    hash_type: "sha256".to_string(),
                    hash: cksum,
                }
            })
            .collect::<Vec<DownloadTask>>();

        parallel_download_files(
            client.clone(),
            base.clone(),
            file_list,
            5,
            self.concurrent_downloads,
            |_, _| progress.inc(1),
        )
        .await;

        let result = base.lock().await.commit().await?;
        info!("{} stale files removed", result);

        Ok(())
    }
}
