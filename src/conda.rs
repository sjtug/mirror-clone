use futures::lock::Mutex;
use futures::StreamExt;
use indicatif::ProgressBar;
use itertools::Itertools;
use overlay::{OverlayDirectory, OverlayFile};
use regex::Regex;
use slog::o;
use slog_scope::{debug, info, warn};
use slog_scope_futures::FutureExt;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;
use crate::tar::tar_gz_entries;
use crate::utils::{content_of, download_to_file, retry, verify_checksum};

pub struct Conda {
    pub repo: String,
    pub base_path: PathBuf,
    pub debug_mode: bool,
    pub concurrent_downloads: usize,
}

impl Conda {
    pub async fn run(&self) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));
        let client = reqwest::Client::new();

        info!("download repo file");

        let repo_file = retry(
            || async {
                let mut repo_file = base.lock().await.create_file_for_write("repo").await?;
                download_to_file(
                    client.clone(),
                    format!("{}/repo", self.repo),
                    &mut repo_file,
                )
                .await?;
                Ok(repo_file)
            },
            5,
            "download repo file".to_string(),
        )
        .await?;

        info!("download repo index");
        let (index, index_content) = retry(
            || async {
                let mut index = base
                    .lock()
                    .await
                    .create_file_for_write("index.tar.gz")
                    .await?;
                let index_content = content_of(
                    client.clone(),
                    format!("{}/index.tar.gz", self.repo),
                    &mut index,
                )
                .await?;
                Ok((index, index_content))
            },
            5,
            "download repo index".to_string(),
        )
        .await?;

        info!("parse repo index");
        let all_packages = parse_index_content(index_content, self.debug_mode)?;
        let original_length = all_packages.len();
        let all_packages: Vec<(String, String, String)> = all_packages
            .into_iter()
            .unique_by(|s| (s.1.clone(), s.2.clone()))
            .collect();
        let filtered_length = original_length - all_packages.len();
        if filtered_length != 0 {
            warn!("filtered {} duplicated packages", filtered_length);
        }

        // let progress = ProgressBar::new(all_packages.len() as u64);
        let progress = ProgressBar::hidden();

        let mut fetches =
            futures::stream::iter(all_packages.into_iter().map(|(name, hash_type, hash)| {
                let name_logger = name;
                let base = base.clone();
                let cache_path = format!("{}/{}/{}", hash_type, &hash[..2], hash);
                let client = client.clone();
                async move {
                    retry(
                        || async {
                            let base = base.lock().await;
                            let download_path = format!("archive/{}", cache_path.clone());
                            if base.add_to_overlay(&download_path).await? {
                                debug!("skip, already exists");
                                return Ok(());
                            }
                            let file = base.create_file_for_write(download_path).await?;
                            drop(base);
                            download_by_hash(
                                client.clone(),
                                file,
                                self.archive_url.clone(),
                                cache_path.clone(),
                                (hash_type.clone(), hash.clone()),
                            )
                            .await?;
                            Ok(())
                        },
                        5,
                        "download file".to_string(),
                    )
                    .await
                }
                .with_logger(slog_scope::logger().new(o!("package" => name_logger)))
            }))
            .buffer_unordered(self.concurrent_downloads);

        while fetches.next().await.is_some() {
            progress.inc(1);
        }

        index.commit().await?;
        repo_file.commit().await?;
        Ok(())
    }
}
