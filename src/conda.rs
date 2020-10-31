use futures::lock::Mutex;
use futures::StreamExt;
use indicatif::ProgressBar;

use overlay::OverlayDirectory;

use serde_json::Value as JsonValue;

use slog_scope::{debug, info, warn};

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;

use crate::utils::{content_of, parallel_download_files, retry_download, DownloadTask};

pub struct Conda {
    pub repo: String,
    pub base_path: PathBuf,
    pub debug_mode: bool,
    pub concurrent_downloads: usize,
}

fn parse_index(data: &[u8]) -> Result<Vec<(String, String, String)>> {
    let v: JsonValue = serde_json::from_slice(data)?;
    let mut result = vec![];

    let package_mapper = |(key, value): (&String, &JsonValue)| -> (String, String, String) {
        (
            key.clone(),
            "sha256".to_string(),
            value["sha256"].as_str().unwrap().to_string(),
        )
    };

    if let Some(JsonValue::Object(map)) = v.get("packages") {
        result.append(&mut map.iter().map(package_mapper).collect());
    }
    if let Some(JsonValue::Object(map)) = v.get("packages.conda") {
        result.append(&mut map.iter().map(package_mapper).collect());
    }

    Ok(result)
}

impl Conda {
    pub async fn run(&self) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));
        let client = reqwest::Client::new();

        info!("download repo index");

        let mut index = retry_download(
            client.clone(),
            base.clone(),
            format!("{}/repodata.json", self.repo),
            "repodata.json",
            5,
            "download repo file",
        )
        .await?;

        let index_content = content_of(&mut index).await?;

        let packages_to_download = parse_index(&index_content)?;

        info!("{} packages to download", packages_to_download.len());

        // let progress = ProgressBar::new(packages_to_download.len() as u64);
        let progress = ProgressBar::hidden();

        let file_list = packages_to_download
            .into_iter()
            .map(|(pkg, hash_type, hash)| {
                let mut path = self.base_path.clone();
                path.push(&pkg);

                DownloadTask {
                    name: pkg.clone(),
                    url: format!("{}/{}", self.repo, pkg),
                    path,
                    hash_type,
                    hash,
                }
            })
            .collect::<Vec<DownloadTask>>();

        parallel_download_files(
            client.clone(),
            base.clone(),
            file_list,
            5,
            self.concurrent_downloads,
            || progress.inc(1),
        )
        .await;

        for filename in &["repodata.json.bz2", "current_repodata.json"] {
            info!("download extra repo index {}", filename);

            let file = retry_download(
                client.clone(),
                base.clone(),
                format!("{}/{}", self.repo, filename),
                filename,
                5,
                format!("download {}", filename),
            )
            .await;

            if let Ok(file) = file {
                file.commit().await?;
            }
        }

        index.commit().await?;
        Ok(())
    }
}
