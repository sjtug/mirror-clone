use futures::lock::Mutex;
use serde_json::Value as JsonValue;
use slog_scope::info;
use std::path::PathBuf;
use std::sync::Arc;

use overlay::OverlayDirectory;

use crate::error::Result;
use crate::oracle::Oracle;

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
    pub async fn run(&self, oracle: Oracle) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));
        let client = &oracle.client;
        let progress = &oracle.progress;

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

        progress.set_length(packages_to_download.len() as u64);

        let file_list = packages_to_download
            .into_iter()
            .map(|(pkg, hash_type, hash)| {
                let path = self.base_path.join(&pkg);

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
            |_, _| progress.inc(1),
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

        let result = base.lock().await.commit().await?;
        info!("{} stale files removed", result);

        Ok(())
    }
}
