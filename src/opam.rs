use crate::error::Result;
use crate::tar::tar_gz_entries;
use crate::utils::{content_of, download_to_file, retry};
use futures::lock::Mutex;
use futures::StreamExt;
use indicatif::ProgressBar;
use log::{info, warn};
use overlay::{OverlayDirectory, OverlayFile};
use regex::Regex;
use std::error;
use std::fmt;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct OpamError(String);

impl fmt::Display for OpamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl error::Error for OpamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub struct Opam {
    pub repo: String,
    pub base_path: PathBuf,
    pub archive_url: String,
    pub debug_mode: bool,
    pub concurrent_downloads: usize,
}

fn parse_index_content(
    index_content: Vec<u8>,
    limit_entries: bool,
) -> Result<Vec<(String, String, String)>> {
    let mut data = Vec::new();
    let mut result = Vec::new();
    let opam_parser = Regex::new(r#""(md5|sha256|sha512)=(.*)""#)?;
    let mut entries = tar_gz_entries(&index_content);
    let tar_gz_iterator = entries.entries()?;
    for (idx, entry) in tar_gz_iterator.enumerate() {
        if limit_entries && idx >= 100 {
            break;
        }

        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let path_str = path.to_string_lossy().to_string();
        if path_str.ends_with("/opam") {
            data.clear();
            entry.read_to_end(&mut data)?;
            let mut cnt = 0;
            for capture in opam_parser.captures_iter(std::str::from_utf8(&data)?) {
                result.push((
                    path_str.clone(),
                    capture[1].to_owned(),
                    capture[2].to_owned(),
                ));
                cnt += 1;
            }
            if cnt == 0 {
                warn!("no checksum found in {}", path_str);
            }
        }
    }

    Ok(result)
}

async fn download_by_hash(
    mut file: OverlayFile,
    archive_url: String,
    name: String,
    cache_path: String,
) -> Result<()> {
    info!("downloading {} from {}", name, cache_path);
    download_to_file(format!("{}/{}", archive_url, cache_path), &mut file).await?;
    file.commit().await?;
    Ok(())
}

impl Opam {
    pub async fn run(&self) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));

        info!("downloading repo file...");

        let repo_file = retry(
            || {
                let base = base.clone();
                async move {
                    let mut repo_file = base.lock().await.create_file_for_write("repo").await?;
                    download_to_file(format!("{}/repo", self.repo), &mut repo_file).await?;
                    Ok(repo_file)
                }
            },
            5,
            "download repo file".to_string(),
        )
        .await?;

        info!("downloading repo index...");
        let mut index = base
            .lock()
            .await
            .create_file_for_write("index.tar.gz")
            .await?;
        let index_content = content_of(format!("{}/index.tar.gz", self.repo), &mut index).await?;

        info!("parsing repo index...");
        let all_packages = parse_index_content(index_content, self.debug_mode)?;

        let progress = ProgressBar::new(all_packages.len() as u64);

        let fetches =
            futures::stream::iter(all_packages.into_iter().map(|(name, hash_type, hash)| {
                let base = base.clone();
                let progress = progress.clone();
                async move {
                    let cache_path = format!("{}/{}/{}", hash_type, &hash[..2], hash);
                    let base = base.lock().await;
                    let file = base
                        .create_file_for_write(format!("archive/{}", cache_path))
                        .await;
                    drop(base);
                    let result = match file {
                        Ok(file) => {
                            match download_by_hash(file, self.archive_url.clone(), name, cache_path)
                                .await
                            {
                                Ok(_) => Ok(()),
                                Err(err) => Err(err),
                            }
                        }
                        Err(err) => Err(err.into()),
                    };
                    progress.inc(1);
                    result
                }
            }))
            .buffer_unordered(self.concurrent_downloads)
            .collect::<Vec<Result<()>>>();

        let results = fetches.await;

        for result in results {
            if let Err(err) = result {
                warn!("{}", err);
            }
        }

        index.commit().await?;
        repo_file.commit().await?;
        Ok(())
    }
}
