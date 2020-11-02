use futures::lock::Mutex;
use itertools::Itertools;
use regex::Regex;
use slog_scope::{info, warn};
use std::collections::HashSet;
use std::io::Read;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::Arc;

use overlay::OverlayDirectory;

use crate::error::Result;
use crate::oracle::Oracle;
use crate::tar::tar_gz_entries;
use crate::utils::{content_of, parallel_download_files, retry_download, DownloadTask};

pub struct Opam {
    pub repo: String,
    pub base_path: PathBuf,
    pub archive_url: Option<String>,
    pub debug_mode: bool,
    pub concurrent_downloads: usize,
}

#[derive(Clone)]
pub struct OpamDownloadTask {
    name: String,
    src: String,
    hashes: Vec<(String, String)>,
}

fn parse_index_content(
    index_content: Vec<u8>,
    limit_entries: bool,
) -> Result<Vec<OpamDownloadTask>> {
    let mut buf = Vec::new();
    let mut result = Vec::new();
    let opam_parser = Regex::new(r#""(md5|sha1|sha256|sha512)=(.*)""#)?;
    let string_finder = Regex::new(r#""(.*)""#)?;

    let mut entries = tar_gz_entries(&index_content);
    let tar_gz_iterator = entries.entries()?;
    for (idx, entry) in tar_gz_iterator.enumerate() {
        if limit_entries && idx >= 2000 {
            break;
        }

        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let path_str = path.to_string_lossy().to_string();
        if path_str.ends_with("/opam") {
            let mut hashes = vec![];
            buf.clear();
            entry.read_to_end(&mut buf)?;

            let name = path_str.split('/').collect::<Vec<&str>>()[2].to_string();
            // only read data after last "src" field
            let data = std::str::from_utf8(&buf)?
                .split("src:")
                .collect::<Vec<&str>>();
            let mut src = String::new();
            if let Some(data) = data.last() {
                if let Some(capture) = string_finder.captures_iter(data).next() {
                    src = capture[1].to_string();
                }

                // only read data after "checksum" field
                let data = data.split("checksum").collect::<Vec<&str>>();

                if let Some(data) = data.get(1) {
                    for capture in opam_parser.captures_iter(data) {
                        hashes.push((capture[1].to_string(), capture[2].to_string()));
                    }
                }
            }
            if hashes.is_empty() {
                warn!("no checksum found in {}", name);
            } else if src == "" {
                warn!("no src found in {}", name);
            } else {
                result.push(OpamDownloadTask { name, src, hashes })
            }
        }
    }

    Ok(result)
}

fn build_hash_url(hash_type: &str, hash: &str) -> String {
    format!("{}/{}/{}", hash_type, &hash[..2], hash)
}

impl Opam {
    pub async fn run(&self, oracle: Oracle) -> Result<()> {
        let base = OverlayDirectory::new(&self.base_path).await?;
        let base = Arc::new(Mutex::new(base));

        let client = &oracle.client;
        let progress = &oracle.progress;

        info!("download repo file");

        let repo_file = retry_download(
            client.clone(),
            base.clone(),
            format!("{}/repo", self.repo),
            "repo",
            5,
            "download repo file",
        )
        .await?;

        info!("download repo index");
        let mut index = retry_download(
            client.clone(),
            base.clone(),
            format!("{}/index.tar.gz", self.repo),
            "index.tar.gz",
            5,
            "download repo index",
        )
        .await?;

        let index_content = content_of(&mut index).await?;

        info!("parse repo index");
        let all_packages = parse_index_content(index_content, self.debug_mode)?;
        progress.set_length(all_packages.len() as u64);

        let mut failed_tasks = vec![];

        // generate download task
        let raw_file_list = all_packages
            .iter()
            .cloned()
            .map(|task| {
                // download file to OPAM cache
                let (hash_type, hash) = task.hashes[0].clone();
                let cache_relative_path = build_hash_url(&hash_type, &hash);
                let path = self.base_path.join(&cache_relative_path);

                let task = DownloadTask {
                    name: task.name.clone(),
                    url: task.src,
                    path,
                    hash_type,
                    hash,
                };

                if let Some(ref archive) = self.archive_url {
                    let mut cache_task = task.clone();
                    cache_task.url = format!("{}/{}", archive, cache_relative_path);
                    return vec![cache_task, task];
                }

                vec![task]
            })
            .collect::<Vec<Vec<DownloadTask>>>();

        info!("{} packages to download", raw_file_list.len());

        let file_list = raw_file_list
            .iter()
            .cloned()
            .unique_by(|task| (task[0].hash_type.clone(), task[0].hash.clone()))
            .collect::<Vec<Vec<DownloadTask>>>();

        if raw_file_list.len() != file_list.len() {
            warn!(
                "found {} duplicated packages",
                raw_file_list.len() - file_list.len()
            );
        }

        if self.archive_url.is_some() {
            let cache_tasks: Vec<DownloadTask> = file_list
                .iter()
                .map(|x| x[0].clone())
                .unique_by(|task| (task.hash_type.clone(), task.hash.clone()))
                .collect();
            let src_tasks: Vec<DownloadTask> = file_list.iter().map(|x| x[1].clone()).collect();

            // first, download from OPAM cache
            parallel_download_files(
                client.clone(),
                base.clone(),
                cache_tasks,
                5,
                self.concurrent_downloads,
                |task, result| {
                    progress.inc(1);
                    if let Err(crate::error::Error::HTTPError(_)) = result {
                        failed_tasks.push(task);
                    }
                },
            )
            .await;

            // then, download from source site
            warn!(
                "{} packages require fetching from source site",
                failed_tasks.len()
            );

            progress.inc_length(failed_tasks.len() as u64);

            let failed_tasks: HashSet<String> =
                HashSet::from_iter(failed_tasks.into_iter().map(|x| x.name));

            let src_tasks = src_tasks
                .into_iter()
                .filter(|x| failed_tasks.get(&x.name).is_some())
                .collect::<Vec<DownloadTask>>();

            parallel_download_files(
                client.clone(),
                base.clone(),
                src_tasks,
                5,
                self.concurrent_downloads,
                |_, _| progress.inc(1),
            )
            .await;
        } else {
            parallel_download_files(
                client.clone(),
                base.clone(),
                file_list
                    .into_iter()
                    .map(|mut x| x.pop().unwrap())
                    .collect(),
                5,
                self.concurrent_downloads,
                |_, _| progress.inc(1),
            )
            .await;
        }

        index.commit().await?;
        repo_file.commit().await?;

        let result = base.lock().await.commit().await?;
        info!("{} stale files removed", result);

        Ok(())
    }
}
