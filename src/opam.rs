use std::collections::HashSet;
use std::io::Read;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::sync::Arc;

use futures::lock::Mutex;
use itertools::Itertools;
use opam_file_format::parser::{Item, Value};
use slog_scope::{info, warn};

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

fn split_once<'a>(in_string: &'a str, pat: &str) -> Option<(&'a str, &'a str)> {
    let mut splitter = in_string.splitn(2, pat);
    let first = splitter.next()?;
    let second = splitter.next()?;
    Some((first, second))
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

    let mut entries = tar_gz_entries(&index_content);
    let tar_gz_iterator = entries.entries()?;
    let mut opam_files: Vec<(String, String)> = vec![];
    for (idx, entry) in tar_gz_iterator.enumerate() {
        if limit_entries && idx >= 2000 {
            break;
        }

        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let path_str = path.to_string_lossy().to_string();
        if path_str.ends_with("/opam") {
            let name = path_str.split('/').collect::<Vec<&str>>()[2].to_string();
            buf.clear();
            entry.read_to_end(&mut buf)?;
            let content = std::str::from_utf8(&buf)?.to_string();
            opam_files.push((name, content))
        }
    }

    Ok(opam_files
        .into_iter()
        .filter_map(|(name, content)| {
            opam_file_format::lex(&content)
                .ok()
                .and_then(|tokens| opam_file_format::parse(tokens.into_iter()).ok())
                .and_then(|ast| {
                    if let Some(box Item::Section { name: _, items }) = ast.items.get("url") {
                        Some(items.clone())
                    } else {
                        warn!("no url section found in {}", name);
                        None
                    }
                })
                .and_then(|url| {
                    let mut result = None;
                    for field in ["src", "http", "archive"].iter() {
                        if let Some(box Item::Variable(Value::String(src))) =
                            url.get(&field.to_string())
                        {
                            result = Some((src.clone(), url));
                            break;
                        }
                    }
                    if result.is_none() {
                        warn!("missing src field in {}", name);
                    };
                    result
                })
                .and_then(|(src, url)| {
                    if let Some(checksums) = url.get("checksum") {
                        if let Item::Variable(Value::String(str)) = &**checksums {
                            if let Some((hash_func, chksum)) = split_once(&str, "=") {
                                Some((src, vec![(hash_func.to_string(), chksum.to_string())]))
                            } else {
                                Some((src, vec![("md5".to_string(), str.to_string())]))
                            }
                        } else if let Item::Variable(Value::List(list)) = &**checksums {
                            let checksums = list
                                .iter()
                                .filter_map(|values| {
                                    if let Value::String(str) = &**values {
                                        if let Some((hash_func, chksum)) = split_once(&str, "=") {
                                            Some((hash_func.to_string(), chksum.to_string()))
                                        } else {
                                            Some(("md5".to_string(), str.to_string()))
                                        }
                                    } else {
                                        warn!("no checksum found in {}", name);
                                        None
                                    }
                                })
                                .collect::<Vec<(String, String)>>();
                            if !checksums.is_empty() {
                                Some((src, checksums))
                            } else {
                                warn!("no checksum found in {}", name);
                                None
                            }
                        } else {
                            warn!("no checksum found in {}", name);
                            None
                        }
                    } else {
                        warn!("no checksum found in {}", name);
                        None
                    }
                })
                .map(|(src, hashes)| OpamDownloadTask { name, src, hashes })
        })
        .collect())
}

fn build_hash_url(hash_type: &str, hash: &str) -> String {
    format!("{}/{}/{}", hash_type, &hash[..2], hash)
}

impl Opam {
    pub async fn run(&self, oracle: Oracle) -> Result<()> {
        info!("scanning existing files");

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
