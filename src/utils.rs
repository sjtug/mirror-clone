pub mod checksum;
pub mod retry;

use futures::lock::Mutex;
use futures_util::StreamExt;
use overlay::{OverlayDirectory, OverlayFile};
use reqwest::Client;
use slog::o;
use slog_scope::{debug, info};
use slog_scope_futures::FutureExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{Error, Result};

pub use retry::{retry, retry_download};

pub use checksum::verify_checksum;

pub async fn download_to_file(
    client: Client,
    url: String,
    file: &mut OverlayFile,
) -> Result<usize> {
    use rand::prelude::*;
    if thread_rng().gen::<f64>() > 0.5 {
        // return Err(Error::MockError("lost connection".to_string()));
    }
    let response = client.get(url.as_str()).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    let mut stream = response.bytes_stream();
    let mut size = 0;
    while let Some(v) = stream.next().await {
        let v = v?;
        file.file().write_all(&v).await?;
        size += v.len();
    }
    Ok(size)
}

pub async fn content_of(file: &mut OverlayFile) -> Result<Vec<u8>> {
    let file = file.file();
    let size = file.metadata().await?.len() as usize;
    let mut buf = Vec::with_capacity(size);
    file.seek(std::io::SeekFrom::Start(0)).await?;
    file.read_to_end(&mut buf).await?;
    assert_eq!(buf.len(), size);
    Ok(buf)
}

pub async fn download_and_check_hash(
    client: reqwest::Client,
    mut file: OverlayFile,
    url: String,
    checksum: (String, String),
    
) -> Result<()> {
    info!("begin download");
    download_to_file(client, url, &mut file).await?;
    verify_checksum(&mut file, checksum.0, checksum.1).await?;
    file.commit().await?;
    Ok(())
}

pub struct DownloadTask {
    pub name: String,
    pub url: String,
    pub path: PathBuf,
    pub hash_type: String,
    pub hash: String,
}

pub async fn parallel_download_files(
    client: reqwest::Client,
    base: Arc<Mutex<OverlayDirectory>>,
    file_list: Vec<DownloadTask>,
    retry_times: usize,
    concurrent_downloads: usize,
    on_new_item: impl Fn() -> (),
) {
    let mut fetches = futures::stream::iter(file_list.into_iter().map(
        |DownloadTask {
             name,
             url,
             path,
             hash_type,
             hash,
         }| {
            let base = base.clone();
            let client = client.clone();
            async move {
                retry(
                    || async {
                        let base = base.lock().await;
                        if base.add_to_overlay(&path).await? {
                            debug!("skip, already exists");
                            return Ok(());
                        }
                        let file = base.create_file_for_write(&path).await?;
                        drop(base);
                        download_and_check_hash(
                            client.clone(),
                            file,
                            url.clone(),
                            (hash_type.clone(), hash.clone()),
                        )
                        .await?;
                        Ok(())
                    },
                    retry_times,
                    "download file".to_string(),
                )
                .await
            }
            .with_logger(slog_scope::logger().new(o!("package" => name)))
        },
    ))
    .buffer_unordered(concurrent_downloads);

    while fetches.next().await.is_some() {
        on_new_item();
    }
}
