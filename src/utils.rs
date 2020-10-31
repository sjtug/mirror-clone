pub mod checksum;
pub mod retry;

use futures_util::StreamExt;
use overlay::OverlayFile;
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{Error, Result};

pub use retry::retry;

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

pub async fn content_of(client: Client, url: String, file: &mut OverlayFile) -> Result<Vec<u8>> {
    let size = download_to_file(client, url, file).await?;
    let mut buf = Vec::with_capacity(size);
    let file = file.file();
    file.seek(std::io::SeekFrom::Start(0)).await?;
    file.read_to_end(&mut buf).await?;
    assert_eq!(buf.len(), size);
    Ok(buf)
}
