use md5::Digest;
use md5::Md5;
use overlay::OverlayFile;
use sha1::Sha1;
use sha2::Sha256;
use sha2::Sha512;
use tokio::io::AsyncReadExt;

use crate::error::{Error, Result};

macro_rules! get_digest {
    ($reader: ident, $digest:ident) => {
        let mut buffer = vec![0 as u8; 1024];
        while let Ok(bytes_read) = $reader.read(&mut buffer[..]).await {
            if bytes_read == 0 {
                break;
            }
            $digest.update(&buffer[0..bytes_read]);
        }
    };
}

pub async fn verify_checksum(
    file: &mut OverlayFile,
    checksum_type: String,
    expected: String,
) -> Result<()> {
    let file = file.file();
    file.seek(std::io::SeekFrom::Start(0)).await?;
    // let mut reader = BufReader::new(file);
    let reader = file;
    let checksum = match checksum_type.as_str() {
        "md5" => {
            let mut digest = Md5::new();
            get_digest!(reader, digest);
            format!("{:x}", digest.finalize())
        }
        "sha1" => {
            let mut digest = Sha1::new();
            get_digest!(reader, digest);
            format!("{:x}", digest.finalize())
        }
        "sha256" => {
            let mut digest = Sha256::new();
            get_digest!(reader, digest);
            format!("{:x}", digest.finalize())
        }
        "sha512" => {
            let mut digest = Sha512::new();
            get_digest!(reader, digest);
            format!("{:x}", digest.finalize())
        }
        _ => panic!("unsupported checksum type: {}", checksum_type),
    };
    if expected == checksum {
        Ok(())
    } else {
        Err(Error::ChecksumError {
            checksum_type,
            expected,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_md5() {
        let data = "23333333333".repeat(100).as_bytes().to_vec();
        let expected_digest = Md5::digest(&data);
        let mut digest = Md5::new();
        let mut reader = BufReader::new(data.as_slice());
        get_digest!(reader, digest);
        assert_eq!(digest.finalize(), expected_digest);
    }

    #[tokio::test]
    async fn test_sha1() {
        let data = "23333333333".repeat(100).as_bytes().to_vec();
        let expected_digest = Sha1::digest(&data);
        let mut digest = Sha1::new();
        let mut reader = BufReader::new(data.as_slice());
        get_digest!(reader, digest);
        assert_eq!(digest.finalize(), expected_digest);
    }

    #[tokio::test]
    async fn test_sha256() {
        let data = "23333333333".repeat(100).as_bytes().to_vec();
        let expected_digest = Sha256::digest(&data);
        let mut digest = Sha256::new();
        let mut reader = BufReader::new(data.as_slice());
        get_digest!(reader, digest);
        assert_eq!(digest.finalize(), expected_digest);
    }

    #[tokio::test]
    async fn test_sha512() {
        let data = "23333333333".repeat(100).as_bytes().to_vec();
        let expected_digest = Sha512::digest(&data);
        let mut digest = Sha512::new();
        let mut reader = BufReader::new(data.as_slice());
        get_digest!(reader, digest);
        assert_eq!(digest.finalize(), expected_digest);
    }
}
