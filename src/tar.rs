use flate2::read::GzDecoder;
use tar::Archive;

pub fn tar_gz_entries(data: &[u8]) -> Archive<GzDecoder<&[u8]>> {
    let tar = GzDecoder::new(&data[..]);
    Archive::new(tar)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use tar::Header;

    fn create_archive() -> Vec<u8> {
        let data = vec![];
        let enc = GzEncoder::new(data, Compression::default());
        let mut tar = tar::Builder::new(enc);
        let mut header = Header::new_gnu();
        let data = b"233333333".to_vec();
        for i in 0..100 {
            header.set_path(format!("data/{}", i)).unwrap();
            header.set_size(data.len() as u64);
            header.set_cksum();
            tar.append(&header, &data[..]).unwrap();
        }
        tar.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn test_archive_iterator() {
        let data = create_archive();
        for (i, entry) in tar_gz_entries(&data).entries().unwrap().enumerate() {
            assert_eq!(
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .into_owned()
                    .to_string_lossy(),
                format!("data/{}", i)
            );
        }
    }
}
