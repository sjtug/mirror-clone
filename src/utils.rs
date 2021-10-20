use std::convert::Infallible;
use std::io::{Error, ErrorKind, Result as IOResult, SeekFrom};
use std::str::FromStr;

use indicatif::ProgressStyle;
use regex::Regex;
use sha2::Digest;
use slog::{o, Drain};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio_io_compat::CompatHelperTrait;

use crate::common::SnapshotPath;
use crate::error::Result;
use crate::metadata::SnapshotMeta;

#[derive(Debug, Clone, Default)]
pub struct CommaSplitVecString(Vec<String>);

impl FromStr for CommaSplitVecString {
    type Err = Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(
            s.split(',')
                .map(str::trim)
                .map(ToString::to_string)
                .collect(),
        ))
    }
}

impl From<CommaSplitVecString> for Vec<String> {
    fn from(v: CommaSplitVecString) -> Self {
        v.0
    }
}

pub fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    #[cfg(not(debug_assertions))]
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

pub fn spinner() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{prefix:.bold.dim} {spinner} {msg}")
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
}

pub fn bar() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{eta_precise}] ({pos}/{len}) {msg}",
        )
        .progress_chars("=> ")
}

pub fn snapshot_string_to_path(snapshot: Vec<String>) -> Vec<SnapshotPath> {
    snapshot.into_iter().map(SnapshotPath::new).collect()
}

pub fn snapshot_string_to_meta(snapshot: Vec<String>) -> Vec<SnapshotMeta> {
    snapshot.into_iter().map(SnapshotMeta::new).collect()
}

pub fn user_agent() -> String {
    format!(
        "mirror-clone / {} ({})",
        env!("CARGO_PKG_VERSION"),
        std::env::var("MIRROR_CLONE_SITE").expect("No MIRROR_CLONE_SITE env variable")
    )
}

pub fn generate_s3_url_encode_map() -> Vec<(&'static str, &'static str)> {
    // reference: https://github.com/GeorgePhillips/node-s3-url-encode/blob/master/index.js
    vec![
        ("+", "%2B"),
        ("!", "%21"),
        ("\"", "%22"),
        ("#", "%23"),
        ("$", "%24"),
        ("&", "%26"),
        ("'", "%27"),
        ("(", "%28"),
        (")", "%29"),
        ("*", "%2A"),
        (",", "%2C"),
        (":", "%3A"),
        (";", "%3B"),
        ("=", "%3D"),
        ("?", "%3F"),
        ("@", "%40"),
    ]
}

pub fn generate_s3_url_reverse_encode_map() -> Vec<(&'static str, &'static str)> {
    generate_s3_url_encode_map()
        .into_iter()
        .map(|(x, y)| (y, x))
        .collect()
}

pub fn rewrite_url_string(url_encode_map: &[(&'static str, &'static str)], key: &str) -> String {
    let mut key = key.to_string();

    for (ch, seq) in url_encode_map {
        key = key.replace(ch, seq);
    }

    key
}

pub fn fn_regex_rewrite(
    pattern: &Regex,
    rewrite: String,
) -> impl Fn(String) -> Result<String> + Sync + Send + '_ {
    move |data| {
        Ok(pattern
            .replace_all(data.as_str(), rewrite.as_str())
            .to_string())
    }
}

pub fn hash_string(key: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

pub fn unix_time() -> u64 {
    let start = std::time::SystemTime::now();
    start
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

async fn sha256(source: &mut (impl AsyncRead + Unpin)) -> IOResult<String> {
    let mut hasher = sha2::Sha256::new();
    tokio::io::copy(source, &mut hasher.tokio_io_mut()).await?;
    Ok(format!("{:x}", hasher.finalize()))
}

pub async fn calc_checksum(
    source: &mut (impl AsyncRead + AsyncSeek + Unpin),
    method: &str,
) -> IOResult<String> {
    source.seek(SeekFrom::Start(0)).await?;
    match method {
        "sha256" => sha256(source).await,
        _ => Err(Error::new(
            ErrorKind::Unsupported,
            "unsupported checksum method",
        )),
    }
}
