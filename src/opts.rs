use crate::conda::CondaConfig;
use crate::crates_io::CratesIo as CratesIoConfig;
use crate::dart::Dart;
use crate::file_backend::FileBackend;
use crate::ghcup::Ghcup as GhcupConfig;
use crate::github_release::GitHubRelease;
use crate::homebrew::Homebrew as HomebrewConfig;
use crate::pypi::Pypi as PypiConfig;
use crate::rsync::Rsync as RsyncConfig;

use crate::{
    error::{Error, Result},
    s3::S3Backend,
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Source {
    #[structopt(about = "PyPI index")]
    Pypi(PypiConfig),
    #[structopt(about = "Homebrew bottles")]
    Homebrew(HomebrewConfig),
    #[structopt(about = "crates.io")]
    CratesIo(CratesIoConfig),
    #[structopt(about = "conda")]
    Conda(CondaConfig),
    #[structopt(about = "rsync")]
    Rsync(RsyncConfig),
    #[structopt(about = "GitHub Releases")]
    GithubRelease(GitHubRelease),
    #[structopt(about = "dart pub.dev")]
    DartPub(Dart),
    #[structopt(about = "ghcup")]
    Ghcup(GhcupConfig),
}

#[derive(Debug)]
pub enum Target {
    S3,
    File,
}

impl From<S3CliConfig> for S3Backend {
    fn from(config: S3CliConfig) -> Self {
        let mut s3_config =
            crate::s3::S3Config::new_jcloud(config.s3_prefix.unwrap(), config.s3_scan_metadata);
        if let Some(endpoint) = config.s3_endpoint {
            s3_config.endpoint = endpoint;
        }
        if let Some(bucket) = config.s3_bucket {
            s3_config.bucket = bucket;
        }
        s3_config.max_keys = config.s3_max_keys;
        s3_config.prefix_hint_mode = config.s3_prefix_hint_mode;
        S3Backend::new(s3_config)
    }
}

impl From<FileBackendConfig> for FileBackend {
    fn from(config: FileBackendConfig) -> Self {
        FileBackend::new(config.file_base_path.unwrap())
    }
}

#[derive(StructOpt, Debug, Clone)]
pub struct S3CliConfig {
    #[structopt(long, help = "Endpoint for S3 backend")]
    pub s3_endpoint: Option<String>,
    #[structopt(long, help = "Bucket of S3 backend")]
    pub s3_bucket: Option<String>,
    #[structopt(long, help = "Prefix of S3 backend")]
    pub s3_prefix: Option<String>,
    #[structopt(long, help = "Buffer data to this temporary directory")]
    pub s3_buffer_path: Option<String>,
    #[structopt(long, help = "Prefix hint mode, to accelerate scanning")]
    pub s3_prefix_hint_mode: Option<String>,
    #[structopt(long, help = "Max keys to list at a time", default_value = "1000")]
    pub s3_max_keys: u64,
    #[structopt(long, help = "Scan metadata (Greatly increase requests)")]
    pub s3_scan_metadata: bool,
}

#[derive(StructOpt, Debug, Clone)]
pub struct FileBackendConfig {
    #[structopt(
        long,
        help = "Base path for file backend",
        required_if("target_type", "file")
    )]
    pub file_base_path: Option<String>,
    #[structopt(
        long,
        help = "Buffer path for file backend, should not be within base path",
        required_if("target_type", "file")
    )]
    pub file_buffer_path: Option<String>,
}

impl std::str::FromStr for Target {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "s3" => Ok(Self::S3),
            "file" => Ok(Self::File),
            _ => Err(Error::ConfigureError("unsupported target".to_string())),
        }
    }
}

#[derive(StructOpt, Debug)]
pub struct TransferConfig {
    #[structopt(long, help = "Concurrent transfer tasks", default_value = "8")]
    pub concurrent_transfer: usize,
    #[structopt(long, help = "Don't delete files")]
    pub no_delete: bool,
    #[structopt(long, help = "Enable dry run mode")]
    pub dry_run: bool,
    #[structopt(
        long,
        help = "Print first n records of transfer plan",
        default_value = "0"
    )]
    pub print_plan: usize,
}

#[derive(StructOpt, Debug)]
#[structopt(version = "2.0", author = "Alex Chi <iskyzh@gmail.com>")]
pub struct Opts {
    #[structopt(subcommand)]
    pub source: Source,
    #[structopt(long, help = "Target to use")]
    pub target_type: Target,
    #[structopt(flatten)]
    pub s3_config: S3CliConfig,
    #[structopt(flatten)]
    pub file_config: FileBackendConfig,
    #[structopt(long, help = "Enable progress bar")]
    pub progress: bool,
    #[structopt(long, help = "Worker threads")]
    pub workers: Option<usize>,
    #[structopt(long, help = "Concurrent resolve tasks", default_value = "64")]
    pub concurrent_resolve: usize,
    #[structopt(flatten)]
    pub transfer_config: TransferConfig,
}
