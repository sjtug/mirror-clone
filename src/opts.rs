use crate::homebrew::Homebrew as HomebrewConfig;
use crate::pypi::Pypi as PypiConfig;
use crate::{crates_io::CratesIo as CratesIoConfig, file_backend::FileBackend};

use crate::{
    error::{Error, Result},
    mirror_intel::MirrorIntel,
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
}

#[derive(Debug)]
pub enum Target {
    Intel,
    S3,
    File,
}

#[derive(StructOpt, Debug)]
pub struct MirrorIntelCliConfig {
    #[structopt(
        long,
        required_if("target_type", "intel"),
        help = "Base URL for mirror-intel backend"
    )]
    pub mirror_intel_base: Option<String>,
}

impl Into<MirrorIntel> for MirrorIntelCliConfig {
    fn into(self) -> MirrorIntel {
        MirrorIntel::new(self.mirror_intel_base.unwrap())
    }
}

impl Into<S3Backend> for S3CliConfig {
    fn into(self) -> S3Backend {
        let mut config = crate::s3::S3Config::new_jcloud(self.s3_prefix.unwrap());
        if let Some(endpoint) = self.s3_endpoint {
            config.endpoint = endpoint;
        }
        if let Some(bucket) = self.s3_bucket {
            config.bucket = bucket;
        }
        S3Backend::new(config)
    }
}

impl Into<FileBackend> for FileBackendConfig {
    fn into(self) -> FileBackend {
        FileBackend::new(self.file_base_path.unwrap())
    }
}

#[derive(StructOpt, Debug)]
pub struct S3CliConfig {
    #[structopt(long, help = "Endpoint for S3 backend")]
    pub s3_endpoint: Option<String>,
    #[structopt(long, help = "Bucket of S3 backend")]
    pub s3_bucket: Option<String>,
    #[structopt(long, help = "Prefix of S3 backend")]
    pub s3_prefix: Option<String>,
    #[structopt(long, help = "Buffer data to this temporary directory")]
    pub s3_buffer_path: Option<String>,
}

#[derive(StructOpt, Debug)]
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
            "intel" => Ok(Self::Intel),
            "s3" => Ok(Self::S3),
            "file" => Ok(Self::File),
            _ => Err(Error::ConfigureError("unsupported target".to_string())),
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(version = "2.0", author = "Alex Chi <iskyzh@gmail.com>")]
pub struct Opts {
    #[structopt(subcommand)]
    pub source: Source,
    #[structopt(long, help = "Target to use")]
    pub target_type: Target,
    #[structopt(flatten)]
    pub mirror_intel_config: MirrorIntelCliConfig,
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
    #[structopt(long, help = "Concurrent transfer tasks", default_value = "8")]
    pub concurrent_transfer: usize,
    #[structopt(long, help = "Don't delete files")]
    pub no_delete: bool,
}
