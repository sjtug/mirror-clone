mod common;
mod crates_io;
mod dart;
mod error;
mod file_backend;
mod github_release;
mod gradle;
mod homebrew;
mod html_scanner;
mod mirror_intel;
mod opts;
mod pypi;
mod rsync;
mod rustup;
mod s3;
mod simple_diff_transfer;
mod stream_pipe;
mod timeout;
mod traits;
mod utils;

use common::SnapshotConfig;
use file_backend::FileBackend;
use mirror_intel::MirrorIntel;
use opts::{Source, Target};
use s3::S3Backend;
use simple_diff_transfer::SimpleDiffTransfer;
use structopt::StructOpt;

macro_rules! transfer {
    ($opts: expr, $source: expr, $transfer_config: expr) => {
        match $opts.target_type {
            Target::Intel => {
                let target: MirrorIntel = $opts.mirror_intel_config.into();
                let transfer = SimpleDiffTransfer::new($source, target, $transfer_config);
                transfer.transfer().await.unwrap();
            }
            Target::S3 => {
                let source = stream_pipe::ByteStreamPipe {
                    source: $source,
                    buffer_path: $opts.s3_config.s3_buffer_path.clone().unwrap(),
                };
                let target: S3Backend = $opts.s3_config.into();
                let transfer = SimpleDiffTransfer::new(source, target, $transfer_config);
                transfer.transfer().await.unwrap();
            }
            Target::File => {
                let source = stream_pipe::ByteStreamPipe {
                    source: $source,
                    buffer_path: $opts.file_config.file_buffer_path.clone().unwrap(),
                };
                let target: FileBackend = $opts.file_config.into();
                let transfer = SimpleDiffTransfer::new(source, target, $transfer_config);
                transfer.transfer().await.unwrap();
            }
        }
    };
}

fn main() {
    let opts: opts::Opts = opts::Opts::from_args();

    // create runtime
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    if let Some(worker) = opts.workers {
        runtime.worker_threads(worker);
    }
    runtime.enable_all();

    let runtime = runtime.build().unwrap();

    // parse config
    let snapshot_config = SnapshotConfig {
        concurrent_resolve: opts.concurrent_resolve,
    };
    let transfer_config = simple_diff_transfer::SimpleDiffTransferConfig {
        progress: opts.progress,
        concurrent_transfer: opts.transfer_config.concurrent_transfer,
        no_delete: opts.transfer_config.no_delete,
        print_plan: opts.transfer_config.print_plan,
        snapshot_config,
    };

    runtime.block_on(async {
        match opts.source {
            Source::Pypi(source) => {
                transfer!(opts, source, transfer_config);
            }
            Source::Homebrew(source) => {
                transfer!(opts, source, transfer_config);
            }
            Source::CratesIo(source) => {
                transfer!(opts, source, transfer_config);
            }
        }
    });
}
