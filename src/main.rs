#![deny(clippy::all)]

mod common;
mod conda;
mod crates_io;
mod dart;
mod error;
mod file_backend;
mod github_release;
mod gradle;
mod homebrew;
mod html_scanner;
mod index_pipe;
mod metadata;
mod mirror_intel;
mod opts;
mod pypi;
mod rewrite_pipe;
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

macro_rules! index_bytes_pipe {
    ($buffer_path: expr, $prefix: expr) => {
        |source| {
            let source = stream_pipe::ByteStreamPipe::new(source, $buffer_path.clone().unwrap());
            index_pipe::IndexPipe::new(
                source,
                $buffer_path.clone().unwrap(),
                $prefix.clone().unwrap(),
            )
        }
    };
}

macro_rules! transfer {
    ($opts: expr, $source: expr, $transfer_config: expr, $pipes: expr) => {
        match $opts.target_type {
            Target::Intel => {
                let target: MirrorIntel = $opts.mirror_intel_config.into();
                let transfer = SimpleDiffTransfer::new($source, target, $transfer_config);
                transfer.transfer().await.unwrap();
            }
            Target::S3 => {
                let target: S3Backend = $opts.s3_config.clone().into();
                let pipes = $pipes;
                let source = pipes($source);
                let transfer = SimpleDiffTransfer::new(source, target, $transfer_config);
                transfer.transfer().await.unwrap();
            }
            Target::File => {
                let target: FileBackend = $opts.file_config.clone().into();
                let pipes = $pipes;
                let source = pipes($source);
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
        let buffer_path = opts
            .s3_config
            .s3_buffer_path
            .clone()
            .or_else(|| opts.file_config.file_buffer_path.clone());
        let prefix = opts
            .s3_config
            .s3_prefix
            .clone()
            .or_else(|| Some(String::from("Root")));
        match opts.source {
            Source::Pypi(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix)
                );
            }
            Source::Homebrew(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix)
                );
            }
            Source::CratesIo(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix)
                );
            }
            Source::Conda(config) => {
                let source = conda::Conda::new(config);
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix)
                );
            }
            Source::Rsync(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix)
                );
            }
        }
    });
}
