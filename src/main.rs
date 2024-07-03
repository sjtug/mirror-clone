#![deny(clippy::all)]
#![allow(clippy::enum_variant_names)]

use std::path::Path;

use lazy_static::lazy_static;
use structopt::StructOpt;

use common::SnapshotConfig;
use error::Result;
use file_backend::FileBackend;
use opts::{Source, Target};
use s3::S3Backend;
use simple_diff_transfer::SimpleDiffTransfer;

use crate::github_release::GitHubRelease;
use crate::homebrew::Homebrew;

mod checksum_pipe;
mod common;
mod conda;
mod crates_io;
mod dart;
mod error;
mod file_backend;
mod filter_pipe;
mod ghcup;
mod github_release;
mod gradle;
mod homebrew;
mod html_scanner;
mod index_pipe;
#[macro_use]
mod merge_pipe;
mod lean;
mod metadata;
mod opts;
mod pypi;
mod python_version;
mod rewrite_pipe;
mod rsync;
mod rustup;
mod s3;
mod simple_diff_transfer;
mod stream_pipe;
mod timeout;
mod traits;
mod utils;

macro_rules! index_bytes_pipe {
    ($buffer_path: expr, $prefix: expr, $use_snapshot_last_modified: expr, $max_depth: expr) => {
        |source| {
            let source = stream_pipe::ByteStreamPipe::new(
                source,
                $buffer_path.clone().unwrap(),
                $use_snapshot_last_modified,
            );
            index_pipe::IndexPipe::new(
                source,
                $buffer_path.clone().unwrap(),
                $prefix.clone().unwrap(),
                $max_depth,
            )
        }
    };
}

macro_rules! index_checksum_bytes_pipe {
    ($buffer_path: expr, $prefix: expr, $use_snapshot_last_modified: expr, $max_depth: expr) => {
        |source| {
            let bytestream = stream_pipe::ByteStreamPipe::new(
                source,
                $buffer_path.clone().unwrap(),
                $use_snapshot_last_modified,
            );
            let checksum = checksum_pipe::ChecksumPipe::new(bytestream);
            index_pipe::IndexPipe::new(
                checksum,
                $buffer_path.clone().unwrap(),
                $prefix.clone().unwrap(),
                $max_depth,
            )
        }
    };
}

macro_rules! id_pipe {
    () => {
        |src| src
    };
}

macro_rules! transfer {
    ($opts: expr, $source: expr, $transfer_config: expr, $pipes: expr) => {
        match $opts.target_type {
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

lazy_static! {
    static ref HASKELL_PATTERN: regex::Regex =
        regex::Regex::new("https://downloads.haskell.org").unwrap();
}
const HLS_URL: &str = "https://github.com/haskell/haskell-language-server";
const STACK_URL: &str = "https://github.com/commercialhaskell/stack";
const HASKELL_URL: &str = "https://downloads.haskell.org";

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
        dry_run: opts.transfer_config.dry_run,
        force_all: opts.transfer_config.force_all,
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
                let pipe = |source| {
                    stream_pipe::ByteStreamPipe::new(source, buffer_path.clone().unwrap(), false)
                };
                transfer!(opts, source, transfer_config, pipe);
            }
            Source::Homebrew(config) => {
                let source = Homebrew::new(config);
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_checksum_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::CratesIo(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_checksum_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::Conda(config) => {
                let source = conda::Conda::new(config);
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_checksum_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::Rsync(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::GithubRelease(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix, true, 999)
                );
            }
            Source::DartPub(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::Gradle(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::Ghcup(source) => {
                let target_mirror = source.target_mirror.clone();

                let script_src = rewrite_pipe::RewritePipe::new(
                    stream_pipe::ByteStreamPipe::new(
                        source.get_script(),
                        buffer_path.clone().expect("buffer path is not present"),
                        false,
                    ),
                    buffer_path.clone().unwrap(),
                    utils::fn_regex_rewrite(
                        &HASKELL_PATTERN,
                        Path::new(&target_mirror)
                            .join("packages")
                            .to_str()
                            .unwrap()
                            .to_string(),
                    ),
                    999999,
                );

                let yaml_rewrite_fn = move |src: String| -> Result<String> {
                    Ok(src
                        .replace(
                            HASKELL_URL,
                            Path::new(&target_mirror).join("packages").to_str().unwrap(),
                        )
                        .replace(
                            STACK_URL,
                            Path::new(&target_mirror).join("stack").to_str().unwrap(),
                        )
                        .replace(
                            HLS_URL,
                            Path::new(&target_mirror).join("hls").to_str().unwrap(),
                        ))
                };
                let yaml_legacy_src = rewrite_pipe::RewritePipe::new(
                    stream_pipe::ByteStreamPipe::new(
                        source.get_yaml(true),
                        buffer_path.clone().unwrap(),
                        true,
                    ),
                    buffer_path.clone().unwrap(),
                    yaml_rewrite_fn,
                    999999,
                );

                let yaml_src = stream_pipe::ByteStreamPipe::new(
                    source.get_yaml(false),
                    buffer_path.clone().unwrap(),
                    true,
                );

                let packages_src = stream_pipe::ByteStreamPipe::new(
                    source.get_packages(),
                    buffer_path.clone().unwrap(),
                    false,
                );
                let stack_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("commercialhaskell/stack"),
                        source.retain_stack_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let hls_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("haskell/haskell-language-server"),
                        source.retain_hls_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );

                let unified = merge_pipe! {
                    packages: packages_src,
                    hls: hls_src,
                    stack: stack_src,
                    yaml: yaml_legacy_src,
                    yaml_v2: yaml_src,
                    script: script_src,
                };

                let indexed = index_pipe::IndexPipe::new(
                    unified,
                    buffer_path.clone().unwrap(),
                    prefix.clone().unwrap(),
                    999,
                );

                transfer!(opts, indexed, transfer_config, id_pipe!());
            }
            Source::Rustup(source) => {
                transfer!(
                    opts,
                    source,
                    transfer_config,
                    index_bytes_pipe!(buffer_path, prefix, false, 999)
                );
            }
            Source::Elan(source) => {
                let elan_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("leanprover/elan"),
                        source.retain_elan_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let glean_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("alissa-tung/glean"),
                        source.retain_glean_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let lean_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("leanprover/lean4"),
                        source.retain_lean_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let lean_nightly_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("leanprover/lean4-nightly"),
                        source.retain_lean_nightly_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let proofwidgets_src = stream_pipe::ByteStreamPipe::new(
                    GitHubRelease::new(
                        String::from("leanprover-community/ProofWidgets4"),
                        source.retain_proofwidgets_versions,
                    ),
                    buffer_path.clone().unwrap(),
                    true,
                );
                let lean_org_repo_src = merge_pipe! {
                    lean4: lean_src,
                    lean4_nightly: lean_nightly_src,
                };
                let unified = merge_pipe! {
                    elan: elan_src,
                    leanprover: lean_org_repo_src,
                    glean: glean_src,
                    proofwidgets: proofwidgets_src,
                };
                let indexed = index_pipe::IndexPipe::new(
                    unified,
                    buffer_path.clone().unwrap(),
                    prefix.clone().unwrap(),
                    999,
                );

                transfer!(opts, indexed, transfer_config, id_pipe!());
            }
        }
    });
}
