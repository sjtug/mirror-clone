#![feature(box_patterns)]
mod conda;
mod crates_io;
mod error;
mod opam;
mod oracle;
mod tar;
mod utils;

use clap::clap_app;
use slog::{o, Drain, Level, LevelFilter};
use slog_scope_futures::FutureExt;
use std::path::PathBuf;

use crate::error::Result;

fn create_logger(level: &str) -> slog::Logger {
    // TODO: warn when using debug while compiled with release
    let level = match level {
        "info" => Level::Info,
        "trace" => Level::Trace,
        "warning" => Level::Warning,
        "debug" => Level::Debug,
        _ => panic!("unsupported log level"),
    };
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(10240).build();
    let drain = LevelFilter::new(drain, level).fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = clap_app!(mirror_clone =>
        (version: "1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
        (about: "An all-in-one mirror utility by SJTUG")
        (@arg progress: --progress ... "enable progress bar")
        (@arg debug: --debug ... "enable debug mode")
        (@arg loglevel: --log +takes_value default_value("info") "set log level")
        (@subcommand opam =>
            (about: "mirror OPAM repository")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg repo: +required "OPAM repository")
            (@arg dir: +required "clone directory")
            (@arg archive: --archive +takes_value "OPAM archive URL")
        )
        (@subcommand conda =>
            (about: "mirror Conda repository")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg repo: +required "Conda repository")
            (@arg dir: +required "clone directory")
        )
        (@subcommand crates_io =>
            (about: "mirror crates.io repository")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg repo: +required "crates.io index path")
            (@arg dir: +required "clone directory")
            (@arg src: +required "crates.io URL")
        )
    )
    .get_matches();

    let _guard =
        slog_scope::set_global_logger(create_logger(matches.value_of("loglevel").unwrap()));

    let oracle = oracle::Oracle {
        client: reqwest::Client::new(),
        progress: if matches.is_present("progress") {
            indicatif::ProgressBar::new(1)
        } else {
            indicatif::ProgressBar::hidden()
        },
    };

    match matches.subcommand() {
        ("opam", Some(opam_matches)) => {
            let repo = opam_matches.value_of("repo").unwrap().to_string();
            opam::Opam {
                base_path: PathBuf::from(opam_matches.value_of("dir").unwrap()),
                repo: repo.clone(),
                archive_url: opam_matches.value_of("archive").map(|x| x.to_string()),
                debug_mode: matches.is_present("debug"),
                concurrent_downloads: 16,
            }
            .run(oracle)
            .with_logger(&slog_scope::logger().new(o!("task" => "opam", "repo" => repo)))
            .await?;
        }
        ("conda", Some(conda_matches)) => {
            let repo = conda_matches.value_of("repo").unwrap().to_string();
            conda::Conda {
                base_path: PathBuf::from(conda_matches.value_of("dir").unwrap()),
                repo: repo.clone(),
                debug_mode: matches.is_present("debug"),
                concurrent_downloads: 16,
            }
            .run(oracle)
            .with_logger(&slog_scope::logger().new(o!("task" => "conda", "repo" => repo)))
            .await?;
        }
        ("crates_io", Some(crates_io_matches)) => {
            crates_io::CratesIo {
                base_path: PathBuf::from(crates_io_matches.value_of("dir").unwrap()),
                repo_path: PathBuf::from(crates_io_matches.value_of("repo").unwrap()),
                crates_io_url: crates_io_matches.value_of("src").unwrap().to_string(),
                debug_mode: matches.is_present("debug"),
                concurrent_downloads: 16,
            }
            .run(oracle)
            .with_logger(&slog_scope::logger().new(o!("task" => "crates.io")))
            .await?;
        }
        _ => {}
    }

    Ok(())
}
