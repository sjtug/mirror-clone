mod conda;
mod error;
mod opam;
mod tar;
mod utils;

use clap::{App, Arg, SubCommand};
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
    let matches = App::new("Mirror Clone")
        .version("1.0")
        .author("Alex Chi <iskyzh@gmail.com>")
        .about("An all-in-one mirror utility by SJTUG")
        .arg(
            Arg::with_name("log_level")
                .help("set log level")
                .default_value("info"),
        )
        .arg(
            Arg::with_name("debug")
                .takes_value(false)
                .help("enable debug mode"),
        )
        .subcommand(
            SubCommand::with_name("opam")
                .about("mirror OPAM repository")
                .version("1.0")
                .author("Alex Chi <iskyzh@gmail.com>")
                .arg(
                    Arg::with_name("repo")
                        .required(true)
                        .help("OPAM repository"),
                )
                .arg(Arg::with_name("dir").required(true).help("clone directory"))
                .arg(
                    Arg::with_name("archive")
                        .required(false)
                        .help("OPAM archive URL"),
                ),
        )
        .subcommand(
            SubCommand::with_name("conda")
                .about("mirror Conda repository")
                .version("1.0")
                .author("Alex Chi <iskyzh@gmail.com>")
                .arg(
                    Arg::with_name("repo")
                        .required(true)
                        .help("Conda repository"),
                )
                .arg(Arg::with_name("dir").required(true).help("clone directory")),
        )
        .get_matches();

    let _guard =
        slog_scope::set_global_logger(create_logger(matches.value_of("log_level").unwrap()));

    match matches.subcommand() {
        ("opam", Some(opam_matches)) => {
            opam::Opam {
                base_path: PathBuf::from(opam_matches.value_of("dir").unwrap()),
                repo: opam_matches.value_of("repo").unwrap().to_string(),
                archive_url: opam_matches.value_of("archive").map(|x| x.to_string()),
                debug_mode: matches.is_present("debug"),
                concurrent_downloads: 16,
            }
            .run()
            .with_logger(&slog_scope::logger().new(o!("task" => "opam")))
            .await?;
        }
        ("conda", Some(conda_matches)) => {
            conda::Conda {
                base_path: PathBuf::from(PathBuf::from(conda_matches.value_of("dir").unwrap())),
                repo: conda_matches.value_of("repo").unwrap().to_string(),
                debug_mode: matches.is_present("debug"),
                concurrent_downloads: 16,
            }
            .run()
            .with_logger(&slog_scope::logger().new(o!("task" => "conda")))
            .await?;
        }
        _ => {}
    }

    Ok(())
}
