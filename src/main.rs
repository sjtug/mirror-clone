mod conda;
mod error;
mod opam;
mod tar;
mod utils;

use slog::{o, Drain, Level, LevelFilter};
use slog_scope_futures::FutureExt;
use std::path::PathBuf;

use crate::error::Result;

fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build();
    let drain = LevelFilter::new(drain, Level::Debug).fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() -> Result<()> {
    // let task = opam::Opam {
    //     base_path: PathBuf::from("/srv/data/opam"),
    //     repo: "http://localhost".to_string(),
    //     archive_url: "https://mirrors.sjtug.sjtu.edu.cn/opam-cache".to_string(),
    //     debug_mode: false,
    //     concurrent_downloads: 16,
    // };

    let task = conda::Conda {
        base_path: PathBuf::from("/srv/data/conda/pkgs/main/win-64"),
        repo: "https://mirrors.sjtug.sjtu.edu.cn/anaconda/pkgs/main/win-64".to_string(),
        debug_mode: false,
        concurrent_downloads: 16,
    };

    let _guard = slog_scope::set_global_logger(create_logger());
    task.run()
        .with_logger(
            &slog_scope::logger().new(o!("task" => "conda", "repo" => "anaconda/pkgs/main/win-64")),
        )
        .await?;
    Ok(())
}
