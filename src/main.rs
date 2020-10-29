mod error;
mod opam;
mod tar;
mod utils;

use async_log::span;
use std::path::PathBuf;

fn setup_logger() {
    let logger = femme::pretty::Logger::new();
    async_log::Logger::wrap(logger, || 12)
        .start(log::LevelFilter::Info)
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logger();

    let task = opam::Opam {
        base_path: PathBuf::from("/srv/data/opam"),
        repo: "http://localhost".to_string(),
        archive_url: "https://mirrors.sjtug.sjtu.edu.cn/opam-cache".to_string(),
        debug_mode: false,
        concurrent_downloads: 16,
    };
    span!("opam", {
        task.run().await?;
    });
    Ok(())
}
