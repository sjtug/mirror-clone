mod common;
mod error;
mod mirror_intel;
mod pypi;
mod simple_diff_transfer;
mod traits;
mod utils;

#[tokio::main]
async fn main() {
    let source = pypi::Pypi::new();
    let target = mirror_intel::MirrorIntel::new(
        "https://siyuan.internal.sjtug.org/pypi-packages".to_string(),
    );
    let mut transfer = simple_diff_transfer::SimpleDiffTransfer::new(source, target);
    transfer.transfer().await.unwrap();
}
