mod common;
mod error;
mod mirror_intel;
mod pypi;
mod simple_diff_transfer;
mod traits;
mod utils;

#[tokio::main]
async fn main() {
    let source = pypi::Pypi {
        simple_base: "https://mirrors.bfsu.edu.cn/pypi/web/simple".to_string(),
        package_base: "https://mirrors.bfsu.edu.cn/pypi/web/packages".to_string(),
        debug: false,
    };
    let target = mirror_intel::MirrorIntel::new(
        "https://siyuan.internal.sjtug.org/pypi-packages".to_string(),
    );
    let mut transfer = simple_diff_transfer::SimpleDiffTransfer::new(source, target);
    transfer.transfer().await.unwrap();
}
