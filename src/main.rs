#![feature(str_split_once)]

mod common;
mod error;
mod mirror_intel;
mod pypi;
mod rsync;
mod simple_diff_transfer;
mod traits;
mod utils;

#[tokio::main]
async fn main() {
    // let source = pypi::Pypi {
    //     simple_base: "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple".to_string(),
    //     package_base: "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/packages".to_string(),
    //     debug: false,
    // };
    let source = rsync::Rsync {
        base: "rsync://nanomirrors.tuna.tsinghua.edu.cn/homebrew-bottles".to_string(),
        debug: false,
    };
    // let source = rsync::Rsync {
    //     base: "rsync://nanomirrors.tuna.tsinghua.edu.cn/llvm-apt".to_string(),
    //     debug: true,
    // };
    let target = mirror_intel::MirrorIntel::new(
        "https://siyuan.internal.sjtug.org/homebrew-bottles".to_string(),
    );
    // let target =
    //     mirror_intel::MirrorIntel::new("https://siyuan.internal.sjtug.org/llvm-apt".to_string());
    let transfer = simple_diff_transfer::SimpleDiffTransfer::new(source, target);
    transfer.transfer().await.unwrap();
}
