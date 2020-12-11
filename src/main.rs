#![feature(str_split_once)]

mod common;
mod crates_io;
mod error;
mod homebrew;
mod html_scanner;
mod mirror_intel;
mod pypi;
mod rsync;
mod rustup;
mod simple_diff_transfer;
mod traits;
mod utils;

use clap::clap_app;

#[tokio::main]
async fn main() {
    let matches = clap_app!(mirror_clone =>
        (version: "1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
        (about: "An all-in-one mirror utility by SJTUG")
        (@arg progress: --progress ... "enable progress bar")
        (@arg debug: --debug ... "enable debug mode")
        (@subcommand pypi =>
            (about: "mirror pypi from tuna to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg simple_base: --simple_base +takes_value default_value("https://nanomirrors.tuna.tsinghua.edu.cn/pypi/web/simple") "simple base")
            (@arg package_base: --package_base +takes_value default_value("https://nanomirrors.tuna.tsinghua.edu.cn/pypi/web/packages") "package base")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/pypi-packages") "mirror-intel target")
        )
        (@subcommand pytorch_wheels =>
            (about: "mirror pytorch stable from download.pytorch.org to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg package_index: --simple_base +takes_value default_value("https://download.pytorch.org/whl/torch_stable.html") "package index")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/pytorch-wheels") "mirror-intel target")
        )
        (@subcommand rustup =>
            (about: "mirror rustup from static.rust-lang.org to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg base: --base +takes_value default_value("https://static.rust-lang.org") "package base")
            (@arg days_to_retain: --days_to_retain +takes_value default_value("30") "days to retain")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/rust-static") "mirror-intel target")
        )
        (@subcommand homebrew_bottles =>
            (about: "mirror homebrew_bottles from brew.sh to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg api_base: --api_base +takes_value default_value("https://formulae.brew.sh/api/formula.json") "formula API")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/homebrew-bottles") "mirror-intel target")
        )
        (@subcommand dart_pub =>
            (about: "mirror dart_pub from tuna to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg base: --base +takes_value default_value("rsync://nanomirrors.tuna.tsinghua.edu.cn/dart-pub") "package base")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/dart-pub") "mirror-intel target")
        )
        (@subcommand crates_io =>
            (about: "mirror crates.io from GitHub to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg zip_master: --zip_master +takes_value default_value("https://github.com/rust-lang/crates.io-index/archive/master.zip") "zip of crates.io-index master")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/crates.io/crates") "mirror-intel target")
        )
    )
    .get_matches();

    let progress = matches.is_present("progress");

    match matches.subcommand() {
        ("pypi", Some(sub_matches)) => {
            let source = pypi::Pypi {
                simple_base: sub_matches.value_of("simple_base").unwrap().to_string(),
                package_base: sub_matches.value_of("package_base").unwrap().to_string(),
                debug: matches.is_present("debug"),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        ("rustup", Some(sub_matches)) => {
            let source = rustup::Rustup {
                base: sub_matches.value_of("base").unwrap().to_string(),
                days_to_retain: sub_matches
                    .value_of("days_to_retain")
                    .unwrap()
                    .parse()
                    .unwrap(),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        ("homebrew_bottles", Some(sub_matches)) => {
            let source = homebrew::Homebrew {
                api_base: sub_matches.value_of("api_base").unwrap().to_string(),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        ("dart_pub", Some(sub_matches)) => {
            let source = rsync::Rsync {
                base: sub_matches.value_of("base").unwrap().to_string(),
                debug: matches.is_present("debug"),
                ignore_prefix: "api".to_string(),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        ("pytorch_wheels", Some(sub_matches)) => {
            let source = html_scanner::HtmlScanner {
                url: sub_matches.value_of("package_index").unwrap().to_string(),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        ("crates_io", Some(sub_matches)) => {
            let source = crates_io::CratesIo {
                zip_master: sub_matches.value_of("zip_master").unwrap().to_string(),
                debug: matches.is_present("debug"),
            };
            let target =
                mirror_intel::MirrorIntel::new(sub_matches.value_of("target").unwrap().to_string());
            let transfer = simple_diff_transfer::SimpleDiffTransfer::new(
                source,
                target,
                simple_diff_transfer::SimpleDiffTransferConfig { progress },
            );
            transfer.transfer().await.unwrap();
        }
        _ => {
            println!("use ./mirror_clone --help to view commands");
        }
    }
}
