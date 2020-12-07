#![feature(str_split_once)]

mod common;
mod error;
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
        (@subcommand rustup =>
            (about: "mirror rustup from static.rust-lang.org to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg base: --base +takes_value default_value("https://static.rust-lang.org") "package base")
            (@arg days_to_retain: --days_to_retain +takes_value default_value("30") "days to retain")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/rust-static") "mirror-intel target")
        )
        (@subcommand homebrew_bottles =>
            (about: "mirror homebrew_bottles from tuna to siyuan mirror-intel with simple diff transfer")
            (version: "1.0")
            (author: "Alex Chi <iskyzh@gmail.com>")
            (@arg base: --base +takes_value default_value("rsync://nanomirrors.tuna.tsinghua.edu.cn/homebrew-bottles") "package base")
            (@arg target: --target +takes_value default_value("https://siyuan.internal.sjtug.org/homebrew-bottles") "mirror-intel target")
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
            let source = rsync::Rsync {
                base: sub_matches.value_of("base").unwrap().to_string(),
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
