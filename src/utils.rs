use crate::common::SnapshotPath;
use indicatif::ProgressStyle;
use slog::{o, Drain};

pub fn create_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    #[cfg(not(debug_assertions))]
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    slog::Logger::root(drain, o!())
}

pub fn spinner() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{prefix:.bold.dim} {spinner} {msg}")
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
}

pub fn bar() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "{prefix:.bold.dim} [{elapsed_precise}] [{bar:40}] [{eta_precise}] ({pos}/{len}) {msg}",
        )
        .progress_chars("=> ")
}

pub fn snapshot_string_to_path(snapshot: Vec<String>) -> Vec<SnapshotPath> {
    snapshot.into_iter().map(|x| SnapshotPath(x)).collect()
}

pub fn user_agent() -> String {
    format!(
        "mirror-clone / {} ({})",
        env!("CARGO_PKG_VERSION"),
        std::env::var("MIRROR_CLONE_SITE").expect("No MIRROR_CLONE_SITE env variable")
    )
}

pub fn generate_s3_url_encode_map() -> Vec<(&'static str, &'static str)> {
    // reference: https://github.com/GeorgePhillips/node-s3-url-encode/blob/master/index.js
    let mut map = vec![];
    map.push(("+", "%2B"));
    map.push(("!", "%21"));
    map.push(("\"", "%22"));
    map.push(("#", "%23"));
    map.push(("$", "%24"));
    map.push(("&", "%26"));
    map.push(("'", "%27"));
    map.push(("(", "%28"));
    map.push((")", "%29"));
    map.push(("*", "%2A"));
    map.push((",", "%2C"));
    map.push((":", "%3A"));
    map.push((";", "%3B"));
    map.push(("=", "%3D"));
    map.push(("?", "%3F"));
    map.push(("@", "%40"));
    map
}

pub fn generate_s3_url_reverse_encode_map() -> Vec<(&'static str, &'static str)> {
    generate_s3_url_encode_map()
        .into_iter()
        .map(|(x, y)| (y, x))
        .collect()
}

pub fn rewrite_url_string(url_encode_map: &[(&'static str, &'static str)], key: &str) -> String {
    let mut key = key.to_string();

    for (ch, seq) in url_encode_map {
        key = key.replace(ch, seq);
    }

    key
}

pub fn rewrite_snapshot(target_snapshot: &mut [SnapshotPath]) {
    let gen_map = generate_s3_url_encode_map();
    for path in target_snapshot {
        path.0 = rewrite_url_string(&gen_map, &path.0);
    }
}
