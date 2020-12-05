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
