//! Rsync source
//!
//! Rsync endpoint helps synchronize files on rsync daemon to other targets.
//! This is done by first running rsync program to get a file list, then
//! downlaod them over HTTP. Currently, symbolic links are not supported.
//!
//! Rsync snapshot provides a snapshot with metadata, which includes path, size,
//! and file modified time.
//!
//! Note that we do not ensure consistency between Rsync snapshot and HTTP downloads.
//! Some servers serve different files under Rsync and HTTP. For example, mirrors.tuna
//! has two servers, and HTTP contents may be not exactly the same as rsync. Users
//! must ensure what's served on HTTP is really what's inside rsync.

use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Error;
use crate::metadata::SnapshotMeta;

use async_trait::async_trait;
use chrono::TimeZone;
use slog::{info, warn};
use std::process::Stdio;
use structopt::StructOpt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone, StructOpt)]
pub struct Rsync {
    /// Rsync endpoint
    #[structopt(long, help = "Base of Rsync")]
    pub rsync_base: String,
    /// Corresponding HTTP endpoint
    #[structopt(long, help = "Base of HTTP")]
    pub http_base: String,
    /// When debug mode is enabled, we only scan first 1000 objects. Be sure
    /// to add `--no-delete` parameter to simple diff transfer when this is enabled.
    #[structopt(long, help = "Debug mode")]
    pub debug: bool,
    /// Prefix to ignore. If this is an empty string, all objects are transferred.
    #[structopt(long, help = "Prefix to ignore", default_value = "")]
    pub ignore_prefix: String,
}

fn parse_rsync_output(line: &str) -> Result<(&str, &str, &str, &str, &str)> {
    let (permission, rest) = line.split_once(' ').ok_or(Error::NoneError)?;
    let rest = rest.trim_start();
    let (size, rest) = rest.split_once(' ').ok_or(Error::NoneError)?;
    let rest = rest.trim_start();
    let (date, rest) = rest.split_once(' ').ok_or(Error::NoneError)?;
    let rest = rest.trim_start();
    let (time, file) = rest.split_once(' ').ok_or(Error::NoneError)?;
    Ok((permission, size, date, time, file))
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for Rsync {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let _client = mission.client;

        info!(logger, "running rsync...");

        let mut cmd = Command::new("rsync");
        cmd.kill_on_drop(true);
        cmd.arg("-r").arg(self.rsync_base.clone()).arg("--no-motd");
        cmd.stdout(Stdio::piped());

        let mut child = cmd.spawn().expect("failed to spawn command");

        let stdout = child
            .stdout
            .take()
            .expect("child did not have a handle to stdout");

        let mut reader = BufReader::new(stdout).lines();

        let result = tokio::spawn(async move {
            let status = child.wait().await.map_err(|err| {
                Error::ProcessError(format!("child process encountered an error: {:?}", err))
            })?;
            Ok::<_, Error>(status)
        });

        let mut snapshot = vec![];
        let mut idx: usize = 0;

        let timezone = chrono::Local::now().timezone();

        while let Some(line) = reader.next_line().await? {
            progress.inc(1);
            idx += 1;
            if self.debug && idx > 1000 {
                continue;
            }

            if let Ok((permission, size, date, time, file)) = parse_rsync_output(&line) {
                progress.set_message(file);
                if !self.ignore_prefix.is_empty() && file.starts_with(&self.ignore_prefix) {
                    continue;
                }
                if permission.starts_with("-r") {
                    let datetime = timezone
                        .datetime_from_str(&format!("{} {}", date, time), "%Y/%m/%d %H:%M:%S")?;
                    let size = size.replace(",", "");
                    let meta = SnapshotMeta {
                        key: file.to_string(),
                        size: Some(size.parse().unwrap()),
                        last_modified: Some(datetime.timestamp() as u64),
                        ..Default::default()
                    };
                    snapshot.push(meta);
                }
                if permission.starts_with("l") {
                    info!(logger, "symbolic link is not supported: {}", file);
                }
            }
        }

        progress.set_message("waiting for rsync to exit");

        let status = result.await.unwrap()?;
        if !status.success() {
            return Err(Error::ProcessError(format!("exit code: {:?}", status)));
        }

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("rsync, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for Rsync {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!("{}/{}", self.http_base, snapshot.key)))
    }
}
