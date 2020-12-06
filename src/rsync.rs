use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::bar;
use crate::{common::Mission, error::Error};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use regex::Regex;
use slog::{info, warn};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use std::process::Stdio;

#[derive(Debug)]
pub struct Rsync {
    pub base: String,
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
impl SnapshotStorage<String> for Rsync {
    async fn snapshot(&mut self, mission: Mission) -> Result<Vec<String>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        info!(logger, "running rsync...");

        let mut cmd = Command::new("rsync");
        cmd.arg("-r").arg(self.base.clone());
        cmd.stdout(Stdio::piped());

        let mut child = cmd.spawn().expect("failed to spawn command");

        let stdout = child
            .stdout
            .take()
            .expect("child did not have a handle to stdout");

        let mut reader = BufReader::new(stdout).lines();

        let result = tokio::spawn(async {
            child.await.map_err(|err| {
                Error::ProcessError(format!("child process encountered an error: {:?}", err))
            })?;
            Ok::<(), Error>(())
        });

        let mut snapshot = vec![];

        while let Some(line) = reader.next_line().await? {
            progress.inc(1);

            if let Ok((permission, size, date, time, file)) = parse_rsync_output(&line) {
                progress.set_message(file);
                if permission.starts_with("-rw") {
                    // only clone files
                    snapshot.push(file.to_string());
                }
            }
        }

        result.await.unwrap()?;

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("rsync, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<String, String> for Rsync {
    async fn get_object(&self, snapshot: String, _mission: &Mission) -> Result<String> {
        Ok(snapshot)
    }
}
