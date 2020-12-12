use indicatif::{MultiProgress, ProgressBar};
use reqwest::ClientBuilder;

use crate::error::{Error, Result};
use crate::utils::{create_logger, spinner};
use crate::{
    common::Mission,
    traits::{SnapshotStorage, SourceStorage, TargetStorage},
};
use rand::prelude::*;

use futures_util::StreamExt;
use slog::{debug, info, o, warn};
use std::sync::Arc;

#[derive(Debug)]
pub struct SimpleDiffTransferConfig {
    pub progress: bool,
}

pub struct SimpleDiffTransfer<Source, Target>
where
    Source: SourceStorage<String, String> + SnapshotStorage<String>,
    Target: TargetStorage<String> + SnapshotStorage<String>,
{
    source: Source,
    target: Target,
    config: SimpleDiffTransferConfig,
}

impl<Source, Target> SimpleDiffTransfer<Source, Target>
where
    Source: SourceStorage<String, String> + SnapshotStorage<String>,
    Target: TargetStorage<String> + SnapshotStorage<String>,
{
    pub fn new(source: Source, target: Target, config: SimpleDiffTransferConfig) -> Self {
        Self {
            source,
            target,
            config,
        }
    }

    fn debug_snapshot(logger: slog::Logger, snapshot: &[String]) {
        let selected: Vec<_> = snapshot
            .choose_multiple(&mut rand::thread_rng(), 50)
            .collect();
        for item in selected {
            debug!(logger, "{}", item);
        }
    }

    pub async fn transfer(mut self) -> Result<()> {
        let logger = create_logger();
        let client = ClientBuilder::new()
            .user_agent("mirror-clone / 0.1 (siyuan.internal.sjtug.org)")
            .build()?;
        info!(logger, "using simple diff transfer"; "config" => format!("{:?}", self.config));
        info!(logger, "begin transfer"; "source" => self.source.info(), "target" => self.target.info());

        info!(logger, "taking snapshot...");

        let all_progress = MultiProgress::new();
        let source_progress = all_progress.add(ProgressBar::new(0));
        source_progress.set_style(spinner());
        source_progress.set_prefix("[source]");
        let target_progress = all_progress.add(ProgressBar::new(0));
        target_progress.set_style(spinner());
        target_progress.set_prefix("[target]");

        let source_mission = Mission {
            client: client.clone(),
            progress: source_progress,
            logger: logger.new(o!("task" => "snapshot.source")),
        };

        let target_mission = Mission {
            client: client.clone(),
            progress: target_progress,
            logger: logger.new(o!("task" => "snapshot.target")),
        };

        let config_progress = self.config.progress;
        let (source_snapshot, target_snapshot, _) = tokio::join!(
            self.source.snapshot(source_mission),
            self.target.snapshot(target_mission),
            tokio::task::spawn_blocking(move || {
                if config_progress {
                    all_progress.join().unwrap()
                }
            })
        );

        let source_snapshot = source_snapshot?;
        let target_snapshot = target_snapshot?;

        info!(
            logger,
            "source {} objects, target {} objects",
            source_snapshot.len(),
            target_snapshot.len()
        );

        Self::debug_snapshot(logger.clone(), &source_snapshot);
        Self::debug_snapshot(logger.clone(), &target_snapshot);

        info!(logger, "mirror in progress...");

        let progress = if self.config.progress {
            ProgressBar::new(source_snapshot.len() as u64)
        } else {
            ProgressBar::hidden()
        };
        progress.set_style(crate::utils::bar());
        progress.set_prefix("mirror");

        let source_mission = Arc::new(Mission {
            client: client.clone(),
            progress: ProgressBar::hidden(),
            logger: logger.new(o!("task" => "mirror.source")),
        });

        let target_mission = Arc::new(Mission {
            client: client.clone(),
            progress: ProgressBar::hidden(),
            logger: logger.new(o!("task" => "mirror.target")),
        });

        // TODO: do diff between two endpoints

        let source = Arc::new(self.source);
        let target = Arc::new(self.target);

        let map_snapshot = |source_snapshot: String| {
            progress.set_message(&source_snapshot);
            let source = source.clone();
            let target = target.clone();
            let source_mission = source_mission.clone();
            let target_mission = target_mission.clone();
            let logger = logger.clone();

            let func = async move {
                let source_object = source.get_object(source_snapshot, &source_mission).await?;
                if let Err(err) = target.put_object(source_object, &target_mission).await {
                    warn!(target_mission.logger, "error while transfer: {:?}", err);
                }
                Ok::<(), Error>(())
            };
            async move {
                if let Err(err) = func.await {
                    warn!(logger, "failed to fetch index {:?}", err);
                }
            }
        };

        let mut results = futures::stream::iter(source_snapshot.into_iter().map(map_snapshot))
            .buffer_unordered(128);

        while let Some(_x) = results.next().await {
            progress.inc(1);
        }

        info!(logger, "transfer complete");

        Ok(())
    }
}
