use futures_util::{stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar};
use reqwest::ClientBuilder;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{SnapshotStorage, SourceStorage, TargetStorage};
use crate::utils::{create_logger, spinner};

use iter_set::{classify, Inclusion};
use rand::prelude::*;
use slog::{debug, info, o, warn};

use std::sync::Arc;
use std::time::Duration;

enum PlanType {
    Update,
    Delete,
}

#[derive(Debug)]
pub struct SimpleDiffTransferConfig {
    pub progress: bool,
    pub concurrent_transfer: usize,
    pub no_delete: bool,
    pub snapshot_config: SnapshotConfig,
    pub print_plan: usize,
}

pub struct SimpleDiffTransfer<Source, Target, Item>
where
    Source: SourceStorage<SnapshotPath, Item> + SnapshotStorage<SnapshotPath>,
    Target: TargetStorage<SnapshotPath, Item> + SnapshotStorage<SnapshotPath>,
{
    source: Source,
    target: Target,
    config: SimpleDiffTransferConfig,
    _phantom: std::marker::PhantomData<Item>,
}

impl<Source, Target, Item> SimpleDiffTransfer<Source, Target, Item>
where
    Source: SourceStorage<SnapshotPath, Item> + SnapshotStorage<SnapshotPath>,
    Target: TargetStorage<SnapshotPath, Item> + SnapshotStorage<SnapshotPath>,
{
    pub fn new(source: Source, target: Target, config: SimpleDiffTransferConfig) -> Self {
        Self {
            source,
            target,
            config,
            _phantom: std::marker::PhantomData,
        }
    }

    fn debug_snapshot(logger: slog::Logger, snapshot: &[SnapshotPath]) {
        let mut selected: Vec<_> = snapshot
            .choose_multiple(&mut rand::thread_rng(), 50)
            .collect();
        selected.sort();
        for item in selected {
            debug!(logger, "{}", item.0);
        }
    }

    pub async fn transfer(mut self) -> Result<()> {
        let logger = create_logger();
        let client = ClientBuilder::new()
            .user_agent(crate::utils::user_agent())
            .connect_timeout(Duration::from_secs(10))
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

        let handle = tokio::task::spawn_blocking(move || {
            if config_progress {
                all_progress.join().unwrap()
            }
        });

        let source_snapshot = self
            .source
            .snapshot(source_mission, &self.config.snapshot_config)
            .await?;

        let target_snapshot = self
            .target
            .snapshot(target_mission, &self.config.snapshot_config)
            .await?;

        handle.await.ok();

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

        info!(logger, "generating transfer plan...");

        let source_count = source_snapshot.len();

        let source_sort = tokio::task::spawn_blocking(move || {
            let mut source_snapshot: Vec<SnapshotPath> = source_snapshot;
            source_snapshot.sort();
            source_snapshot.dedup();
            source_snapshot
        });

        let target_count = target_snapshot.len();

        let target_sort = tokio::task::spawn_blocking(move || {
            let mut target_snapshot: Vec<SnapshotPath> = target_snapshot;
            target_snapshot.sort();
            target_snapshot.dedup();
            target_snapshot
        });

        let (source_snapshot, target_snapshot) = tokio::join!(source_sort, target_sort);

        let source_snapshot = source_snapshot
            .map_err(|err| Error::ProcessError(format!("error while sorting: {:?}", err)))?;
        let target_snapshot = target_snapshot
            .map_err(|err| Error::ProcessError(format!("error while sorting: {:?}", err)))?;

        if source_count != source_snapshot.len() {
            warn!(
                logger,
                "source: {} duplicated items",
                source_count - source_snapshot.len()
            );
        }

        if target_count != target_snapshot.len() {
            warn!(
                logger,
                "target: {} duplicated items",
                target_count - target_snapshot.len()
            );
        }

        info!(
            logger,
            "source {} objects -> target {} objects",
            source_snapshot.len(),
            target_snapshot.len()
        );

        let mut updates = vec![];
        let mut deletions = vec![];

        let mut max_info = 0;
        for result in classify(source_snapshot, target_snapshot) {
            match result {
                Inclusion::Left(source) => {
                    if max_info < self.config.print_plan {
                        info!(logger, "+ {:?}", source.0);
                        max_info += 1;
                    }
                    updates.push(source);
                }
                Inclusion::Both(_, _) => {}
                Inclusion::Right(target) => {
                    if max_info < self.config.print_plan {
                        info!(logger, "- {:?}", target.0);
                        max_info += 1;
                    }
                    deletions.push(target);
                }
            }
        }

        info!(
            logger,
            "update {} objects, delete {} objects",
            updates.len(),
            deletions.len()
        );

        info!(logger, "updating objects");

        let source = Arc::new(self.source);
        let target = Arc::new(self.target);

        progress.set_length(updates.len() as u64);
        progress.set_position(0);

        let map_snapshot = |snapshot: SnapshotPath, plan: PlanType| {
            progress.set_message(&snapshot.0);
            let source = source.clone();
            let target = target.clone();
            let source_mission = source_mission.clone();
            let target_mission = target_mission.clone();
            let logger = logger.clone();

            let func = async move {
                match plan {
                    PlanType::Update => match source.get_object(&snapshot, &source_mission).await {
                        Ok(source_object) => {
                            if let Err(err) = target
                                .put_object(&snapshot, source_object, &target_mission)
                                .await
                            {
                                warn!(
                                    target_mission.logger,
                                    "error while put {}: {:?}", snapshot.0, err
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                target_mission.logger,
                                "error while get {}: {:?}", snapshot.0, err
                            );
                        }
                    },
                    PlanType::Delete => {
                        if let Err(err) = target
                            .delete_object(&snapshot, &target_mission)
                            .timeout(Duration::from_secs(60))
                            .await
                            .into_result()
                        {
                            warn!(
                                target_mission.logger,
                                "error while delete {}: {:?}", snapshot.0, err
                            );
                        }
                    }
                }

                Ok::<(), Error>(())
            };

            async move {
                if let Err(err) = func.await {
                    warn!(logger, "failed to transfer {:?}", err);
                }
            }
        };

        let mut results = stream::iter(
            updates
                .into_iter()
                .map(|plan| map_snapshot(plan, PlanType::Update)),
        )
        .buffer_unordered(self.config.concurrent_transfer);

        while let Some(_x) = results.next().await {
            progress.inc(1);
        }

        if !self.config.no_delete {
            info!(logger, "deleting objects");

            progress.set_length(deletions.len() as u64);
            progress.set_position(0);

            let mut results = stream::iter(
                deletions
                    .into_iter()
                    .map(|plan| map_snapshot(plan, PlanType::Delete)),
            )
            .buffer_unordered(self.config.concurrent_transfer);

            while let Some(_x) = results.next().await {
                progress.inc(1);
            }
        }

        info!(logger, "transfer complete");

        Ok(())
    }
}
