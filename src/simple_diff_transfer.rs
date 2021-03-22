//! Simple Diff Transfer
//!
//! Simple Diff Transfer simply takes snapshots of source and target,
//! compare them, and generate a transfer plan. The plan is constructed
//! as follows:
//!
//! 1. Snapshot object not in source but in target, delete
//! 2. Snapshot object not in target but in source, add
//! 3. Snapshot object in both source and target but different, update
//!
//! Then, it will concurrently transfer the objects between two endpoints.
//! The snapshot object should support `Metadata` trait, and simple diff
//! transfer will transfer them from highest priority to lowest priority.
//!
//! If transfer of an object fails, it will be simply ignored. We could
//! later implement some kind of retry logic.

use futures_util::{stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar};
use reqwest::ClientBuilder;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::timeout::{TryTimeoutExt, TryTimeoutFutureExt};
use crate::traits::{Diff, Key, Metadata, SnapshotStorage, SourceStorage, TargetStorage};
use crate::utils::{create_logger, spinner};

use iter_set::{classify_by, Inclusion};
use rand::prelude::*;
use slog::{debug, info, o, warn};

use std::sync::Arc;
use std::time::Duration;

enum PlanType {
    Update,
    Delete,
}

#[derive(Debug, Copy, Clone)]
pub struct SimpleDiffTransferConfig {
    pub progress: bool,
    pub concurrent_transfer: usize,
    pub no_delete: bool,
    pub dry_run: bool,
    pub snapshot_config: SnapshotConfig,
    pub print_plan: usize,
}

pub struct SimpleDiffTransfer<Snapshot, Source, Target, Item>
where
    Snapshot: Diff + Key + Metadata,
    Source: SourceStorage<Snapshot, Item> + SnapshotStorage<Snapshot>,
    Target: TargetStorage<Snapshot, Item> + SnapshotStorage<Snapshot>,
{
    source: Source,
    target: Target,
    config: SimpleDiffTransferConfig,
    _phantom: std::marker::PhantomData<(Item, Snapshot)>,
}

impl<Snapshot, Source, Target, Item> SimpleDiffTransfer<Snapshot, Source, Target, Item>
where
    Snapshot: Diff + Key + Metadata,
    Source: SourceStorage<Snapshot, Item> + SnapshotStorage<Snapshot>,
    Target: TargetStorage<Snapshot, Item> + SnapshotStorage<Snapshot>,
{
    pub fn new(source: Source, target: Target, config: SimpleDiffTransferConfig) -> Self {
        Self {
            source,
            target,
            config,
            _phantom: std::marker::PhantomData,
        }
    }

    fn debug_snapshot(logger: slog::Logger, snapshot: &[Snapshot]) {
        let mut selected: Vec<_> = snapshot
            .choose_multiple(&mut rand::thread_rng(), 50)
            .collect();
        selected.sort_by(|a, b| a.key().cmp(b.key()));
        for item in selected {
            debug!(logger, "{}", item.key());
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
            let mut source_snapshot: Vec<Snapshot> = source_snapshot;
            source_snapshot.sort_by(|a, b| a.key().cmp(b.key()));
            source_snapshot.dedup_by(|a, b| a.key().eq(b.key()));
            source_snapshot
        });

        let target_count = target_snapshot.len();

        let target_sort = tokio::task::spawn_blocking(move || {
            let mut target_snapshot: Vec<Snapshot> = target_snapshot;
            target_snapshot.sort_by(|a, b| a.key().cmp(b.key()));
            target_snapshot.dedup_by(|a, b| a.key().eq(b.key()));
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
        for result in classify_by(source_snapshot, target_snapshot, |a, b| {
            a.key().cmp(b.key())
        }) {
            match result {
                Inclusion::Left(source) => {
                    if max_info < self.config.print_plan {
                        info!(logger, "+ {:?}", source.key());
                        max_info += 1;
                    }
                    updates.push(source);
                }
                Inclusion::Both(l, r) => {
                    if l.diff(&r) {
                        if max_info < self.config.print_plan {
                            info!(logger, "= {:?}", l.key());
                            max_info += 1;
                        }
                        updates.push(l);
                    }
                }
                Inclusion::Right(target) => {
                    if max_info < self.config.print_plan {
                        info!(logger, "- {:?}", target.key());
                        max_info += 1;
                    }
                    deletions.push(target);
                }
            }
        }

        // sort plan by priority
        updates.sort_by_key(|snapshot| -snapshot.priority());
        deletions.sort_by_key(|snapshot| -snapshot.priority());

        info!(
            logger,
            "update {} objects, delete {} objects",
            updates.len(),
            deletions.len()
        );

        if self.config.dry_run {
            return Ok(());
        }

        info!(logger, "updating objects");

        let source = Arc::new(self.source);
        let target = Arc::new(self.target);

        progress.set_length(updates.len() as u64);
        progress.set_position(0);

        let map_snapshot = |snapshot: Snapshot, plan: PlanType| {
            progress.set_message(&snapshot.key());
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
                                    "error while put {}: {:?}",
                                    snapshot.key(),
                                    err
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                target_mission.logger,
                                "error while get {}: {:?}",
                                snapshot.key(),
                                err
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
                                "error while delete {}: {:?}",
                                snapshot.key(),
                                err
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
