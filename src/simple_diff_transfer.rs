use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{header, Client, ClientBuilder};

use crate::error::Result;
use crate::utils::{create_logger, spinner};
use crate::{
    common::Mission,
    traits::{SnapshotStorage, SourceStorage, TargetStorage},
};

use slog::{info, o, warn, Drain};

use console::style;

pub struct SimpleDiffTransfer<Source, Target>
where
    Source: SourceStorage<String, String> + SnapshotStorage<String>,
    Target: TargetStorage<String> + SnapshotStorage<String>,
{
    source: Source,
    target: Target,
}

impl<Source, Target> SimpleDiffTransfer<Source, Target>
where
    Source: SourceStorage<String, String> + SnapshotStorage<String>,
    Target: TargetStorage<String> + SnapshotStorage<String>,
{
    pub fn new(source: Source, target: Target) -> Self {
        Self { source, target }
    }

    pub async fn transfer(&mut self) -> Result<()> {
        let logger = create_logger();
        let client = ClientBuilder::new()
            .user_agent("mirror-clone / 0.1 (siyuan.internal.sjtug.org)")
            .build()?;
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

        let (source, target, _) = tokio::join!(
            self.source.snapshot(source_mission),
            self.target.snapshot(target_mission),
            tokio::task::spawn_blocking(move || {
                #[cfg(debug_assertions)]
                all_progress.join().unwrap()
            })
        );

        let source = source?;
        let target = target?;

        info!(
            logger,
            "source {} objects, target {} objects",
            source.len(),
            target.len()
        );

        info!(logger, "mirror in progress...");

        for source_snapshot in source {
            let source_object = self.source.get_object(source_snapshot).await?;
            self.target.put_object(source_object).await?;
        }

        info!(logger, "transfer complete");

        Ok(())
    }
}
