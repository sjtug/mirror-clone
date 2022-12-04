//! MergePipe merge several sources into one.
//!
//! Sometimes it may not be sufficient to mirror the whole repository from
//! a single site (e.g. ghcup).
//! In such case, several different sources of distinct base urls can be
//! implemented, and they should be unified by `MergePipe` later.
//!
//! # Example
//! ```ignore
//! merge_pipe! {
//!     metadata: MetaSource,
//!     pkg: PackageSource,
//! }

use async_trait::async_trait;
use slog::info;

use crate::common::{Mission, SnapshotConfig};
use crate::error::{Error, Result};
use crate::traits::{Key, SnapshotStorage, SourceStorage};

/// Generate MergePipe from a list of sources.
macro_rules! merge_pipe {
    ($name:ident: $source:expr, $($tt: tt)+) => {
        crate::merge_pipe::MergePipe::new(stringify!($name), $source, merge_pipe!($($tt)+))
    };
    ($name:ident: $source:expr $(,)?) => {
        crate::merge_pipe::MergePipe::new(stringify!($name), $source, crate::merge_pipe::NilPipe)
    };
}

pub struct MergePipe<Source1, Source2> {
    prefix: String,
    s1: Source1,
    s2: Source2,
}

impl<Source1, Source2> MergePipe<Source1, Source2> {
    pub fn new(prefix: &str, s1: Source1, s2: Source2) -> Self {
        let prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };
        Self { prefix, s1, s2 }
    }
}

#[async_trait]
impl<Source1, Source2, SnapshotItem> SnapshotStorage<SnapshotItem> for MergePipe<Source1, Source2>
where
    SnapshotItem: Key + Clone,
    Source1: SnapshotStorage<SnapshotItem> + Send + 'static,
    Source2: SnapshotStorage<SnapshotItem> + Send + 'static,
{
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotItem>> {
        let logger = mission.logger.clone();

        info!(logger, "merge_pipe: snapshotting {}", self.prefix);
        let mut snapshot1 = self.s1.snapshot(mission.clone(), config).await?;
        snapshot1.iter_mut().for_each(|item| {
            *item.key_mut() = format!("{}{}", self.prefix, item.key());
        });

        let mut snapshot2 = self.s2.snapshot(mission.clone(), config).await?;

        snapshot1.append(&mut snapshot2);

        Ok(snapshot1)
    }

    fn info(&self) -> String {
        format!("MergePipe (<{}>, <{}>)", self.s1.info(), self.s2.info())
    }
}

#[async_trait]
impl<Source1, Source2, Source, SnapshotItem> SourceStorage<SnapshotItem, Source>
    for MergePipe<Source1, Source2>
where
    SnapshotItem: Key + Clone,
    Source: Send + Sync + 'static,
    Source1: SourceStorage<SnapshotItem, Source> + Send + 'static,
    Source2: SourceStorage<SnapshotItem, Source> + Send + 'static,
{
    async fn get_object(&self, snapshot: &SnapshotItem, mission: &Mission) -> Result<Source> {
        let path = snapshot.key();

        if let Some(key) = path.strip_prefix(&self.prefix) {
            let mut snapshot = snapshot.clone();
            *snapshot.key_mut() = String::from(key);
            self.s1.get_object(&snapshot, mission).await
        } else {
            self.s2.get_object(snapshot, mission).await
        }
    }
}

pub struct NilPipe;

#[async_trait]
impl<T> SnapshotStorage<T> for NilPipe {
    async fn snapshot(&mut self, _: Mission, _: &SnapshotConfig) -> Result<Vec<T>> {
        Ok(vec![])
    }

    fn info(&self) -> String {
        String::from("nil")
    }
}

#[async_trait]
impl<T: Sync, U> SourceStorage<T, U> for NilPipe {
    async fn get_object(&self, _: &T, _: &Mission) -> Result<U> {
        Err(Error::PipeError(String::from("unexpected prefix")))
    }
}
