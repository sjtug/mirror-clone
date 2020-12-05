mod error;

use async_trait::async_trait;

use error::Result;

#[async_trait]
pub trait SnapshotStorage<SnapshotItem> {
    async fn snapshot(&mut self) -> Result<Vec<SnapshotItem>>;
}

#[async_trait]
pub trait SourceStorage<SourceItem> {
    async fn get_object(&self) -> Result<SourceItem>;
}

#[async_trait]
pub trait TargetStorage<TargetItem> {
    async fn put_object(&self, item: TargetItem) -> Result<()>;
}

pub struct Pypi;

#[async_trait]
impl SnapshotStorage<String> for Pypi {
    async fn snapshot(&mut self) -> Result<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait]
impl SourceStorage<String> for Pypi {
    async fn get_object(&self) -> Result<String> {
        Ok("".to_string())
    }
}

pub struct S3Backend;

#[async_trait]
impl SnapshotStorage<String> for S3Backend {
    async fn snapshot(&mut self) -> Result<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait]
impl TargetStorage<String> for S3Backend {
    async fn put_object(&self, item: String) -> Result<()> {
        Ok(())
    }
}

pub struct SimpleDiffTransfer<From, To>
where
    From: SourceStorage<String> + SnapshotStorage<String>,
    To: TargetStorage<String> + SnapshotStorage<String>,
{
    from: From,
    to: To,
}

impl<From, To> SimpleDiffTransfer<From, To>
where
    From: SourceStorage<String> + SnapshotStorage<String>,
    To: TargetStorage<String> + SnapshotStorage<String>,
{
    async fn transfer() -> Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("Hello, World!");
}
