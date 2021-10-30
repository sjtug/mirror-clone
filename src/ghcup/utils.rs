use std::fmt;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use itertools::Itertools;
use lazy_static::lazy_static;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::Result;

use super::GhcupRepoConfig;

lazy_static! {
    static ref YAML_CONFIG_PATTERN: regex::Regex =
        regex::Regex::new(r"ghcup-(?P<ver>\d.\d.\d).yaml$").unwrap();
}

// order is reverted to derive Ord ;)
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version {
    pub patch: usize,
    pub minor: usize,
    pub major: usize,
}

impl Version {
    pub const fn new(major: usize, minor: usize, patch: usize) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for Version {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        s.split('.')
            .collect_tuple()
            .and_then(|(major, minor, patch): (&str, &str, &str)| {
                Some(Version::new(
                    major.parse().ok()?,
                    minor.parse().ok()?,
                    patch.parse().ok()?,
                ))
            })
            .ok_or(())
    }
}

#[derive(Debug, Deserialize)]
struct TreeMeta {
    tree: Vec<FileMeta>,
}

#[derive(Debug, Deserialize)]
pub struct FileMeta {
    path: String,
    #[serde(rename = "type")]
    ty: NodeType,
    url: String,
}

#[derive(Debug, Copy, Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum NodeType {
    Tree,
    Blob,
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub name: String,
    pub path: String,
    pub version: Version,
}

#[derive(Debug, Clone)]
pub struct ObjectInfoWithUrl {
    pub name: String,
    pub path: String,
    pub version: Version,
    pub url: String,
}

#[derive(Debug, Deserialize)]
struct ContentMeta {
    download_url: String,
}

pub async fn list_files(
    client: &Client,
    config: &GhcupRepoConfig,
    commit: &str,
) -> Result<Vec<FileMeta>> {
    let tree_url = format!(
        "https://api.github.com/repos/{}/git/trees/{}",
        config.repo, commit
    );

    let tree_meta: TreeMeta = client.get(tree_url).send().await?.json().await?;
    Ok(tree_meta
        .tree
        .into_iter()
        .filter(|item| item.ty == NodeType::Blob)
        .collect())
}

pub fn filter_map_file_objs(
    files: impl IntoIterator<Item = FileMeta>,
) -> impl Iterator<Item = ObjectInfo> {
    files.into_iter().filter_map(|f: FileMeta| {
        YAML_CONFIG_PATTERN.captures(&*f.path).and_then(|c| {
            c.name("ver").and_then(|m| {
                let name = f.path.split('/').last().unwrap().to_string();
                Some(ObjectInfo {
                    name,
                    path: f.path.clone(),
                    version: Version::from_str(m.as_str()).ok()?,
                })
            })
        })
    })
}

pub async fn get_raw_blob_url(
    client: &Client,
    config: &GhcupRepoConfig,
    object: ObjectInfo,
) -> Result<ObjectInfoWithUrl> {
    let content: ContentMeta = client
        .get(format!(
            "https://api.github.com/repos/{}/contents/{}",
            config.repo, object.path
        ))
        .send()
        .await?
        .json()
        .await?;
    Ok(ObjectInfoWithUrl {
        name: object.name,
        path: object.path,
        version: object.version,
        url: content.download_url,
    })
}
