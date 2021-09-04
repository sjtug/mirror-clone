use std::fmt;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use itertools::Itertools;
use lazy_static::lazy_static;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::GhcupRepoConfig;

lazy_static! {
    static ref YAML_CONFIG_PATTERN: regex::Regex =
        regex::Regex::new(r"ghcup-(?P<ver>\d.\d.\d).yaml").unwrap();
}

macro_rules! t {
    ($e: expr) => {
        if let Ok(e) = $e {
            e
        } else {
            return None;
        }
    };
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
                    t!(major.parse()),
                    t!(minor.parse()),
                    t!(patch.parse()),
                ))
            })
            .ok_or(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    id: String,
    name: String,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagInfo {
    name: String,
    commit: CommitInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitInfo {
    id: String,
}

impl TagInfo {
    pub fn id(&self) -> &str {
        &self.commit.id
    }
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    id: String,
    name: String,
    version: Version,
}

impl ObjectInfo {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> Version {
        self.version
    }
}

pub async fn fetch_last_tag(client: &Client, config: &GhcupRepoConfig) -> Result<String> {
    let req = client.get(format!(
        "https://{}/api/v4/projects/{}/repository/tags",
        config.host,
        urlencoding::encode(&*config.repo)
    ));

    let tags: Vec<TagInfo> = serde_json::from_slice(&*req.send().await?.bytes().await?)
        .map_err(Error::JsonDecodeError)?;

    Ok(tags
        .first()
        .ok_or_else(|| Error::ProcessError(String::from("no tag found")))?
        .id()
        .to_string())
}

pub async fn list_files(
    client: &Client,
    config: &GhcupRepoConfig,
    commit: String,
) -> Result<Vec<FileInfo>> {
    let mut output = Vec::new();
    for page in 1.. {
        let req = client
            .get(format!(
                "https://{}/api/v4/projects/{}/repository/tree",
                config.host,
                urlencoding::encode(&*config.repo)
            ))
            .query(&[("per_page", config.pagination), ("page", page)])
            .query(&[("ref", commit.clone())]);
        let res: Vec<FileInfo> = serde_json::from_slice(&*req.send().await?.bytes().await?)
            .map_err(Error::JsonDecodeError)?;

        if !res.is_empty() {
            output.extend(res);
        } else {
            break;
        }
    }
    Ok(output)
}

pub fn filter_map_file_objs(
    files: impl IntoIterator<Item = FileInfo>,
) -> impl Iterator<Item = ObjectInfo> {
    files.into_iter().filter_map(|f| {
        YAML_CONFIG_PATTERN.captures(&*f.name).and_then(|c| {
            c.name("ver").and_then(|m| {
                Some(ObjectInfo {
                    id: f.id.clone(),
                    name: f.name.clone(),
                    version: t!(Version::from_str(m.as_str())),
                })
            })
        })
    })
}
