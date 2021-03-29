use std::collections::{HashMap, HashSet};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DownloadSource {
    pub dl_uri: String,
    pub dl_subdir: Option<String>,
    pub dl_hash: String,
}

type DistributionRelease = HashMap<String, BinarySource>;
type BinarySource = HashMap<String, DownloadSource>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Release {
    pub vi_tags: Vec<String>,
    pub vi_source_dl: Option<DownloadSource>,
    pub vi_arch: HashMap<String, DistributionRelease>,
}

impl Release {
    pub fn uris(&self) -> HashSet<&str> {
        let mut binary_uris: HashSet<&str> = self
            .vi_arch
            .values()
            .into_iter()
            .flat_map(|dist| {
                dist.values()
                    .into_iter()
                    .flat_map(|bin_src| bin_src.values().into_iter().map(|src| src.dl_uri.as_str()))
            })
            .collect();
        if let Some(src) = self.vi_source_dl.as_ref() {
            binary_uris.insert(src.dl_uri.as_str());
        }
        binary_uris
    }
}

impl Release {
    pub fn is_old(&self) -> bool {
        self.vi_tags.iter().any(|item| item == "old")
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Components {
    #[serde(rename = "Cabal")]
    pub cabal: HashMap<String, Release>,
    #[serde(rename = "HLS")]
    pub hls: HashMap<String, Release>,
    #[serde(rename = "GHCup")]
    pub ghcup: HashMap<String, Release>,
    #[serde(rename = "GHC")]
    pub ghc: HashMap<String, Release>,
}

impl Components {
    pub fn uris(&self, include_old_versions: bool) -> HashSet<&str> {
        let fields: [&HashMap<String, Release>; 4] =
            [&self.cabal, &self.hls, &self.ghcup, &self.ghc];
        fields
            .iter()
            .flat_map(|field| {
                field.values().into_iter().flat_map(|release| {
                    if !include_old_versions && release.is_old() {
                        HashSet::new()
                    } else {
                        release.uris()
                    }
                })
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GhcupYamlParser {
    pub ghcup_downloads: Components,
}
