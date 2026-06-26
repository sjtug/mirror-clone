//! GHCup source
//!
//! The mirroring of ghcup is split into several sub-sources that are combined
//! via `MergePipe`:
//!
//! - `GhcupPackages` — parses the latest ghcup config YAML from
//!   `haskell/ghcup-metadata` and produces a snapshot entry for every
//!   `dlUri` under `ghcupDownloads`, dropping releases tagged with
//!   `viTags: ["old"]` unless `--include-old-versions` is set.
//!   Set `--target-mirror` to rewrite download URLs to your mirror.
//!
//! - `GhcupYaml` — mirrors the ghcup config YAML files themselves
//!   (`ghcup-*.yaml` and their `.sig` signatures) from the GitHub repo.
//!   Legacy YAMLs go through a `RewritePipe` that rewrites download URLs
//!   to the mirror path.
//!
//! - `GhcupScript` — mirrors the install script from `get-ghcup.haskell.org`.
//!   The script also passes through a `RewritePipe` for URL substitution.
//!
//! It's recommended to mirror HLS and Stack packages using `GithubRelease`
//! source for better control over retained versions (`--retain-hls-versions`,
//! `--retain-stack-versions`). All five pipelines can be combined with
//! `merge_pipe!`.
//!
//! `GhcupPackages` does not validate the YAML schema — `parse_uris_from_yaml`
//! walks the entire document tree and collects every value under a `dlUri`
//! key. While this is resilient to minor structural changes, a major upstream
//! format break requires manual intervention.
//!
//! - Version mismatch warning logged when the config filename version differs
//!   from `EXPECTED_CONFIG_VERSION`. This is non-fatal — check whether the
//!   snapshot still contains the expected set of URIs.
//! - Missing or extra URIs in the snapshot are the primary symptom of a
//!   format break. Compare against a known-good snapshot after an upstream
//!   update.
//!
//! **Migration steps**
//!
//! 1. Download the new config YAML and inspect the `ghcupDownloads` tree.
//! 2. If `dlUri` keys appear in unexpected sections, update `EXPECTED_CONFIG_VERSION`
//!    and verify the snapshot output.
//! 3. If `dlUri` is renamed or the structure fundamentally changes, replace
//!    `parse_uris_from_yaml` with a new format-aware parser (the old typed-
//!    serde approach that was replaced by this parser can serve as a template).
//! 4. Bump `EXPECTED_CONFIG_VERSION` to the new version and verify the
//!    snapshot reflects the correct set of download URIs.

use structopt::StructOpt;

use crate::ghcup::packages::GhcupPackages;
use crate::ghcup::script::GhcupScript;
use crate::ghcup::yaml::GhcupYaml;
use crate::utils::CommaSplitVecString;

mod packages;
mod parser;
mod script;
mod utils;
mod yaml;

#[derive(Debug, Clone, StructOpt)]
pub struct Ghcup {
    #[structopt(flatten)]
    pub ghcup_repo_config: GhcupRepoConfig,
    #[structopt(long, default_value = "https://get-ghcup.haskell.org/")]
    pub script_url: String,
    #[structopt(long, help = "Include legacy versions of packages")]
    pub include_old_versions: bool,
    #[structopt(long, help = "mirror url for packages")]
    pub target_mirror: String,
    #[structopt(long, help = "Stack versions to retain", default_value = "3")]
    pub retain_stack_versions: usize,
    #[structopt(long, help = "Hls versions to retain", default_value = "3")]
    pub retain_hls_versions: usize,
    #[structopt(
        long,
        default_value = "ghcup-0.0.4.yaml,ghcup-0.0.5.yaml,ghcup-0.0.6.yaml"
    )]
    #[expect(dead_code)]
    pub additional_yaml: CommaSplitVecString,
}

#[derive(Debug, Clone, StructOpt)]
pub struct GhcupRepoConfig {
    #[structopt(
        long,
        help = "Ghcup github repo",
        default_value = "haskell/ghcup-metadata"
    )]
    repo: String,
    #[structopt(long, help = "Ghcup github branch", default_value = "master")]
    branch: String,
}

impl Ghcup {
    pub fn get_script(&self) -> GhcupScript {
        GhcupScript {
            script_url: self.script_url.clone(),
        }
    }
    pub fn get_yaml(&self, legacy: bool) -> GhcupYaml {
        GhcupYaml::new(self.ghcup_repo_config.clone(), legacy)
    }
    pub fn get_packages(&self) -> GhcupPackages {
        GhcupPackages {
            ghcup_repo_config: self.ghcup_repo_config.clone(),
            include_old_versions: self.include_old_versions,
        }
    }
}
