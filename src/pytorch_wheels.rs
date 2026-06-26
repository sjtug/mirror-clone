//! PyTorch wheels source.
//!
//! Crawls the `download.pytorch.org/whl` Simple API tree, generates mirror-local
//! HTML indexes, and schedules PyTorch-hosted wheel (and `.metadata`) objects for
//! transfer to S3.  Non-PyTorch links (pythonhosted, nvidia) are rewritten to
//! point at existing mirrors rather than re-hosted.
//!
//! The source implements `SnapshotStorage<SnapshotPath>` + `SourceStorage<SnapshotPath,
//! ByteStream>` directly (no `ByteStreamPipe`): generated HTML is served from
//! in-memory `Bytes`, and remote wheels are fetched into `Bytes` at `get_object`
//! time.

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use slog::{info, warn};
use structopt::StructOpt;
use url::Url;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::bar;

#[derive(Debug, Clone, StructOpt)]
pub struct PyTorchWheels {
    /// Base URL of the Simple index tree (used for crawling).
    #[structopt(
        long,
        default_value = "https://download.pytorch.org/whl",
        help = "Base of the PyTorch wheel Simple index"
    )]
    pub whl_base: String,

    /// Base URL for PyTorch-hosted remote objects (R2 CDN).
    #[structopt(
        long,
        default_value = "https://download-r2.pytorch.org/whl",
        help = "Base URL for PyTorch-hosted remote objects"
    )]
    pub r2_base: String,

    /// Crawl results populated by `snapshot`, read by `get_object`.
    #[structopt(skip)]
    pub objects: HashMap<String, ObjectKind>,
}

/// Internal descriptor for a scheduled object.
#[derive(Debug, Clone)]
pub enum ObjectKind {
    /// Generated HTML body (project page or track index).
    GeneratedHtml(String),
    /// Remote URL to fetch (a wheel or `.metadata` sidecar on download-r2).
    Remote(String),
}

/// Classification of an anchor href by upstream host.
enum HostKind {
    /// PyTorch-hosted on download-r2 or root-relative `/whl/...`.
    /// `(rest_after_whl, remote_url)`
    PyTorch(String, String),
    /// `files.pythonhosted.org/packages/<rest>`.
    /// `rest` is the path after `packages/`.
    PythonHosted(String),
    /// `pypi.nvidia.com/<path>`.
    /// `path` is the full path after the host.
    Nvidia(String),
}

/// Parsed anchor: href, display text, and preserved attributes.
struct Anchor {
    href: String,
    text: String,
    /// Original attribute pairs (excluding `href`) to preserve in generated HTML.
    attrs: Vec<(String, String)>,
}

impl Anchor {
    /// Whether this anchor carries PEP 658 metadata attributes.
    fn has_pep658_metadata(&self) -> bool {
        self.attrs.iter().any(|(k, _)| {
            k == "data-dist-info-metadata" || k == "data-core-metadata"
        })
    }

    /// The fragment portion of the href (e.g. `#sha256=...`), if any.
    fn fragment(&self) -> &str {
        match self.href.find('#') {
            Some(i) => &self.href[i..],
            None => "",
        }
    }
}

/// Classify an anchor href into a `HostKind`.
fn classify_href(href: &str, r2_base: &str) -> Result<HostKind> {
    let path = match href.find('#') {
        Some(i) => &href[..i],
        None => href,
    };

    // Absolute URL.
    if let Ok(url) = Url::parse(path) {
        match url.host_str() {
            Some("download-r2.pytorch.org") => {
                let rest = path_after_prefix(path, "https://download-r2.pytorch.org/whl/")
                    .ok_or_else(|| {
                        Error::ConfigureError(format!(
                            "download-r2 URL missing /whl/ prefix: {path}"
                        ))
                    })?;
                let remote_url = format!("{}{}", "https://download-r2.pytorch.org/whl", ensure_slash(&rest));
                Ok(HostKind::PyTorch(rest.to_string(), remote_url))
            }
            Some("files.pythonhosted.org") => {
                let rest = path_after_prefix(path, "https://files.pythonhosted.org/packages/")
                    .ok_or_else(|| {
                        Error::ConfigureError(format!(
                            "pythonhosted URL missing /packages/ prefix: {path}"
                        ))
                    })?;
                Ok(HostKind::PythonHosted(rest.to_string()))
            }
            Some("pypi.nvidia.com") => {
                let p = path_after_prefix(path, "https://pypi.nvidia.com/")
                    .unwrap_or_default();
                Ok(HostKind::Nvidia(p.to_string()))
            }
            Some(other) => Err(Error::ConfigureError(format!(
                "unknown host in PyTorch wheel index: {other}"
            ))),
            None => Err(Error::ConfigureError(format!(
                "URL without host in PyTorch wheel index: {path}"
            ))),
        }
    } else {
        // Root-relative `/whl/<rest>`.
        if let Some(rest) = path.strip_prefix("/whl/") {
            let rest = rest.to_string();
            let remote_url = format!("{}/{}", r2_base.trim_end_matches('/'), rest);
            Ok(HostKind::PyTorch(rest, remote_url))
        } else {
            Err(Error::ConfigureError(format!(
                "unrecognised relative href in PyTorch wheel index: {path}"
            )))
        }
    }
}

/// Return the portion of `url` after `prefix`, URL-decoded.
fn path_after_prefix(url: &str, prefix: &str) -> Option<String> {
    let raw = url.strip_prefix(prefix)?;
    Some(percent_decode(raw))
}

/// Percent-decode a string (e.g. `%2B` → `+`).
fn percent_decode(s: &str) -> String {
    urlencoding::decode(s)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| s.to_string())
}

fn ensure_slash(s: &str) -> String {
    if s.starts_with('/') {
        s.to_string()
    } else {
        format!("/{s}")
    }
}

/// Fetch a URL and return the response body as text.
async fn fetch_text(client: &Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(Error::HTTPError(status));
    }
    Ok(resp.text().await?)
}

/// Parse an HTML document and extract all `<a>` anchors with preserved attributes.
fn parse_anchors(html: &str) -> Vec<Anchor> {
    let dom = tl::parse(html, Default::default());
    let dom = match dom {
        Ok(d) => d,
        Err(_) => return vec![],
    };

    let mut anchors = vec![];
    for node in dom.nodes() {
        if let tl::Node::Tag(tag) = node {
            if tag.name().as_utf8_str() != "a" {
                continue;
            }
            let attrs_map = tag.attributes();
            let href = attrs_map
                .get("href")
                .flatten()
                .map(|b| b.as_utf8_str().into_owned())
                .unwrap_or_default();
            if href.is_empty() {
                continue;
            }
            let text = tag.inner_text(dom.parser()).into_owned();
            let preserved: Vec<(String, String)> = attrs_map
                .iter()
                .filter(|(k, _)| k != "href")
                .filter_map(|(k, v)| {
                    let v = v.as_ref().map(|v| v.clone().into_owned());
                    v.map(|v| (k.into_owned(), v))
                })
                .collect();
            anchors.push(Anchor {
                href,
                text,
                attrs: preserved,
            });
        }
    }
    anchors
}

/// Parse a top-level or track-level index and return link texts (project/track names).
fn parse_link_names(html: &str) -> Vec<String> {
    let dom = tl::parse(html, Default::default());
    let dom = match dom {
        Ok(d) => d,
        Err(_) => return vec![],
    };
    dom.nodes()
        .iter()
        .filter_map(|node| {
            if let tl::Node::Tag(tag) = node
                && tag.name().as_utf8_str() == "a"
            {
                    let text = tag.inner_text(dom.parser()).into_owned();
                    let name = text.trim_end_matches('/').to_string();
                    if !name.is_empty() {
                        return Some(name);
                    }
                }
            None
        })
        .collect()
}

/// Build a generated HTML project page from a list of rewritten anchors.
fn build_project_html(project: &str, anchors_html: &[String]) -> String {
    let mut body = String::new();
    body.push_str("<!DOCTYPE html>\n<html>\n  <body>\n");
    body.push_str(&format!("    <h1>Links for {project}</h1>\n"));
    for a in anchors_html {
        body.push_str("    ");
        body.push_str(a);
        body.push_str("<br/>\n");
    }
    body.push_str("  </body>\n</html>\n");
    body
}

/// Build a generated HTML track index listing included projects.
fn build_track_html(track: &str, projects: &[String]) -> String {
    let mut body = String::new();
    body.push_str("<!DOCTYPE html>\n<html>\n  <body>\n");
    for project in projects {
        body.push_str(&format!(
            "    <a href=\"/pytorch-wheels/{track}/{project}\">{project}</a><br/>\n"
        ));
    }
    body.push_str("  </body>\n</html>\n");
    body
}

/// Build a rewritten `<a>` tag string for generated HTML.
fn build_anchor_html(rewritten_href: &str, text: &str, attrs: &[(String, String)]) -> String {
    let mut s = format!("<a href=\"{rewritten_href}\"");
    for (k, v) in attrs {
        s.push_str(&format!(" {k}=\"{v}\""));
    }
    s.push_str(&format!(">{text}</a>"));
    s
}

/// Result of processing a single project page.
struct ProjectResult {
    /// Generated HTML anchors (for the project page), or `None` if the project
    /// has no PyTorch-hosted links and should be skipped.
    anchors_html: Option<Vec<String>>,
    /// Remote objects to schedule: `(key, remote_url)`.
    remote_objects: Vec<(String, String)>,
}

/// A crawled project, located either at the root or under a track.
struct CrawledProject {
    /// S3 key for the generated project page: `project` (root) or
    /// `{track}/{project}`.
    page_key: String,
    /// `None` for root-level projects; `Some(track)` for projects under a track.
    track: Option<String>,
    project: String,
    result: ProjectResult,
}

/// Whether a fetched page is a leaf project page (its anchors contain
/// wheel/source URLs) rather than an intermediate index (anchors are bare
/// sub-directory links).  Content-based, no name heuristics.
fn is_leaf_project_page(anchors: &[Anchor], r2_base: &str) -> bool {
    anchors.iter().any(|a| classify_href(&a.href, r2_base).is_ok())
}

/// Process a single project page's anchors.
fn process_project_page(
    anchors: &[Anchor],
    r2_base: &str,
) -> Result<ProjectResult> {
    let mut anchors_html = vec![];
    let mut remote_objects = vec![];
    let mut has_pytorch = false;

    for anchor in anchors {
        let kind = classify_href(&anchor.href, r2_base)?;
        match kind {
            HostKind::PyTorch(rest, remote_url) => {
                has_pytorch = true;
                let key = rest.clone();
                let fragment = anchor.fragment();
                let rewritten = format!("/pytorch-wheels/{}{}", key, fragment);
                anchors_html.push(build_anchor_html(&rewritten, &anchor.text, &anchor.attrs));
                remote_objects.push((key.clone(), remote_url.clone()));
                if anchor.has_pep658_metadata() {
                    remote_objects.push((
                        format!("{key}.metadata"),
                        format!("{remote_url}.metadata"),
                    ));
                }
            }
            HostKind::PythonHosted(rest) => {
                let fragment = anchor.fragment();
                let rewritten = format!("/pypi-packages/{}{}", rest, fragment);
                anchors_html.push(build_anchor_html(&rewritten, &anchor.text, &anchor.attrs));
            }
            HostKind::Nvidia(path) => {
                let fragment = anchor.fragment();
                let rewritten = format!("https://pypi.nvidia.cn/{}{}", path, fragment);
                anchors_html.push(build_anchor_html(&rewritten, &anchor.text, &anchor.attrs));
            }
        }
    }

    Ok(ProjectResult {
        anchors_html: if has_pytorch { Some(anchors_html) } else { None },
        remote_objects,
    })
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for PyTorchWheels {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;

        // 1. Fetch the root Simple index and collect every top-level entry.
        //    We do NOT filter by name (e.g. `cu[0-9]+`): we only care about the
        //    source URLs in the project pages beneath each entry, not the
        //    entry/track names.
        info!(logger, "fetching PyTorch wheel root index...");
        let root_html = fetch_text(&client, &format!("{}/", self.whl_base)).await?;
        let top_names = parse_link_names(&root_html);
        info!(logger, "discovered {} top-level entries", top_names.len());

        let r2_base = self.r2_base.clone();
        let whl_base = self.whl_base.clone();

        // 2. Fetch each top-level page concurrently, then classify it by
        //    content: a leaf project page has wheel/source anchors; an
        //    intermediate index has only bare sub-directory links.
        progress.set_length(top_names.len() as u64);
        progress.set_style(bar());

        let top_pages: Vec<(String, Vec<Anchor>)> =
            stream::iter(top_names.into_iter().map(|name| {
                let client = client.clone();
                let whl_base = whl_base.clone();
                let progress = progress.clone();
                async move {
                    progress.set_message(&name);
                    let url = format!("{}/{}/", whl_base, name);
                    let html = fetch_text(&client, &url).await?;
                    let anchors = parse_anchors(&html);
                    progress.inc(1);
                    Ok::<_, Error>((name, anchors))
                }
            }))
            .buffer_unordered(config.concurrent_resolve)
            .try_collect()
            .await?;

        // 3. Process each top-level page: either treat it as a leaf project
        //    (process its anchors directly) or recurse one level into its
        //    sub-links as projects.
        let mut crawled: Vec<CrawledProject> = vec![];
        let mut sub_pairs: Vec<(String, String)> = vec![];

        for (name, anchors) in &top_pages {
            if is_leaf_project_page(anchors, &r2_base) {
                let result = process_project_page(anchors, &r2_base)?;
                crawled.push(CrawledProject {
                    page_key: name.clone(),
                    track: None,
                    project: name.clone(),
                    result,
                });
            } else {
                // Intermediate index: every anchor is a sub-directory link.
                for a in anchors {
                    let sub = a.text.trim_end_matches('/').to_string();
                    if !sub.is_empty() {
                        sub_pairs.push((name.clone(), sub));
                    }
                }
            }
        }

        // 4. Fetch each sub-project page concurrently and process it.
        progress.set_length(sub_pairs.len() as u64);
        progress.set_style(bar());

        let sub_results: Vec<CrawledProject> =
            stream::iter(sub_pairs.into_iter().map(|(track, project)| {
                let client = client.clone();
                let whl_base = whl_base.clone();
                let r2_base = r2_base.clone();
                let progress = progress.clone();
                async move {
                    progress.set_message(&format!("{track}/{project}"));
                    let url = format!("{}/{}/{}/", whl_base, track, project);
                    let html = fetch_text(&client, &url).await?;
                    let anchors = parse_anchors(&html);
                    let result = process_project_page(&anchors, &r2_base)?;
                    progress.inc(1);
                    Ok::<_, Error>(CrawledProject {
                        page_key: format!("{track}/{project}"),
                        track: Some(track),
                        project,
                        result,
                    })
                }
            }))
            .buffer_unordered(config.concurrent_resolve)
            .try_collect()
            .await?;

        crawled.extend(sub_results);

        // 5. Build objects map and snapshot list from the crawled projects.
        let mut objects = HashMap::new();
        let mut snapshot = vec![];
        let mut track_projects_map: HashMap<String, Vec<String>> = HashMap::new();

        for c in crawled {
            if let Some(anchors_html) = c.result.anchors_html {
                let html = build_project_html(&c.project, &anchors_html);
                objects.insert(c.page_key.clone(), ObjectKind::GeneratedHtml(html));
                snapshot.push(SnapshotPath::force(c.page_key.clone()));
                if let Some(track) = c.track {
                    track_projects_map.entry(track).or_default().push(c.project);
                }
            } else {
                warn!(
                    logger,
                    "skipping project {}: no PyTorch-hosted links",
                    c.page_key
                );
            }
            for (key, remote_url) in c.result.remote_objects {
                if !objects.contains_key(&key) {
                    objects.insert(key.clone(), ObjectKind::Remote(remote_url));
                    snapshot.push(SnapshotPath::new(key));
                }
            }
        }

        // 6. Generate one track-index page per intermediate index that has at
        //    least one included project.
        let mut sorted_tracks: Vec<_> = track_projects_map.keys().cloned().collect();
        sorted_tracks.sort();
        for track in &sorted_tracks {
            let mut projects = track_projects_map.remove(track).unwrap();
            projects.sort();
            let html = build_track_html(track, &projects);
            objects.insert(track.clone(), ObjectKind::GeneratedHtml(html));
            snapshot.push(SnapshotPath::force(track.clone()));
        }

        self.objects = objects;
        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("pytorch-wheels, {:?}", self.whl_base)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, ByteStream> for PyTorchWheels {
    async fn get_object(
        &self,
        snapshot: &SnapshotPath,
        mission: &Mission,
    ) -> Result<ByteStream> {
        let kind = self
            .objects
            .get(&snapshot.0)
            .ok_or_else(|| Error::PipeError(format!("unknown pytorch-wheels key: {}", snapshot.0)))?;

        match kind {
            ObjectKind::GeneratedHtml(body) => {
                let bytes = Bytes::from(body.clone());
                let length = bytes.len() as u64;
                Ok(ByteStream {
                    object: ByteObject::Bytes(Some(bytes)),
                    length,
                    modified_at: crate::utils::unix_time(),
                    content_type: Some("text/html; charset=utf-8".to_string()),
                })
            }
            ObjectKind::Remote(url) => {
                let resp = mission.client.get(url).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    return Err(Error::HTTPError(status));
                }
                let content_length = resp.content_length();
                let content_type = resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let modified_at = resp
                    .headers()
                    .get(reqwest::header::LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|h| DateTime::parse_from_rfc2822(h).ok())
                    .map(|dt| dt.timestamp() as u64)
                    .unwrap_or_else(crate::utils::unix_time);
                let bytes = resp.bytes().await?;
                let length = content_length.unwrap_or(bytes.len() as u64);
                Ok(ByteStream {
                    object: ByteObject::Bytes(Some(bytes)),
                    length,
                    modified_at,
                    content_type,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Crawl discrimination (content-based, no name heuristics) ---

    #[test]
    fn leaf_project_page_detected_by_wheel_anchors() {
        // A leaf project page has wheel/source anchors.
        let anchors = vec![Anchor {
            href: "/whl/certifi-2022.12.7-py3-none-any.whl#sha256=def".to_string(),
            text: "certifi-2022.12.7-py3-none-any.whl".to_string(),
            attrs: vec![],
        }];
        assert!(is_leaf_project_page(&anchors, "https://download-r2.pytorch.org/whl"));
    }

    #[test]
    fn intermediate_index_detected_by_subdir_links() {
        // An intermediate index has only bare sub-directory links (no wheel URLs).
        // `classify_href` fails on these, so the page is treated as intermediate.
        let anchors = vec![
            Anchor { href: "torch/".to_string(), text: "torch".to_string(), attrs: vec![] },
            Anchor { href: "certifi/".to_string(), text: "certifi".to_string(), attrs: vec![] },
        ];
        assert!(!is_leaf_project_page(&anchors, "https://download-r2.pytorch.org/whl"));
    }

    #[test]
    fn classify_download_r2_pytorch() {
        let kind = classify_href(
            "https://download-r2.pytorch.org/whl/cu128/torch-2.10.0%2Bcu128-cp310-cp310-manylinux_2_28_x86_64.whl#sha256=abc",
            "https://download-r2.pytorch.org/whl",
        ).unwrap();
        match kind {
            HostKind::PyTorch(rest, remote) => {
                assert_eq!(rest, "cu128/torch-2.10.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl");
                assert!(remote.starts_with("https://download-r2.pytorch.org/whl/"));
            }
            _ => panic!("expected PyTorch"),
        }
    }

    #[test]
    fn classify_root_relative_pytorch() {
        let kind = classify_href(
            "/whl/certifi-2022.12.7-py3-none-any.whl#sha256=def",
            "https://download-r2.pytorch.org/whl",
        ).unwrap();
        match kind {
            HostKind::PyTorch(rest, remote) => {
                assert_eq!(rest, "certifi-2022.12.7-py3-none-any.whl");
                assert_eq!(remote, "https://download-r2.pytorch.org/whl/certifi-2022.12.7-py3-none-any.whl");
            }
            _ => panic!("expected PyTorch"),
        }
    }

    #[test]
    fn classify_pythonhosted() {
        let kind = classify_href(
            "https://files.pythonhosted.org/packages/aa/bb/numpy-1.0.1.tar.gz#sha256=xyz",
            "https://download-r2.pytorch.org/whl",
        ).unwrap();
        match kind {
            HostKind::PythonHosted(rest) => {
                assert_eq!(rest, "aa/bb/numpy-1.0.1.tar.gz");
            }
            _ => panic!("expected PythonHosted"),
        }
    }

    #[test]
    fn classify_nvidia() {
        let kind = classify_href(
            "https://pypi.nvidia.com/nvidia-cublas-cu12/nvidia_cublas_cu12-12.0.1.189-py3-none-manylinux1_x86_64.whl#sha256=123",
            "https://download-r2.pytorch.org/whl",
        ).unwrap();
        match kind {
            HostKind::Nvidia(path) => {
                assert!(path.starts_with("nvidia-cublas-cu12/"));
            }
            _ => panic!("expected Nvidia"),
        }
    }

    #[test]
    fn classify_unknown_host_errors() {
        let result = classify_href(
            "https://example.com/some-file.whl",
            "https://download-r2.pytorch.org/whl",
        );
        assert!(result.is_err());
    }

    #[test]
    fn process_pytorch_hosted_schedules_wheel_and_metadata() {
        let anchors = vec![Anchor {
            href: "https://download-r2.pytorch.org/whl/cu128/torch-2.10.0%2Bcu128-cp310-cp310-manylinux_2_28_x86_64.whl#sha256=abc".to_string(),
            text: "torch-2.10.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl".to_string(),
            attrs: vec![("data-dist-info-metadata".to_string(), "sha256=def".to_string())],
        }];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        assert!(result.anchors_html.is_some());
        // Wheel + metadata sidecar
        assert_eq!(result.remote_objects.len(), 2);
        assert_eq!(result.remote_objects[0].0, "cu128/torch-2.10.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl");
        assert_eq!(result.remote_objects[1].0, "cu128/torch-2.10.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl.metadata");
    }

    #[test]
    fn process_non_pytorch_only_skips_project() {
        let anchors = vec![Anchor {
            href: "https://pypi.nvidia.com/nvidia-cublas-cu12/foo.whl#sha256=abc".to_string(),
            text: "foo.whl".to_string(),
            attrs: vec![],
        }];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        assert!(result.anchors_html.is_none());
        assert!(result.remote_objects.is_empty());
    }

    #[test]
    fn process_pythonhosted_no_remote_object() {
        let anchors = vec![Anchor {
            href: "https://files.pythonhosted.org/packages/aa/bb/pkg.whl#sha256=abc".to_string(),
            text: "pkg.whl".to_string(),
            attrs: vec![],
        }];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        assert!(result.anchors_html.is_none()); // no PyTorch-hosted → skipped
        assert!(result.remote_objects.is_empty());
    }

    #[test]
    fn process_mixed_pytorch_and_pythonhosted() {
        let anchors = vec![
            Anchor {
                href: "https://download-r2.pytorch.org/whl/cu128/torch-1.0%2Bcu128.whl#sha256=abc".to_string(),
                text: "torch-1.0+cu128.whl".to_string(),
                attrs: vec![],
            },
            Anchor {
                href: "https://files.pythonhosted.org/packages/aa/bb/numpy-1.0.whl#sha256=def".to_string(),
                text: "numpy-1.0.whl".to_string(),
                attrs: vec![],
            },
        ];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        assert!(result.anchors_html.is_some());
        // Only the PyTorch-hosted wheel, not the pythonhosted one
        assert_eq!(result.remote_objects.len(), 1);
        assert_eq!(result.remote_objects[0].0, "cu128/torch-1.0+cu128.whl");
    }

    #[test]
    fn build_track_html_lists_projects() {
        let html = build_track_html("cu128", &["torch".to_string(), "torchvision".to_string()]);
        assert!(html.contains("/pytorch-wheels/cu128/torch"));
        assert!(html.contains("/pytorch-wheels/cu128/torchvision"));
    }

    #[test]
    fn build_project_html_contains_anchors() {
        let anchors = vec!["<a href=\"/pytorch-wheels/cu128/torch-1.0.whl\">torch</a>".to_string()];
        let html = build_project_html("torch", &anchors);
        assert!(html.contains("Links for torch"));
        assert!(html.contains("/pytorch-wheels/cu128/torch-1.0.whl"));
    }

    #[test]
    fn parse_anchors_extracts_href_text_and_attrs() {
        let html = r#"<!DOCTYPE html><html><body>
            <a href="https://download-r2.pytorch.org/whl/cu128/torch-1.0%2Bcu128.whl#sha256=abc"
               data-dist-info-metadata="sha256=def" data-upload-time="2025-01-01T00:00:00Z">torch-1.0+cu128.whl</a>
        </body></html>"#;
        let anchors = parse_anchors(html);
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].text, "torch-1.0+cu128.whl");
        assert!(anchors[0].has_pep658_metadata());
        assert_eq!(anchors[0].fragment(), "#sha256=abc");
    }

    #[test]
    fn parse_link_names_extracts_names() {
        let html = r#"<!DOCTYPE html><html><body>
            <a href="cu128/">cu128</a><br/>
            <a href="torch/">torch</a><br/>
            <a href="cpu/">cpu</a><br/>
        </body></html>"#;
        let names = parse_link_names(html);
        assert_eq!(names, vec!["cu128", "torch", "cpu"]);
    }

    // --- Spec conformance: link rewriting ---

    /// Spec: cache PyTorch-hosted links.
    /// A PyTorch-hosted wheel href must be rewritten to `/pytorch-wheels/<key>`
    /// and scheduled as a `Remote` object (i.e. fetched and stored in S3).
    #[test]
    fn spec_cache_pytorch_hosted() {
        let anchors = vec![Anchor {
            href:
                "https://download-r2.pytorch.org/whl/cu128/torch-1.0%2Bcu128-cp310-cp310-manylinux_2_28_x86_64.whl#sha256=abc"
                    .to_string(),
            text: "torch-1.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl".to_string(),
            attrs: vec![],
        }];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        let html = result.anchors_html.expect("project should be included");
        assert_eq!(html.len(), 1);
        // Rewritten to a mirror-local `/pytorch-wheels/...` href.
        assert!(html[0].contains(r#"href="/pytorch-wheels/cu128/torch-1.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl#sha256=abc""#));
        // Scheduled for caching (exactly one remote wheel object).
        assert_eq!(result.remote_objects.len(), 1);
        assert_eq!(result.remote_objects[0].0, "cu128/torch-1.0+cu128-cp310-cp310-manylinux_2_28_x86_64.whl");
    }

    /// Spec: redirect `files.pythonhosted.org` links to `/pypi-packages`.
    /// The rewritten href must be `/pypi-packages/<rest>` with the duplicated
    /// `packages/` path segment dropped, and no remote object scheduled.
    #[test]
    fn spec_redirect_pythonhosted_to_pypi_packages() {
        let anchors = vec![
            Anchor {
                href: "https://files.pythonhosted.org/packages/aa/bb/numpy-1.0.1.tar.gz#sha256=xyz"
                    .to_string(),
                text: "numpy-1.0.1.tar.gz".to_string(),
                attrs: vec![],
            },
            // Include a PyTorch anchor so the project page is emitted at all.
            Anchor {
                href: "https://download-r2.pytorch.org/whl/cu128/torch-1.0%2Bcu128.whl#sha256=abc"
                    .to_string(),
                text: "torch-1.0+cu128.whl".to_string(),
                attrs: vec![],
            },
        ];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        let html = result.anchors_html.expect("project should be included");
        assert_eq!(html.len(), 2);
        // The pythonhosted anchor is rewritten to /pypi-packages/<rest>,
        // duplicating only the `aa/bb/...` portion (NOT `packages/aa/bb/...`).
        assert!(html
            .iter()
            .any(|h| h.contains(r#"href="/pypi-packages/aa/bb/numpy-1.0.1.tar.gz#sha256=xyz""#)));
        assert!(html.iter().all(|h| !h.contains("/pypi-packages/packages/")));
        // Only the PyTorch-hosted wheel is scheduled for caching.
        assert_eq!(result.remote_objects.len(), 1);
    }

    /// Spec: redirect `pypi.nvidia.com` links to `pypi.nvidia.cn`.
    /// The rewritten href must be an absolute `https://pypi.nvidia.cn/<path>`
    /// URL and no remote object must be scheduled.
    #[test]
    fn spec_redirect_nvidia_to_pypi_nvidia_cn() {
        let anchors = vec![
            Anchor {
                href:
                    "https://pypi.nvidia.com/nvidia-cublas-cu12/nvidia_cublas_cu12-12.0.1.189-py3-none-manylinux1_x86_64.whl#sha256=123"
                        .to_string(),
                text: "nvidia_cublas_cu12-12.0.1.189-py3-none-manylinux1_x86_64.whl".to_string(),
                attrs: vec![],
            },
            // Include a PyTorch anchor so the project page is emitted at all.
            Anchor {
                href: "https://download-r2.pytorch.org/whl/cu128/torch-1.0%2Bcu128.whl#sha256=abc"
                    .to_string(),
                text: "torch-1.0+cu128.whl".to_string(),
                attrs: vec![],
            },
        ];
        let result = process_project_page(&anchors, "https://download-r2.pytorch.org/whl").unwrap();
        let html = result.anchors_html.expect("project should be included");
        assert_eq!(html.len(), 2);
        // The nvidia anchor is rewritten to an absolute pypi.nvidia.cn URL.
        assert!(html.iter().any(|h| h.contains(
            r#"href="https://pypi.nvidia.cn/nvidia-cublas-cu12/nvidia_cublas_cu12-12.0.1.189-py3-none-manylinux1_x86_64.whl#sha256=123""#
        )));
        // Only the PyTorch-hosted wheel is scheduled for caching.
        assert_eq!(result.remote_objects.len(), 1);
    }
}
