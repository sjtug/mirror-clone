//! Parser and renderer for the PyPA Simple Repository API.
//!
//! This crate deliberately knows nothing about a particular mirror, storage
//! backend, or upstream vendor. It accepts either PEP 503 HTML or PEP 691 JSON,
//! resolves links against the page URL, and renders matching HTML/JSON views.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

pub const HTML_CONTENT_TYPE: &str = "application/vnd.pypi.simple.v1+html; charset=utf-8";
pub const JSON_CONTENT_TYPE: &str = "application/vnd.pypi.simple.v1+json";

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("invalid JSON index: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid URL in index: {0}")]
    Url(#[from] url::ParseError),
    #[error("expected a repository or project index")]
    UnknownDocument,
    #[error("invalid project name: {0}")]
    InvalidProjectName(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamProject {
    pub name: String,
    pub url: Url,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectLink {
    pub name: String,
    #[serde(skip)]
    pub normalized_name: String,
}

impl ProjectLink {
    pub fn with_normalized(name: impl Into<String>, normalized_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            normalized_name: normalized_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreMetadata {
    Available,
    Hashes(BTreeMap<String, String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectFile {
    pub filename: String,
    /// Absolute upstream URL while parsed; callers may replace it before rendering.
    /// Hashes live in `hashes`, never in this URL's fragment.
    pub url: String,
    pub hashes: BTreeMap<String, String>,
    pub requires_python: Option<String>,
    /// `Some("")` means yanked without a reason.
    pub yanked: Option<String>,
    pub core_metadata: Option<CoreMetadata>,
    pub size: Option<u64>,
    pub upload_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectIndex {
    pub name: String,
    /// Published versions from JSON API 1.1 or later. HTML indexes do not
    /// expose this information.
    pub versions: Option<Vec<String>>,
    pub files: Vec<ProjectFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedPage {
    Repository(Vec<UpstreamProject>),
    Project(ProjectIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedIndex {
    pub html: String,
    pub json: String,
}

pub fn is_valid_project_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.'))
}

fn validate_project_name(name: &str) -> Result<()> {
    is_valid_project_name(name)
        .then_some(())
        .ok_or_else(|| IndexError::InvalidProjectName(name.to_string()))
}

pub fn normalize_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut separator = false;
    for ch in name.chars().flat_map(char::to_lowercase) {
        if matches!(ch, '-' | '_' | '.') {
            if !separator {
                normalized.push('-');
                separator = true;
            }
        } else {
            normalized.push(ch);
            separator = false;
        }
    }
    normalized
}

/// Parse a repository or project page from PEP 503 HTML or PEP 691 JSON.
///
/// `fallback_name` is used by HTML project pages, which do not expose the
/// project name structurally. JSON project pages carry their own name.
pub fn parse_page(page_url: &Url, body: &[u8], fallback_name: Option<&str>) -> Result<ParsedPage> {
    let first = body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace());
    if first == Some(b'{') {
        parse_json_page(page_url, body)
    } else {
        parse_html_page(page_url, body, fallback_name)
    }
}

pub fn render_root(projects: &[ProjectLink]) -> Result<RenderedIndex> {
    for project in projects {
        validate_project_name(&project.name)?;
        validate_project_name(&project.normalized_name)?;
        if normalize_name(&project.normalized_name) != project.normalized_name {
            return Err(IndexError::InvalidProjectName(
                project.normalized_name.clone(),
            ));
        }
    }
    let mut projects = projects.to_vec();
    projects.sort_by(|a, b| a.normalized_name.cmp(&b.normalized_name));
    projects.dedup_by(|a, b| a.normalized_name == b.normalized_name);

    let links = projects
        .iter()
        .map(|project| {
            format!(
                "    <a href=\"{}/\">{}</a><br/>\n",
                html_escape::encode_double_quoted_attribute(&project.normalized_name),
                html_escape::encode_text(&project.name)
            )
        })
        .collect::<String>();
    let html = format!(
        "<!DOCTYPE html>\n<html>\n  <head>\n    <meta name=\"pypi:repository-version\" content=\"1.0\">\n  </head>\n  <body>\n{links}  </body>\n</html>\n"
    );
    let json = serde_json::to_string(&json!({
        "meta": { "api-version": "1.0" },
        "projects": projects,
    }))?;
    Ok(RenderedIndex { html, json })
}

pub fn render_project(project: &ProjectIndex) -> Result<RenderedIndex> {
    validate_project_name(&project.name)?;
    let mut files = project.files.clone();
    files.sort_by(|a, b| a.filename.cmp(&b.filename));
    files.dedup_by(|a, b| a.filename == b.filename && a.url == b.url);
    // PEP 700 requires both top-level versions and a size for every file in
    // API 1.1. HTML-only sources cannot supply either reliably, so render a
    // truthful 1.0 response unless the complete 1.1 data set is available.
    let api_1_1 = project
        .versions
        .as_ref()
        .is_some_and(|versions| files.is_empty() || !versions.is_empty())
        && files.iter().all(|file| file.size.is_some());

    let links = files
        .iter()
        .map(|file| {
            let mut attrs = String::new();
            if let Some(requires_python) = &file.requires_python {
                attrs.push_str(&format!(
                    " data-requires-python=\"{}\"",
                    html_escape::encode_double_quoted_attribute(requires_python)
                ));
            }
            if let Some(reason) = &file.yanked {
                attrs.push_str(&format!(
                    " data-yanked=\"{}\"",
                    html_escape::encode_double_quoted_attribute(reason)
                ));
            }
            if let Some(metadata) = &file.core_metadata {
                let metadata = metadata_attribute(metadata);
                let metadata = html_escape::encode_double_quoted_attribute(&metadata);
                attrs.push_str(&format!(
                    " data-core-metadata=\"{metadata}\" data-dist-info-metadata=\"{metadata}\""
                ));
            }
            let fragment = preferred_hash(&file.hashes)
                .map(|(algorithm, digest)| format!("#{algorithm}={digest}"))
                .unwrap_or_default();
            let href = format!("{}{fragment}", file.url);
            format!(
                "    <a href=\"{}\"{}>{}</a><br/>\n",
                html_escape::encode_double_quoted_attribute(&href),
                attrs,
                html_escape::encode_text(&file.filename)
            )
        })
        .collect::<String>();
    let name = html_escape::encode_text(&project.name);
    let html = format!(
        "<!DOCTYPE html>\n<html>\n  <head>\n    <meta name=\"pypi:repository-version\" content=\"1.0\">\n    <title>Links for {name}</title>\n  </head>\n  <body>\n    <h1>Links for {name}</h1>\n{links}  </body>\n</html>\n"
    );

    let json_files = files
        .iter()
        .map(|file| {
            let mut value = json!({
                "filename": file.filename,
                "url": file.url,
                "hashes": file.hashes,
                "yanked": yanked_json(file.yanked.as_deref()),
                "core-metadata": metadata_json(file.core_metadata.as_ref()),
            });
            if let Some(requires_python) = &file.requires_python {
                value["requires-python"] = Value::String(requires_python.clone());
            }
            if api_1_1 {
                value["size"] = Value::from(file.size.expect("API 1.1 requires file size"));
                if let Some(upload_time) = file
                    .upload_time
                    .as_deref()
                    .filter(|value| valid_upload_time(value))
                {
                    value["upload-time"] = Value::String(upload_time.to_string());
                }
            }
            value
        })
        .collect::<Vec<_>>();
    let mut document = json!({
        "meta": { "api-version": if api_1_1 { "1.1" } else { "1.0" } },
        "name": normalize_name(&project.name),
        "files": json_files,
    });
    if api_1_1 {
        let mut versions = project.versions.clone().unwrap_or_default();
        versions.sort();
        versions.dedup();
        document["versions"] = json!(versions);
    }
    let json = serde_json::to_string(&document)?;
    Ok(RenderedIndex { html, json })
}

#[derive(Debug, Deserialize)]
struct JsonProjectLink {
    name: String,
}

#[derive(Debug, Deserialize)]
struct JsonFile {
    filename: String,
    url: String,
    #[serde(default)]
    hashes: BTreeMap<String, String>,
    #[serde(rename = "requires-python")]
    requires_python: Option<String>,
    yanked: Option<Value>,
    #[serde(rename = "core-metadata")]
    core_metadata: Option<Value>,
    #[serde(rename = "dist-info-metadata")]
    dist_info_metadata: Option<Value>,
    // Tolerate the briefly deployed non-standard PyPI spelling, but never
    // emit it. PEP 714 defines `dist-info-metadata` as the legacy JSON key.
    #[serde(rename = "data-dist-info-metadata")]
    nonstandard_data_dist_info_metadata: Option<Value>,
    size: Option<u64>,
    #[serde(rename = "upload-time")]
    upload_time: Option<String>,
}

fn parse_json_page(page_url: &Url, body: &[u8]) -> Result<ParsedPage> {
    let value: Value = serde_json::from_slice(body)?;
    if let Some(projects) = value.get("projects") {
        let projects: Vec<JsonProjectLink> = serde_json::from_value(projects.clone())?;
        let projects = projects
            .into_iter()
            .map(|project| {
                validate_project_name(&project.name)?;
                let normalized = normalize_name(&project.name);
                let url = page_url.join(&format!("{normalized}/"))?;
                Ok(UpstreamProject {
                    name: project.name,
                    url,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(ParsedPage::Repository(projects));
    }
    if let Some(files) = value.get("files") {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or(IndexError::UnknownDocument)?
            .to_string();
        validate_project_name(&name)?;
        let versions: Option<Vec<String>> = value
            .get("versions")
            .map(|versions| serde_json::from_value(versions.clone()))
            .transpose()?;
        let files: Vec<JsonFile> = serde_json::from_value(files.clone())?;
        let files = files
            .into_iter()
            .map(|file| {
                let mut url = page_url.join(&file.url)?;
                let mut hashes = file.hashes;
                hashes.extend(fragment_hashes(url.fragment()));
                url.set_fragment(None);
                Ok(ProjectFile {
                    filename: file.filename,
                    url: url.to_string(),
                    hashes,
                    requires_python: file.requires_python,
                    yanked: parse_yanked(file.yanked),
                    core_metadata: parse_metadata(
                        file.core_metadata
                            .or(file.dist_info_metadata)
                            .or(file.nonstandard_data_dist_info_metadata),
                    ),
                    size: file.size,
                    upload_time: file.upload_time,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(ParsedPage::Project(ProjectIndex {
            name,
            versions,
            files,
        }));
    }
    Err(IndexError::UnknownDocument)
}

#[derive(Debug)]
struct HtmlAnchor {
    href: String,
    text: String,
    attrs: BTreeMap<String, Option<String>>,
}

fn parse_html_page(page_url: &Url, body: &[u8], fallback_name: Option<&str>) -> Result<ParsedPage> {
    let document = String::from_utf8_lossy(body);
    let anchors = parse_anchors(&document);
    // PEP 503 repository links are directories; project links are files. This
    // content-based distinction supports arbitrary distribution extensions.
    let is_project = anchors.iter().any(|anchor| {
        page_url
            .join(&anchor.href)
            .is_ok_and(|url| !url.path().ends_with('/'))
    });
    if !is_project {
        let mut projects = anchors
            .into_iter()
            .filter_map(|anchor| {
                let url = page_url.join(&anchor.href).ok()?;
                if !url.path().ends_with('/') {
                    return None;
                }
                let name = if anchor.text.is_empty() {
                    url.path_segments()?
                        .rfind(|segment| !segment.is_empty())?
                        .to_string()
                } else {
                    anchor.text.trim_end_matches('/').to_string()
                };
                Some(UpstreamProject { name, url })
            })
            .collect::<Vec<_>>();
        projects.sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));
        projects.dedup_by(|a, b| a.url == b.url);
        for project in &projects {
            validate_project_name(&project.name)?;
        }
        return Ok(ParsedPage::Repository(projects));
    }

    let name = fallback_name
        .ok_or(IndexError::UnknownDocument)?
        .to_string();
    validate_project_name(&name)?;
    let files = anchors
        .into_iter()
        .filter_map(|anchor| {
            let mut url = page_url.join(&anchor.href).ok()?;
            if url.path().ends_with('/') {
                return None;
            }
            let hashes = fragment_hashes(url.fragment());
            url.set_fragment(None);
            let filename = if anchor.text.is_empty() {
                url.path_segments()?.next_back().map(ToString::to_string)?
            } else {
                anchor.text.clone()
            };
            let core_metadata = anchor
                .attr("data-core-metadata")
                .or_else(|| anchor.attr("data-dist-info-metadata"))
                .map(parse_metadata_attribute)
                .or_else(|| {
                    (anchor.has_attr("data-core-metadata")
                        || anchor.has_attr("data-dist-info-metadata"))
                    .then_some(CoreMetadata::Available)
                });
            let yanked = anchor
                .has_attr("data-yanked")
                .then(|| anchor.attr("data-yanked").unwrap_or_default().to_string());
            Some(ProjectFile {
                filename,
                url: url.to_string(),
                hashes,
                requires_python: anchor.attr("data-requires-python").map(ToString::to_string),
                yanked,
                core_metadata,
                size: None,
                upload_time: None,
            })
        })
        .collect();
    Ok(ParsedPage::Project(ProjectIndex {
        name,
        versions: None,
        files,
    }))
}

impl HtmlAnchor {
    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs.get(name).and_then(Option::as_deref)
    }

    fn has_attr(&self, name: &str) -> bool {
        self.attrs.contains_key(name)
    }
}

fn parse_anchors(document: &str) -> Vec<HtmlAnchor> {
    let Ok(dom) = tl::parse(document, Default::default()) else {
        return vec![];
    };
    dom.nodes()
        .iter()
        .filter_map(|node| {
            let tl::Node::Tag(tag) = node else {
                return None;
            };
            if !tag.name().as_utf8_str().eq_ignore_ascii_case("a") {
                return None;
            }
            let attributes = tag.attributes();
            let href = attributes
                .get("href")
                .flatten()
                .map(|value| value.as_utf8_str().into_owned())?;
            let attrs = attributes
                .iter()
                .filter(|(name, _)| name.as_ref() != "href")
                .map(|(name, value)| {
                    (
                        name.as_ref().to_ascii_lowercase(),
                        value.as_ref().map(|value| value.clone().into_owned()),
                    )
                })
                .collect();
            Some(HtmlAnchor {
                href,
                text: tag.inner_text(dom.parser()).trim().to_string(),
                attrs,
            })
        })
        .collect()
}

fn fragment_hashes(fragment: Option<&str>) -> BTreeMap<String, String> {
    fragment
        .into_iter()
        .flat_map(|fragment| url::form_urlencoded::parse(fragment.as_bytes()))
        .filter(|(algorithm, digest)| !algorithm.is_empty() && !digest.is_empty())
        .map(|(algorithm, digest)| (algorithm.into_owned(), digest.into_owned()))
        .collect()
}

fn parse_yanked(value: Option<Value>) -> Option<String> {
    match value {
        Some(Value::Bool(true)) => Some(String::new()),
        Some(Value::String(reason)) => Some(reason),
        _ => None,
    }
}

fn parse_metadata(value: Option<Value>) -> Option<CoreMetadata> {
    match value {
        Some(Value::Bool(true)) => Some(CoreMetadata::Available),
        Some(Value::Object(hashes)) => Some(CoreMetadata::Hashes(
            hashes
                .into_iter()
                .filter_map(|(name, value)| value.as_str().map(|value| (name, value.to_string())))
                .collect(),
        )),
        Some(Value::String(value)) => Some(parse_metadata_attribute(&value)),
        _ => None,
    }
}

fn parse_metadata_attribute(value: &str) -> CoreMetadata {
    match value.split_once('=') {
        Some((algorithm, digest)) => CoreMetadata::Hashes(BTreeMap::from([(
            algorithm.to_string(),
            digest.to_string(),
        )])),
        None => CoreMetadata::Available,
    }
}

fn preferred_hash(hashes: &BTreeMap<String, String>) -> Option<(&str, &str)> {
    hashes
        .get_key_value("sha256")
        .or_else(|| hashes.first_key_value())
        .map(|(name, digest)| (name.as_str(), digest.as_str()))
}

fn valid_upload_time(value: &str) -> bool {
    let Some(timestamp) = value.strip_suffix('Z') else {
        return false;
    };
    if let Some((_, fraction)) = timestamp.rsplit_once('.')
        && (fraction.is_empty()
            || fraction.len() > 6
            || !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    OffsetDateTime::parse(value, &Rfc3339).is_ok()
}

fn metadata_attribute(metadata: &CoreMetadata) -> String {
    match metadata {
        CoreMetadata::Available => "true".to_string(),
        CoreMetadata::Hashes(hashes) => preferred_hash(hashes)
            .map(|(algorithm, digest)| format!("{algorithm}={digest}"))
            .unwrap_or_else(|| "true".to_string()),
    }
}

fn yanked_json(reason: Option<&str>) -> Value {
    match reason {
        None => Value::Bool(false),
        Some("") => Value::Bool(true),
        Some(reason) => Value::String(reason.to_string()),
    }
}

fn metadata_json(metadata: Option<&CoreMetadata>) -> Value {
    match metadata {
        None => Value::Bool(false),
        Some(CoreMetadata::Available) => Value::Bool(true),
        Some(CoreMetadata::Hashes(hashes)) => json!(hashes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_only_repository_and_project() {
        let root = Url::parse("https://wheels.example/simple/cpu/").unwrap();
        let page = parse_page(
            &root,
            br#"{"meta":{"api-version":"1.4"},"projects":[{"name":"Pyg_Lib"}]}"#,
            None,
        )
        .unwrap();
        let ParsedPage::Repository(projects) = page else {
            panic!("expected repository");
        };
        assert_eq!(
            projects[0].url.as_str(),
            "https://wheels.example/simple/cpu/pyg-lib/"
        );

        let project = parse_page(
            &projects[0].url,
            br#"{"meta":{"api-version":"1.4"},"name":"pyg-lib","versions":["1.0"],"files":[{"filename":"pyg.whl","url":"https://wheels.example/artifacts/abc/pyg.whl","hashes":{"sha256":"abc"},"core-metadata":{"sha256":"def"},"size":42}]}"#,
            Some("pyg-lib"),
        )
        .unwrap();
        let ParsedPage::Project(project) = project else {
            panic!("expected project");
        };
        assert_eq!(project.versions, Some(vec!["1.0".to_string()]));
        assert_eq!(project.files[0].size, Some(42));
        assert_eq!(project.files[0].hashes["sha256"], "abc");
    }

    #[test]
    fn parses_relative_html_artifacts() {
        let url = Url::parse("https://pypi.example/nvidia-cublas/").unwrap();
        let page = parse_page(
            &url,
            br#"<a href="nvidia_cublas-1.0.whl#sha256=abc">nvidia_cublas-1.0.whl</a>"#,
            Some("nvidia-cublas"),
        )
        .unwrap();
        let ParsedPage::Project(project) = page else {
            panic!("expected project");
        };
        assert_eq!(
            project.files[0].url,
            "https://pypi.example/nvidia-cublas/nvidia_cublas-1.0.whl"
        );
        assert_eq!(project.files[0].hashes["sha256"], "abc");
    }

    #[test]
    fn renders_matching_html_and_json() {
        let rendered = render_project(&ProjectIndex {
            name: "Demo_Package".to_string(),
            versions: Some(vec!["1.0".to_string(), "1.0".to_string()]),
            files: vec![ProjectFile {
                filename: "demo-1.0.whl".to_string(),
                url: "/wheels/demo-1.0.whl".to_string(),
                hashes: BTreeMap::from([("sha256".to_string(), "abc".to_string())]),
                requires_python: Some(">=3.9".to_string()),
                yanked: None,
                core_metadata: None,
                size: Some(42),
                upload_time: Some("2026-08-29T00:00:00.123456Z".to_string()),
            }],
        })
        .unwrap();
        assert!(rendered.html.contains("#sha256=abc"));
        let json: Value = serde_json::from_str(&rendered.json).unwrap();
        assert_eq!(json["meta"]["api-version"], "1.1");
        assert_eq!(json["name"], "demo-package");
        assert_eq!(json["versions"], json!(["1.0"]));
        assert_eq!(json["files"][0]["url"], "/wheels/demo-1.0.whl");
        assert_eq!(json["files"][0]["size"], 42);
        assert_eq!(
            json["files"][0]["upload-time"],
            "2026-08-29T00:00:00.123456Z"
        );
        assert!(json["files"][0].get("data-dist-info-metadata").is_none());
    }

    #[test]
    fn renders_api_1_0_when_pep_700_fields_are_incomplete() {
        let rendered = render_project(&ProjectIndex {
            name: "demo".to_string(),
            versions: None,
            files: vec![ProjectFile {
                filename: "demo-1.0.whl".to_string(),
                url: "/wheels/demo-1.0.whl".to_string(),
                hashes: BTreeMap::new(),
                requires_python: None,
                yanked: None,
                core_metadata: None,
                size: Some(42),
                upload_time: Some("2026-08-29T00:00:00Z".to_string()),
            }],
        })
        .unwrap();
        let json: Value = serde_json::from_str(&rendered.json).unwrap();
        assert_eq!(json["meta"]["api-version"], "1.0");
        assert!(json.get("versions").is_none());
        assert!(json["files"][0].get("size").is_none());
        assert!(json["files"][0].get("upload-time").is_none());
        assert!(
            rendered
                .html
                .contains("name=\"pypi:repository-version\" content=\"1.0\"")
        );

        let missing_size = render_project(&ProjectIndex {
            name: "demo".to_string(),
            versions: Some(vec!["1.0".to_string()]),
            files: vec![ProjectFile {
                filename: "demo-1.0.whl".to_string(),
                url: "/wheels/demo-1.0.whl".to_string(),
                hashes: BTreeMap::new(),
                requires_python: None,
                yanked: None,
                core_metadata: None,
                size: None,
                upload_time: None,
            }],
        })
        .unwrap();
        let json: Value = serde_json::from_str(&missing_size.json).unwrap();
        assert_eq!(json["meta"]["api-version"], "1.0");
        assert!(json.get("versions").is_none());
    }

    #[test]
    fn omits_invalid_upload_time() {
        let rendered = render_project(&ProjectIndex {
            name: "demo".to_string(),
            versions: Some(vec!["1.0".to_string()]),
            files: vec![ProjectFile {
                filename: "demo-1.0.whl".to_string(),
                url: "/wheels/demo-1.0.whl".to_string(),
                hashes: BTreeMap::new(),
                requires_python: None,
                yanked: None,
                core_metadata: None,
                size: Some(42),
                upload_time: Some("2026-08-29T00:00:00.1234567Z".to_string()),
            }],
        })
        .unwrap();
        let json: Value = serde_json::from_str(&rendered.json).unwrap();
        assert_eq!(json["meta"]["api-version"], "1.1");
        assert!(json["files"][0].get("upload-time").is_none());
    }

    #[test]
    fn rejects_invalid_project_names() {
        let url = Url::parse("https://pypi.example/simple/").unwrap();
        let error = parse_page(
            &url,
            "{\"meta\":{\"api-version\":\"1.0\"},\"projects\":[{\"name\":\"café\"}]}".as_bytes(),
            None,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::InvalidProjectName(_)));
    }

    #[test]
    fn parses_standard_legacy_json_metadata_key() {
        let url = Url::parse("https://pypi.example/demo/").unwrap();
        let page = parse_page(
            &url,
            br#"{"meta":{"api-version":"1.0"},"name":"demo","files":[{"filename":"demo.whl","url":"demo.whl","hashes":{},"dist-info-metadata":true}]}"#,
            Some("demo"),
        )
        .unwrap();
        let ParsedPage::Project(project) = page else {
            panic!("expected project");
        };
        assert_eq!(
            project.files[0].core_metadata,
            Some(CoreMetadata::Available)
        );
    }
}
