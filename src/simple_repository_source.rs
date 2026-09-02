//! Generic metadata-only PyPA Simple Repository source.
//!
//! The crawler accepts PEP 503 HTML and PEP 691 JSON, follows nested repository
//! indexes by content, emits HTML and JSON views, and never downloads artifacts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use pypa_simple::{
    HTML_CONTENT_TYPE, JSON_CONTENT_TYPE, ParsedPage, ProjectIndex, ProjectLink, RenderedIndex,
    UpstreamProject, normalize_name, parse_page, render_project, render_root,
};
use reqwest::Client;
use slog::{info, warn};
use structopt::StructOpt;
use url::Url;

use crate::common::{Mission, SnapshotConfig, SnapshotPath};
use crate::error::{Error, Result};
use crate::stream_pipe::{ByteObject, ByteStream};
use crate::traits::{SnapshotStorage, SourceStorage};
use crate::utils::{bar, unix_time};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlPrefixRewrite {
    from: String,
    to: String,
}

impl FromStr for UrlPrefixRewrite {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (from, to) = value.split_once('=').ok_or_else(|| {
            "URL rewrite must be FROM=TO (for example https://host/files/=/mirror/)".to_string()
        })?;
        if from.is_empty() || to.is_empty() {
            return Err("URL rewrite FROM and TO must not be empty".to_string());
        }
        if !from.ends_with('/') || !to.ends_with('/') {
            return Err("URL rewrite FROM and TO must end with /".to_string());
        }
        Url::parse(from).map_err(|error| format!("invalid rewrite source URL: {error}"))?;
        if !to.starts_with('/') {
            Url::parse(to).map_err(|error| format!("invalid rewrite destination URL: {error}"))?;
        }
        Ok(Self {
            from: from.to_string(),
            to: to.to_string(),
        })
    }
}

#[derive(Debug, Clone, StructOpt)]
pub struct SimpleRepository {
    #[structopt(long, help = "Root URL of a PEP 503/691 repository index")]
    pub index_base: String,

    #[structopt(
        long = "rewrite-url-prefix",
        help = "Rewrite artifact URL prefix FROM=TO; repeat for multiple origins"
    )]
    pub rewrites: Vec<UrlPrefixRewrite>,

    #[structopt(
        long,
        default_value = "3",
        help = "Maximum nested repository-index depth"
    )]
    pub max_depth: usize,

    #[structopt(skip)]
    objects: HashMap<String, GeneratedObject>,
}

#[derive(Debug, Clone)]
struct GeneratedObject {
    body: Bytes,
    content_type: &'static str,
}

/// Shares immutable rendered bodies that appear at multiple object keys.
///
/// Nested repositories commonly repeat the same project index under many
/// channels, so retaining one allocation per key can make the logical output
/// size become the process's resident memory.
#[derive(Debug, Default)]
struct RenderedBodyPool {
    bodies: HashMap<Bytes, ()>,
    logical_bytes: u64,
    unique_bytes: u64,
}

impl RenderedBodyPool {
    fn intern(&mut self, body: String) -> Bytes {
        let body = Bytes::from(body);
        self.logical_bytes += body.len() as u64;
        match self.bodies.entry(body.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.key().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.unique_bytes += body.len() as u64;
                entry.insert(());
                body
            }
        }
    }
}

#[derive(Debug)]
struct IndexNode {
    prefix: String,
    projects: Vec<UpstreamProject>,
    depth: usize,
}

pub fn s3_index_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    if prefix == "simple" || prefix.ends_with("/simple") {
        prefix.to_string()
    } else {
        format!("{prefix}/simple")
    }
}

enum PageFetchResult {
    Found(Bytes),
    Missing,
    Forbidden,
}

async fn fetch_page(client: &Client, url: &Url) -> Result<PageFetchResult> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let response = client.get(url.clone()).send().await?;
        match response.status().as_u16() {
            404 | 410 => return Ok(PageFetchResult::Missing),
            403 => return Ok(PageFetchResult::Forbidden),
            _ => {}
        }
        if !response.status().is_success() {
            return Err(Error::PipeError(format!(
                "failed to fetch package index {url}: {}",
                response.status()
            )));
        }
        Ok(PageFetchResult::Found(response.bytes().await?))
    })
    .await
    .map_err(|_| Error::TimeoutError(()))?
}

fn parse_index_page(url: &Url, body: &[u8], fallback_name: Option<&str>) -> Result<ParsedPage> {
    parse_page(url, body, fallback_name)
        .map_err(|error| Error::PipeError(format!("invalid package index at {url}: {error}")))
}

fn output_segment(project: &UpstreamProject) -> Result<String> {
    let segment = project
        .url
        .path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .ok_or_else(|| {
            Error::PipeError(format!(
                "package index child has no path segment: {}",
                project.url
            ))
        })?;
    if segment == "."
        || segment == ".."
        || segment.starts_with('.')
        || segment.starts_with('*')
        || segment.ends_with(':')
        || segment.ends_with('<')
        || segment.ends_with('>')
    {
        return Err(Error::PipeError(format!(
            "package index child has unsafe path segment {segment:?}: {}",
            project.url
        )));
    }
    Ok(segment.to_string())
}

fn join_prefix(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

fn rewrite_project(project: &mut ProjectIndex, rewrites: &[UrlPrefixRewrite]) {
    for file in &mut project.files {
        if let Some(rule) = rewrites
            .iter()
            .filter(|rule| file.url.starts_with(&rule.from))
            .max_by_key(|rule| rule.from.len())
        {
            let rest = &file.url[rule.from.len()..];
            file.url = format!("{}{rest}", rule.to);
        }
    }
}

fn object_key(prefix: &str, filename: &str) -> String {
    join_prefix(prefix, filename)
}

fn insert_rendered(
    objects: &mut HashMap<String, GeneratedObject>,
    body_pool: &mut RenderedBodyPool,
    prefix: &str,
    rendered: RenderedIndex,
) {
    objects.insert(
        object_key(prefix, "index.v1_html"),
        GeneratedObject {
            body: body_pool.intern(rendered.html),
            content_type: HTML_CONTENT_TYPE,
        },
    );
    objects.insert(
        object_key(prefix, "index.v1_json"),
        GeneratedObject {
            body: body_pool.intern(rendered.json),
            content_type: JSON_CONTENT_TYPE,
        },
    );
}

#[async_trait]
impl SnapshotStorage<SnapshotPath> for SimpleRepository {
    async fn snapshot(
        &mut self,
        mission: Mission,
        config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotPath>> {
        let logger = mission.logger;
        let progress = mission.progress;
        let client = mission.client;
        let root_url = Url::parse(&format!("{}/", self.index_base.trim_end_matches('/')))
            .map_err(|error| Error::ConfigureError(format!("invalid index base URL: {error}")))?;

        info!(logger, "fetching package repository index"; "url" => root_url.as_str());
        let root_body = match fetch_page(&client, &root_url).await? {
            PageFetchResult::Found(body) => body,
            PageFetchResult::Missing => {
                return Err(Error::PipeError(format!(
                    "package repository index not found: {root_url}"
                )));
            }
            PageFetchResult::Forbidden => {
                return Err(Error::PipeError(format!(
                    "package repository index is forbidden (403): {root_url}"
                )));
            }
        };
        let ParsedPage::Repository(root_projects) = parse_index_page(&root_url, &root_body, None)?
        else {
            return Err(Error::PipeError(format!(
                "index base is a project page, not a repository: {root_url}"
            )));
        };

        let mut objects = HashMap::new();
        let mut body_pool = RenderedBodyPool::default();
        let mut visited_indexes = HashSet::from([root_url.to_string()]);
        let mut indexes = VecDeque::from([IndexNode {
            prefix: String::new(),
            projects: root_projects,
            depth: 0,
        }]);

        while let Some(index) = indexes.pop_front() {
            progress.set_length(index.projects.len() as u64);
            progress.set_style(bar());
            let mut children = stream::iter(index.projects.into_iter().map(|project| {
                let client = client.clone();
                let progress = progress.clone();
                async move {
                    progress.set_message(&project.name);
                    let result = fetch_page(&client, &project.url).await;
                    progress.inc(1);
                    result.map(|result| (project, result))
                }
            }))
            .buffer_unordered(config.concurrent_resolve);

            let mut rendered_projects = Vec::new();
            while let Some(result) = children.next().await {
                let (upstream_project, result) = result?;
                let body = match result {
                    PageFetchResult::Found(body) => body,
                    PageFetchResult::Missing => {
                        warn!(logger, "skipping vanished package index"; "url" => upstream_project.url.as_str());
                        continue;
                    }
                    PageFetchResult::Forbidden => {
                        warn!(
                            logger,
                            "skipping forbidden package index; fix upstream access or remove it from the parent listing";
                            "url" => upstream_project.url.as_str(),
                            "status" => 403
                        );
                        continue;
                    }
                };
                let page =
                    parse_index_page(&upstream_project.url, &body, Some(&upstream_project.name))?;
                match page {
                    ParsedPage::Project(mut project) => {
                        rewrite_project(&mut project, &self.rewrites);
                        let normalized = normalize_name(&project.name);
                        insert_rendered(
                            &mut objects,
                            &mut body_pool,
                            &join_prefix(&index.prefix, &normalized),
                            render_project(&project).map_err(|error| {
                                Error::PipeError(format!("failed to render project index: {error}"))
                            })?,
                        );
                        rendered_projects
                            .push(ProjectLink::with_normalized(project.name, normalized));
                    }
                    ParsedPage::Repository(projects) => {
                        if !visited_indexes.insert(upstream_project.url.to_string()) {
                            warn!(logger, "skipping duplicate package index"; "url" => upstream_project.url.as_str());
                            continue;
                        }
                        if index.depth >= self.max_depth {
                            return Err(Error::PipeError(format!(
                                "package index nesting exceeds --max-depth at {}",
                                upstream_project.url
                            )));
                        }
                        indexes.push_back(IndexNode {
                            prefix: join_prefix(&index.prefix, &output_segment(&upstream_project)?),
                            projects,
                            depth: index.depth + 1,
                        });
                    }
                }
            }

            insert_rendered(
                &mut objects,
                &mut body_pool,
                &index.prefix,
                render_root(&rendered_projects).map_err(|error| {
                    Error::PipeError(format!("failed to render repository index: {error}"))
                })?,
            );
        }

        let mut snapshot = objects
            .keys()
            .cloned()
            .map(SnapshotPath::force)
            .collect::<Vec<_>>();
        snapshot.sort_by(|a, b| a.0.cmp(&b.0));
        self.objects = objects;
        progress.finish_with_message("done");
        info!(
            logger,
            "generated {} package index objects", snapshot.len();
            "logical_bytes" => body_pool.logical_bytes,
            "unique_bytes" => body_pool.unique_bytes,
            "unique_bodies" => body_pool.bodies.len(),
        );
        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("simple-repository, {}", self.index_base)
    }
}

#[async_trait]
impl SourceStorage<SnapshotPath, ByteStream> for SimpleRepository {
    async fn get_object(&self, snapshot: &SnapshotPath, _mission: &Mission) -> Result<ByteStream> {
        let object = self.objects.get(&snapshot.0).ok_or_else(|| {
            Error::PipeError(format!("unknown generated package index: {}", snapshot.0))
        })?;
        Ok(ByteStream {
            object: ByteObject::Bytes(Some(object.body.clone())),
            length: object.body.len() as u64,
            modified_at: unix_time(),
            content_type: Some(object.content_type.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indicatif::ProgressBar;
    use slog::{Discard, Logger, o};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    async fn spawn_fixture_server(
        routes: Vec<(&'static str, u16, &'static str)>,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request_count = routes.len();
        let routes = routes
            .into_iter()
            .map(|(path, status, body)| (path, (status, body)))
            .collect::<HashMap<_, _>>();
        let server = tokio::spawn(async move {
            for _ in 0..request_count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 2048];
                while !request.ends_with(b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let request = String::from_utf8(request).unwrap();
                let path = request
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let (status, body) = routes
                    .get(path)
                    .unwrap_or_else(|| panic!("unexpected fixture request: {path}"));
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        (format!("http://{address}/simple"), server)
    }

    fn test_source(index_base: String) -> SimpleRepository {
        SimpleRepository {
            index_base,
            rewrites: vec![],
            max_depth: 3,
            objects: HashMap::new(),
        }
    }

    fn test_mission() -> Mission {
        Mission {
            progress: ProgressBar::hidden(),
            client: Client::new(),
            logger: Logger::root(Discard, o!()),
        }
    }

    #[test]
    fn longest_url_prefix_wins() {
        let mut project = ProjectIndex {
            name: "demo".to_string(),
            versions: None,
            files: vec![pypa_simple::ProjectFile {
                filename: "demo.whl".to_string(),
                url: "https://host/files/cpu/demo.whl".to_string(),
                hashes: Default::default(),
                requires_python: None,
                yanked: None,
                core_metadata: None,
                size: None,
                upload_time: None,
            }],
        };
        rewrite_project(
            &mut project,
            &[
                "https://host/files/=/generic/".parse().unwrap(),
                "https://host/files/cpu/=/cpu/".parse().unwrap(),
            ],
        );
        assert_eq!(project.files[0].url, "/cpu/demo.whl");
    }

    #[tokio::test]
    async fn crawls_nested_html_and_json_indexes_without_fetching_artifacts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 2048];
                while !request.ends_with(b"\r\n\r\n") {
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&buffer[..read]);
                }
                let first_line = String::from_utf8(request)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string();
                let path = first_line.split_whitespace().nth(1).unwrap();
                let body = match path {
                    "/simple/" => r#"<a href="torch/">torch</a><a href="cu132/">cu132</a>"#,
                    "/simple/torch/" => {
                        r#"<a href="https://files.example/artifacts/torch%2Bcpu.whl#sha256=abc">torch+cpu.whl</a>"#
                    }
                    "/simple/cu132/" => {
                        r#"{"meta":{"api-version":"1.4"},"projects":[{"name":"Flash_Attn"}]}"#
                    }
                    "/simple/cu132/flash-attn/" => {
                        r#"{"meta":{"api-version":"1.4"},"name":"flash-attn","files":[{"filename":"flash.whl","url":"https://files.example/artifacts/flash.whl","hashes":{"sha256":"def"}}]}"#
                    }
                    path => panic!("unexpected fixture request: {path}"),
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });

        let mut source = SimpleRepository {
            index_base: format!("http://{address}/simple"),
            rewrites: vec!["https://files.example/artifacts/=/wheels/".parse().unwrap()],
            max_depth: 3,
            objects: HashMap::new(),
        };
        let snapshot = source
            .snapshot(
                Mission {
                    progress: ProgressBar::hidden(),
                    client: Client::new(),
                    logger: Logger::root(Discard, o!()),
                },
                &SnapshotConfig {
                    concurrent_resolve: 4,
                },
            )
            .await
            .unwrap();
        server.await.unwrap();

        let keys = snapshot
            .into_iter()
            .map(|snapshot| snapshot.0)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 8);
        assert!(keys.contains(&"torch/index.v1_json".to_string()));
        assert!(keys.contains(&"cu132/index.v1_html".to_string()));
        assert!(keys.contains(&"cu132/flash-attn/index.v1_json".to_string()));
        assert!(keys.iter().all(|key| !key.ends_with(".whl")));
        let project = source
            .objects
            .get("cu132/flash-attn/index.v1_json")
            .unwrap();
        let project: serde_json::Value = serde_json::from_slice(&project.body).unwrap();
        assert_eq!(project["meta"]["api-version"], "1.0");
        assert_eq!(project["files"][0]["url"], "/wheels/flash.whl");
        assert!(project.get("versions").is_none());
        assert!(project["files"][0].get("size").is_none());
    }

    #[tokio::test]
    async fn snapshot_interns_duplicate_rendered_bodies() {
        let project = r#"<a href="https://files.example/demo.whl">demo.whl</a>"#;
        let (index_base, server) = spawn_fixture_server(vec![
            (
                "/simple/",
                200,
                r#"<a href="cpu/">cpu</a><a href="nightly/">nightly</a>"#,
            ),
            ("/simple/cpu/", 200, r#"<a href="demo/">demo</a>"#),
            ("/simple/cpu/demo/", 200, project),
            ("/simple/nightly/", 200, r#"<a href="demo/">demo</a>"#),
            ("/simple/nightly/demo/", 200, project),
        ])
        .await;
        let mut source = test_source(index_base);

        source
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 2,
                },
            )
            .await
            .unwrap();
        server.await.unwrap();

        for filename in ["index.v1_html", "index.v1_json"] {
            let cpu = &source.objects[&format!("cpu/demo/{filename}")].body;
            let nightly = &source.objects[&format!("nightly/demo/{filename}")].body;
            assert_eq!(cpu, nightly);
            assert_eq!(cpu.as_ptr(), nightly.as_ptr());
        }
    }

    #[tokio::test]
    async fn snapshot_skips_forbidden_child_but_keeps_valid_projects() {
        let (index_base, server) = spawn_fixture_server(vec![
            (
                "/simple/",
                200,
                r#"<a href="valid/">valid</a><a href="forbidden/">forbidden</a>"#,
            ),
            (
                "/simple/valid/",
                200,
                r#"<a href="https://files.example/valid.whl">valid.whl</a>"#,
            ),
            ("/simple/forbidden/", 403, "forbidden"),
        ])
        .await;
        let mut source = test_source(index_base);

        let snapshot = source
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 2,
                },
            )
            .await
            .unwrap();
        server.await.unwrap();

        let keys = snapshot
            .into_iter()
            .map(|snapshot| snapshot.0)
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                "index.v1_html",
                "index.v1_json",
                "valid/index.v1_html",
                "valid/index.v1_json",
            ]
        );
        assert!(keys.iter().all(|key| !key.contains("forbidden")));
        let root: serde_json::Value =
            serde_json::from_slice(&source.objects.get("index.v1_json").unwrap().body).unwrap();
        assert_eq!(root["projects"], serde_json::json!([{"name": "valid"}]));
    }

    #[tokio::test]
    async fn snapshot_rejects_forbidden_root() {
        let (index_base, server) = spawn_fixture_server(vec![("/simple/", 403, "forbidden")]).await;
        let mut source = test_source(index_base);

        let error = source
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 1,
                },
            )
            .await
            .unwrap_err();
        server.await.unwrap();

        assert!(error.to_string().contains("403"), "{error:?}");
    }

    #[tokio::test]
    async fn snapshot_rejects_server_error_from_child() {
        let (index_base, server) = spawn_fixture_server(vec![
            ("/simple/", 200, r#"<a href="broken/">broken</a>"#),
            ("/simple/broken/", 503, "unavailable"),
        ])
        .await;
        let mut source = test_source(index_base);

        let error = source
            .snapshot(
                test_mission(),
                &SnapshotConfig {
                    concurrent_resolve: 1,
                },
            )
            .await
            .unwrap_err();
        server.await.unwrap();

        assert!(error.to_string().contains("503"), "{error:?}");
    }
}
