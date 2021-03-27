use chrono::DateTime;
use reqwest::Client;

use crate::error::{Error, Result};

pub async fn get_yaml_url<'a>(base_url: &'a str, client: &'a Client) -> Result<String> {
    let version_matcher = regex::Regex::new("ghcupURL.*(?P<url>https://.*yaml)").unwrap();

    let ghcup_version_module = client
        .get(&format!("{}/lib/GHCup/Version.hs", base_url))
        .send()
        .await?
        .text()
        .await?;

    version_matcher
        .captures(ghcup_version_module.as_str())
        .and_then(|capture| capture.name("url"))
        .map(|group| String::from(group.as_str()))
        .ok_or_else(|| {
            Error::ProcessError(String::from(
                "unable to parse ghcup version from haskell src",
            ))
        })
}

pub async fn get_last_modified<'a>(client: &'a Client, url: &'a str) -> Result<Option<u64>> {
    Ok(client
        .head(url)
        .send()
        .await?
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|value| std::str::from_utf8(value.as_bytes()).ok())
        .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
        .map(|dt| dt.timestamp() as u64))
}
