use reqwest::Client;

use crate::error::{Error, Result};
use super::parser::CONFIG_VERSION;

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
        .and_then(|url| {
            if !url.ends_with(format!("{}.yaml", CONFIG_VERSION).as_str()) {
                Err(Error::ProcessError(String::from("unsupported version of ghcup config")))
            } else {
                Ok(url)
            }
        })
}
