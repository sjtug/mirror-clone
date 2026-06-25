use std::collections::HashSet;

use serde_yaml::Value;

use super::utils::Version;
use crate::error::Result;

pub const EXPECTED_CONFIG_VERSION: Version = Version::new(0, 1, 0);

pub fn parse_uris_from_yaml(yaml_data: &[u8], include_old_versions: bool) -> Result<Vec<String>> {
    let yaml_value: serde_yaml::Value = serde_yaml::from_slice(yaml_data)?;
    let mut dl_uris = HashSet::new();
    collect_dl_uris(&yaml_value, include_old_versions, &mut dl_uris);
    Ok(dl_uris.into_iter().map(|s| s.to_string()).collect())
}

fn collect_dl_uris<'a>(
    value: &'a Value,
    include_old_versions: bool,
    dl_uris: &mut HashSet<&'a str>,
) {
    let Some(mapping) = value.as_mapping() else {
        if let Some(sequence) = value.as_sequence() {
            for item in sequence {
                collect_dl_uris(item, include_old_versions, dl_uris);
            }
        }
        return;
    };

    if !include_old_versions && has_old_tag(value) {
        return;
    }

    for (key, value) in mapping {
        if key.as_str() == Some("dlUri")
            && let Some(uri) = value.as_str()
        {
            dl_uris.insert(uri);
        }

        collect_dl_uris(value, include_old_versions, dl_uris);
    }
}

fn has_old_tag(value: &Value) -> bool {
    let Some(tags) = value.get("viTags").and_then(|v| v.as_sequence()) else {
        return false;
    };
    tags.iter().any(|s| s.as_str() == Some("old"))
}
