use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;

use crate::io::StoreContext;

/// Parse an Iceberg metadata file version number from a metadata file path.
///
/// Accepts both the versioned filename format (`v<N>.metadata.json`) and
/// the UUID-based format (`<N>-<uuid>.metadata.json`).
pub fn parse_metadata_version_from_path(path: &str) -> Option<i32> {
    let filename = path.rsplit('/').next()?;
    if let Some(version) = filename
        .strip_prefix('v')
        .and_then(|s| s.strip_suffix(".metadata.json"))
    {
        return version.parse::<i32>().ok();
    }
    filename
        .split_once('-')
        .and_then(|(version, _)| version.parse::<i32>().ok())
}

/// List all metadata files in the table's `metadata/` prefix that correspond to the
/// given version number.
pub async fn metadata_files_for_version(
    store_ctx: &StoreContext,
    version: i32,
) -> Result<Vec<String>> {
    let prefix = object_store::path::Path::from("metadata/");
    let mut stream = store_ctx.prefixed.list(Some(&prefix));
    let mut matches = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if parse_metadata_version_from_path(meta.location.as_ref()) == Some(version) {
            matches.push(meta.location.to_string());
        }
    }
    Ok(matches)
}
