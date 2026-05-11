use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;

use crate::io::StoreContext;
use crate::table::metadata_loader::metadata_file_version_from_path;

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
        if metadata_file_version_from_path(meta.location.as_ref()) == Some(version) {
            matches.push(meta.location.to_string());
        }
    }
    Ok(matches)
}
