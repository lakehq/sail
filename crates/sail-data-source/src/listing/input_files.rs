use std::any::Any;
use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use glob::Pattern;
use url::Url;

use crate::listing::table::ListingTableSource;
use crate::listing::utils::list_all_files;

/// Deduplicated, percent-encoded URIs of the files composing `plan` (Spark's `DataFrame.inputFiles`).
pub async fn input_files(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
    // Optimize first, like Spark, so eliminated scans (e.g. `WHERE false`) contribute no files.
    let state = ctx.state();
    let plan = state.optimize(&plan)?;

    // Collect the listing table sources referenced anywhere in the plan.
    let mut listing_sources: Vec<ListingTableSource> = vec![];
    plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = node {
            // Upcast to `Any` to downcast to the concrete source.
            let source: &dyn Any = source.as_ref();
            if let Some(listing) = source.downcast_ref::<ListingTableSource>() {
                listing_sources.push(listing.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut files: Vec<String> = vec![];
    // List each (path, filter) once so a repeated scan (e.g. `df.union(df)`) isn't re-listed.
    let mut listed: HashSet<(String, Option<String>)> = HashSet::new();

    for source in &listing_sources {
        // Apply the format-level name filter (e.g. binary `pathGlobFilter`).
        let raw_glob = source.config().read_format.input_file_name_glob();
        let name_glob = raw_glob
            .map(Pattern::new)
            .transpose()
            .map_err(|e| DataFusionError::Plan(format!("invalid path glob filter: {e}")))?;
        for table_path in &source.config().table_paths {
            if !listed.insert((
                table_path.as_str().to_string(),
                raw_glob.map(str::to_string),
            )) {
                continue;
            }
            let store = ctx.runtime_env().object_store(table_path)?;
            let base = Url::parse(table_path.object_store().as_str())
                .map_err(|e| DataFusionError::Internal(format!("invalid object store URL: {e}")))?;
            let metas = list_all_files(table_path, &state, store.as_ref())
                .await?
                .try_filter(|meta| {
                    let name = meta.location.filename().unwrap_or_default();
                    let included = name_glob.as_ref().is_none_or(|glob| glob.matches(name));
                    futures::future::ready(included)
                })
                .try_collect::<Vec<_>>()
                .await?;
            for meta in metas {
                // Percent-encode the path, as Spark returns encoded URIs.
                let mut uri = base.clone();
                uri.path_segments_mut()
                    .map_err(|()| {
                        DataFusionError::Internal("object store URL cannot be a base".to_string())
                    })?
                    .clear()
                    .extend(meta.location.parts().map(|part| part.as_ref().to_string()));
                files.push(uri.to_string());
            }
        }
    }

    // Deduplicate, matching Spark's semantics.
    files.sort();
    files.dedup();

    Ok(files)
}
