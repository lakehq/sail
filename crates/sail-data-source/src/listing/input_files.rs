// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use url::Url;

use crate::listing::registered::prune_paths;
use crate::listing::table::ListingTableSource;
use crate::listing::utils::{is_direct_input, list_all_files};

/// Deduplicated, percent-encoded URIs of the files composing `plan` (Spark's `DataFrame.inputFiles`).
pub async fn input_files(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
    // Optimize first, like Spark, so eliminated scans (e.g. `WHERE false`) contribute no files.
    let state = ctx.state();
    let plan = state.optimize(&plan)?;

    // Collect the listing table sources referenced anywhere in the plan.
    let mut configs = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<ListingTableSource>()
        {
            let mut config = source.config().clone();
            if source.registered_read.is_some() {
                let (filters, _) =
                    source.split_partition_filters(unnormalize_cols(scan.filters.clone()));
                prune_paths(&mut config, &state, &filters)?;
            }
            configs.push(config);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut files: Vec<String> = vec![];
    // List each (path, filter) once so a repeated scan (e.g. `df.union(df)`) isn't re-listed.
    let mut listed: HashSet<(String, Option<String>, bool)> = HashSet::new();

    for config in &configs {
        let path_glob_filter = config.path_glob_filter.as_ref();
        let direct_only = config.direct_files_only();
        for (table_path, _) in &config.table_paths {
            if !listed.insert((
                table_path.as_str().to_string(),
                path_glob_filter.map(|filter| filter.as_str().to_string()),
                direct_only,
            )) {
                continue;
            }
            let store = ctx.runtime_env().object_store(table_path)?;
            let base = Url::parse(table_path.object_store().as_str())
                .map_err(|e| DataFusionError::Internal(format!("invalid object store URL: {e}")))?;
            let metas = list_all_files(table_path, &state, store.as_ref(), path_glob_filter)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            for meta in metas {
                if direct_only && !is_direct_input(table_path, &meta.location) {
                    continue;
                }
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
