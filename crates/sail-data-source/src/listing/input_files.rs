// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use url::Url;

use crate::listing::registered::RegisteredTableSource;
use crate::listing::table::ListingTableSource;
use crate::listing::utils::{is_direct_input, list_all_files, list_partition_files};

/// Deduplicated, percent-encoded URIs of the files composing `plan` (Spark's `DataFrame.inputFiles`).
pub async fn input_files(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
    // Optimize first, like Spark, so eliminated scans (e.g. `WHERE false`) contribute no files.
    let state = ctx.state();
    let plan = state.optimize(&plan)?;

    // Collect the listing table sources referenced anywhere in the plan.
    let mut scans = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            scans.push((scan.source.clone(), unnormalize_cols(scan.filters.clone())));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut files: Vec<String> = vec![];
    // List each path with the same options once, even when a scan is repeated.
    let mut listed = HashSet::new();

    for (source, filters) in scans {
        let source = if let Some(registered) = source.downcast_ref::<RegisteredTableSource>() {
            let Some(resolved) = registered.resolve(&state, &filters).await? else {
                continue;
            };
            resolved
        } else {
            source
        };
        let Some(source) = source.downcast_ref::<ListingTableSource>() else {
            continue;
        };
        let config = source.config();
        let path_glob_filter = config.path_glob_filter.as_ref();
        let direct_only = config.direct_files_only();
        for (table_path, values) in &config.table_paths {
            if !listed.insert((
                table_path.as_str().to_string(),
                path_glob_filter.map(|filter| filter.as_str().to_string()),
                direct_only,
                config.partition_directories_only,
                values.is_some(),
            )) {
                continue;
            }
            let store = ctx.runtime_env().object_store(table_path)?;
            let base = Url::parse(table_path.object_store().as_str())
                .map_err(|e| DataFusionError::Internal(format!("invalid object store URL: {e}")))?;
            let metas = if values.is_some() {
                list_partition_files(table_path, store.as_ref(), path_glob_filter).await?
            } else {
                list_all_files(
                    table_path,
                    &state,
                    store.as_ref(),
                    path_glob_filter,
                    config.partition_directories_only,
                )
                .await?
            }
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
