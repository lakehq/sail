use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;

use crate::datasource::scan::{build_file_scan_config, FileScanParams, TableStatsMode};
use crate::datasource::{df_logical_schema, simplify_expr, DeltaScanConfig};
use crate::options::TableDeltaOptions;
use crate::physical_plan::planner::metadata_predicate::{
    build_metadata_filter, predicate_requires_stats,
};
use crate::physical_plan::planner::utils::LogReplayOptions;
use crate::physical_plan::planner::{DeltaTableConfig as PlannerTableConfig, PlannerContext};
use crate::physical_plan::{DeltaDiscoveryExec, DeltaScanByAddsExec, RelaxedTzCastExec};
use crate::schema::get_physical_schema;
use crate::spec::{Add, ColumnMappingMode, StructType};
use crate::storage::LogStoreRef;
use crate::table::DeltaSnapshot;

fn expand_scan_projection(
    full_logical_schema: &ArrowSchema,
    used_columns: &[usize],
    filters: &[Expr],
    table_partition_cols: &[String],
) -> Result<Vec<usize>> {
    let mut scan_projection = used_columns.to_vec();
    let filter_expr = conjunction(filters.iter().cloned());

    // Partition-filter columns can be removed from the original projection by the optimizer,
    // but they still need to be present for predicate pruning and scan planning.
    if let Some(expr) = &filter_expr {
        for column in expr.column_refs() {
            let idx = full_logical_schema.index_of(column.name.as_str())?;
            if !scan_projection.contains(&idx) {
                scan_projection.push(idx);
            }
        }
    }

    for partition_col in table_partition_cols {
        if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str()) {
            if !scan_projection.contains(&idx) {
                scan_projection.push(idx);
            }
        }
    }

    Ok(scan_projection)
}

fn split_scan_filters(filters: &[Expr], table_partition_cols: &[String]) -> (Vec<Expr>, Vec<Expr>) {
    let predicates: Vec<&Expr> = filters.iter().collect();
    let pushdown_filters =
        crate::datasource::get_pushdown_filters(&predicates, table_partition_cols);

    let mut pruning_filters = Vec::new();
    let mut parquet_pushdown_filters = Vec::new();
    for (filter, pushdown) in filters.iter().zip(pushdown_filters) {
        match pushdown {
            datafusion::logical_expr::TableProviderFilterPushDown::Exact => {
                pruning_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact => {
                pruning_filters.push(filter.clone());
                parquet_pushdown_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported => {}
        }
    }

    (pruning_filters, parquet_pushdown_filters)
}

fn build_file_schema(snapshot: &DeltaSnapshot) -> Result<Arc<ArrowSchema>> {
    let kmode: ColumnMappingMode = snapshot.effective_column_mapping_mode();
    let kschema_arc = snapshot.schema();
    let logical_kernel = StructType::try_from(kschema_arc)?;
    let physical_arrow: ArrowSchema = get_physical_schema(&logical_kernel, kmode);
    let physical_partition_cols: HashSet<String> = snapshot
        .physical_partition_columns()
        .into_iter()
        .map(|(_, physical)| physical)
        .collect();

    let file_fields = physical_arrow
        .fields()
        .iter()
        .filter(|f| !physical_partition_cols.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    Ok(Arc::new(ArrowSchema::new(file_fields)))
}

type PrunedFiles = (Option<Arc<Vec<Add>>>, Option<Vec<bool>>);

fn maybe_prune_files(
    files: Option<Arc<Vec<Add>>>,
    table_schema: Arc<ArrowSchema>,
    pruning_predicate: Option<&Arc<dyn PhysicalExpr>>,
) -> Result<PrunedFiles> {
    match files {
        Some(files) => {
            if let Some(predicate) = pruning_predicate {
                let source_files = files.as_ref().clone();
                let pruning_mask = crate::datasource::pruning::prune_adds_by_physical_predicate(
                    source_files.clone(),
                    table_schema,
                    Arc::clone(predicate),
                )?;
                let pruned_files = source_files
                    .into_iter()
                    .zip(pruning_mask.iter().copied())
                    .filter_map(|(add, keep)| keep.then_some(add))
                    .collect::<Vec<_>>();
                Ok((Some(Arc::new(pruned_files)), Some(pruning_mask)))
            } else {
                Ok((Some(files), None))
            }
        }
        None => Ok((None, None)),
    }
}

pub(crate) async fn plan_delta_scan(
    session: &dyn Session,
    snapshot: &DeltaSnapshot,
    log_store: &LogStoreRef,
    config: &DeltaScanConfig,
    files: Option<Arc<Vec<Add>>>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = config.clone();
    snapshot
        .ensure_data_read_supported()
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

    let schema = match config.schema.clone() {
        Some(value) => Ok(value),
        // Change from `arrow_schema` to input_schema for Spark compatibility
        None => snapshot.input_schema(),
    }?;

    let full_logical_schema = df_logical_schema(
        snapshot,
        &config.file_column_name,
        &config.commit_version_column_name,
        &config.commit_timestamp_column_name,
        Some(schema.clone()),
    )?;
    let table_partition_cols = snapshot.metadata().partition_columns().clone();
    let (logical_schema, scan_projection, projection_prefix_len) =
        if let Some(used_columns) = projection {
            let scan_projection = expand_scan_projection(
                full_logical_schema.as_ref(),
                used_columns,
                filters,
                &table_partition_cols,
            )?;
            let fields = scan_projection
                .iter()
                .map(|idx| full_logical_schema.field(*idx).to_owned())
                .collect::<Vec<_>>();
            (
                Arc::new(ArrowSchema::new(fields)),
                Some(scan_projection),
                Some(used_columns.len()),
            )
        } else {
            (Arc::clone(&full_logical_schema), None, None)
        };

    // Separate filters for pruning vs pushdown.
    //
    // Exact and Inexact filters are used for pruning; Inexact are additionally pushed down.
    let (pruning_filters, parquet_pushdown_filters) =
        split_scan_filters(filters, &table_partition_cols);

    let table_schema = snapshot
        .input_schema()
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

    let pruning_expr = conjunction(pruning_filters);
    let pruning_predicate = if let Some(expr) = pruning_expr.as_ref() {
        let df_schema = logical_schema.clone().to_dfschema()?;
        Some(
            simplify_expr(session, &df_schema, expr.clone()).map_err(|e| {
                datafusion::common::DataFusionError::Plan(format!(
                    "failed to simplify scan pruning filter: {e}"
                ))
            })?,
        )
    } else {
        None
    };

    let (files, pruning_mask) =
        maybe_prune_files(files, table_schema.clone(), pruning_predicate.as_ref())?;

    // Build physical file schema (non-partition columns)
    let file_schema = build_file_schema(snapshot)?;

    // Prepare pushdown filter for Parquet.
    let pushdown_filter = if !parquet_pushdown_filters.is_empty() {
        let df_schema = full_logical_schema.clone().to_dfschema()?;
        let pushdown_expr = conjunction(parquet_pushdown_filters);
        pushdown_expr
            .map(|expr| {
                simplify_expr(session, &df_schema, expr).map_err(|e| {
                    datafusion::common::DataFusionError::Plan(format!(
                        "failed to simplify parquet pushdown filter: {e}"
                    ))
                })
            })
            .transpose()?
    } else {
        None
    };

    if let Some(files) = files {
        let file_scan_config = build_file_scan_config(
            snapshot,
            log_store,
            &files,
            &config,
            FileScanParams {
                pruning_mask: pruning_mask.as_deref(),
                projection,
                limit,
                pushdown_filter,
                sort_order: None,
                table_stats_mode: TableStatsMode::Snapshot,
            },
            session,
            file_schema,
        )?;

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);

        // Rename columns from physical back to logical names expected by `schema`
        let logical_names = full_logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let renamed = rename_projected_physical_plan(scan_exec, &logical_names, projection)?;
        let output_schema = if let Some(used_columns) = projection {
            let fields = used_columns
                .iter()
                .map(|idx| full_logical_schema.field(*idx).to_owned())
                .collect::<Vec<_>>();
            Arc::new(ArrowSchema::new(fields))
        } else {
            Arc::clone(&full_logical_schema)
        };
        return Ok(
            Arc::new(RelaxedTzCastExec::new(renamed, output_schema)) as Arc<dyn ExecutionPlan>
        );
    }

    // Metadata-as-data path: log scan -> replay -> discovery -> scan by adds.
    let table_url = log_store.config().location.clone();

    let planner_options = TableDeltaOptions {
        delta_log_replay_strategy: config.delta_log_replay_strategy,
        delta_log_replay_hash_threshold: config.delta_log_replay_hash_threshold,
        ..TableDeltaOptions::default()
    };

    let planner_ctx = PlannerContext::new(
        session,
        PlannerTableConfig::new(
            table_url.clone(),
            planner_options,
            HashMap::new(),
            table_partition_cols.clone(),
            None,
            true,
        ),
    );
    let log_replay_options = LogReplayOptions {
        include_stats_json: pruning_expr
            .as_ref()
            .is_some_and(|expr| predicate_requires_stats(expr, &table_partition_cols)),
        ..Default::default()
    };

    let meta_scan: Arc<dyn ExecutionPlan> =
        crate::physical_plan::planner::utils::build_log_replay_pipeline_with_options(
            &planner_ctx,
            snapshot,
            log_replay_options,
        )
        .await
        .map_err(|e| {
            datafusion::common::DataFusionError::Plan(format!(
                "failed to build log replay pipeline: {e}"
            ))
        })?;
    let meta_scan: Arc<dyn ExecutionPlan> = if let Some(predicate) = pruning_expr {
        build_metadata_filter(session, meta_scan, snapshot, predicate)?
    } else {
        meta_scan
    };
    // TODO(metadata-as-data-aqe): This path intentionally prioritizes metadata scalability and
    // low TTFB over perfect static CBO. Add a runtime re-optimization hook after replay/discovery
    // so downstream repartitioning and join strategy can react to exact file cardinality/bytes.

    let find_files: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        table_url.clone(),
        None,
        None,
        snapshot.version(),
        table_partition_cols.clone(),
        false,
    )?);

    // TODO(adaptive-partitioning): Replace fixed fan-out with adaptive planning.
    // Plan: (1) pick partition count from discovered `size_bytes` (clamped to [1, target_partitions]
    // and gated by `optimizer.repartition_file_min_size`), then (2) replace round-robin with
    // size-aware distribution to reduce small-task overhead and skew.
    let target_partitions = session.config().target_partitions().max(1);
    let find_files: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        find_files,
        Partitioning::RoundRobinBatch(target_partitions),
    )?);

    let mut scan_exec: Arc<dyn ExecutionPlan> = Arc::new(
        DeltaScanByAddsExec::new(
            find_files,
            table_url,
            snapshot.version(),
            table_schema,
            logical_schema.clone(),
            config.clone(),
            scan_projection.clone(),
            limit,
            pushdown_filter,
        )
        .with_table_statistics(snapshot.datafusion_table_statistics(None)),
    );

    // NOTE: Keep filtering inside DeltaScanByAddsExec pushdown path for now.
    // Wrapping an additional FilterExec here can trigger DataFusion interval
    // inference assertion failures on some nullable predicates in metadata-as-data
    // scans (tracked separately).

    if let Some(prefix_len) = projection_prefix_len {
        let mut proj_exprs = Vec::with_capacity(prefix_len);
        for idx in 0..prefix_len {
            let field = logical_schema.field(idx);
            let expr = Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
            proj_exprs.push((expr, field.name().clone()));
        }
        scan_exec = Arc::new(ProjectionExec::try_new(proj_exprs, scan_exec)?);
    }

    Ok(scan_exec)
}
