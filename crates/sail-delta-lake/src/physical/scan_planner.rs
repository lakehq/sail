use std::collections::HashSet;
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
use delta_kernel::table_features::ColumnMappingMode;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;

use crate::datasource::scan::{build_file_scan_config, FileScanParams, TableStatsMode};
use crate::datasource::{
    df_logical_schema, simplify_expr, DataFusionMixins, DeltaScanConfig, DeltaTableStateExt,
};
use crate::kernel::models::Add;
use crate::options::TableDeltaOptions;
use crate::physical_plan::planner::utils::{LogReplayFilter, LogReplayOptions};
use crate::physical_plan::planner::{DeltaTableConfig as PlannerTableConfig, PlannerContext};
use crate::physical_plan::{DeltaDiscoveryExec, DeltaScanByAddsExec};
use crate::schema::get_physical_schema;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

pub(crate) async fn plan_delta_scan(
    session: &dyn Session,
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
    config: &DeltaScanConfig,
    files: Option<Arc<Vec<Add>>>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = config.clone();

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

    let logical_schema = if let Some(used_columns) = projection {
        let mut fields = vec![];
        for idx in used_columns {
            fields.push(full_logical_schema.field(*idx).to_owned());
        }
        // partition filters with Exact pushdown were removed from projection by DF optimizer,
        // we need to add them back for the predicate pruning to work
        let filter_expr = conjunction(filters.iter().cloned());
        if let Some(expr) = &filter_expr {
            for c in expr.column_refs() {
                let idx = full_logical_schema.index_of(c.name.as_str())?;
                if !used_columns.contains(&idx) {
                    fields.push(full_logical_schema.field(idx).to_owned());
                }
            }
        }
        // Ensure all partition columns are included in logical schema
        for partition_col in table_partition_cols.iter() {
            if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str()) {
                if !used_columns.contains(&idx) && !fields.iter().any(|f| f.name() == partition_col)
                {
                    fields.push(full_logical_schema.field(idx).to_owned());
                }
            }
        }
        Arc::new(ArrowSchema::new(fields))
    } else {
        Arc::clone(&full_logical_schema)
    };

    let (scan_projection, projection_prefix_len) = if let Some(used_columns) = projection {
        let mut scan_projection = used_columns.clone();
        let filter_expr = conjunction(filters.iter().cloned());
        if let Some(expr) = &filter_expr {
            for c in expr.column_refs() {
                let idx = full_logical_schema.index_of(c.name.as_str())?;
                if !scan_projection.contains(&idx) {
                    scan_projection.push(idx);
                }
            }
        }
        for partition_col in table_partition_cols.iter() {
            if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str()) {
                if !scan_projection.contains(&idx) {
                    scan_projection.push(idx);
                }
            }
        }
        (Some(scan_projection), Some(used_columns.len()))
    } else {
        (None, None)
    };

    // Separate filters for pruning vs pushdown.
    //
    // Exact and Inexact filters are used for pruning; Inexact are additionally pushed down.
    let partition_cols = &table_partition_cols;
    let predicates: Vec<&Expr> = filters.iter().collect();
    let pushdown_filters =
        crate::datasource::get_pushdown_filters(&predicates, partition_cols.as_slice());

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

    let table_schema = snapshot
        .input_schema()
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

    let pruning_expr = conjunction(pruning_filters);
    let pruning_predicate = if let Some(expr) = pruning_expr {
        let df_schema = logical_schema.clone().to_dfschema()?;
        Some(simplify_expr(session, &df_schema, expr).map_err(|e| {
            datafusion::common::DataFusionError::Plan(format!(
                "failed to simplify scan pruning filter: {e}"
            ))
        })?)
    } else {
        None
    };

    let (files, pruning_mask): (Option<Arc<Vec<Add>>>, Option<Vec<bool>>) = match files {
        Some(files) => {
            if let Some(predicate) = pruning_predicate.as_ref() {
                let source_files = files.as_ref().clone();
                let pruning_mask = crate::datasource::pruning::prune_adds_by_physical_predicate(
                    source_files.clone(),
                    table_schema.clone(),
                    Arc::clone(predicate),
                )?;
                let pruned_files = source_files
                    .into_iter()
                    .zip(pruning_mask.iter().copied())
                    .filter_map(|(add, keep)| keep.then_some(add))
                    .collect::<Vec<_>>();
                (Some(Arc::new(pruned_files)), Some(pruning_mask))
            } else {
                (Some(files), None)
            }
        }
        None => (None, None),
    };

    // Build physical file schema (non-partition columns)
    let kmode: ColumnMappingMode = snapshot.effective_column_mapping_mode();
    let kschema_arc = snapshot.snapshot().table_configuration().schema();
    let physical_arrow: ArrowSchema = get_physical_schema(&kschema_arc, kmode);
    let physical_partition_cols: HashSet<String> = table_partition_cols
        .iter()
        .map(|col| {
            kschema_arc
                .field(col)
                .map(|f| f.physical_name(kmode).to_string())
                .unwrap_or_else(|| col.clone())
        })
        .collect();

    let file_fields = physical_arrow
        .fields()
        .iter()
        .filter(|f| !physical_partition_cols.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    let file_schema = Arc::new(ArrowSchema::new(file_fields));

    // Prepare pushdown filter for Parquet.
    let pushdown_filter = if !parquet_pushdown_filters.is_empty() {
        let df_schema = logical_schema.clone().to_dfschema()?;
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
        return Ok(renamed);
    }

    // Metadata-as-data path: log scan -> replay -> discovery -> scan by adds.
    let table_url = log_store.config().location.clone();
    let kernel_snapshot = snapshot.snapshot().snapshot().inner.clone();
    let log_segment = kernel_snapshot.log_segment();
    let checkpoint_files = log_segment
        .checkpoint_parts
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();
    let commit_files = log_segment
        .ascending_commit_files
        .iter()
        .map(|p| p.filename.clone())
        .collect::<Vec<_>>();

    let mut planner_options = TableDeltaOptions::default();
    planner_options.delta_log_replay_strategy = config.delta_log_replay_strategy;
    planner_options.delta_log_replay_hash_threshold = config.delta_log_replay_hash_threshold;

    let planner_ctx = PlannerContext::new(
        session,
        PlannerTableConfig::new(
            table_url.clone(),
            planner_options,
            table_partition_cols.clone(),
            None,
            true,
        ),
    );

    let mut log_replay_options = LogReplayOptions::default();
    if let Some(predicate) = pruning_predicate.as_ref() {
        let mut expr_props =
            crate::datasource::PredicateProperties::new(table_partition_cols.clone());
        expr_props
            .analyze_predicate(predicate)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        log_replay_options.include_stats_json = !expr_props.partition_only;
        if expr_props.partition_only {
            log_replay_options.log_filter = Some(LogReplayFilter {
                predicate: Arc::clone(predicate),
                table_schema: table_schema.clone(),
            });
        }
    } else {
        log_replay_options.include_stats_json = false;
    }

    let partition_columns_map = table_partition_cols
        .iter()
        .map(|col| {
            let physical = kschema_arc
                .field(col)
                .map(|f| f.physical_name(kmode).to_string())
                .unwrap_or_else(|| col.clone());
            (col.clone(), physical)
        })
        .collect::<Vec<_>>();

    let meta_scan: Arc<dyn ExecutionPlan> =
        crate::physical_plan::planner::utils::build_log_replay_pipeline_with_options(
            &planner_ctx,
            table_url.clone(),
            snapshot.version(),
            partition_columns_map,
            checkpoint_files,
            commit_files,
            log_replay_options,
        )
        .await
        .map_err(|e| {
            datafusion::common::DataFusionError::Plan(format!(
                "failed to build log replay pipeline: {e}"
            ))
        })?;

    let find_files: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::with_input(
        meta_scan,
        table_url.clone(),
        pruning_predicate.clone(),
        Some(table_schema.clone()),
        snapshot.version(),
        table_partition_cols.clone(),
        false,
    )?);

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
