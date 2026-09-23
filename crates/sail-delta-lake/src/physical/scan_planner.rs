use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, ToDFSchema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sail_data_source::options::ResolveOptions;

use crate::datasource::pruning::{partition_filter_mask, select_files_for_limit};
use crate::datasource::scan::{
    FileScanParams, TableStatsMode, build_file_scan_config, file_scan_projection_for_schema,
    map_statistics_to_schema,
};
use crate::datasource::{
    DeltaMetadataAggregateConfig, DeltaScanConfig, df_logical_schema,
    rewrite_predicate_for_column_mapping, simplify_expr,
};
use crate::delta_log::LogStoreRef;
use crate::logical::table_source::DeltaMetadataAggregateSource;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::planner::metadata_predicate::{
    build_metadata_filter, predicate_requires_stats,
};
use crate::physical_plan::planner::utils::LogReplayOptions;
use crate::physical_plan::planner::{DeltaPlannerConfig, PlannerContext};
use crate::physical_plan::{
    DeltaDiscoveryExec, DeltaScanByAddsExec, delta_action_schema, encode_actions,
};
use crate::schema::{attach_column_mapping_metadata, get_physical_schema};
use crate::spec::{Action, Add, ColumnMappingMode, StructType};
use crate::table::DeltaSnapshot;

#[derive(Debug, Clone)]
pub(crate) enum DeltaFileSource {
    /// The snapshot loaded the authoritative active Add set (`require_files = true`).
    Eager(Arc<Vec<Add>>),
    /// Active files were intentionally omitted and must be reconstructed from the log at runtime.
    Replay,
}

pub(crate) async fn plan_delta_scan(
    session: &dyn Session,
    snapshot: &DeltaSnapshot,
    log_store: &LogStoreRef,
    config: &DeltaScanConfig,
    file_source: DeltaFileSource,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = config.clone();
    snapshot
        .ensure_data_read_supported()
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

    let kmode = snapshot.effective_column_mapping_mode();
    let schema = match config.schema.clone() {
        Some(requested) if kmode != ColumnMappingMode::None => {
            let schema = Arc::new(attach_column_mapping_metadata(
                requested.as_ref(),
                snapshot.schema(),
            ));
            config.schema = Some(Arc::clone(&schema));
            schema
        }
        Some(requested) => requested,
        None => Arc::new(snapshot.schema().clone()),
    };

    let full_logical_schema = df_logical_schema(
        snapshot,
        &config.file_column_name,
        &config.row_index_column_name,
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
            if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str())
                && !used_columns.contains(&idx)
                && !fields.iter().any(|f| f.name() == partition_col)
            {
                fields.push(full_logical_schema.field(idx).to_owned());
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
            if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str())
                && !scan_projection.contains(&idx)
            {
                scan_projection.push(idx);
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
    let exact_filters = pushdown_filters
        .iter()
        .all(|pushdown| *pushdown == datafusion::logical_expr::TableProviderFilterPushDown::Exact);

    let mut partition_filters = Vec::new();
    let mut parquet_pushdown_filters = Vec::new();
    for (filter, pushdown) in filters.iter().zip(pushdown_filters) {
        match pushdown {
            datafusion::logical_expr::TableProviderFilterPushDown::Exact => {
                partition_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact => {
                parquet_pushdown_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported => {}
        }
    }

    let stats_source_schema = Arc::new(snapshot.schema().clone());

    let pruning_expr = conjunction(
        partition_filters
            .iter()
            .chain(&parquet_pushdown_filters)
            .cloned(),
    );
    let partition_predicate = conjunction(partition_filters);
    let pruning_predicate = if let Some(expr) = conjunction(parquet_pushdown_filters.clone()) {
        let df_schema = logical_schema.clone().to_dfschema()?;
        Some(simplify_expr(session, &df_schema, expr).map_err(|e| {
            datafusion::common::DataFusionError::Plan(format!(
                "failed to simplify scan pruning filter: {e}"
            ))
        })?)
    } else {
        None
    };

    let file_source = match file_source {
        DeltaFileSource::Eager(files) => {
            let files = if let Some(predicate) = partition_predicate {
                let values =
                    partition_filter_mask(session, snapshot, &logical_schema, &files, predicate)?;
                Arc::new(
                    files
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| values.is_valid(*index) && values.value(*index))
                        .map(|(_, add)| add.clone())
                        .collect::<Vec<_>>(),
                )
            } else {
                files
            };
            if let Some(predicate) = pruning_predicate.as_ref() {
                let pruning_mask = crate::datasource::pruning::prune_adds_by_physical_predicate(
                    files.as_ref(),
                    Arc::clone(&logical_schema),
                    Arc::clone(predicate),
                    kmode,
                )?;
                let pruned_files = match Arc::try_unwrap(files) {
                    Ok(files) => files
                        .into_iter()
                        .zip(pruning_mask)
                        .filter_map(|(add, keep)| keep.then_some(add))
                        .collect(),
                    Err(files) => files
                        .iter()
                        .zip(pruning_mask)
                        .filter(|(_, keep)| *keep)
                        .map(|(add, _)| add.clone())
                        .collect(),
                };
                DeltaFileSource::Eager(Arc::new(pruned_files))
            } else {
                DeltaFileSource::Eager(files)
            }
        }
        DeltaFileSource::Replay => DeltaFileSource::Replay,
    };
    let file_source = match (file_source, limit) {
        (DeltaFileSource::Eager(files), Some(limit)) if exact_filters => {
            DeltaFileSource::Eager(Arc::new(select_files_for_limit(&files, limit)))
        }
        (source, _) => source,
    };

    // Build physical file schema (non-partition columns)
    let kschema_arc = snapshot.schema();
    let logical_kernel = StructType::try_from(kschema_arc)?;
    let physical_arrow: ArrowSchema = get_physical_schema(&logical_kernel, kmode)?;
    let physical_partition_cols: HashSet<String> = snapshot
        .physical_partition_columns()
        .into_iter()
        .map(|column| column.physical_name)
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
    // The parquet scan resolves predicate columns against the physical file schema,
    // so column-mapped tables need the predicate rewritten from logical to physical names.
    let pushdown_filter = pushdown_filter
        .map(|expr| rewrite_predicate_for_column_mapping(expr, snapshot.schema(), kmode))
        .transpose()?;

    let row_index_projected = config
        .row_index_column_name
        .as_ref()
        .is_some_and(|name| logical_schema.field_with_name(name).is_ok());
    let has_row_filter = pushdown_filter.is_some();
    if let DeltaFileSource::Eager(files) = &file_source
        && !row_index_projected
        && !files.iter().any(|add| add.deletion_vector.is_some())
    {
        let output_schema = if let Some(used_columns) = projection {
            let fields = used_columns
                .iter()
                .map(|idx| full_logical_schema.field(*idx).to_owned())
                .collect::<Vec<_>>();
            Arc::new(ArrowSchema::new(fields))
        } else {
            Arc::clone(&full_logical_schema)
        };
        let file_scan_projection =
            file_scan_projection_for_schema(snapshot, &config, &file_schema, &output_schema)?;

        let file_scan_config = build_file_scan_config(
            snapshot,
            log_store,
            files,
            &config,
            FileScanParams {
                projection: Some(&file_scan_projection),
                limit,
                pushdown_filter,
                sort_order: None,
                table_stats_mode: TableStatsMode::Snapshot,
            },
            session,
            file_schema,
        )?;

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        return align_delta_scan_output(scan_exec, output_schema);
    }

    let table_url = log_store.config().location.clone();
    let target_partitions = session.config().target_partitions().max(1);
    let (find_files, output_statistics): (Arc<dyn ExecutionPlan>, Option<_>) = match file_source {
        DeltaFileSource::Eager(files) => {
            let output_statistics = snapshot
                .datafusion_table_statistics_for_adds(files.as_ref())
                .map(|statistics| {
                    map_statistics_to_schema(&statistics, &stats_source_schema, &logical_schema)
                })
                .map(|statistics| {
                    if has_row_filter || limit.is_some() {
                        // A predicate may be applied inside only some file scans, and a pushed limit
                        // is enforced per execution partition. Selected-file statistics remain useful
                        // upper-bound estimates but no longer describe this node exactly.
                        statistics.to_inexact()
                    } else {
                        statistics
                    }
                });
            (
                build_eager_adds_input(files.as_ref(), target_partitions)?,
                output_statistics,
            )
        }
        DeltaFileSource::Replay => {
            let include_stats = pruning_expr
                .as_ref()
                .is_some_and(|expr| predicate_requires_stats(expr, &table_partition_cols));
            (
                build_replayed_adds_input(
                    session,
                    snapshot,
                    log_store,
                    &config,
                    pruning_expr,
                    include_stats,
                )
                .await?,
                None,
            )
        }
    };

    let mut scan_exec: Arc<dyn ExecutionPlan> = Arc::new(
        DeltaScanByAddsExec::new(
            find_files,
            table_url,
            snapshot.version(),
            stats_source_schema,
            logical_schema.clone(),
            config.clone(),
            scan_projection.clone(),
            limit,
            pushdown_filter,
            None,
            snapshot.load_config().catalog_managed_commits.clone(),
        )
        .with_output_statistics(output_statistics),
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

pub(crate) async fn plan_delta_metadata_aggregate(
    session: &dyn Session,
    source: &DeltaMetadataAggregateSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = &source.table;
    let snapshot = table.snapshot();
    snapshot
        .ensure_data_read_supported()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut config = table.config().clone();
    config.metadata_aggregate = Some(DeltaMetadataAggregateConfig {
        group_columns: source.group_columns.clone(),
    });
    let find_files = build_replayed_adds_input(
        session,
        snapshot,
        table.log_store(),
        &config,
        conjunction(source.filters.clone()),
        true,
    )
    .await?;
    Ok(Arc::new(DeltaScanByAddsExec::new(
        find_files,
        table.log_store().config().location.clone(),
        snapshot.version(),
        Arc::new(snapshot.schema().clone()),
        Arc::clone(&source.schema),
        config,
        None,
        None,
        None,
        None,
        snapshot.load_config().catalog_managed_commits.clone(),
    )))
}

async fn build_replayed_adds_input(
    session: &dyn Session,
    snapshot: &DeltaSnapshot,
    log_store: &LogStoreRef,
    config: &DeltaScanConfig,
    pruning_expr: Option<Expr>,
    include_stats_json: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url = log_store.config().location.clone();
    let partition_columns = snapshot.metadata().partition_columns();
    let mut planner_options = DeltaWriteOptions::resolve(session, Vec::new())?;
    planner_options.delta_log_replay_strategy = config.delta_log_replay_strategy;
    let planner_ctx = PlannerContext::new(
        session,
        DeltaPlannerConfig::new(
            table_url.clone(),
            planner_options,
            HashMap::new(),
            partition_columns.clone(),
            None,
            true,
        ),
    );
    let options = LogReplayOptions {
        include_stats_json,
        ..Default::default()
    };
    let meta_scan = crate::physical_plan::planner::utils::build_log_replay_pipeline_with_options(
        &planner_ctx,
        snapshot,
        options,
    )
    .await?;
    let meta_scan = if let Some(predicate) = pruning_expr {
        build_metadata_filter(
            session,
            meta_scan,
            snapshot,
            config
                .schema
                .as_deref()
                .unwrap_or_else(|| snapshot.schema()),
            predicate,
        )?
    } else {
        meta_scan
    };
    let find_files: Arc<dyn ExecutionPlan> = Arc::new(DeltaDiscoveryExec::new(
        meta_scan,
        table_url,
        snapshot.version(),
        partition_columns.clone(),
        false,
    )?);
    Ok(Arc::new(RepartitionExec::try_new(
        find_files,
        Partitioning::RoundRobinBatch(session.config().target_partitions().max(1)),
    )?))
}

fn build_eager_adds_input(
    adds: &[Add],
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_count = target_partitions.max(1).min(adds.len().max(1));
    let mut actions = vec![Vec::new(); partition_count];
    let mut queue = (0..partition_count)
        .map(|partition| std::cmp::Reverse((0u64, partition)))
        .collect::<std::collections::BinaryHeap<_>>();
    let mut files = adds.iter().collect::<Vec<_>>();
    files.sort_by_key(|add| std::cmp::Reverse(add.size));
    for add in files {
        let Some(std::cmp::Reverse((bytes, partition))) = queue.pop() else {
            return Err(DataFusionError::Internal(
                "Delta scan has no file partitions".into(),
            ));
        };
        let cost = (add.size.max(0) as u64).saturating_add(4 * 1024 * 1024);
        actions[partition].push(Action::Add(add.clone()));
        queue.push(std::cmp::Reverse((bytes.saturating_add(cost), partition)));
    }
    const EAGER_ADD_BATCH_FILES: usize = 1024;
    let partitions = actions
        .into_iter()
        .map(|actions| {
            let mut actions = actions.into_iter();
            std::iter::from_fn(|| {
                let chunk = actions
                    .by_ref()
                    .take(EAGER_ADD_BATCH_FILES)
                    .collect::<Vec<_>>();
                (!chunk.is_empty()).then(|| encode_actions(chunk, None))
            })
            .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let input: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&partitions, delta_action_schema()?, None)?;
    Ok(input)
}

fn align_delta_scan_output(
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    if input_schema.fields().len() != target_schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "cannot align Delta scan with {} fields to logical schema with {} fields",
            input_schema.fields().len(),
            target_schema.fields().len()
        )));
    }
    if input_schema == target_schema {
        return Ok(input);
    }

    let expressions = input_schema
        .fields()
        .iter()
        .zip(target_schema.fields())
        .enumerate()
        .map(|(index, (input_field, target_field))| {
            let column = Arc::new(Column::new(input_field.name(), index)) as Arc<dyn PhysicalExpr>;
            let renamed_input_field = input_field.as_ref().clone().with_name(target_field.name());
            let expression: Arc<dyn PhysicalExpr> = if &renamed_input_field == target_field.as_ref()
            {
                column
            } else {
                if input_field.data_type() != target_field.data_type()
                    && matches!(
                        (input_field.data_type(), target_field.data_type()),
                        (
                            ArrowDataType::Timestamp(_, _),
                            ArrowDataType::Timestamp(_, _)
                        )
                    )
                {
                    return Err(DataFusionError::Plan(format!(
                        "Delta Parquet scan did not restore timestamp field '{}' from {} to {}",
                        target_field.name(),
                        input_field.data_type(),
                        target_field.data_type()
                    )));
                }
                Arc::new(CastExpr::new_with_target_field(
                    column,
                    Arc::clone(target_field),
                    None,
                ))
            };
            Ok((expression, target_field.name().clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    let projection =
        Arc::new(ProjectionExec::try_new(expressions, input)?) as Arc<dyn ExecutionPlan>;

    if projection.schema() != target_schema {
        return Err(DataFusionError::Plan(format!(
            "Delta scan projection produced schema {} instead of {}",
            projection.schema(),
            target_schema
        )));
    }
    Ok(projection)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    #[test]
    fn align_delta_scan_output_reuses_exact_input() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;

        let aligned = align_delta_scan_output(Arc::clone(&input), schema)?;

        assert!(Arc::ptr_eq(&aligned, &input));
        Ok(())
    }

    #[test]
    fn align_delta_scan_output_rejects_unrestored_timestamp_timezone() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        )]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("America/Los_Angeles")),
            ),
            true,
        )]));

        let Err(error) =
            align_delta_scan_output(Arc::new(EmptyExec::new(input_schema)), target_schema)
        else {
            return Err(DataFusionError::Plan(
                "expected unrestored timestamp timezone to be rejected".to_string(),
            ));
        };

        assert!(
            error
                .to_string()
                .contains("did not restore timestamp field")
        );
        Ok(())
    }
}
