use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use futures::TryStreamExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;
use sail_data_source::options::ResolveOptions;
use serde_json::{Value, json};

use super::maintenance::DeltaMaintenancePredicate;
use super::{optional_bool, optional_string};
use crate::datasource::DeltaScanConfig;
use crate::delta_log::LogStoreRef;
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical::scan_planner::{DeltaFileSource, plan_delta_scan};
use crate::physical_plan::{
    DeltaWriterExec, DeltaWriterExecOptions, decode_adds_from_batch, prepare_delta_write_context,
};
use crate::snapshot::DeltaSnapshot;
use crate::spec::{Add, CommitAction, DeltaError, DeltaOperation, RemoveOptions, TableFeature};
use crate::transaction::{CommitBuilder, CommitProperties};

#[expect(clippy::too_many_arguments)]
pub(super) async fn execute(
    session: &SessionState,
    context: Arc<TaskContext>,
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    scan_config: DeltaScanConfig,
    lakehouse_table: Option<LakehouseExecutionContext>,
    invocation: &LakeProcedureInvocation,
    options: &BTreeMap<String, String>,
    filter: Option<&DeltaMaintenancePredicate>,
) -> Result<RecordBatch> {
    let started = Utc::now().timestamp_millis();
    if optional_bool(invocation, "full")?.unwrap_or(false)
        || optional_string(invocation, "zorder")?.is_some()
    {
        return not_impl_err!("OPTIMIZE FULL and ZORDER are not implemented");
    }
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::Clustering)
    {
        return not_impl_err!("OPTIMIZE liquid-clustered tables is not implemented");
    }
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::RowTracking)
    {
        return not_impl_err!("OPTIMIZE row-tracking preservation is not implemented");
    }
    let min_size = positive_size(
        options,
        "spark.databricks.delta.optimize.minFileSize",
        1 << 30,
    )?;
    let max_size = positive_size(
        options,
        "spark.databricks.delta.optimize.maxFileSize",
        1 << 30,
    )?;
    let ratio = options
        .get("spark.databricks.delta.optimize.maxDeletedRowsRatio")
        .map(|value| value.parse::<f64>())
        .transpose()
        .map_err(|e| DeltaError::generic(e.to_string()))?
        .unwrap_or(0.05);
    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
        return plan_err!("OPTIMIZE maxDeletedRowsRatio must be between zero and one");
    }
    let predicate = optional_string(invocation, "predicate")?;
    let candidates = select_files(&snapshot, filter)?;
    let considered = candidates.len();
    let mut partitions: BTreeMap<BTreeMap<String, Option<String>>, Vec<Add>> = BTreeMap::new();
    for add in candidates {
        let dv_rewrite = add.deletion_vector.as_ref().is_some_and(|dv| {
            let records = add
                .get_stats()
                .ok()
                .flatten()
                .map(|stats| stats.num_records);
            records.is_none_or(|records| {
                records <= 0 || dv.cardinality as f64 / records as f64 > ratio
            })
        });
        if add.size < min_size || dv_rewrite {
            partitions
                .entry(
                    add.partition_values
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                )
                .or_default()
                .push(add);
        }
    }
    let mut bins = vec![];
    let mut optimized_partitions = 0;
    for mut files in partitions.into_values() {
        files.sort_by(|a, b| a.size.cmp(&b.size).then(a.path.cmp(&b.path)));
        let before = bins.len();
        let mut bin = vec![];
        let mut size = 0_i64;
        for file in files {
            if size.saturating_add(file.size) > max_size && !bin.is_empty() {
                retain_bin(&mut bins, std::mem::take(&mut bin));
                size = 0;
            }
            size = size
                .checked_add(file.size)
                .ok_or_else(|| DeltaError::generic("OPTIMIZE bin size overflow"))?;
            bin.push(file);
        }
        retain_bin(&mut bins, bin);
        if bins.len() > before {
            optimized_partitions += 1;
        }
    }
    let removed = bins.iter().flatten().cloned().collect::<Vec<_>>();
    let mut added = vec![];
    let mut writer_options =
        DeltaWriterExecOptions::from(DeltaWriteOptions::resolve(session, vec![])?);
    writer_options.target_file_size = max_size as u64;
    let schema = snapshot.input_schema()?;
    let operation = DeltaOperation::Optimize {
        predicate: predicate.map(str::to_string),
    };
    let write_context = prepare_delta_write_context(
        snapshot.table_url(),
        Some(&snapshot),
        &writer_options,
        snapshot.metadata().configuration(),
        snapshot.metadata().partition_columns(),
        &PhysicalSinkMode::Append,
        true,
        &schema,
        Some(operation.clone()),
    )?;
    for bin in &bins {
        let scan = plan_delta_scan(
            session,
            &snapshot,
            &log_store,
            &scan_config,
            DeltaFileSource::Eager(Arc::new(bin.clone())),
            None,
            &[],
            None,
        )
        .await?;
        let input = Arc::new(CoalescePartitionsExec::new(scan));
        let writer = DeltaWriterExec::new(
            input,
            snapshot.table_url().clone(),
            writer_options.clone(),
            snapshot.metadata().configuration().clone(),
            snapshot.metadata().partition_columns().clone(),
            PhysicalSinkMode::Append,
            true,
            schema.clone(),
            write_context.clone(),
            lakehouse_table.clone(),
        )?;
        let mut stream = writer.execute(0, context.clone())?;
        while let Some(batch) = stream.try_next().await? {
            added.extend(decode_adds_from_batch(&batch)?);
        }
    }
    let finished = Utc::now().timestamp_millis();
    if !removed.is_empty() {
        let mut actions = added
            .iter()
            .cloned()
            .map(|mut add| {
                add.data_change = false;
                CommitAction::Add(add)
            })
            .collect::<Vec<_>>();
        actions.extend(removed.iter().cloned().map(|add| {
            let mut remove = add.into_remove_with_options(
                finished,
                RemoveOptions {
                    extended_file_metadata: Some(true),
                    include_tags: true,
                },
            );
            remove.data_change = false;
            CommitAction::Remove(remove)
        }));
        let mut metrics = HashMap::new();
        for (key, value) in [
            ("numAddedFiles", added.len() as i64),
            ("numRemovedFiles", removed.len() as i64),
            ("numAddedBytes", total_size(&added)?),
            ("numRemovedBytes", total_size(&removed)?),
            (
                "numDeletionVectorsRemoved",
                removed
                    .iter()
                    .filter(|file| file.deletion_vector.is_some())
                    .count() as i64,
            ),
        ] {
            metrics.insert(key.to_string(), json!(value));
        }
        CommitBuilder::from(CommitProperties::default().with_operation_metrics(metrics))
            .with_actions(actions)
            .build(Some(snapshot.clone()), log_store, operation)
            .await?;
    }
    let dvs = removed
        .iter()
        .filter_map(|file| file.deletion_vector.as_ref())
        .collect::<Vec<_>>();
    let stats_columns = snapshot.table_properties().data_skipping_num_indexed_cols;
    let indexed = match stats_columns {
        Some(crate::spec::DataSkippingNumIndexedCols::NumColumns(n)) => {
            n.min(schema.fields().len() as u64) as i64
        }
        Some(crate::spec::DataSkippingNumIndexedCols::AllColumns) => -1,
        None => schema.fields().len().min(32) as i64,
    };
    let row = json!({"path": snapshot.table_url().as_str().trim_end_matches('/'), "metrics": {
        "numFilesAdded": added.len(), "numFilesRemoved": removed.len(),
        "filesAdded": sizes(&added)?, "filesRemoved": sizes(&removed)?, "partitionsOptimized": optimized_partitions,
        "zOrderStats": null, "clusteringStats": null, "numBins": bins.len(), "numBatches": 1,
        "totalConsideredFiles": considered, "totalFilesSkipped": considered - removed.len(),
        "preserveInsertionOrder": false, "numFilesSkippedToReduceWriteAmplification": 0,
        "numBytesSkippedToReduceWriteAmplification": 0, "startTimeMs": started, "endTimeMs": finished,
        "totalClusterParallelism": 1, "totalScheduledTasks": bins.len(), "autoCompactParallelismStats": null,
        "deletionVectorStats": if dvs.is_empty() { Value::Null } else { json!({"numDeletionVectorsRemoved": dvs.len(), "numDeletionVectorRowsRemoved": dvs.iter().map(|dv| dv.cardinality).sum::<i64>()}) },
        "numTableColumns": schema.fields().len(), "numTableColumnsWithStats": indexed
    }});
    serde_arrow::to_record_batch(invocation.procedure.schema().fields(), &[row])
        .map_err(|e| DeltaError::generic_err(e).into())
}

fn retain_bin(bins: &mut Vec<Vec<Add>>, bin: Vec<Add>) {
    if bin.len() > 1
        || bin
            .first()
            .is_some_and(|file| file.deletion_vector.is_some())
    {
        bins.push(bin);
    }
}

fn positive_size(options: &BTreeMap<String, String>, key: &str, default: i64) -> Result<i64> {
    let size = options
        .get(key)
        .map(|value| value.parse::<i64>())
        .transpose()
        .map_err(|e| DeltaError::generic(e.to_string()))?
        .unwrap_or(default);
    if size <= 0 {
        return plan_err!("{key} must be positive");
    }
    Ok(size)
}

fn select_files(
    snapshot: &DeltaSnapshot,
    filter: Option<&DeltaMaintenancePredicate>,
) -> Result<Vec<Add>> {
    let Some(filter) = filter else {
        return Ok(snapshot.adds().to_vec());
    };
    if snapshot.schema().fields() != filter.schema.fields() {
        return plan_err!("OPTIMIZE table schema changed after predicate planning");
    }
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    filter.expr.apply(|expr| {
        if let Some(column) = expr.downcast_ref::<datafusion::physical_expr::expressions::Column>()
            && !snapshot
                .metadata()
                .partition_columns()
                .contains(&column.name().to_string())
        {
            return plan_err!("OPTIMIZE predicates may reference only partition columns");
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let mut columns = vec![];
    for field in filter.schema.fields() {
        if snapshot
            .metadata()
            .partition_columns()
            .contains(field.name())
        {
            let physical = crate::schema::arrow_field_physical_name(
                field,
                snapshot.effective_column_mapping_mode(),
            );
            let values = snapshot
                .adds()
                .iter()
                .map(|add| {
                    crate::conversion::parse_optional_partition_value(
                        add.partition_values
                            .get(physical)
                            .and_then(Option::as_deref),
                        field.data_type(),
                    )
                })
                .collect::<crate::spec::DeltaResult<Vec<_>>>()?;
            columns.push(if values.is_empty() {
                datafusion::arrow::array::new_empty_array(field.data_type())
            } else {
                datafusion_common::ScalarValue::iter_to_array(values)?
            });
        } else {
            columns.push(datafusion::arrow::array::new_null_array(
                field.data_type(),
                snapshot.adds().len(),
            ));
        }
    }
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
        filter
            .schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new_with_options(
        schema,
        columns,
        &datafusion::arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(snapshot.adds().len())),
    )?;
    let values = filter.expr.evaluate(&batch)?.into_array(batch.num_rows())?;
    let mask = values
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .ok_or_else(|| DeltaError::generic("OPTIMIZE predicate must be Boolean"))?;
    Ok(snapshot
        .adds()
        .iter()
        .enumerate()
        .filter(|(i, _)| mask.is_valid(*i) && mask.value(*i))
        .map(|(_, add)| add.clone())
        .collect())
}

fn total_size(files: &[Add]) -> Result<i64> {
    files.iter().try_fold(0_i64, |sum, file| {
        sum.checked_add(file.size)
            .ok_or_else(|| DeltaError::generic("OPTIMIZE file size overflow").into())
    })
}

fn sizes(files: &[Add]) -> Result<Value> {
    let size = total_size(files)?;
    Ok(
        json!({"min": files.iter().map(|file| file.size).min(), "max": files.iter().map(|file| file.size).max(),
        "avg": if files.is_empty() { 0.0 } else { size as f64 / files.len() as f64 }, "totalFiles": files.len(), "totalSize": size}),
    )
}
