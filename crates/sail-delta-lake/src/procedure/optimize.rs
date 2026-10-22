use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::catalog::Session;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;
use sail_data_source::options::ResolveOptions;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::maintenance::{DeltaMaintenanceExec, DeltaMaintenanceNode, DeltaMaintenancePredicate};
use super::rewrite_exec::DeltaOptimizeRewriteExec;
use super::{optional_bool, optional_string};
use crate::DeltaLakeSource;
use crate::delta_log::LogStoreRef;
use crate::options::r#gen::{DeltaReadOptions, DeltaWriteOptions};
use crate::physical_plan::{
    DeltaScanByAddsExec, DeltaWriterExec, DeltaWriterExecOptions, delta_action_schema,
    encode_actions, prepare_delta_write_context,
};
use crate::snapshot::DeltaSnapshot;
use crate::spec::{
    Action, Add, CommitAction, DeltaError, DeltaOperation, RemoveOptions, TableFeature,
};
use crate::table::load_delta_read_state;
use crate::transaction::{CommitBuilder, CommitProperties};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaOptimizePlan {
    pub table_url: String,
    pub table_id: String,
    pub version: i64,
    pub removed: Vec<Add>,
    pub considered: usize,
    pub optimized_partitions: usize,
    pub num_bins: usize,
    pub parallelism: usize,
}

pub(crate) async fn plan(
    session: &dyn Session,
    node: &DeltaMaintenanceNode,
    filter: Option<DeltaMaintenancePredicate>,
) -> Result<DeltaMaintenanceExec> {
    let invocation = &node.call.invocation;
    let options = &node.options;
    let table_url = DeltaLakeSource::parse_table_url(session, node.paths.clone()).await?;
    let read_options = DeltaReadOptions::resolve(session, node.read_options.clone())?;
    let (snapshot, _, scan_config) = load_delta_read_state(
        session,
        table_url,
        None,
        read_options,
        false,
        node.lakehouse_table.clone(),
    )
    .await?;
    super::maintenance::validate_snapshot(&snapshot, invocation)?;
    if optional_bool(invocation, "full")?.unwrap_or(false)
        || optional_string(invocation, "zorder")?.is_some()
    {
        return not_impl_err!("OPTIMIZE FULL and ZORDER are not implemented");
    }
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::Clustering)
    {
        // TODO: Implement liquid-clustering rewrites and maintain their domain metadata.
        return not_impl_err!("OPTIMIZE liquid-clustered tables is not implemented");
    }
    if snapshot
        .protocol()
        .has_writer_feature(&TableFeature::RowTracking)
    {
        // TODO: Preserve row IDs and row commit versions across OPTIMIZE rewrites.
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
    let candidates = select_files(&snapshot, filter.as_ref())?;
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
    let mut writer_options =
        DeltaWriterExecOptions::from(DeltaWriteOptions::resolve(session, vec![])?);
    writer_options.target_file_size = max_size as u64;
    let schema = snapshot.input_schema()?;
    let operation = DeltaOperation::Optimize {
        predicate: optional_string(invocation, "predicate")?.map(str::to_string),
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
    let num_bins = bins.len();
    let parallelism = session
        .config()
        .target_partitions()
        .max(1)
        .min(num_bins.max(1));
    let rewrite_plan = DeltaOptimizePlan {
        table_url: snapshot.table_url().to_string(),
        table_id: snapshot.metadata().id().to_string(),
        version: snapshot.version(),
        removed,
        considered,
        optimized_partitions,
        num_bins,
        parallelism,
    };
    let mut assignments = vec![Vec::new(); parallelism];
    let mut assigned_bytes = vec![0_i64; parallelism];
    let mut order = bins
        .iter()
        .enumerate()
        .map(|(index, files)| Ok((index, total_size(files)?)))
        .collect::<Result<Vec<_>>>()?;
    order.sort_by_key(|(index, bytes)| (std::cmp::Reverse(*bytes), *index));
    for (index, bytes) in order {
        let task = (0..parallelism)
            .min_by_key(|task| assigned_bytes[*task])
            .ok_or_else(|| DeltaError::generic("OPTIMIZE requires a rewrite task"))?;
        assignments[task].push(index);
        assigned_bytes[task] = assigned_bytes[task].saturating_add(bytes);
    }
    let mut partitions = bins
        .into_iter()
        .map(|files| {
            encode_actions(files.into_iter().map(Action::Add).collect(), None)
                .map(|batch| vec![batch])
        })
        .collect::<Result<Vec<_>>>()?;
    if partitions.is_empty() {
        partitions.push(vec![]);
        assignments[0].push(0);
    }
    let files = DataSourceExec::from_data_source(MemorySourceConfig::try_new(
        &partitions,
        delta_action_schema()?,
        None,
    )?);
    let scan = Arc::new(
        DeltaScanByAddsExec::new(
            files,
            snapshot.table_url().clone(),
            snapshot.version(),
            schema.clone(),
            schema.clone(),
            scan_config,
            None,
            None,
            None,
            node.lakehouse_table.clone(),
            None,
        )
        .preserve_file_groups(),
    );
    let writer = Arc::new(DeltaWriterExec::new(
        scan,
        snapshot.table_url().clone(),
        writer_options,
        snapshot.metadata().configuration().clone(),
        snapshot.metadata().partition_columns().clone(),
        PhysicalSinkMode::Append,
        true,
        schema,
        write_context,
        node.lakehouse_table.clone(),
    )?);
    let input = Arc::new(DeltaOptimizeRewriteExec::try_new(writer, assignments)?);
    DeltaMaintenanceExec::try_new(node.call.clone(), node.options.clone(), filter)?
        .with_optimize(input, rewrite_plan)
}

pub(super) async fn finish(
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    invocation: &LakeProcedureInvocation,
    plan: &DeltaOptimizePlan,
    added: Vec<Add>,
    started: i64,
) -> Result<RecordBatch> {
    let DeltaOptimizePlan {
        removed,
        considered,
        optimized_partitions,
        num_bins,
        parallelism,
        ..
    } = plan;
    let schema = snapshot.input_schema()?;
    let operation = DeltaOperation::Optimize {
        predicate: optional_string(invocation, "predicate")?.map(str::to_string),
    };
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
            ("numRemovedBytes", total_size(removed)?),
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
        "filesAdded": sizes(&added)?, "filesRemoved": sizes(removed)?, "partitionsOptimized": optimized_partitions,
        "zOrderStats": null, "clusteringStats": null, "numBins": num_bins, "numBatches": 1,
        "totalConsideredFiles": considered, "totalFilesSkipped": considered - removed.len(),
        "preserveInsertionOrder": false, "numFilesSkippedToReduceWriteAmplification": 0,
        "numBytesSkippedToReduceWriteAmplification": 0, "startTimeMs": started, "endTimeMs": finished,
        "totalClusterParallelism": parallelism, "totalScheduledTasks": if *num_bins == 0 { 0 } else { *parallelism }, "autoCompactParallelismStats": null,
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::delta_log::{StorageConfig, default_logstore};
    use crate::snapshot::DeltaSnapshotConfig;
    use crate::spec::inspection::DeltaProcedureType;
    use crate::spec::{DataType, Metadata, Protocol, SaveMode, StructField, StructType};

    #[tokio::test]
    async fn commit_uses_planned_snapshot_for_concurrent_changes() -> Result<()> {
        for concurrent_change in ["append", "remove", "metadata"] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let url = url::Url::parse("memory:///").unwrap();
            let log_store = default_logstore(store.clone(), store, &url, &StorageConfig);
            let protocol = Protocol::new(1, 2, None, None);
            let metadata = Metadata::try_new(
                None,
                None,
                StructType::try_new([StructField::not_null("id", DataType::LONG)])?,
                vec![],
                0,
                HashMap::new(),
            )?;
            let file = |path: &str| Add {
                path: path.to_string(),
                size: 10,
                data_change: true,
                stats: Some(r#"{"numRecords":1}"#.to_string()),
                ..Default::default()
            };
            let removed = vec![file("a.parquet"), file("b.parquet")];
            let mut actions = vec![
                CommitAction::Protocol(protocol.clone()),
                CommitAction::Metadata(metadata.clone()),
            ];
            actions.extend(removed.iter().cloned().map(CommitAction::Add));
            CommitBuilder::default()
                .with_actions(actions)
                .build(
                    None,
                    log_store.clone(),
                    DeltaOperation::Create {
                        mode: SaveMode::ErrorIfExists,
                        location: url.to_string(),
                        protocol: Box::new(protocol),
                        metadata: Box::new(metadata.clone()),
                    },
                )
                .await?;
            let snapshot = Arc::new(
                DeltaSnapshot::try_new(
                    log_store.as_ref(),
                    DeltaSnapshotConfig::default(),
                    Some(0),
                    None,
                )
                .await?,
            );
            let plan = DeltaOptimizePlan {
                table_url: url.to_string(),
                table_id: metadata.id().to_string(),
                version: 0,
                removed: removed.clone(),
                considered: 2,
                optimized_partitions: 1,
                num_bins: 1,
                parallelism: 1,
            };
            let concurrent_action = match concurrent_change {
                "append" => CommitAction::Add(file("appended.parquet")),
                "remove" => CommitAction::Remove(
                    removed[0]
                        .clone()
                        .into_remove_with_options(1, RemoveOptions::default()),
                ),
                _ => CommitAction::Metadata(metadata),
            };
            CommitBuilder::default()
                .with_actions(vec![concurrent_action])
                .build(
                    Some(snapshot.clone()),
                    log_store.clone(),
                    DeltaOperation::Write {
                        mode: SaveMode::Append,
                        partition_by: None,
                        predicate: None,
                    },
                )
                .await?;
            let procedure = super::super::descriptor(DeltaProcedureType::Optimize).unwrap();
            let invocation = LakeProcedureInvocation {
                arguments: vec![
                    sail_common_datafusion::lakeprocedure::LakeProcedureValue::Null;
                    procedure.parameters.len()
                ],
                procedure,
            };
            let result = finish(
                snapshot,
                log_store.clone(),
                &invocation,
                &plan,
                vec![file("optimized.parquet")],
                0,
            )
            .await;
            let latest = DeltaSnapshot::try_new(
                log_store.as_ref(),
                DeltaSnapshotConfig::default(),
                None,
                None,
            )
            .await?;
            if concurrent_change == "append" {
                result?;
                assert_eq!(latest.version(), 2);
                let mut paths = latest
                    .adds()
                    .iter()
                    .map(|add| add.path.as_str())
                    .collect::<Vec<_>>();
                paths.sort();
                assert_eq!(paths, ["appended.parquet", "optimized.parquet"]);
                assert!(
                    !latest
                        .adds()
                        .iter()
                        .find(|add| add.path == "optimized.parquet")
                        .unwrap()
                        .data_change
                );
            } else {
                let Err(datafusion::common::DataFusionError::External(error)) = result else {
                    return plan_err!("{concurrent_change} must invalidate the rewrite");
                };
                assert!(matches!(
                    error.downcast_ref::<crate::spec::TransactionError>(),
                    Some(crate::spec::TransactionError::CommitConflict(_))
                ));
                assert_eq!(latest.version(), 1);
                assert!(
                    latest
                        .adds()
                        .iter()
                        .all(|add| add.path != "optimized.parquet")
                );
            }
        }
        Ok(())
    }
}
