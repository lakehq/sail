// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::PhysicalSinkMode;

use super::context::PlannerContext;
use crate::datasource::{
    collect_physical_columns, DataFusionMixins, DeltaScanConfigBuilder, DeltaTableProvider,
    PredicateProperties,
};
use crate::kernel::DeltaTableError;
use crate::physical_plan::{
    create_projection, create_repartition, create_sort, DeltaCommitExec,
    DeltaPhysicalExprAdapterFactory, DeltaWriterExec,
};
use crate::table::DeltaTableState;

pub fn build_standard_write_layers(
    ctx: &PlannerContext<'_>,
    input: Arc<dyn ExecutionPlan>,
    sink_mode: &PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
    original_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = create_projection(Arc::clone(&input), ctx.partition_columns().to_vec())?;
    let plan = create_repartition(plan, ctx.partition_columns().to_vec())?;
    let plan = create_sort(plan, ctx.partition_columns().to_vec(), sort_order)?;

    let writer_schema = plan.schema();
    let writer = Arc::new(DeltaWriterExec::new(
        plan,
        ctx.table_url().clone(),
        ctx.options().clone(),
        ctx.partition_columns().to_vec(),
        sink_mode.clone(),
        ctx.table_exists(),
        writer_schema,
        None,
        None,
    ));

    Ok(Arc::new(DeltaCommitExec::new(
        writer,
        ctx.table_url().clone(),
        ctx.partition_columns().to_vec(),
        ctx.table_exists(),
        original_schema,
        sink_mode.clone(),
    )))
}

pub fn align_schemas_for_union(
    new_data_plan: Arc<dyn ExecutionPlan>,
    old_data_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let new_schema = new_data_plan.schema();
    let old_schema = old_data_plan.schema();

    if new_schema.fields().len() != old_schema.fields().len() {
        return Err(DataFusionError::Plan(
            "Schema mismatch between new and old data - schema evolution not yet implemented"
                .to_string(),
        ));
    }

    let mut new_projections = Vec::new();
    let mut old_projections = Vec::new();

    for (i, field) in new_schema.fields().iter().enumerate() {
        new_projections.push((
            Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));

        if let Some((old_idx, _)) = old_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, old_field)| old_field.name() == field.name())
        {
            old_projections.push((
                Arc::new(Column::new(field.name(), old_idx)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Field '{}' not found in old data schema",
                field.name()
            )));
        }
    }

    let aligned_new = Arc::new(ProjectionExec::try_new(new_projections, new_data_plan)?);
    let aligned_old = Arc::new(ProjectionExec::try_new(old_projections, old_data_plan)?);

    Ok((aligned_new, aligned_old))
}

pub fn adapt_predicate_to_schema(
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory.create(logical_schema, physical_schema);
    adapter.rewrite(predicate)
}

/// Build a "touched files" subplan that yields a single column of file paths.
///
/// This is designed to be *visible* in EXPLAIN: the leaf will be a `DataSourceExec`.
/// Downstream plans can convert these paths into `Add(JSON)` using `DeltaFileLookupExec`.
pub async fn build_touched_file_plan(
    ctx: &PlannerContext<'_>,
    snapshot: &DeltaTableState,
    log_store: crate::storage::LogStoreRef,
    predicate: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let logical_schema = snapshot
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Decide if we can do a partition-only plan. For now we keep the old, cheap approach
    // (memory scan) elsewhere; this helper focuses on the DataSourceExec-backed path.
    let mut expr_properties =
        PredicateProperties::new(snapshot.metadata().partition_columns().clone());
    expr_properties
        .analyze_predicate(&predicate)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if expr_properties.partition_only {
        // Build an in-memory "Add actions table" (path + partition cols), filter it, then project
        // down to the path column. This still shows up as a DataSourceExec leaf (MemorySource).
        let batch = snapshot
            .add_actions_table(true)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        // Always include `path`
        let path_array = batch.column_by_name("path").ok_or_else(|| {
            DataFusionError::External(Box::new(DeltaTableError::generic(
                "Column with name `path` does not exist",
            )))
        })?;
        arrays.push(path_array.clone());
        fields.push(datafusion::arrow::datatypes::Field::new(
            "path",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ));

        let batch_schema = batch.schema();

        // Partition columns in add_actions_table are prefixed with "partition."
        for partition_column in snapshot.metadata().partition_columns() {
            let partition_column_name = format!("partition.{}", partition_column);
            if let Some(array) = batch.column_by_name(&partition_column_name) {
                arrays.push(array.clone());
                // Rename field back to logical partition column name
                let field = batch_schema
                    .field_with_name(&partition_column_name)
                    .map_err(|e| {
                        DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
                    })?;
                fields.push(datafusion::arrow::datatypes::Field::new(
                    partition_column,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
        }

        let partition_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields.clone()));
        let partition_batch =
            datafusion::arrow::record_batch::RecordBatch::try_new(partition_schema.clone(), arrays)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let memory_source =
            MemorySourceConfig::try_new(&[vec![partition_batch]], partition_schema.clone(), None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let memory_exec = DataSourceExec::from_data_source(memory_source);

        // Adapt predicate to the partition-only schema
        let adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(DeltaPhysicalExprAdapterFactory {});
        let adapter = adapter_factory.create(logical_schema.clone(), partition_schema.clone());
        let adapted_predicate = adapter.rewrite(predicate).map_err(|e| {
            DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
        })?;

        let filtered = Arc::new(FilterExec::try_new(adapted_predicate, memory_exec)?);

        let path_idx = partition_schema.index_of("path").map_err(|e| {
            DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
        })?;
        let projections = vec![(
            Arc::new(Column::new("path", path_idx)) as Arc<dyn PhysicalExpr>,
            "path".to_string(),
        )];
        return Ok(Arc::new(ProjectionExec::try_new(projections, filtered)?));
    }

    // 1) Stats-based pruning using Delta log data.
    let pruning_schema = snapshot
        .arrow_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let pruning_predicate = PruningPredicate::try_new(predicate.clone(), pruning_schema.clone())
        .map_err(|e| {
            DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
        })?;

    let log_data = snapshot.log_data();
    let prune_mask = pruning_predicate.prune(&log_data).map_err(|e| {
        DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
    })?;

    let candidate_adds = log_data
        .into_iter()
        .zip(prune_mask)
        .filter_map(|(file, keep)| if keep { Some(file.add_action()) } else { None })
        .collect::<Vec<_>>();

    // 2) Build a scan plan over only the candidate files.
    let scan_config = DeltaScanConfigBuilder::new()
        .with_file_column(true)
        .build(snapshot)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let full_logical_schema = crate::datasource::df_logical_schema(
        snapshot,
        &scan_config.file_column_name,
        Some(logical_schema.clone()),
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let file_column_name = scan_config.file_column_name.clone().ok_or_else(|| {
        DataFusionError::External(Box::new(DeltaTableError::generic(
            "File column name not found in scan config",
        )))
    })?;

    // Only project columns needed by the predicate + file path column.
    let mut used_columns_set = collect_physical_columns(&predicate);
    used_columns_set.insert(file_column_name.clone());

    let mut projection_indices = Vec::new();
    for (i, field) in full_logical_schema.fields().iter().enumerate() {
        if used_columns_set.contains(field.name()) {
            projection_indices.push(i);
        }
    }

    let table_provider = DeltaTableProvider::try_new(snapshot.clone(), log_store, scan_config)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_files(candidate_adds);

    let scan_plan = table_provider
        .scan(ctx.session(), Some(&projection_indices), &[], None)
        .await?;

    let scan_schema = scan_plan.schema();

    // 3) Adapt predicate to scan schema and filter rows.
    let adapter_factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(DeltaPhysicalExprAdapterFactory {});
    let adapter = adapter_factory.create(full_logical_schema.clone(), scan_schema.clone());
    let adapted_predicate = adapter.rewrite(predicate).map_err(|e| {
        DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
    })?;
    let filtered = Arc::new(FilterExec::try_new(adapted_predicate, scan_plan)?);

    // 4) Project down to a single "file path" column (first-stage touched file stream).
    let file_idx = scan_schema.index_of(&file_column_name).map_err(|e| {
        DataFusionError::External(Box::new(DeltaTableError::generic(e.to_string())))
    })?;
    let projections = vec![(
        Arc::new(Column::new(&file_column_name, file_idx)) as Arc<dyn PhysicalExpr>,
        file_column_name,
    )];
    Ok(Arc::new(ProjectionExec::try_new(projections, filtered)?))
}
