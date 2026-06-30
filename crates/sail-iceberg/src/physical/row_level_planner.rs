use std::sync::Arc;

use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{
    PhysicalSinkMode, RowLevelCommand, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN,
};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::options::gen::IcebergWriteOptions;
use crate::physical_plan::{
    IcebergCommitExec, IcebergEqualityDeleteWriterExec, IcebergMergeDataRowsExec,
    IcebergPositionDeleteWriterExec, IcebergWriterExec, IcebergWriterExecOptions,
};
use crate::table::Table;
use crate::table_format::{
    catalog_managed_iceberg_from_options, metadata_location_from_options,
    split_iceberg_write_options_and_table_properties, IcebergTableFormat,
};

pub(crate) async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match node.command() {
        RowLevelCommand::Delete => plan_iceberg_delete(session_state, planner, node).await,
        RowLevelCommand::Merge => plan_iceberg_merge(session_state, node, physical_inputs).await,
        command => not_impl_err!("Iceberg row-level {command:?} operations"),
    }
}

async fn plan_iceberg_merge(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal("Iceberg MERGE missing write plan input".to_string())
    })?;
    if node.touched_files_plan().is_some() && physical_inputs.len() < 2 {
        return plan_err!("Iceberg MERGE missing touched-file plan input");
    }
    let row_index_delete_plan = if node.row_index_delete_plan().is_some() {
        physical_inputs.last().cloned()
    } else {
        None
    };

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table
        .then_some(metadata_location.clone())
        .flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    let partition_columns = IcebergTableFormat::partition_columns_from_metadata(&table)?;
    let writer_options = resolve_row_level_writer_options(session_state, node)?;

    let data_rows: Arc<dyn ExecutionPlan> =
        Arc::new(IcebergMergeDataRowsExec::try_new(write_plan)?);
    let data_rows_schema = data_rows.schema();
    let writer_input: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(data_rows));
    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        writer_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        Some(data_rows_schema),
    ));

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(row_index_delete_plan) =
        row_index_delete_plan
    {
        let delete_input: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(row_index_delete_plan));
        let delete_writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergPositionDeleteWriterExec::new(
            delete_input,
            table_url.clone(),
            writer_options.table_properties.clone(),
            writer_options.write_data_path.clone(),
            writer_options.write_folder_storage_path.clone(),
            MERGE_FILE_COLUMN,
            MERGE_ROW_INDEX_COLUMN,
        ));
        UnionExec::try_new(vec![writer, delete_writer])?
    } else {
        writer
    };

    Ok(Arc::new(IcebergCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(commit_input)),
        table_url,
        writer_options.lakehouse_table.clone(),
    )))
}

async fn plan_iceberg_delete(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: Support conditionless DELETE by scanning all rows into equality deletes.
    let condition = node.condition().ok_or_else(|| {
        DataFusionError::Plan(
            "Iceberg equality-delete MOR DELETE requires a WHERE condition".to_string(),
        )
    })?;
    let delete_plan = LogicalPlanBuilder::from(node.raw_target().as_ref().clone())
        .filter(condition.expr.clone())?
        .build()?;
    let physical_delete = planner
        .create_physical_plan(&delete_plan, session_state)
        .await?;

    let table_url =
        IcebergTableFormat::parse_table_url(vec![node.target_location().to_string()]).await?;
    let writer_options = resolve_row_level_writer_options(session_state, node)?;

    let delete_input: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(physical_delete));
    let delete_writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergEqualityDeleteWriterExec::new(
        delete_input,
        table_url.clone(),
        writer_options.table_properties.clone(),
        writer_options.write_data_path.clone(),
        writer_options.write_folder_storage_path.clone(),
        writer_options.lakehouse_table.clone(),
    ));

    Ok(Arc::new(IcebergCommitExec::new(
        Arc::new(CoalescePartitionsExec::new(delete_writer)),
        table_url,
        writer_options.lakehouse_table.clone(),
    )))
}

fn resolve_row_level_writer_options(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<IcebergWriterExecOptions> {
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let variant_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(session_state, clean_options)?;
    let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
    writer_options.apply_variant_shredding_option_presence(variant_presence);
    writer_options.table_properties = table_properties;
    writer_options.lakehouse_table = node.target_lakehouse_table().cloned();
    Ok(writer_options)
}
