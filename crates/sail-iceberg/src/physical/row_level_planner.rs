use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, not_impl_err, plan_err};
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{PhysicalSinkMode, RowLevelCommand, RowLevelWriteMode};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::row_level::RowLevelWriteNode;

use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_options, metadata_location_from_options,
    resolve_iceberg_metadata_location, split_iceberg_write_options_and_table_properties,
};
use crate::operations::SnapshotUpdateKind;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical_plan::equality_delete_writer_exec::validate_equality_delete_schema;
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::{
    IcebergCommitExec, IcebergEqualityDeleteWriterExec, IcebergWriterExec,
    IcebergWriterExecOptions, prepare_iceberg_write_context,
};
use crate::table::Table;

pub(crate) async fn plan_iceberg_row_level_write(
    session: &dyn Session,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match (node.mode(), node.command()) {
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Delete) => {
            plan_iceberg_delete(session, planner, node).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Merge) => {
            plan_iceberg_merge(session, node, physical_inputs).await
        }
        (RowLevelWriteMode::MergeOnRead, command) => {
            not_impl_err!("Iceberg row-level {command:?} operations")
        }
        (RowLevelWriteMode::CopyOnWrite, command) => {
            not_impl_err!("Iceberg row-level {command:?} copy-on-write operations")
        }
    }
}

async fn plan_iceberg_merge(
    session: &dyn Session,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let [write_plan] = physical_inputs else {
        return plan_err!("Iceberg MERGE requires exactly one write-plan input");
    };
    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = resolve_iceberg_metadata_location(
        node.target_lakehouse_table(),
        metadata_location,
        catalog_managed_table,
    )?;
    let table =
        Table::load_with_metadata_location(session, table_url.clone(), metadata_location_for_load)
            .await?;
    ensure_current_row_level_mode(&table, RowLevelCommand::Merge)?;
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(&table)?;
    let writer_options = resolve_row_level_writer_options(session, node)?;

    let merge_projection = IcebergMergeRowProjection::try_new(write_plan.schema())?;
    let data_rows_schema = merge_projection.data_schema();
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        data_rows_schema.as_ref(),
    )?;
    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new_merge(
        Arc::clone(write_plan),
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        write_context,
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            writer,
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::RowDelta,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

async fn plan_iceberg_delete(
    session: &dyn Session,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: Support conditionless DELETE by scanning all rows into equality deletes.
    let condition = node.condition().ok_or_else(|| {
        DataFusionError::Plan(
            "Iceberg equality-delete MOR DELETE requires a WHERE condition".to_string(),
        )
    })?;

    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = resolve_iceberg_metadata_location(
        node.target_lakehouse_table(),
        metadata_location,
        catalog_managed_table,
    )?;
    let table =
        Table::load_with_metadata_location(session, table_url.clone(), metadata_location_for_load)
            .await?;
    ensure_current_row_level_mode(&table, RowLevelCommand::Delete)?;
    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    validate_equality_delete_schema(current_schema)?;

    let delete_plan = LogicalPlanBuilder::from(node.raw_target().as_ref().clone())
        .filter(condition.expr.clone())?
        .build()?;
    let physical_delete = planner.create_physical_plan(&delete_plan, session).await?;

    let writer_options = resolve_row_level_writer_options(session, node)?;
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(&table)?;
    let current_arrow_schema =
        crate::datasource::type_converter::iceberg_schema_to_arrow(current_schema)?;
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        &current_arrow_schema,
    )?;

    let delete_input: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(physical_delete));
    let delete_writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergEqualityDeleteWriterExec::new(
        delete_input,
        table_url.clone(),
        writer_options.table_properties.clone(),
        writer_options.write_data_path.clone(),
        writer_options.write_folder_storage_path.clone(),
        write_context,
        writer_options.lakehouse_table.clone(),
    )?);

    Ok(Arc::new(
        IcebergCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(delete_writer)),
            table_url,
            writer_options.lakehouse_table.clone(),
            SnapshotUpdateKind::RowDelta,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

fn ensure_current_row_level_mode(table: &Table, command: RowLevelCommand) -> Result<()> {
    let (operation, property) = match command {
        RowLevelCommand::Delete => ("DELETE", "write.delete.mode"),
        RowLevelCommand::Merge => ("MERGE", "write.merge.mode"),
        RowLevelCommand::Update => ("UPDATE", "write.update.mode"),
    };
    let mode = table
        .metadata()
        .properties
        .get(property)
        .map_or("copy-on-write", String::as_str);
    if mode.eq_ignore_ascii_case("merge-on-read") {
        return Ok(());
    }
    if mode.eq_ignore_ascii_case("copy-on-write") {
        return not_impl_err!(
            "Iceberg {operation} with `{property}=copy-on-write` is not supported yet; set `{property}=merge-on-read`"
        );
    }
    plan_err!(
        "Unknown Iceberg row-level operation mode for `{property}`: {mode}; expected `copy-on-write` or `merge-on-read`"
    )
}

fn resolve_row_level_writer_options(
    session: &dyn Session,
    node: &RowLevelWriteNode,
) -> Result<IcebergWriterExecOptions> {
    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let variant_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(session, clean_options)?;
    let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
    writer_options.apply_variant_shredding_option_presence(variant_presence);
    writer_options.table_properties = table_properties;
    writer_options.lakehouse_table = node.target_lakehouse_table().cloned();
    Ok(writer_options)
}
