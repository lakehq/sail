use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result, not_impl_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
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
use crate::physical_plan::action_schema::{CommitMeta, encode_commit_meta};
use crate::physical_plan::equality_delete_writer_exec::validate_equality_delete_schema;
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::{
    IcebergCommitExec, IcebergEqualityDeleteWriterExec, IcebergWriterExec,
    IcebergWriterExecOptions, prepare_iceberg_write_context,
};
use crate::table::Table;

pub(crate) async fn plan_iceberg_row_level_write(
    session: &dyn Session,
    _planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match (node.mode(), node.command()) {
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Delete) => {
            plan_iceberg_delete(session, node, physical_inputs).await
        }
        (RowLevelWriteMode::MergeOnRead, RowLevelCommand::Merge) => {
            plan_iceberg_merge(session, node, physical_inputs).await
        }
        (RowLevelWriteMode::MergeOnRead, command) => {
            not_impl_err!("Iceberg row-level {command:?} operations")
        }
        (RowLevelWriteMode::CopyOnWrite, _) => {
            plan_iceberg_copy_on_write(session, node, physical_inputs).await
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
    ensure_current_row_level_mode(&table, node)?;
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
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let [delete_rows] = physical_inputs else {
        return plan_err!("Iceberg DELETE requires exactly one write-plan input");
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
    ensure_current_row_level_mode(&table, node)?;
    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
    validate_equality_delete_schema(current_schema)?;

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
        Arc::new(CoalescePartitionsExec::new(Arc::clone(delete_rows)));
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

async fn plan_iceberg_copy_on_write(
    session: &dyn Session,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = resolve_iceberg_metadata_location(
        node.target_lakehouse_table(),
        metadata_location_from_options(node.target_options()),
        catalog_managed_iceberg_from_options(node.target_options()),
    )?;
    let table =
        Table::load_with_metadata_location(session, table_url.clone(), metadata_location).await?;
    ensure_current_row_level_mode(&table, node)?;
    let [input] = physical_inputs else {
        return plan_err!("Iceberg copy-on-write requires exactly one write-plan input");
    };
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(&table)?;
    let writer_options = resolve_row_level_writer_options(session, node)?;
    let distribution_key = match node.command() {
        RowLevelCommand::Delete => "write.delete.distribution-mode",
        RowLevelCommand::Update => "write.update.distribution-mode",
        RowLevelCommand::Merge => "write.merge.distribution-mode",
    };
    let mode = table
        .metadata()
        .properties
        .get(distribution_key)
        .or_else(|| table.metadata().properties.get("write.distribution-mode"))
        .map(String::as_str)
        .unwrap_or("hash");
    let mut writer_options = writer_options;
    writer_options.copy_on_write_partitioning = match mode.to_ascii_lowercase().as_str() {
        "none" => false,
        "hash" => true,
        "range" => return not_impl_err!("Iceberg copy-on-write range distribution"),
        _ => return plan_err!("Unknown Iceberg write distribution mode: {mode}"),
    };
    let data_schema = IcebergMergeRowProjection::try_new(input.schema())?.data_schema();
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        data_schema.as_ref(),
    )?;
    if node.command() == RowLevelCommand::Delete
        && let Some(paths) = crate::logical::row_level::target_provider(node.raw_target())?
            .metadata_delete_paths(session)
            .await?
    {
        let batch = encode_commit_meta(CommitMeta {
            table_uri: table_url.to_string(),
            removed_data_file_paths: paths,
            skip_empty_commit: true,
            requirements: write_context.requirements,
            table_properties: writer_options.table_properties.clone(),
            lakehouse_table: writer_options.lakehouse_table.clone(),
            ..Default::default()
        })?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
        return Ok(Arc::new(
            IcebergCommitExec::new(
                input,
                table_url,
                writer_options.lakehouse_table,
                SnapshotUpdateKind::RowLevelRewrite,
            )
            .with_expected_snapshot_id(node.expected_snapshot_id()),
        ));
    }
    let writer = Arc::new(IcebergWriterExec::new_copy_on_write(
        Arc::clone(input),
        table_url.clone(),
        partition_columns,
        writer_options.clone(),
        write_context,
    )?);
    let snapshot_update_kind = if node.merge_options().is_some_and(|options| {
        options.matched_clauses.is_empty() && options.not_matched_by_source_clauses.is_empty()
    }) {
        SnapshotUpdateKind::FastAppend
    } else {
        SnapshotUpdateKind::RowLevelRewrite
    };
    Ok(Arc::new(
        IcebergCommitExec::new(
            writer,
            table_url,
            writer_options.lakehouse_table.clone(),
            snapshot_update_kind,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

fn ensure_current_row_level_mode(table: &Table, node: &RowLevelWriteNode) -> Result<()> {
    let mode = crate::logical::row_level::IcebergRowLevelOptions::from(table.metadata())
        .mode(node.command())?;
    if mode != node.mode() {
        return plan_err!(
            "Iceberg row-level write mode changed after planning; retry the operation"
        );
    }
    Ok(())
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
