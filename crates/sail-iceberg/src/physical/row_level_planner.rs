use std::sync::Arc;

use datafusion::common::{
    DataFusionError, JoinType, NullEquality, Result, ScalarValue, TableReference, internal_err,
    not_impl_err, plan_err,
};
use datafusion::execution::SessionState;
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{Operator, TableScan, TableSource};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, IsNullExpr, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_planner::PhysicalPlanner;
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, PhysicalSinkMode, RowLevelCommand, RowLevelOperationType,
};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::row_level::RowLevelWriteNode;

use crate::lake_source::{
    IcebergLakeSource, catalog_managed_iceberg_from_options, metadata_location_from_options,
    split_iceberg_write_options_and_table_properties,
};
use crate::logical::IcebergTableSource;
use crate::operations::SnapshotUpdateKind;
use crate::options::r#gen::{IcebergReadOptions, IcebergWriteOptions};
use crate::physical_plan::merge_row_projection::IcebergMergeRowProjection;
use crate::physical_plan::{
    IcebergCommitExec, IcebergEqualityDeleteWriterExec, IcebergRemoveDataFilesExec,
    IcebergWriterExec, IcebergWriterExecOptions, prepare_iceberg_write_context,
};
use crate::row_level_metadata::{MERGE_PARTITION_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};
use crate::table::Table;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergRowLevelMode {
    CopyOnWrite,
    MergeOnRead,
}

pub(crate) async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match node.command() {
        RowLevelCommand::Delete => {
            plan_iceberg_delete(session_state, planner, node, physical_inputs).await
        }
        RowLevelCommand::Update => plan_iceberg_update(session_state, node, physical_inputs).await,
        RowLevelCommand::Merge => plan_iceberg_merge(session_state, node, physical_inputs).await,
    }
}

async fn plan_iceberg_merge(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let (table_url, table) = load_row_level_table(session_state, node).await?;
    match current_row_level_mode(&table, RowLevelCommand::Merge)? {
        IcebergRowLevelMode::CopyOnWrite => plan_iceberg_copy_on_write(
            session_state,
            node,
            physical_inputs,
            table_url,
            &table,
            SnapshotUpdateKind::CopyOnWrite,
        ),
        IcebergRowLevelMode::MergeOnRead => {
            plan_iceberg_merge_on_read(session_state, node, physical_inputs, table_url, &table)
        }
    }
}

fn plan_iceberg_merge_on_read(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    table_url: url::Url,
    table: &Table,
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal("Iceberg MERGE missing write plan input".to_string())
    })?;
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(table)?;
    let writer_options = resolve_row_level_writer_options(session_state, node)?;

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
        write_plan,
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
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let (table_url, table) = load_row_level_table(session_state, node).await?;
    match current_row_level_mode(&table, RowLevelCommand::Delete)? {
        IcebergRowLevelMode::CopyOnWrite => plan_iceberg_copy_on_write(
            session_state,
            node,
            physical_inputs,
            table_url,
            &table,
            SnapshotUpdateKind::CopyOnWriteDelete,
        ),
        IcebergRowLevelMode::MergeOnRead => {
            plan_iceberg_delete_merge_on_read(session_state, planner, node, table_url, &table).await
        }
    }
}

async fn plan_iceberg_update(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    let (table_url, table) = load_row_level_table(session_state, node).await?;
    match current_row_level_mode(&table, RowLevelCommand::Update)? {
        IcebergRowLevelMode::CopyOnWrite => plan_iceberg_copy_on_write(
            session_state,
            node,
            physical_inputs,
            table_url,
            &table,
            SnapshotUpdateKind::CopyOnWrite,
        ),
        IcebergRowLevelMode::MergeOnRead => not_impl_err!(
            "Iceberg UPDATE with `write.update.mode=merge-on-read` is not supported yet"
        ),
    }
}

async fn plan_iceberg_delete_merge_on_read(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    node: &RowLevelWriteNode,
    table_url: url::Url,
    table: &Table,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: Support conditionless DELETE by scanning all rows into equality deletes.
    let condition = node.condition().ok_or_else(|| {
        DataFusionError::Plan(
            "Iceberg equality-delete MOR DELETE requires a WHERE condition".to_string(),
        )
    })?;

    let read_options = IcebergReadOptions::resolve(session_state, node.target_options().to_vec())?;
    let provider = Arc::new(table.to_provider(&read_options)?);
    let table_source: Arc<dyn TableSource> = Arc::new(IcebergTableSource::new(provider));
    let target_scan = datafusion::logical_expr::LogicalPlan::TableScan(TableScan::try_new(
        table_reference_from_parts(node.target_table_name()),
        table_source,
        None,
        vec![],
        None,
    )?);
    let delete_plan = LogicalPlanBuilder::from(target_scan)
        .filter(condition.expr.clone())?
        .build()?;
    let delete_plan = session_state.optimize(&delete_plan)?;
    let physical_delete = planner
        .create_physical_plan(&delete_plan, session_state)
        .await?;

    let writer_options = resolve_row_level_writer_options(session_state, node)?;
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(table)?;
    let current_schema = table.metadata().current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing current schema".to_string())
    })?;
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

    let delete_input = strip_iceberg_metadata_columns(physical_delete)?;
    let delete_input: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(delete_input));
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

fn table_reference_from_parts(parts: &[String]) -> TableReference {
    match parts {
        [table] => TableReference::Bare {
            table: table.as_str().into(),
        },
        [schema, table] => TableReference::Partial {
            schema: schema.as_str().into(),
            table: table.as_str().into(),
        },
        [catalog, schema, table] => TableReference::Full {
            catalog: catalog.as_str().into(),
            schema: schema.as_str().into(),
            table: table.as_str().into(),
        },
        _ => TableReference::Bare {
            table: parts.join(".").into(),
        },
    }
}

async fn load_row_level_table(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
) -> Result<(url::Url, Table)> {
    let table_url =
        IcebergLakeSource::parse_table_url(vec![node.target_location().to_string()]).await?;
    let metadata_location = metadata_location_from_options(node.target_options());
    let catalog_managed_table = catalog_managed_iceberg_from_options(node.target_options());
    let metadata_location_for_load = catalog_managed_table.then_some(metadata_location).flatten();
    let table = Table::load_with_metadata_location(
        session_state,
        table_url.clone(),
        metadata_location_for_load,
    )
    .await?;
    Ok((table_url, table))
}

fn current_row_level_mode(table: &Table, command: RowLevelCommand) -> Result<IcebergRowLevelMode> {
    let property = match command {
        RowLevelCommand::Delete => "write.delete.mode",
        RowLevelCommand::Merge => "write.merge.mode",
        RowLevelCommand::Update => "write.update.mode",
    };
    let mode = table
        .metadata()
        .properties
        .get(property)
        .map_or("copy-on-write", String::as_str);
    if mode.eq_ignore_ascii_case("merge-on-read") {
        return Ok(IcebergRowLevelMode::MergeOnRead);
    }
    if mode.eq_ignore_ascii_case("copy-on-write") {
        return Ok(IcebergRowLevelMode::CopyOnWrite);
    }
    plan_err!(
        "Unknown Iceberg row-level operation mode for `{property}`: {mode}; expected `copy-on-write` or `merge-on-read`"
    )
}

fn plan_iceberg_copy_on_write(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    table_url: url::Url,
    table: &Table,
    snapshot_update_kind: SnapshotUpdateKind,
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Iceberg {:?} COW missing write plan input",
            node.command()
        ))
    })?;
    let insert_only_merge = node.command() == RowLevelCommand::Merge
        && node.merge_options().is_some_and(|options| {
            options.matched_clauses.is_empty()
                && options.not_matched_by_source_clauses.is_empty()
                && !options.not_matched_by_target_clauses.is_empty()
        });

    let writer_input = if insert_only_merge {
        write_plan
    } else {
        let touched_plan = physical_inputs.get(1).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Iceberg {:?} COW missing touched-file plan input",
                node.command()
            ))
        })?;
        build_targeted_writer_input(&write_plan, touched_plan)?
    };
    let (writer_input, logical_data_schema) = project_copy_on_write_data_rows(writer_input)?;
    let partition_columns = IcebergLakeSource::partition_columns_from_metadata(table)?;
    let writer_options = resolve_row_level_writer_options(session_state, node)?;
    let write_context = prepare_iceberg_write_context(
        &table_url,
        Some(table.metadata()),
        &writer_options,
        &partition_columns,
        &PhysicalSinkMode::Append,
        logical_data_schema.as_ref(),
    )?;
    let writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergWriterExec::new(
        writer_input,
        table_url.clone(),
        partition_columns,
        PhysicalSinkMode::Append,
        true,
        writer_options.clone(),
        write_context,
    )?);

    let (commit_input, snapshot_update_kind): (Arc<dyn ExecutionPlan>, _) = if insert_only_merge {
        (writer, SnapshotUpdateKind::FastAppend)
    } else {
        let touched_plan = reset_plan_states(Arc::clone(&physical_inputs[1]))?;
        let remove_actions: Arc<dyn ExecutionPlan> = Arc::new(IcebergRemoveDataFilesExec::try_new(
            touched_plan,
            MERGE_FILE_COLUMN,
        )?);
        (
            UnionExec::try_new(vec![writer, remove_actions])?,
            snapshot_update_kind,
        )
    };

    Ok(Arc::new(
        IcebergCommitExec::new(
            Arc::new(CoalescePartitionsExec::new(commit_input)),
            table_url,
            writer_options.lakehouse_table.clone(),
            snapshot_update_kind,
        )
        .with_expected_snapshot_id(node.expected_snapshot_id()),
    ))
}

fn build_targeted_writer_input(
    write_plan: &Arc<dyn ExecutionPlan>,
    touched_plan: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let write_schema = write_plan.schema();
    let path_index = write_schema.index_of(MERGE_FILE_COLUMN).map_err(|_| {
        DataFusionError::Internal(format!(
            "Iceberg COW writer input is missing path column '{MERGE_FILE_COLUMN}'"
        ))
    })?;
    let touched_schema = touched_plan.schema();
    let touched_path_index = touched_schema.index_of(MERGE_FILE_COLUMN).map_err(|_| {
        DataFusionError::Internal(format!(
            "Iceberg COW touched-file input is missing path column '{MERGE_FILE_COLUMN}'"
        ))
    })?;

    let insert_predicate: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::new(Column::new(
        MERGE_FILE_COLUMN,
        path_index,
    ))));
    let insert_rows: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
        insert_predicate,
        Arc::clone(write_plan),
    )?);

    let join = Arc::new(HashJoinExec::try_new(
        reset_plan_states(Arc::clone(touched_plan))?,
        reset_plan_states(Arc::clone(write_plan))?,
        vec![(
            Arc::new(Column::new(MERGE_FILE_COLUMN, touched_path_index)),
            Arc::new(Column::new(MERGE_FILE_COLUMN, path_index)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let touched_field_count = touched_schema.fields().len();
    let projection = write_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), touched_field_count + index))
                    as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    let touched_rows: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(projection, join)?);
    UnionExec::try_new(vec![insert_rows, touched_rows])
}

fn project_copy_on_write_data_rows(
    input: Arc<dyn ExecutionPlan>,
) -> Result<(
    Arc<dyn ExecutionPlan>,
    datafusion::arrow::datatypes::SchemaRef,
)> {
    let schema = input.schema();
    let operation_index = schema.index_of(sail_common_datafusion::datasource::OPERATION_COLUMN)?;
    let operation: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
        sail_common_datafusion::datasource::OPERATION_COLUMN,
        operation_index,
    ));
    let predicate = [
        RowLevelOperationType::Copy,
        RowLevelOperationType::Update,
        RowLevelOperationType::Insert,
        RowLevelOperationType::MatchedUpdate,
        RowLevelOperationType::NotMatchedBySourceUpdate,
    ]
    .into_iter()
    .map(|operation_type| {
        Arc::new(BinaryExpr::new(
            Arc::clone(&operation),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(
                operation_type.as_i32(),
            )))),
        )) as Arc<dyn PhysicalExpr>
    })
    .reduce(|left, right| {
        Arc::new(BinaryExpr::new(left, Operator::Or, right)) as Arc<dyn PhysicalExpr>
    })
    .ok_or_else(|| DataFusionError::Internal("Iceberg COW operation filter is empty".into()))?;
    let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, input)?);

    let row_projection = IcebergMergeRowProjection::try_new(schema)?;
    let filtered_schema = filtered.schema();
    let data_projection = row_projection
        .data_indices()
        .iter()
        .map(|index| {
            let field = filtered_schema.field(*index);
            (
                Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    Ok((
        Arc::new(ProjectionExec::try_new(data_projection, filtered)?),
        row_projection.data_schema(),
    ))
}

fn strip_iceberg_metadata_columns(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let internal_columns = [
        MERGE_FILE_COLUMN,
        sail_common_datafusion::datasource::MERGE_ROW_INDEX_COLUMN,
        MERGE_PARTITION_SPEC_ID_COLUMN,
        MERGE_PARTITION_COLUMN,
    ];
    let projection = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !internal_columns.contains(&field.name().as_str()))
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    if projection.len() == schema.fields().len() {
        return Ok(input);
    }
    if projection.is_empty() {
        return internal_err!("Iceberg MOR DELETE target contains no data columns");
    }
    Ok(Arc::new(ProjectionExec::try_new(projection, input)?))
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
