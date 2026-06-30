use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{not_impl_err, plan_err, DataFusionError, Result};
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_common_datafusion::datasource::{
    PhysicalSinkMode, RowLevelCommand, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN,
};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::{MergeCardinalityCheckNode, RowLevelWriteNode};
use sail_physical_plan::merge_cardinality_check::MergeCardinalityCheckExec;

use crate::logical::IcebergTableSource;
use crate::options::gen::IcebergWriteOptions;
use crate::physical_plan::{
    IcebergCommitExec, IcebergMergeDataRowsExec, IcebergPositionDeleteWriterExec,
    IcebergWriterExec, IcebergWriterExecOptions,
};
use crate::table::Table;
use crate::table_format::{
    catalog_managed_iceberg_from_options, metadata_location_from_options, plan_iceberg_write,
    split_iceberg_write_options_and_table_properties, IcebergTableFormat, IcebergWriteNode,
};

pub struct IcebergPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for IcebergPhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<IcebergWriteNode>() {
            let [logical_input] = logical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one logical input"
                );
            };
            let [physical_input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "IcebergWriteNode requires exactly one physical input"
                );
            };
            return plan_iceberg_write(session_state, logical_input, physical_input.clone(), node)
                .await
                .map(Some);
        }

        if let Some(node) = node.as_any().downcast_ref::<MergeCardinalityCheckNode>() {
            let [input] = physical_inputs else {
                return datafusion_common::internal_err!(
                    "MergeCardinalityCheckNode requires exactly one physical input"
                );
            };
            let exec = MergeCardinalityCheckExec::new(
                Arc::clone(input),
                node.target_row_id_col(),
                node.target_present_col(),
                node.source_present_col(),
            )?;
            return Ok(Some(Arc::new(exec)));
        }

        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !node.target_format().eq_ignore_ascii_case("iceberg") {
                return Ok(None);
            }
            return plan_iceberg_row_level_write(session_state, node, physical_inputs)
                .await
                .map(Some);
        }

        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
            return Ok(None);
        };
        let filters = unnormalize_cols(scan.filters.clone());
        let plan = source
            .provider()
            .scan(
                session_state,
                scan.projection.as_ref(),
                &filters,
                scan.fetch,
            )
            .await?;
        Ok(Some(plan))
    }
}

async fn plan_iceberg_row_level_write(
    session_state: &SessionState,
    node: &RowLevelWriteNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    if node.command() != RowLevelCommand::Merge {
        return not_impl_err!("Iceberg row-level {:?} operations", node.command());
    }

    let write_plan = physical_inputs.first().cloned().ok_or_else(|| {
        DataFusionError::Internal("Iceberg MERGE missing write plan input".to_string())
    })?;
    if node.touched_files_plan().is_some() && physical_inputs.len() < 2 {
        return plan_err!("Iceberg MERGE missing touched-file plan input");
    }
    let deletion_plan = if node.deletion_vector_plan().is_some() {
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

    let (clean_options, table_properties) =
        split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
    let variant_presence =
        IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
    let iceberg_options = IcebergWriteOptions::resolve(session_state, clean_options)?;
    let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
    writer_options.apply_variant_shredding_option_presence(variant_presence);
    writer_options.table_properties = table_properties;
    writer_options.lakehouse_table = node.target_lakehouse_table().cloned();

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

    let commit_input: Arc<dyn ExecutionPlan> = if let Some(deletion_plan) = deletion_plan {
        let deletion_input: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(deletion_plan));
        let delete_writer: Arc<dyn ExecutionPlan> = Arc::new(IcebergPositionDeleteWriterExec::new(
            deletion_input,
            table_url.clone(),
            writer_options.table_properties.clone(),
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
