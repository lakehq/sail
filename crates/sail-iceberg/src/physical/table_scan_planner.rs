use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Result, not_impl_err};
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use sail_common_datafusion::datasource::{RowLevelCommand, SourceInfo};
use sail_data_source::options::ResolveOptions;
use sail_logical_plan::merge::RowLevelWriteNode;

use crate::logical::IcebergTableSource;
use crate::options::r#gen::IcebergWriteOptions;
use crate::physical_plan::IcebergWriterExecOptions;
use crate::table_format::{
    IcebergWriteNode, build_iceberg_provider, plan_iceberg_write,
    split_iceberg_write_options_and_table_properties,
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
        if let Some(node) = node.as_any().downcast_ref::<RowLevelWriteNode>() {
            if !node.target_format().eq_ignore_ascii_case("iceberg") {
                return Ok(None);
            }
            return plan_iceberg_row_level_write(session_state, node)
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
) -> Result<Arc<dyn ExecutionPlan>> {
    match node.command() {
        RowLevelCommand::Delete => {
            let source_info = SourceInfo {
                paths: vec![node.target_location().to_string()],
                lakehouse_table: node.target_lakehouse_table().cloned(),
                schema: None,
                constraints: Default::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: node.target_options().to_vec(),
                read_case_sensitive: true,
            };
            let provider = build_iceberg_provider(session_state, source_info).await?;
            let (clean_options, table_properties) =
                split_iceberg_write_options_and_table_properties(node.target_options().to_vec())?;
            let variant_shredding_option_presence =
                IcebergWriterExecOptions::variant_shredding_option_presence(&clean_options);
            let iceberg_options = IcebergWriteOptions::resolve(session_state, clean_options)?;
            let mut writer_options = IcebergWriterExecOptions::from(iceberg_options);
            writer_options
                .apply_variant_shredding_option_presence(variant_shredding_option_presence);
            writer_options.table_properties = table_properties;
            writer_options.lakehouse_table = node.target_lakehouse_table().cloned();
            provider
                .build_cow_delete_plan(session_state, node.condition().cloned(), writer_options)
                .await
        }
        RowLevelCommand::Update | RowLevelCommand::Merge => {
            not_impl_err!("Iceberg {:?} row-level write", node.command())
        }
    }
}
