use std::sync::Arc;

use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, Result, plan_err};
use datafusion_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFunctionDefinition, col, lit,
};
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, MERGE_SOURCE_METRIC_COLUMN, OPERATION_COLUMN,
    RowLevelCommand, RowLevelOperationType, RowLevelWriteMode,
};

use crate::datasource::provider::IcebergTableProvider;
use crate::logical::IcebergTableSource;
use crate::spec::TableMetadata;

#[derive(Clone, Debug, Default)]
pub(crate) struct IcebergRowLevelOptions {
    delete_mode: Option<String>,
    update_mode: Option<String>,
    merge_mode: Option<String>,
}

impl From<&TableMetadata> for IcebergRowLevelOptions {
    fn from(metadata: &TableMetadata) -> Self {
        Self {
            delete_mode: metadata.properties.get("write.delete.mode").cloned(),
            update_mode: metadata.properties.get("write.update.mode").cloned(),
            merge_mode: metadata.properties.get("write.merge.mode").cloned(),
        }
    }
}

impl IcebergRowLevelOptions {
    pub(crate) fn mode(&self, command: RowLevelCommand) -> Result<RowLevelWriteMode> {
        let (property, value) = match command {
            RowLevelCommand::Delete => ("write.delete.mode", &self.delete_mode),
            RowLevelCommand::Update => ("write.update.mode", &self.update_mode),
            RowLevelCommand::Merge => ("write.merge.mode", &self.merge_mode),
        };
        let value = value.as_deref().unwrap_or("copy-on-write");
        if value.eq_ignore_ascii_case("copy-on-write") {
            Ok(RowLevelWriteMode::CopyOnWrite)
        } else if value.eq_ignore_ascii_case("merge-on-read") {
            Ok(RowLevelWriteMode::MergeOnRead)
        } else {
            plan_err!(
                "Unknown Iceberg row-level operation mode for `{property}`: {value}; expected `copy-on-write` or `merge-on-read`"
            )
        }
    }
}

pub(crate) fn validate_row_level_columns(
    input_schema: &DFSchemaRef,
    resolved_names: &[String],
    case_sensitive: bool,
) -> Result<()> {
    for column in [
        MERGE_FILE_COLUMN,
        MERGE_ROW_INDEX_COLUMN,
        MERGE_SOURCE_METRIC_COLUMN,
        crate::row_level_metadata::MERGE_PARTITION_COLUMN,
        crate::row_level_metadata::MERGE_PARTITION_SPEC_ID_COLUMN,
    ] {
        sail_logical_plan::row_level::validate_row_level_internal_columns(
            input_schema,
            resolved_names,
            column,
            None,
            case_sensitive,
        )?;
    }
    Ok(())
}

pub(crate) fn target_write_state(
    plan: &LogicalPlan,
    command: RowLevelCommand,
) -> Result<(RowLevelWriteMode, Option<i64>)> {
    let provider = target_provider(plan)?;
    Ok((
        provider.row_level_options.mode(command)?,
        provider
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()),
    ))
}

pub(crate) fn target_provider(plan: &LogicalPlan) -> Result<Arc<IcebergTableProvider>> {
    let mut provider = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<IcebergTableSource>()
        {
            provider = Some(Arc::clone(source.provider()));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    provider.ok_or_else(|| {
        datafusion_common::plan_datafusion_err!("Missing Iceberg row-level target scan")
    })
}

pub(crate) fn select_copy_on_write_candidates(
    plan: LogicalPlan,
    predicate: Expr,
) -> Result<LogicalPlan> {
    plan.transform_up(|plan| {
        if let LogicalPlan::TableScan(mut scan) = plan {
            if let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() {
                let provider = source
                    .provider()
                    .as_ref()
                    .clone()
                    .select_copy_on_write_candidates(predicate.clone());
                scan.source = Arc::new(IcebergTableSource::new(Arc::new(provider)));
                return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
            }
            return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
        }
        Ok(Transformed::no(plan))
    })
    .map(|transformed| transformed.data)
}

/// Retain every row of a touched file, including delete intents, and new inserts.
/// A window keeps file selection and row values on the same evaluation of the input.
pub(crate) fn select_copy_on_write_rows(plan: LogicalPlan) -> Result<LogicalPlan> {
    let columns = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let touched = Expr::WindowFunction(Box::new(WindowFunction {
        fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
        params: WindowFunctionParams {
            args: vec![col(OPERATION_COLUMN)],
            partition_by: vec![col(MERGE_FILE_COLUMN)],
            order_by: vec![],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: None,
            distinct: false,
        },
    }));
    // All target intents other than Copy change a row. Metric/no-op rows are
    // absent from Iceberg's expansion; insert rows have a null file path.
    let window = LogicalPlanBuilder::from(plan)
        .window(vec![touched])?
        .build()?;
    let touched_column = window.schema().columns().pop().ok_or_else(|| {
        datafusion_common::internal_datafusion_err!("Missing Iceberg touched-file window column")
    })?;
    LogicalPlanBuilder::from(window)
        .filter(Expr::Column(touched_column).not_eq(lit(RowLevelOperationType::Copy.as_i32())))?
        .project(columns)?
        .build()
}

pub(crate) fn write_effects(
    plan: LogicalPlan,
) -> sail_logical_plan::row_level::RowLevelEffectPlans {
    sail_logical_plan::row_level::RowLevelEffectPlans::new(Some(Arc::new(plan)), None, None)
}
