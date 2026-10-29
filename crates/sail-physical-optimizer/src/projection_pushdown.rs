use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::LambdaVariable;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, replace_children_if_necessary,
};
use sail_common_datafusion::input_file::projection_references_input_file_metadata;

/// Runs projection pushdown without crossing lambda schemas or file-context boundaries.
#[derive(Debug, Default)]
pub struct ContextSafeProjectionPushdown {
    datafusion_projection_pushdown: ProjectionPushdown,
}

impl ContextSafeProjectionPushdown {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for ContextSafeProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(install_optimizer_boundary)
            .map(|result| result.data)?;
        let plan = self.datafusion_projection_pushdown.optimize(plan, config)?;
        plan.transform_up(remove_optimizer_boundary)
            .map(|result| result.data)
    }

    fn name(&self) -> &str {
        self.datafusion_projection_pushdown.name()
    }

    fn schema_check(&self) -> bool {
        self.datafusion_projection_pushdown.schema_check()
    }
}

fn install_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // A projection can reach this sort through other projections or filters during
    // pushdown, so guard the sort itself rather than only its original parent.
    if plan.is::<SortExec>() {
        return Ok(Transformed::yes(Arc::new(ProjectionBoundaryExec::new(
            plan,
            ProjectionBoundary::InputFileMetadata,
        ))));
    }
    if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>()
        && let Some(filter) = join.filter()
        && expression_contains_lambda_variable(filter.expression())?
    {
        return Ok(Transformed::yes(Arc::new(
            LambdaJoinFilterBoundaryExec::new(plan),
        )));
    }

    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_contains_lambda_variable(projection)? {
        return Ok(Transformed::no(plan));
    }

    let boundary: Arc<dyn ExecutionPlan> = Arc::new(ProjectionBoundaryExec::new(
        Arc::clone(projection.input()),
        ProjectionBoundary::Lambda,
    ));
    let plan = replace_children_if_necessary(plan, vec![boundary])?;
    Ok(Transformed::yes(plan))
}

fn remove_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(boundary) = plan.downcast_ref::<LambdaJoinFilterBoundaryExec>() {
        return Ok(Transformed::yes(Arc::clone(&boundary.join)));
    }
    let Some(boundary) = plan.downcast_ref::<ProjectionBoundaryExec>() else {
        return Ok(Transformed::no(plan));
    };
    Ok(Transformed::yes(Arc::clone(&boundary.input)))
}

fn expression_contains_lambda_variable(expression: &Arc<dyn PhysicalExpr>) -> Result<bool> {
    expression.exists(|expression| Ok(expression.is::<LambdaVariable>()))
}

fn projection_contains_lambda_variable(projection: &ProjectionExec) -> Result<bool> {
    for projection_expr in projection.expr() {
        if expression_contains_lambda_variable(&projection_expr.expr)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Hides a lambda-bearing nested-loop join from DataFusion's join-filter
/// projection pushdown while keeping its children visible to the optimizer.
#[derive(Debug)]
struct LambdaJoinFilterBoundaryExec {
    join: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LambdaJoinFilterBoundaryExec {
    fn new(join: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(join.properties());
        Self { join, properties }
    }
}

impl DisplayAs for LambdaJoinFilterBoundaryExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "LambdaJoinFilterBoundaryExec")
    }
}

impl ExecutionPlan for LambdaJoinFilterBoundaryExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.join.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.join.apply_expressions(f)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = Arc::clone(&self.join).replace_children(children, options)?;
        Ok(Arc::new(Self::new(join)))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.join.execute(partition, context)
    }
}

#[derive(Debug, Clone, Copy)]
enum ProjectionBoundary {
    Lambda,
    InputFileMetadata,
}

/// A transient boundary protecting positional lambda variables or source metadata.
/// Metadata boundaries still delegate ordinary projection swaps to the wrapped node.
#[derive(Debug)]
struct ProjectionBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    boundary: ProjectionBoundary,
}

impl ProjectionBoundaryExec {
    fn new(input: Arc<dyn ExecutionPlan>, boundary: ProjectionBoundary) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            properties,
            boundary,
        }
    }
}

impl DisplayAs for ProjectionBoundaryExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "ProjectionBoundaryExec")
    }
}

impl ExecutionPlan for ProjectionBoundaryExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "{} expects exactly one child, got {}",
                self.name(),
                children.len()
            );
        }
        Ok(Arc::new(Self::new(children.swap_remove(0), self.boundary)))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if matches!(self.boundary, ProjectionBoundary::Lambda)
            || projection_references_input_file_metadata(projection.projection_expr())
        {
            return Ok(None);
        }
        Ok(self
            .input
            .try_swapping_with_projection(projection)?
            .map(|input| Arc::new(Self::new(input, self.boundary)) as Arc<dyn ExecutionPlan>))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions::expr_fn::{coalesce, concat, input_file_name};
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{Expr, ScalarUDF, cast, col, lit};
    use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion_physical_expr::create_physical_exprs;
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};

    use super::*;
    use crate::input_file_name::{
        PushDownInputFileMetadata, RewriteInputFileMetadataFallback, WrapInputFileMetadataSource,
    };

    fn sorted_scan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));
        let scan = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema)),
        )
        .build();
        Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
                "id", 0,
            )))])
            .unwrap(),
            DataSourceExec::from_data_source(scan),
        ))
    }

    fn physical_expressions(
        input: &Arc<dyn ExecutionPlan>,
        expr: &[Expr],
    ) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let schema = Arc::new(DFSchema::try_from(input.schema())?);
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(&schema))
                .build(),
        );
        let expr = expr
            .iter()
            .cloned()
            .map(|expr| simplifier.simplify(expr))
            .collect::<Result<Vec<_>>>()?;
        create_physical_exprs(
            &expr,
            &schema,
            &ExecutionProps::default(),
            &PhysicalPlanningContext::default(),
        )
    }

    #[test]
    fn metadata_narrowing_stays_above_sort() -> Result<()> {
        let sum = col("x") + col("y");
        let cases = [
            (
                concat(vec![
                    coalesce(vec![input_file_name(), lit("")]),
                    lit(":"),
                    cast(sum.clone(), DataType::Utf8),
                ]),
                ScalarValue::Utf8(Some(":110".to_string())),
            ),
            (
                ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]) + sum.clone(),
                ScalarValue::Int64(Some(109)),
            ),
            (
                ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]) + sum,
                ScalarValue::Int64(Some(109)),
            ),
        ];
        for (expr, expected) in cases {
            for with_filter in [false, true] {
                let mut input = sorted_scan();
                if with_filter {
                    let predicate =
                        physical_expressions(&input, &[col("id").gt(lit(0_i64))])?.remove(0);
                    input = Arc::new(FilterExec::try_new(predicate, input)?);
                }
                let expressions = physical_expressions(&input, &[col("id"), expr.clone()])?;
                let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                    expressions
                        .into_iter()
                        .zip(["id", "value"])
                        .map(|(expr, name)| ProjectionExpr::new(expr, name)),
                    input,
                )?);
                let config = ConfigOptions::default();
                let plan = WrapInputFileMetadataSource::new().optimize(plan, &config)?;
                let plan = PushDownInputFileMetadata::new().optimize(plan, &config)?;
                let mut metadata_projections = 0;
                plan.apply(|node| {
                    assert!(!node.is::<ProjectionBoundaryExec>());
                    if let Some(projection) = node.downcast_ref::<ProjectionExec>()
                        && projection_references_input_file_metadata(projection.projection_expr())
                    {
                        metadata_projections += 1;
                        assert!(
                            projection
                                .input()
                                .exists(|node| Ok(node.is::<SortExec>()))?
                        );
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert_eq!(metadata_projections, 1);
                let fallback = RewriteInputFileMetadataFallback::new().optimize(plan, &config)?;
                fallback.apply(|node| {
                    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
                        let batch = RecordBatch::try_new(
                            projection.input().schema(),
                            vec![
                                Arc::new(Int64Array::from(vec![1])),
                                Arc::new(Int64Array::from(vec![10])),
                                Arc::new(Int64Array::from(vec![100])),
                            ],
                        )?;
                        let value = projection.expr()[1].expr.evaluate(&batch)?.into_array(1)?;
                        assert_eq!(ScalarValue::try_from_array(&value, 0)?, expected);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
        }
        Ok(())
    }

    #[test]
    fn ordinary_narrowing_still_crosses_sort() -> Result<()> {
        let input = sorted_scan();
        let expr = physical_expressions(&input, &[col("id"), col("x") + col("y")])?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            expr.into_iter()
                .zip(["id", "value"])
                .map(|(expr, name)| ProjectionExpr::new(expr, name)),
            input,
        )?);
        let optimized =
            ContextSafeProjectionPushdown::new().optimize(plan, &ConfigOptions::default())?;
        assert!(optimized.is::<SortExec>());
        optimized.apply(|node| {
            assert!(!node.is::<ProjectionBoundaryExec>());
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
}
