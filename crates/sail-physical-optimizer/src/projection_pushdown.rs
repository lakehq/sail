use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{CastExpr, Column, LambdaVariable};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::{ProjectionExec, remove_unnecessary_projections};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, replace_children_if_necessary,
};
use sail_common_datafusion::udf::get_field::SparkGetField;
use sail_common_datafusion::udf::get_field::physical::{
    rewrite_parquet_field_access, struct_field_path,
};

/// Runs DataFusion projection pushdown without moving physical lambda variables
/// across their planned schema boundary. Struct narrowing is limited to projections
/// accepted directly by a Parquet scan.
#[derive(Debug, Default)]
pub struct LambdaSafeProjectionPushdown {
    datafusion_projection_pushdown: ProjectionPushdown,
}

impl LambdaSafeProjectionPushdown {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for LambdaSafeProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_up(|plan| {
                install_lambda_optimizer_boundary(plan)?.transform_data(prune_struct_projection)
            })
            .map(|result| result.data)?;
        let plan = self.datafusion_projection_pushdown.optimize(plan, config)?;
        plan.transform_up(remove_lambda_optimizer_boundary)
            .map(|result| result.data)
    }

    fn name(&self) -> &str {
        self.datafusion_projection_pushdown.name()
    }

    fn schema_check(&self) -> bool {
        self.datafusion_projection_pushdown.schema_check()
    }
}

/// Preserve decoder predicate pushdown for supported fields read from Parquet.
/// The accessor retains ancestor nulls while exposing the leaf dependency to the reader.
#[derive(Debug, Default)]
pub struct ParquetFieldFilterPushdown {
    datafusion_filter_pushdown: FilterPushdown,
}

impl ParquetFieldFilterPushdown {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for ParquetFieldFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan.transform_up(restore_parquet_field_access)?.data;
        self.datafusion_filter_pushdown.optimize(plan, config)
    }

    fn name(&self) -> &str {
        self.datafusion_filter_pushdown.name()
    }

    fn schema_check(&self) -> bool {
        self.datafusion_filter_pushdown.schema_check()
    }
}

// Follow only column aliases: substituting a computed struct beneath the native
// dependency would hide its leaf path from the Parquet schema adapter.
fn is_parquet_column(plan: &Arc<dyn ExecutionPlan>, index: usize) -> bool {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let Some(column) = projection
            .expr()
            .get(index)
            .and_then(|expression| expression.expr.downcast_ref::<Column>())
        else {
            return false;
        };
        if projection.schema().field(index).metadata()
            != projection.input().schema().field(column.index()).metadata()
        {
            return false;
        }
        return is_parquet_column(projection.input(), column.index());
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let index = filter
            .projection()
            .as_ref()
            .map_or(index, |projection| projection[index]);
        return is_parquet_column(filter.input(), index);
    }
    plan.downcast_ref::<DataSourceExec>()
        .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
        .is_some_and(|config| {
            config.file_source.file_type() == "parquet"
                && config.file_source.projection().is_none_or(|expressions| {
                    expressions
                        .as_ref()
                        .get(index)
                        .is_some_and(|expression| expression.expr.is::<Column>())
                })
        })
}

fn restore_parquet_field_access(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input = if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        projection.input()
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        filter.input()
    } else {
        return Ok(Transformed::no(plan));
    };
    let schema = input.schema();
    let rewrite = |expression: Arc<dyn PhysicalExpr>| {
        // Filter pushdown and projection merging can absorb alias/extraction
        // chains together. Convert their column-backed field accesses first.
        if expression.exists(|expression| {
            Ok(struct_field_path(expression)
                .is_some_and(|(index, _)| !is_parquet_column(input, index)))
        })? {
            return Ok(Transformed::no(expression));
        }
        rewrite_parquet_field_access(expression, &schema)
    };
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let mut transformed = false;
        let expressions = ProjectionExprs::from(projection.expr()).try_map_exprs(|expression| {
            let result = rewrite(expression)?;
            transformed |= result.transformed;
            Ok(result.data)
        })?;
        if !transformed {
            return Ok(Transformed::no(plan));
        }
        return Ok(Transformed::yes(Arc::new(
            ProjectionExec::try_new_with_schema_metadata(
                expressions.iter().cloned(),
                Arc::clone(input),
                projection.schema().as_ref(),
            )?,
        )));
    }
    let Some(filter) = plan.downcast_ref::<FilterExec>() else {
        return Ok(Transformed::no(plan));
    };
    let predicate = rewrite(Arc::clone(filter.predicate()))?;
    if !predicate.transformed {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(Arc::new(
        FilterExecBuilder::from(filter)
            .with_predicate(predicate.data)
            .build()?,
    )))
}

/// A terminal selection consumes the whole field, including all its descendants.
#[derive(Default)]
struct StructSelection {
    whole: bool,
    fields: BTreeMap<String, Self>,
}

impl StructSelection {
    fn insert(&mut self, path: &[String]) {
        if let Some((first, rest)) = path.split_first() {
            self.fields.entry(first.clone()).or_default().insert(rest);
        } else {
            self.whole = true;
        }
    }

    fn narrow(&self, field: &FieldRef) -> FieldRef {
        if self.whole {
            return Arc::clone(field);
        }
        let DataType::Struct(fields) = field.data_type() else {
            return Arc::clone(field);
        };
        let fields = fields
            .iter()
            .filter_map(|field| self.fields.get(field.name()).map(|s| s.narrow(field)))
            .collect();
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(fields)),
        )
    }
}

fn prune_struct_projection(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    let Some(scan) = projection.input().downcast_ref::<DataSourceExec>() else {
        return Ok(Transformed::no(plan));
    };
    let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(Transformed::no(plan));
    };
    if config.file_source.file_type() != "parquet" {
        return Ok(Transformed::no(plan));
    }
    let schema = projection.input().schema();
    if !schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Struct(_)))
    {
        return Ok(Transformed::no(plan));
    }
    let mut selections = BTreeMap::<usize, StructSelection>::new();
    for expression in projection.expr() {
        expression.expr.apply(|expression| {
            if let Some((index, path)) = struct_field_path(expression) {
                selections.entry(index).or_default().insert(&path);
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(column) = expression.downcast_ref::<Column>()
                && schema
                    .fields()
                    .get(column.index())
                    .is_some_and(|field| matches!(field.data_type(), DataType::Struct(_)))
            {
                selections.entry(column.index()).or_default().whole = true;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let targets = selections
        .into_iter()
        .filter_map(|(index, selection)| {
            if selection.whole {
                return None;
            }
            let field = schema.fields().get(index)?;
            let target = selection.narrow(field);
            (target != *field).then_some((index, target))
        })
        .collect::<BTreeMap<_, _>>();
    if targets.is_empty() {
        return Ok(Transformed::no(plan));
    }
    // Merging through computed expressions or multiple aliases of one root can
    // require a full read even if the scan accepts the narrowed projection.
    if let Some(expressions) = config.file_source.projection() {
        let mut columns = HashSet::new();
        if !expressions.iter().all(|expression| {
            expression
                .expr
                .downcast_ref::<Column>()
                .is_some_and(|column| columns.insert(column.index()))
        }) {
            return Ok(Transformed::no(plan));
        }
    }
    // Parquet understands narrowing casts, but not SparkGetField. All accesses
    // to a root must share one target: different targets force a full read.
    // Keep the accessor itself so null ancestors still mask their descendants.
    let expressions = ProjectionExprs::from(projection.expr()).try_map_exprs(|expression| {
        expression
            .transform_up(|expression| {
                if let Some(column) = expression.downcast_ref::<Column>()
                    && let Some(target) = targets.get(&column.index())
                {
                    return Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
                        expression,
                        Arc::clone(target),
                        None,
                    ))
                        as Arc<dyn PhysicalExpr>));
                }
                if let Some(access) =
                    ScalarFunctionExpr::try_downcast_func::<SparkGetField>(expression.as_ref())
                {
                    // Nested accesses may now return a narrower intermediate
                    // struct. Recompute its promised type from the new children.
                    return Ok(Transformed::yes(Arc::new(ScalarFunctionExpr::try_new(
                        Arc::new(access.fun().clone()),
                        access.args().to_vec(),
                        schema.as_ref(),
                        Arc::new(access.config_options().clone()),
                    )?)
                        as Arc<dyn PhysicalExpr>));
                }
                Ok(Transformed::no(expression))
            })
            .map(|result| result.data)
    })?;
    let narrowed = ProjectionExec::try_new_with_schema_metadata(
        expressions.iter().cloned(),
        Arc::clone(projection.input()),
        projection.schema().as_ref(),
    )?;
    // A Parquet ancestor is insufficient: filters, limits, and lambda boundaries
    // can prevent pushdown, leaving a runtime cast without reducing scan I/O.
    // Keep the original plan unless the scan accepts the entire projection.
    let pushed = remove_unnecessary_projections(Arc::new(narrowed))?;
    Ok(
        if pushed.transformed && pushed.data.is::<DataSourceExec>() {
            pushed
        } else {
            Transformed::no(plan)
        },
    )
}

fn install_lambda_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
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

    let boundary: Arc<dyn ExecutionPlan> = Arc::new(LambdaProjectionBoundaryExec::new(Arc::clone(
        projection.input(),
    )));
    let plan = replace_children_if_necessary(plan, vec![boundary])?;
    Ok(Transformed::yes(plan))
}

fn remove_lambda_optimizer_boundary(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(boundary) = plan.downcast_ref::<LambdaJoinFilterBoundaryExec>() {
        return Ok(Transformed::yes(Arc::clone(&boundary.join)));
    }
    let Some(boundary) = plan.downcast_ref::<LambdaProjectionBoundaryExec>() else {
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

/// An optimizer-only boundary whose default projection-swap implementation
/// prevents the parent projection from being rewritten against another schema.
#[derive(Debug)]
struct LambdaProjectionBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LambdaProjectionBoundaryExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for LambdaProjectionBoundaryExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(formatter, "LambdaProjectionBoundaryExec")
    }
}

impl ExecutionPlan for LambdaProjectionBoundaryExec {
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
        Ok(Arc::new(Self::new(children.swap_remove(0))))
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
}
