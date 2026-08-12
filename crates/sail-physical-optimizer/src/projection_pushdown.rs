use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::HigherOrderFunctionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Runs DataFusion's projection pushdown without moving a projection containing
/// a higher-order function across its input projection.
///
/// A physical lambda variable is indexed after the schema against which its
/// higher-order function was planned. DataFusion's projection unification
/// rewrites regular [`datafusion::physical_expr::expressions::Column`] nodes,
/// but not lambda variables. If it removes the input projection, the wider
/// child batch can therefore turn a lambda parameter index into an apparent
/// outer-column capture. The temporary boundary below keeps that input schema
/// stable while the ordinary rule still optimizes the rest of the plan.
///
/// The boundary follows the projection containing the higher-order function,
/// rather than guarding filters or sorts that contain one. Pushing a parent
/// projection through those operators can only narrow their evaluation batch,
/// so a lambda-parameter index remains outside the batch and cannot become a
/// capture. Projection-on-projection unification is different: it can remove a
/// narrowing child and expose a wider batch, which is the unsafe operation.
///
/// DataFusion also extracts one-sided nested-loop-join filter expressions into
/// a side projection before projection pushdown. Moving a higher-order function
/// that way changes its evaluation schema without rebasing lambda variables, so
/// such a join is temporarily hidden while its children remain traversable.
#[derive(Default)]
pub struct HigherOrderProjectionPushdown {
    inner: ProjectionPushdown,
}

impl HigherOrderProjectionPushdown {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for HigherOrderProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let protected = plan
            .transform_down(|plan| {
                let has_higher_order_join_filter = match plan
                    .downcast_ref::<NestedLoopJoinExec>()
                    .and_then(NestedLoopJoinExec::filter)
                {
                    Some(filter) => filter
                        .expression()
                        .exists(|expr| Ok(expr.is::<HigherOrderFunctionExpr>()))?,
                    None => false,
                };
                if has_higher_order_join_filter {
                    return Ok(Transformed::yes(
                        Arc::new(ProjectionBoundaryExec::around_node(plan))
                            as Arc<dyn ExecutionPlan>,
                    ));
                }

                let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
                    return Ok(Transformed::no(plan));
                };
                let has_higher_order_function =
                    projection
                        .expr()
                        .iter()
                        .try_fold(false, |found, projection| {
                            if found {
                                Ok(true)
                            } else {
                                projection
                                    .expr
                                    .exists(|expr| Ok(expr.is::<HigherOrderFunctionExpr>()))
                            }
                        })?;
                if !has_higher_order_function || !projection.input().is::<ProjectionExec>() {
                    return Ok(Transformed::no(plan));
                }

                let boundary = Arc::new(ProjectionBoundaryExec::around_input(Arc::clone(
                    projection.input(),
                ))) as Arc<dyn ExecutionPlan>;
                let projection = ProjectionExec::try_new(projection.expr().to_vec(), boundary)?;
                Ok(Transformed::yes(
                    Arc::new(projection) as Arc<dyn ExecutionPlan>
                ))
            })?
            .data;

        let optimized = self.inner.optimize(protected, config)?;

        Ok(optimized
            .transform_up(|plan| {
                let Some(boundary) = plan.downcast_ref::<ProjectionBoundaryExec>() else {
                    return Ok(Transformed::no(plan));
                };
                Ok(Transformed::yes(Arc::clone(boundary.input())))
            })?
            .data)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

impl Debug for HigherOrderProjectionPushdown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// An optimizer-only boundary. Every instance is removed before this rule
/// returns, so it cannot reach execution or distributed-plan serialization.
#[derive(Debug, Clone)]
struct ProjectionBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    around_node: bool,
}

impl ProjectionBoundaryExec {
    fn around_input(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            properties: Arc::new(input.properties().as_ref().clone()),
            input,
            around_node: false,
        }
    }

    fn around_node(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            properties: Arc::new(input.properties().as_ref().clone()),
            input,
            around_node: true,
        }
    }

    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ProjectionBoundaryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProjectionBoundaryExec")
    }
}

impl ExecutionPlan for ProjectionBoundaryExec {
    fn name(&self) -> &'static str {
        "ProjectionBoundaryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if self.around_node {
            self.input.children()
        } else {
            vec![&self.input]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        if self.around_node {
            self.input.maintains_input_order()
        } else {
            vec![true]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.around_node {
            let input = Arc::clone(&self.input).with_new_children(children)?;
            Ok(Arc::new(Self::around_node(input)))
        } else {
            let [input] = children.as_slice() else {
                return internal_err!("ProjectionBoundaryExec requires exactly one child");
            };
            Ok(Arc::new(Self::around_input(Arc::clone(input))))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::functions_nested::array_any_match::array_any_match_higher_order_function;
    use datafusion::functions_nested::array_transform::array_transform_higher_order_function;
    use datafusion::physical_expr::expressions::{Column, LambdaVariable, is_not_null, lambda};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::NestedLoopJoinExec;
    use datafusion::physical_plan::joins::utils::JoinFilter;
    use datafusion::physical_plan::projection::ProjectionExpr;

    use super::*;

    #[test]
    fn keeps_higher_order_function_on_its_planning_schema() -> Result<()> {
        let element = Arc::new(Field::new("item", DataType::Int32, false));
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int32, false),
            Field::new("array", DataType::List(Arc::clone(&element)), false),
        ]));
        let source = Arc::new(EmptyExec::new(source_schema)) as Arc<dyn ExecutionPlan>;
        let input = Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: Arc::new(Column::new("array", 1)),
                alias: "array".to_string(),
            }],
            source,
        )?) as Arc<dyn ExecutionPlan>;

        // The lambda is planned after the one-column projection, so its first
        // parameter has physical index 1. Removing that projection would make
        // index 1 refer to the source's `array` column instead.
        let parameter = Arc::new(Field::new("x", DataType::Int32, false));
        let lambda = lambda(
            ["x"],
            Arc::new(LambdaVariable::new(1, Arc::clone(&parameter))),
        )?;
        let higher_order = HigherOrderFunctionExpr::try_new_with_schema(
            array_transform_higher_order_function(),
            vec![Arc::new(Column::new("array", 0)), lambda],
            input.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        let plan = Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: Arc::new(higher_order),
                alias: "result".to_string(),
            }],
            input,
        )?) as Arc<dyn ExecutionPlan>;

        let unprotected =
            ProjectionPushdown::new().optimize(Arc::clone(&plan), &ConfigOptions::default())?;
        let unprotected = unprotected
            .downcast_ref::<ProjectionExec>()
            .expect("root must remain a projection");
        assert_eq!(unprotected.input().schema().fields().len(), 2);

        let optimized =
            HigherOrderProjectionPushdown::new().optimize(plan, &ConfigOptions::default())?;
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("root must remain a projection");

        assert_eq!(projection.input().schema().fields().len(), 1);
        assert!(projection.input().is::<ProjectionExec>());
        assert!(!optimized.exists(|plan| Ok(plan.is::<ProjectionBoundaryExec>()))?);
        Ok(())
    }

    #[test]
    fn pushes_higher_order_projection_through_filter() -> Result<()> {
        let element = Arc::new(Field::new("item", DataType::Int32, false));
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("keep", DataType::Boolean, false),
            Field::new("unused", DataType::Int32, false),
            Field::new("array", DataType::List(Arc::clone(&element)), false),
        ]));
        let source = Arc::new(EmptyExec::new(source_schema)) as Arc<dyn ExecutionPlan>;
        let filter = Arc::new(FilterExec::try_new(
            Arc::new(Column::new("keep", 0)),
            source,
        )?) as Arc<dyn ExecutionPlan>;

        let parameter = Arc::new(Field::new("x", DataType::Int32, false));
        let lambda = lambda(
            ["x"],
            Arc::new(LambdaVariable::new(3, Arc::clone(&parameter))),
        )?;
        let higher_order = HigherOrderFunctionExpr::try_new_with_schema(
            array_transform_higher_order_function(),
            vec![Arc::new(Column::new("array", 2)), lambda],
            filter.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        let plan = Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: Arc::new(Column::new("keep", 0)),
                    alias: "keep".to_string(),
                },
                ProjectionExpr {
                    expr: Arc::new(higher_order),
                    alias: "result".to_string(),
                },
            ],
            filter,
        )?) as Arc<dyn ExecutionPlan>;

        let optimized =
            HigherOrderProjectionPushdown::new().optimize(plan, &ConfigOptions::default())?;
        let filter = optimized
            .downcast_ref::<FilterExec>()
            .expect("projection should be pushed through the filter");
        assert!(filter.input().is::<ProjectionExec>());
        assert!(!optimized.exists(|plan| Ok(plan.is::<ProjectionBoundaryExec>()))?);
        Ok(())
    }

    #[test]
    fn does_not_extract_higher_order_nested_loop_join_filter() -> Result<()> {
        let element = Arc::new(Field::new("item", DataType::Int32, false));
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int32, false),
            Field::new("array", DataType::List(Arc::clone(&element)), false),
        ]));
        let left = Arc::new(EmptyExec::new(Arc::clone(&left_schema))) as Arc<dyn ExecutionPlan>;
        let right = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "right",
            DataType::Int32,
            false,
        )])))) as Arc<dyn ExecutionPlan>;

        // JoinFilter evaluates against only the referenced array field, so x
        // is planned at index 1. DataFusion's join-filter prepass otherwise
        // moves this HOF onto the two-column left input without rebasing x.
        let filter_schema = Arc::new(Schema::new(vec![left_schema.field(1).clone()]));
        let parameter = Arc::new(Field::new("x", DataType::Int32, false));
        let predicate = is_not_null(Arc::new(LambdaVariable::new(1, Arc::clone(&parameter))))?;
        let lambda = lambda(["x"], predicate)?;
        let higher_order = HigherOrderFunctionExpr::try_new_with_schema(
            array_any_match_higher_order_function(),
            vec![Arc::new(Column::new("array", 0)), lambda],
            filter_schema.as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        let filter = JoinFilter::new(
            Arc::new(higher_order),
            JoinFilter::build_column_indices(vec![1], vec![]),
            filter_schema,
        );
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            left,
            right,
            Some(filter),
            &JoinType::Inner,
            None,
        )?) as Arc<dyn ExecutionPlan>;

        let unprotected =
            ProjectionPushdown::new().optimize(Arc::clone(&plan), &ConfigOptions::default())?;
        let unprotected = unprotected
            .downcast_ref::<NestedLoopJoinExec>()
            .expect("root must remain a nested-loop join");
        assert!(unprotected.left().is::<ProjectionExec>());

        let optimized =
            HigherOrderProjectionPushdown::new().optimize(plan, &ConfigOptions::default())?;
        let join = optimized
            .downcast_ref::<NestedLoopJoinExec>()
            .expect("root must remain a nested-loop join");
        assert!(!join.left().is::<ProjectionExec>());
        assert!(
            join.filter()
                .expect("join filter must remain")
                .expression()
                .exists(|expr| Ok(expr.is::<HigherOrderFunctionExpr>()))?
        );
        assert!(!optimized.exists(|plan| Ok(plan.is::<ProjectionBoundaryExec>()))?);
        Ok(())
    }
}
