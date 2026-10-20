use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::{Column, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFunctionDefinition, expr,
};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::datetime_utils::{cast_temporal, common_temporal_type};
use crate::resolver::PlanResolver;
use crate::resolver::interval::{IntervalShape, plan_interval_shapes};
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    fn set_operation_interval_shapes(
        left: &LogicalPlan,
        right: &LogicalPlan,
        state: &PlanResolverState,
    ) -> Vec<IntervalShape> {
        plan_interval_shapes(left, state)
            .into_iter()
            .zip(plan_interval_shapes(right, state))
            .map(|(left, right)| left.merge(right))
            .collect()
    }

    fn project_set_interval_shapes(
        plan: LogicalPlan,
        shapes: Vec<IntervalShape>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut changed = false;
        let expressions = plan
            .schema()
            .columns()
            .into_iter()
            .zip(shapes)
            .map(|(column, shape)| {
                if shape.contains_interval() && state.interval_shape(&column.name) != Some(&shape) {
                    let name = state.get_field_info(&column.name)?.name().to_owned();
                    let id = state.register_field_name(name);
                    state.register_interval_shape(&id, shape);
                    changed = true;
                    Ok(Expr::Column(column).alias(id))
                } else {
                    Ok(Expr::Column(column))
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        if changed {
            Ok(project(plan, expressions)?)
        } else {
            Ok(plan)
        }
    }

    fn coerce_set_timestamp_columns(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        if left.schema().fields().len() != right.schema().fields().len() {
            return Ok((left, right));
        }
        let mut changed = false;
        let mut left_expressions = Vec::new();
        let mut right_expressions = Vec::new();
        for (left_column, right_column) in left
            .schema()
            .columns()
            .into_iter()
            .zip(right.schema().columns())
        {
            let left_type = left
                .schema()
                .qualified_field_from_column(&left_column)?
                .1
                .data_type();
            let right_type = right
                .schema()
                .qualified_field_from_column(&right_column)?
                .1
                .data_type();
            let mut left_expr = Expr::Column(left_column.clone());
            let mut right_expr = Expr::Column(right_column.clone());
            if let Some(target) = common_temporal_type(
                &[left_type.clone(), right_type.clone()],
                &self.config.session_timezone,
            ) {
                changed |= left_type != &target || right_type != &target;
                left_expr = cast_temporal(
                    left_expr,
                    left_type,
                    target.clone(),
                    &self.config.session_timezone,
                    false,
                )?
                .alias_qualified(left_column.relation, left_column.name);
                right_expr = cast_temporal(
                    right_expr,
                    right_type,
                    target,
                    &self.config.session_timezone,
                    false,
                )?
                .alias_qualified(right_column.relation, right_column.name);
            }
            left_expressions.push(left_expr);
            right_expressions.push(right_expr);
        }
        if changed {
            Ok((
                project(left, left_expressions)?,
                project(right, right_expressions)?,
            ))
        } else {
            Ok((left, right))
        }
    }

    pub(super) async fn resolve_query_set_operation(
        &self,
        op: spec::SetOperation,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::SetOpType;

        let spec::SetOperation {
            left,
            right,
            set_op_type,
            is_all,
            by_name,
            allow_missing_columns,
        } = op;
        let left = self.resolve_query_plan(*left, state).await?;
        let right = self.resolve_query_plan(*right, state).await?;
        match set_op_type {
            SetOpType::Intersect => {
                let (left, right) = self.coerce_set_timestamp_columns(left, right)?;
                let shapes = Self::set_operation_interval_shapes(&left, &right, state);
                let plan = LogicalPlanBuilder::intersect(left, right, is_all)?;
                Self::project_set_interval_shapes(plan, shapes, state)
            }
            SetOpType::Union => {
                let (left, right) = if by_name {
                    let left_names = Self::get_field_names(left.schema(), state)?;
                    let right_names = Self::get_field_names(right.schema(), state)?;
                    let (mut left_reordered_columns, mut right_reordered_columns): (
                        Vec<Expr>,
                        Vec<Expr>,
                    ) = left_names
                        .iter()
                        .enumerate()
                        .map(|(left_idx, left_name)| {
                            match right_names
                                .iter()
                                .position(|right_name| left_name.eq_ignore_ascii_case(right_name))
                            {
                                Some(right_idx) => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Column(Column::from(
                                        right.schema().qualified_field(right_idx),
                                    )),
                                )),
                                None if allow_missing_columns => Ok((
                                    Expr::Column(Column::from(
                                        left.schema().qualified_field(left_idx),
                                    )),
                                    Expr::Literal(ScalarValue::Null, None)
                                        .alias(state.register_field_name(left_name)),
                                )),
                                None => Err(PlanError::invalid(format!(
                                    "right column not found: {left_name}"
                                ))),
                            }
                        })
                        .collect::<PlanResult<Vec<(Expr, Expr)>>>()?
                        .into_iter()
                        .unzip();
                    if allow_missing_columns {
                        let (left_extra_columns, right_extra_columns): (Vec<Expr>, Vec<Expr>) =
                            right_names
                                .into_iter()
                                .enumerate()
                                .filter(|(_, right_name)| {
                                    !left_names
                                        .iter()
                                        .any(|left_name| left_name.eq_ignore_ascii_case(right_name))
                                })
                                .map(|(right_idx, right_name)| {
                                    (
                                        Expr::Literal(ScalarValue::Null, None)
                                            .alias(state.register_field_name(right_name)),
                                        Expr::Column(Column::from(
                                            right.schema().qualified_field(right_idx),
                                        )),
                                    )
                                })
                                .collect::<Vec<(Expr, Expr)>>()
                                .into_iter()
                                .unzip();
                        right_reordered_columns.extend(right_extra_columns);
                        left_reordered_columns.extend(left_extra_columns);
                        (
                            project(left, left_reordered_columns)?,
                            project(right, right_reordered_columns)?,
                        )
                    } else {
                        (left, project(right, right_reordered_columns)?)
                    }
                } else {
                    (left, right)
                };
                let (left, right) = self.coerce_set_timestamp_columns(left, right)?;
                // Spark Connect treats Union as a leaf for DataFrame plan IDs.
                // WithCTE restores separately reachable definition bindings.
                for field in left.schema().fields() {
                    state.clear_field_plan_ids(field.name())?;
                }
                let shapes = Self::set_operation_interval_shapes(&left, &right, state);
                let plan = if is_all {
                    LogicalPlanBuilder::new(left).union(right)?.build()?
                } else {
                    LogicalPlanBuilder::new(left)
                        .union_distinct(right)?
                        .build()?
                };
                Self::project_set_interval_shapes(plan, shapes, state)
            }
            SetOpType::Except => {
                let (left, right) = self.coerce_set_timestamp_columns(left, right)?;
                let left_len = left.schema().fields().len();
                let right_len = right.schema().fields().len();

                if left_len != right_len {
                    return Err(PlanError::invalid(format!(
                        "`EXCEPT ALL` must have the same number of columns. Left has {left_len} columns, right has {right_len} columns."
                    )));
                }

                let shapes = Self::set_operation_interval_shapes(&left, &right, state);

                let mut join_keys = left
                    .schema()
                    .fields()
                    .iter()
                    .zip(right.schema().fields().iter())
                    .map(|(left_field, right_field)| {
                        (
                            Column::from_name(left_field.name()),
                            Column::from_name(right_field.name()),
                        )
                    })
                    .collect::<Vec<_>>();

                let plan = if is_all {
                    // The row numbers are internal and must not be referenceable by name.
                    let left_row_number_alias = state.next_field_id();
                    let right_row_number_alias = state.next_field_id();
                    let left_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: left
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(left_row_number_alias.as_str());
                    let right_row_number_window =
                        Expr::WindowFunction(Box::new(expr::WindowFunction {
                            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                            params: WindowFunctionParams {
                                args: vec![],
                                partition_by: right
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|field| Expr::Column(Column::from_name(field.name())))
                                    .collect::<Vec<_>>(),
                                order_by: vec![],
                                window_frame: WindowFrame::new(None),
                                filter: None,
                                null_treatment: Some(NullTreatment::RespectNulls),
                                distinct: false,
                            },
                        }))
                        .alias(right_row_number_alias.as_str());
                    let left = LogicalPlanBuilder::from(left)
                        .window(vec![left_row_number_window])?
                        .build()?;
                    let right = LogicalPlanBuilder::from(right)
                        .window(vec![right_row_number_window])?
                        .build()?;
                    let left_join_columns = join_keys
                        .iter()
                        .map(|(left_col, _)| left_col.clone())
                        .collect::<Vec<_>>();
                    join_keys.push((
                        Column::from_name(left_row_number_alias),
                        Column::from_name(right_row_number_alias),
                    ));
                    LogicalPlanBuilder::from(left)
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .project(left_join_columns)?
                        .build()
                } else {
                    LogicalPlanBuilder::from(left)
                        .distinct()?
                        .join_detailed(
                            right,
                            JoinType::LeftAnti,
                            join_keys.into_iter().unzip(),
                            None,
                            NullEquality::NullEqualsNull,
                        )?
                        .build()
                }?;
                Self::project_set_interval_shapes(plan, shapes, state)
            }
        }
    }
}
