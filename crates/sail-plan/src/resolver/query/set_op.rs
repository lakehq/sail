use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::{Column, DFSchema, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFunctionDefinition, expr,
};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::datetime_utils::{cast_to_common_timestamp_type, session_timestamp_targets};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
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
                let (left, right) = self.coerce_set_operation_timestamps(left, right)?;
                Ok(LogicalPlanBuilder::intersect(left, right, is_all)?)
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
                let (left, right) = self.coerce_set_operation_timestamps(left, right)?;
                // Spark Connect treats Union as a leaf for DataFrame plan IDs.
                // WithCTE restores separately reachable definition bindings.
                for field in left.schema().fields() {
                    state.clear_field_plan_ids(field.name())?;
                }
                if is_all {
                    Ok(LogicalPlanBuilder::new(left).union(right)?.build()?)
                } else {
                    Ok(LogicalPlanBuilder::new(left)
                        .union_distinct(right)?
                        .build()?)
                }
            }
            SetOpType::Except => {
                let left_len = left.schema().fields().len();
                let right_len = right.schema().fields().len();

                if left_len != right_len {
                    return Err(PlanError::invalid(format!(
                        "`EXCEPT ALL` must have the same number of columns. Left has {left_len} columns, right has {right_len} columns."
                    )));
                }
                let (left, right) = self.coerce_set_operation_timestamps(left, right)?;

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
                Ok(plan)
            }
        }
    }

    /// Spark's common type for TIMESTAMP_NTZ and TIMESTAMP columns of a set operation.
    fn coerce_set_operation_timestamps(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        let left_schema = Arc::clone(left.schema());
        let right_schema = Arc::clone(right.schema());
        let timezone = &self.config.session_timezone;
        let is_mix = |left: &DataType, right: &DataType| {
            session_timestamp_targets(left, right, timezone).is_some()
        };
        if left_schema.fields().len() != right_schema.fields().len()
            || !left_schema
                .fields()
                .iter()
                .zip(right_schema.fields())
                .any(|(left, right)| is_mix(left.data_type(), right.data_type()))
        {
            return Ok((left, right));
        }
        let coerce = |schema: &DFSchema, other: &DFSchema| {
            schema
                .iter()
                .zip(other.fields())
                .map(|((qualifier, field), other)| {
                    let column = Expr::Column(Column::from((qualifier, field.as_ref())));
                    match session_timestamp_targets(field.data_type(), other.data_type(), timezone)
                    {
                        Some((target, _)) => cast_to_common_timestamp_type(
                            column,
                            field.data_type(),
                            &target,
                            timezone,
                        )
                        .alias_qualified(qualifier.cloned(), field.name()),
                        None => column,
                    }
                })
                .collect::<Vec<_>>()
        };
        let left_expressions = coerce(&left_schema, &right_schema);
        let right_expressions = coerce(&right_schema, &left_schema);
        Ok((
            project(left, left_expressions)?,
            project(right, right_expressions)?,
        ))
    }
}
