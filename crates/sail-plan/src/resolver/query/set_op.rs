use datafusion::arrow::datatypes::DataType;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion_common::{Column, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFunctionDefinition, cast, expr,
};
use sail_common::spec;

use crate::coercion::spark_wider_numeric_type;
use crate::error::{PlanError, PlanResult};
use crate::function::common::spark_type_name;
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
                let (left, right) = self.widen_numeric_columns(left, right, "INTERSECT")?;
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
                let (left, right) = self.widen_numeric_columns(left, right, "UNION")?;
                if is_all {
                    Ok(LogicalPlanBuilder::new(left).union(right)?.build()?)
                } else {
                    Ok(LogicalPlanBuilder::new(left)
                        .union_distinct(right)?
                        .build()?)
                }
            }
            SetOpType::Except => {
                let (left, right) = self.widen_numeric_columns(left, right, "EXCEPT")?;
                let left_len = left.schema().fields().len();
                let right_len = right.schema().fields().len();

                if left_len != right_len {
                    return Err(PlanError::invalid(format!(
                        "`EXCEPT ALL` must have the same number of columns. Left has {left_len} columns, right has {right_len} columns."
                    )));
                }

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
                    let left_row_number_alias = state.register_field_name("row_num");
                    let right_row_number_alias = state.register_field_name("row_num");
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

    /// Widens each positional pair of NUMERIC columns of a set operation to their common type, the
    /// way `WidenSetOperationTypes` does -- `Except` (`TypeCoercionBase.scala:194`), `Intersect`
    /// (`:208`) and `Union` (`:222`) alike. The plan is built from the LEFT
    /// input's schema, so `SELECT -2147483648 UNION ALL SELECT 3000000000L` declared an INT while
    /// carrying a BIGINT value: the rows were right and the schema lied, which broke `toArrow`.
    /// Only numeric pairs are widened here; everything else is left to DataFusion.
    fn widen_numeric_columns(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        operator: &str,
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        let left_fields = left.schema().fields();
        let right_fields = right.schema().fields();
        if left_fields.len() != right_fields.len() {
            return Ok((left, right));
        }
        for (index, (left_field, right_field)) in
            left_fields.iter().zip(right_fields.iter()).enumerate()
        {
            if set_operation_has_no_common_type(left_field.data_type(), right_field.data_type()) {
                return Err(PlanError::analysis(format!(
                    "[INCOMPATIBLE_COLUMN_TYPE] {operator} can only be performed on tables with compatible column types. The {} column of the second table is \"{}\" type which is not compatible with \"{}\" at the same column of the first table.",
                    ordinal(index + 1),
                    spark_type_name(right_field.data_type()),
                    spark_type_name(left_field.data_type()),
                )));
            }
        }
        let common = left_fields
            .iter()
            .zip(right_fields.iter())
            .map(|(left_field, right_field)| {
                let (left_type, right_type) = (left_field.data_type(), right_field.data_type());
                if left_type == right_type || !left_type.is_numeric() || !right_type.is_numeric() {
                    return None;
                }
                spark_wider_numeric_type(left_type, right_type, self.config.ansi_mode)
            })
            .collect::<Vec<_>>();
        if common.iter().all(Option::is_none) {
            return Ok((left, right));
        }
        let project_widened = |plan: LogicalPlan| -> PlanResult<LogicalPlan> {
            let columns = plan
                .schema()
                .iter()
                .zip(common.iter())
                .map(|((qualifier, field), common)| {
                    let column = Expr::Column(Column::from((qualifier, field)));
                    match common {
                        Some(data_type) if field.data_type() != data_type => {
                            cast(column, data_type.clone()).alias(field.name().to_string())
                        }
                        _ => column,
                    }
                })
                .collect::<Vec<_>>();
            Ok(project(plan, columns)?)
        };
        Ok((project_widened(left)?, project_widened(right)?))
    }
}

/// The `WidenSetOperationTypes` leaf where Spark finds no common type and leaves the set operation
/// unresolved. `CheckAnalysis` then raises `INCOMPATIBLE_COLUMN_TYPE` (TypeCoercionBase.scala:
/// 190-222; CheckAnalysis.scala:840-854). Keep this deliberately to pairs that have no string,
/// numeric, temporal, or NULL promotion: the remaining type-coercion leaves stay with DataFusion.
fn set_operation_has_no_common_type(left: &DataType, right: &DataType) -> bool {
    if left == right || left.is_null() || right.is_null() {
        return false;
    }
    let is_date = |data_type: &DataType| matches!(data_type, DataType::Date32 | DataType::Date64);
    (is_date(left) && right.is_numeric())
        || (left.is_numeric() && is_date(right))
        || (left.is_nested() != right.is_nested())
}

fn ordinal(number: usize) -> String {
    match number {
        1 => "first".to_string(),
        2 => "second".to_string(),
        3 => "third".to_string(),
        _ => format!("{number}th"),
    }
}
