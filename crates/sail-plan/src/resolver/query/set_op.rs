use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::optimizer::analyzer::type_coercion::coerce_union_schema;
use datafusion_common::{Column, DFSchema, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Union, WindowFrame, WindowFunctionDefinition, expr,
};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
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
            SetOpType::Intersect => Ok(LogicalPlanBuilder::intersect(left, right, is_all)?),
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
                let mut union =
                    Union::try_new_with_loose_types(vec![Arc::new(left), Arc::new(right)])?;
                // Conditional coercion needs the common UNION schema.
                // TODO: Match Spark's ANSI string coercion for UNION inputs. DataFusion
                //  widens numeric/STRING inputs to STRING, so typeof also reports STRING.
                // TODO: Widen DECIMAL with FLOAT/DOUBLE, and ANSI BIGINT with FLOAT, to DOUBLE
                //  like Spark. DataFusion keeps DECIMAL or FLOAT for these UNION inputs.
                let coerced = coerce_union_schema(&union.inputs)?;
                // Take only types and nullability from the coerced schema, since DataFusion lets
                // the last input's field metadata (such as a Spark interval qualifier) win.
                // Columns keep the loose type where DataFusion's common type differs from Spark:
                // DATE with TIMESTAMP becomes a nanosecond TIMESTAMP_NTZ, DOUBLE with DECIMAL
                // becomes DECIMAL, and ANSI numeric with STRING becomes STRING, which store
                // assignment rejects.
                // TODO: Widen these columns and interval qualifiers like Spark.
                let fields = union
                    .schema
                    .iter()
                    .zip(coerced.fields())
                    .enumerate()
                    .map(|(i, ((qualifier, field), coerced_field))| {
                        let types = union
                            .inputs
                            .iter()
                            .map(|input| input.schema().field(i).data_type())
                            .collect::<Vec<_>>();
                        let has = |f: fn(&DataType) -> bool| types.iter().any(|t| f(t));
                        // TODO: Widen nested UNION types while preserving their field metadata.
                        // Keep the existing type until coercion can retain interval qualifiers.
                        let has_nested_metadata = Schema::new(vec![Arc::clone(field)])
                            .flattened_fields()
                            .iter()
                            .skip(1)
                            .any(|field| !field.metadata().is_empty());
                        let keep_loose_type = has_nested_metadata
                            || (field.data_type() == &DataType::Float64
                                && coerced_field.data_type().is_decimal())
                            || (has(|t| matches!(t, DataType::Date32 | DataType::Date64))
                                && has(|t| matches!(t, DataType::Timestamp(_, _))))
                            || (self.config.ansi_mode
                                && has(DataType::is_string)
                                && has(DataType::is_numeric));
                        let field = if keep_loose_type {
                            Arc::clone(field)
                        } else {
                            Arc::new(
                                field
                                    .as_ref()
                                    .clone()
                                    .with_data_type(coerced_field.data_type().clone())
                                    .with_nullable(coerced_field.is_nullable()),
                            )
                        };
                        (qualifier.cloned(), field)
                    })
                    .collect();
                union.schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    union.schema.metadata().clone(),
                )?);
                let plan = LogicalPlan::Union(union);
                if is_all {
                    Ok(plan)
                } else {
                    Ok(LogicalPlanBuilder::new(plan).distinct()?.build()?)
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
}
