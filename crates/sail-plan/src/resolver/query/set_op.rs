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
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Align nested structs by name before UNION type coercion. The null parent
    /// remains null rather than becoming a non-null struct of null children.
    fn align_union_structs(
        &self,
        left: Expr,
        left_type: &datafusion_common::arrow::datatypes::DataType,
        right: Expr,
        right_type: &datafusion_common::arrow::datatypes::DataType,
        allow_missing: bool,
    ) -> PlanResult<(Expr, Expr)> {
        use datafusion_common::arrow::datatypes::DataType;
        use datafusion_expr::{ScalarUDF, lit, when};
        use sail_function::scalar::struct_function::StructFunction;
        let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
            (left_type, right_type)
        else {
            return Ok((left, right));
        };
        if !allow_missing {
            return Ok((left, right));
        }
        let mut names = vec![];
        let mut left_values = vec![];
        let mut right_values = vec![];
        let field_expr = |expr: &Expr, name: &str| {
            datafusion_functions::core::get_field().call(vec![expr.clone(), lit(name.to_string())])
        };
        for field in left_fields {
            names.push(field.name().clone());
            let l = field_expr(&left, field.name());
            if let Some(r) = right_fields
                .iter()
                .find(|r| self.merge_name_key(r.name()) == self.merge_name_key(field.name()))
            {
                let (l, r) = self.align_union_structs(
                    l,
                    field.data_type(),
                    field_expr(&right, r.name()),
                    r.data_type(),
                    true,
                )?;
                left_values.push(l);
                right_values.push(r);
            } else {
                left_values.push(l);
                right_values.push(lit(ScalarValue::try_from(field.data_type())?));
            }
        }
        for field in right_fields {
            if !left_fields
                .iter()
                .any(|l| self.merge_name_key(l.name()) == self.merge_name_key(field.name()))
            {
                names.push(field.name().clone());
                left_values.push(lit(ScalarValue::try_from(field.data_type())?));
                right_values.push(field_expr(&right, field.name()));
            }
        }
        let function = ScalarUDF::from(StructFunction::new(names));
        Ok((
            when(left.is_null(), lit(ScalarValue::Null)).otherwise(function.call(left_values))?,
            when(right.is_null(), lit(ScalarValue::Null)).otherwise(function.call(right_values))?,
        ))
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
                            match right_names.iter().position(|right_name| {
                                self.merge_name_key(left_name) == self.merge_name_key(right_name)
                            }) {
                                Some(right_idx) => {
                                    let left_col =
                                        Column::from(left.schema().qualified_field(left_idx));
                                    let right_col =
                                        Column::from(right.schema().qualified_field(right_idx));
                                    let (left_expr, right_expr) = self.align_union_structs(
                                        Expr::Column(left_col.clone()),
                                        left.schema().field(left_idx).data_type(),
                                        Expr::Column(right_col.clone()),
                                        right.schema().field(right_idx).data_type(),
                                        allow_missing_columns,
                                    )?;
                                    Ok((
                                        left_expr.alias_qualified(left_col.relation, left_col.name),
                                        right_expr
                                            .alias_qualified(right_col.relation, right_col.name),
                                    ))
                                }
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
                                    !left_names.iter().any(|left_name| {
                                        self.merge_name_key(left_name)
                                            == self.merge_name_key(right_name)
                                    })
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
            SetOpType::Except | SetOpType::Intersect => {
                let join_type = if matches!(set_op_type, SetOpType::Intersect) {
                    JoinType::LeftSemi
                } else {
                    JoinType::LeftAnti
                };
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
                            join_type,
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
                            join_type,
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
