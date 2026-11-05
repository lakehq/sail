use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::{Column, DFSchema, JoinType, NullEquality, ScalarValue};
use datafusion_expr::builder::project;
use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable, WindowFunctionParams};
use datafusion_expr::{
    Expr, ExprSchemable, HigherOrderUDF, LogicalPlan, LogicalPlanBuilder, ScalarUDF, WindowFrame,
    WindowFunctionDefinition, cast, expr, lit, when,
};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_common_datafusion::type_coercion::binary_type_coercion::set_operation_coercion;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::timestamp_cast::cast_timestamp_ntz_to_ltz;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::struct_function::StructFunction;
use sail_logical_optimizer::type_coercion::TypeCoercion;

use crate::error::{PlanError, PlanResult};
use crate::function::datetime_utils::{cast_to_common_timestamp_type, session_timestamp_targets};
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
            SetOpType::Except | SetOpType::Intersect => {
                // Set equality and multiplicity apply to coerced values. In
                // particular, distinct integers may become the same float.
                let config = self.ctx.state();
                let left = TypeCoercion::new().analyze(left, config.config_options())?;
                let right = TypeCoercion::new().analyze(right, config.config_options())?;
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
                let (left, right) = self.coerce_set_operation_timestamps(left, right)?;

                let timezone = self.config.session_timezone_label();
                let types = left
                    .schema()
                    .fields()
                    .iter()
                    .zip(right.schema().fields())
                    .map(|(left, right)| {
                        set_operation_coercion(
                            left.data_type(),
                            right.data_type(),
                            self.config.ansi_mode,
                            &timezone,
                        )
                        .ok_or_else(|| {
                            PlanError::analysis(format!(
                                "incompatible set operation types: {} and {}",
                                left.data_type(),
                                right.data_type()
                            ))
                        })
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let coerce = |plan: LogicalPlan| -> PlanResult<LogicalPlan> {
                    let exprs = plan
                        .schema()
                        .columns()
                        .into_iter()
                        .zip(&types)
                        .map(|(column, data_type)| {
                            let value = Expr::Column(column.clone());
                            let source_type = value.get_type(plan.schema())?;
                            let value = self
                                .rewrite_set_operation_temporal_cast(
                                    &value,
                                    &source_type,
                                    data_type,
                                    0,
                                )?
                                .unwrap_or(value)
                                .cast_to(data_type, plan.schema())?;
                            Ok(value.alias_qualified(column.relation, column.name))
                        })
                        .collect::<PlanResult<Vec<_>>>()?;
                    Ok(LogicalPlanBuilder::from(plan).project(exprs)?.build()?)
                };
                let left = coerce(left)?;
                let right = coerce(right)?;

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

    /// Rewrite only the temporal conversions that differ from Arrow's cast.
    /// Numeric widening and unchanged subtrees stay in the final generic cast.
    fn rewrite_set_operation_temporal_cast(
        &self,
        value: &Expr,
        source_type: &DataType,
        target_type: &DataType,
        list_depth: usize,
    ) -> PlanResult<Option<Expr>> {
        if source_type == target_type {
            return Ok(None);
        }
        let rewritten = match (source_type, target_type) {
            (
                DataType::List(source)
                | DataType::LargeList(source)
                | DataType::FixedSizeList(source, _),
                DataType::List(target)
                | DataType::LargeList(target)
                | DataType::FixedSizeList(target, _),
            ) => {
                let parameter = format!("__sail_set_operation_element_{list_depth}");
                let variable = Expr::LambdaVariable(LambdaVariable::new(
                    parameter.clone(),
                    Some(Arc::clone(source)),
                ));
                let Some(body) = self.rewrite_set_operation_temporal_cast(
                    &variable,
                    source.data_type(),
                    target.data_type(),
                    list_depth + 1,
                )?
                else {
                    return Ok(None);
                };
                // transform accepts variable-size lists; the final cast restores
                // the common list representation and element field metadata.
                let value = if matches!(source_type, DataType::FixedSizeList(_, _)) {
                    cast(value.clone(), DataType::List(Arc::clone(source)))
                } else {
                    value.clone()
                };
                Expr::HigherOrderFunction(HigherOrderFunction::new(
                    Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())),
                    vec![value, Expr::Lambda(Lambda::new(vec![parameter], body))],
                ))
            }
            (DataType::Struct(source), DataType::Struct(target)) => {
                // Match the final Arrow cast by name without scanning the target
                // schema for each source field.
                let mut targets = HashMap::with_capacity(target.len());
                for field in target {
                    targets.entry(field.name()).or_insert(field);
                }
                let mut changed = false;
                let mut children = Vec::with_capacity(source.len());
                for field in source {
                    let child = get_field().call(vec![value.clone(), lit(field.name().clone())]);
                    let rewritten = match targets.get(field.name()) {
                        Some(target) => self.rewrite_set_operation_temporal_cast(
                            &child,
                            field.data_type(),
                            target.data_type(),
                            list_depth,
                        )?,
                        None => None,
                    };
                    changed |= rewritten.is_some();
                    children.push(rewritten.unwrap_or(child));
                }
                if !changed {
                    return Ok(None);
                }
                // Build all converted fields together. A chain of field updates
                // would copy the whole struct once per converted field.
                let names = source.iter().map(|field| field.name().clone()).collect();
                let rebuilt = ScalarUDF::from(StructFunction::new(names)).call(children);
                when(value.clone().is_null(), lit(ScalarValue::Null)).otherwise(rebuilt)?
            }
            (
                DataType::Date32 | DataType::Date64,
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            ) => cast_timestamp_ntz_to_ltz(
                cast(
                    value.clone(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ),
                timezone,
                false,
            ),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ) => ScalarUDF::from(SparkTimestamp::try_new(
                timezone.clone(),
                self.config.ansi_mode,
                false,
            )?)
            .call(vec![value.clone()]),
            (
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => ScalarUDF::from(
                SparkToUtf8::new().with_timezone(Some(self.config.session_timezone_label())),
            )
            .call(vec![value.clone()]),
            _ => return Ok(None),
        };
        Ok(Some(rewritten))
    }

    /// Spark's common type for TIMESTAMP_NTZ and TIMESTAMP columns of a set operation.
    fn coerce_set_operation_timestamps(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> PlanResult<(LogicalPlan, LogicalPlan)> {
        let left_schema = Arc::clone(left.schema());
        let right_schema = Arc::clone(right.schema());
        let timezone = &self.config.session_timezone_label();
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
