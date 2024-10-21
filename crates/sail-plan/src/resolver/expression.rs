use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::{plan_err, Column, DFSchemaRef, DataFusionError};
use datafusion_expr::expr::PlannedReplaceSelectItem;
use datafusion_expr::{
    expr, expr_fn, window_frame, AggregateUDF, ExprSchemable, Operator, ScalarUDF,
};
use log::debug;
use num_traits::Float;
use sail_common::spec;
use sail_common::spec::PySparkUdfType;
use sail_python_udf::cereal::pyspark_udf::{deserialize_partial_pyspark_udf, PySparkUdfObject};
use sail_python_udf::udf::pyspark_udaf::PySparkAggregateUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;
use sail_python_udf::udf::unresolved_pyspark_udf::UnresolvedPySparkUDF;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::drop_struct_field::DropStructField;
use crate::extension::function::update_struct_field::UpdateStructField;
use crate::function::common::{AggFunctionContext, FunctionContext};
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, get_built_in_window_function,
};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct NamedExpr {
    /// The name of the expression to be used in projection.
    /// The name can be empty if the expression is not supposed to exist in the resolved
    /// projection (a wildcard expression, a sort expression, etc.).
    /// A list of names may be present for multi-expression (a temporary expression
    /// to be expanded into multiple ones in the projection).
    pub name: Vec<String>,
    pub expr: expr::Expr,
    pub metadata: Vec<(String, String)>,
}

impl NamedExpr {
    pub fn new(name: Vec<String>, expr: expr::Expr) -> Self {
        Self {
            name,
            expr,
            metadata: vec![],
        }
    }

    pub fn try_from_alias_expr(expr: expr::Expr) -> PlanResult<Self> {
        match expr {
            expr::Expr::Alias(alias) => Ok(Self::new(vec![alias.name], *alias.expr)),
            _ => Err(PlanError::invalid(
                "alias expected to create named expression",
            )),
        }
    }

    pub fn try_from_column_expr(
        expr: expr::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<Self> {
        match expr {
            expr::Expr::Column(column) => {
                let name = state.get_field_name(column.name())?;
                Ok(Self::new(vec![name.clone()], expr::Expr::Column(column)))
            }
            _ => Err(PlanError::invalid(
                "column expected to create named expression",
            )),
        }
    }

    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn into_alias_expr(self) -> PlanResult<expr::Expr> {
        match &self.expr {
            // TODO: This seems hacky. Is there a better way to handle this?
            // There is no need to add alias to an alias expression.
            expr::Expr::Alias(_)
            // We do not add alias for literal when it is used as function arguments,
            // since some function (e.g. `struct()`) may need to determine struct field names
            // for literals.
            | expr::Expr::Literal(_)
            // We should not add alias to expressions that will be rewritten in logical plans.
            // Otherwise, some logical plan optimizers may not work correctly.
            | expr::Expr::Wildcard { .. }
            | expr::Expr::GroupingSet(_)
            | expr::Expr::Placeholder(_)
            | expr::Expr::Unnest(_) => return Ok(self.expr),
            _ => (),
        };
        let relation = match &self.expr {
            expr::Expr::Column(Column { relation, .. }) => relation.clone(),
            _ => None,
        };
        let name = self
            .name
            .one()
            .map_err(|_| PlanError::invalid("named expression must have a single name"))?;
        Ok(self.expr.alias_qualified(relation, name))
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_sort_order(
        &self,
        sort: spec::SortOrder,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Sort> {
        use spec::{NullOrdering, SortDirection};

        let spec::SortOrder {
            child,
            direction,
            null_ordering,
        } = sort;
        let asc = match direction {
            SortDirection::Ascending => true,
            SortDirection::Descending => false,
            SortDirection::Unspecified => true,
        };
        let nulls_first = match null_ordering {
            NullOrdering::NullsFirst => true,
            NullOrdering::NullsLast => false,
            NullOrdering::Unspecified => asc,
        };

        match child.as_ref() {
            spec::Expr::Literal(literal) if resolve_literals => {
                let num_fields = schema.fields().len();
                let position = match literal {
                    spec::Literal::Integer(value) => *value as usize,
                    spec::Literal::Long(value) => *value as usize,
                    _ => {
                        return Ok(expr::Sort {
                            expr: self.resolve_expression(*child, schema, state).await?,
                            asc,
                            nulls_first,
                        })
                    }
                };
                if position > 0 && position <= num_fields {
                    Ok(expr::Sort {
                        expr: expr::Expr::Column(Column::from(
                            schema.qualified_field(position - 1),
                        )),
                        asc,
                        nulls_first,
                    })
                } else {
                    Err(PlanError::invalid(format!(
                        "Cannot resolve column position {position}. Valid positions are 1 to {num_fields}."
                    )))
                }
            }
            _ => Ok(expr::Sort {
                expr: self.resolve_expression(*child, schema, state).await?,
                asc,
                nulls_first,
            }),
        }
    }

    pub(super) async fn resolve_sort_orders(
        &self,
        sort: Vec<spec::SortOrder>,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Sort>> {
        let mut results: Vec<expr::Sort> = Vec::with_capacity(sort.len());
        for s in sort {
            let expr = self
                .resolve_sort_order(s, resolve_literals, schema, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }

    fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
    ) -> PlanResult<window_frame::WindowFrame> {
        use spec::WindowFrameType;
        use window_frame::WindowFrameUnits;

        let spec::WindowFrame {
            frame_type,
            lower,
            upper,
        } = frame;

        let units = match frame_type {
            WindowFrameType::Undefined => return Err(PlanError::invalid("undefined frame type")),
            WindowFrameType::Row => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };
        let (start, end) = match units {
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => (
                self.resolve_window_boundary_offset(lower, WindowBoundaryKind::Lower)?,
                self.resolve_window_boundary_offset(upper, WindowBoundaryKind::Upper)?,
            ),
            WindowFrameUnits::Range => (
                self.resolve_window_boundary_value(
                    lower,
                    WindowBoundaryKind::Lower,
                    order_by,
                    schema,
                )?,
                self.resolve_window_boundary_value(
                    upper,
                    WindowBoundaryKind::Upper,
                    order_by,
                    schema,
                )?,
            ),
        };
        Ok(window_frame::WindowFrame::new_bounds(units, start, end))
    }

    fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        kind: WindowBoundaryKind,
    ) -> PlanResult<window_frame::WindowFrameBound> {
        let unbounded = || match kind {
            WindowBoundaryKind::Lower => {
                window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(None))
            }
            WindowBoundaryKind::Upper => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
            }
        };

        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(window_frame::WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::Unbounded => Ok(unbounded()),
            spec::WindowFrameBoundary::Value(value) => {
                let value = match *value {
                    spec::Expr::Literal(literal) => literal,
                    _ => {
                        return Err(PlanError::invalid(
                            "window boundary offset must be a literal",
                        ))
                    }
                };
                let value = self.resolve_literal(value)?;
                match value {
                    ScalarValue::UInt32(None)
                    | ScalarValue::Int32(None)
                    | ScalarValue::UInt64(None)
                    | ScalarValue::Int64(None)
                    | ScalarValue::Float16(None)
                    | ScalarValue::Float32(None)
                    | ScalarValue::Float64(None) => Ok(unbounded()),
                    ScalarValue::UInt32(Some(v)) => Ok(WindowBoundaryOffset::from(v).into()),
                    ScalarValue::Int32(Some(v)) => Ok(WindowBoundaryOffset::from(v).into()),
                    ScalarValue::UInt64(Some(v)) => Ok(WindowBoundaryOffset::from(v).into()),
                    ScalarValue::Int64(Some(v)) => Ok(WindowBoundaryOffset::from(v).into()),
                    ScalarValue::Float16(Some(v)) => {
                        Ok(WindowBoundaryOffset::try_from(WindowBoundaryFloatOffset(v))?.into())
                    }
                    ScalarValue::Float32(Some(v)) => {
                        Ok(WindowBoundaryOffset::try_from(WindowBoundaryFloatOffset(v))?.into())
                    }
                    ScalarValue::Float64(Some(v)) => {
                        Ok(WindowBoundaryOffset::try_from(WindowBoundaryFloatOffset(v))?.into())
                    }
                    _ => Err(PlanError::invalid(format!(
                        "invalid window boundary offset: {:?}",
                        value
                    ))),
                }
            }
        }
    }

    fn resolve_window_boundary_value(
        &self,
        value: spec::WindowFrameBoundary,
        kind: WindowBoundaryKind,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
    ) -> PlanResult<window_frame::WindowFrameBound> {
        let unbounded = || match kind {
            WindowBoundaryKind::Lower => {
                window_frame::WindowFrameBound::Preceding(ScalarValue::Null)
            }
            WindowBoundaryKind::Upper => {
                window_frame::WindowFrameBound::Following(ScalarValue::Null)
            }
        };

        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(window_frame::WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::Unbounded => Ok(unbounded()),
            spec::WindowFrameBoundary::Value(value) => {
                let value = match *value {
                    spec::Expr::Literal(literal) => literal,
                    _ => {
                        return Err(PlanError::invalid(
                            "window boundary value must be a literal",
                        ))
                    }
                };
                let value = self.resolve_literal(value)?;
                if value.is_null() {
                    Ok(unbounded())
                } else {
                    if order_by.len() != 1 {
                        return Err(PlanError::invalid(
                            "range window frame requires exactly one order by expression",
                        ));
                    }
                    let (data_type, _) = order_by[0].expr.data_type_and_nullable(schema)?;
                    let value = value.cast_to(&data_type)?;
                    // We always return the "following" bound since the value can be signed.
                    Ok(window_frame::WindowFrameBound::Following(value))
                }
            }
        }
    }

    #[async_recursion]
    pub(super) async fn resolve_named_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        use spec::Expr;

        match expr {
            Expr::Literal(literal) => self.resolve_expression_literal(literal),
            Expr::UnresolvedAttribute { name, plan_id } => {
                self.resolve_expression_attribute(name, plan_id, schema, state)
            }
            Expr::UnresolvedFunction {
                function_name,
                arguments,
                is_distinct,
                is_user_defined_function: _, // FIXME: is_user_defined_function is always false.
            } => {
                self.resolve_expression_function(
                    function_name,
                    arguments,
                    is_distinct,
                    schema,
                    state,
                )
                .await
            }
            Expr::UnresolvedStar {
                target,
                wildcard_options,
            } => {
                self.resolve_expression_wildcard(target, wildcard_options, schema, state)
                    .await
            }
            Expr::Alias {
                expr,
                name,
                metadata,
            } => {
                self.resolve_expression_alias(*expr, name, metadata, schema, state)
                    .await
            }
            Expr::Cast { expr, cast_to_type } => {
                self.resolve_expression_cast(*expr, cast_to_type, schema, state)
                    .await
            }
            Expr::UnresolvedRegex { col_name, plan_id } => {
                self.resolve_expression_regex(col_name, plan_id, schema, state)
                    .await
            }
            Expr::SortOrder(sort) => {
                self.resolve_expression_sort_order(sort, schema, state)
                    .await
            }
            Expr::LambdaFunction {
                function,
                arguments,
            } => {
                self.resolve_expression_lambda_function(*function, arguments, schema, state)
                    .await
            }
            Expr::Window {
                window_function,
                partition_spec,
                order_spec,
                frame_spec,
            } => {
                self.resolve_expression_window(
                    *window_function,
                    partition_spec,
                    order_spec,
                    frame_spec,
                    schema,
                    state,
                )
                .await
            }
            Expr::UnresolvedExtractValue { child, extraction } => {
                self.resolve_expression_extract_value(*child, *extraction, schema, state)
                    .await
            }
            Expr::UpdateFields {
                struct_expression,
                field_name,
                value_expression,
            } => {
                self.resolve_expression_update_fields(
                    *struct_expression,
                    field_name,
                    value_expression.map(|x| *x),
                    schema,
                    state,
                )
                .await
            }
            Expr::UnresolvedNamedLambdaVariable(variable) => {
                self.resolve_expression_named_lambda_variable(variable, schema, state)
                    .await
            }
            Expr::CommonInlineUserDefinedFunction(function) => {
                self.resolve_expression_common_inline_udf(function, schema, state)
                    .await
            }
            Expr::CallFunction {
                function_name,
                arguments,
            } => {
                self.resolve_expression_call_function(function_name, arguments, schema, state)
                    .await
            }
            Expr::Placeholder(placeholder) => {
                self.resolve_expression_placeholder(placeholder).await
            }
            Expr::Rollup(rollup) => self.resolve_expression_rollup(rollup, schema, state).await,
            Expr::Cube(cube) => self.resolve_expression_cube(cube, schema, state).await,
            Expr::GroupingSets(grouping_sets) => {
                self.resolve_expression_grouping_sets(grouping_sets, schema, state)
                    .await
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.resolve_expression_in_subquery(*expr, *subquery, negated, schema, state)
                    .await
            }
            Expr::ScalarSubquery { subquery } => {
                self.resolve_expression_scalar_subquery(*subquery, schema, state)
                    .await
            }
            Expr::Exists { subquery, negated } => {
                self.resolve_expression_exists(*subquery, negated, schema, state)
                    .await
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                self.resolve_expression_in_list(*expr, list, negated, schema, state)
                    .await
            }
            Expr::IsFalse(expr) => self.resolve_expression_is_false(*expr, schema, state).await,
            Expr::IsNotFalse(expr) => {
                self.resolve_expression_is_not_false(*expr, schema, state)
                    .await
            }
            Expr::IsTrue(expr) => self.resolve_expression_is_true(*expr, schema, state).await,
            Expr::IsNotTrue(expr) => {
                self.resolve_expression_is_not_true(*expr, schema, state)
                    .await
            }
            Expr::IsNull(expr) => self.resolve_expression_is_null(*expr, schema, state).await,
            Expr::IsNotNull(expr) => {
                self.resolve_expression_is_not_null(*expr, schema, state)
                    .await
            }
            Expr::IsUnknown(expr) => {
                self.resolve_expression_is_unknown(*expr, schema, state)
                    .await
            }
            Expr::IsNotUnknown(expr) => {
                self.resolve_expression_is_not_unknown(*expr, schema, state)
                    .await
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.resolve_expression_between(*expr, negated, *low, *high, schema, state)
                    .await
            }
            Expr::IsDistinctFrom { left, right } => {
                self.resolve_expression_is_distinct_from(*left, *right, schema, state)
                    .await
            }
            Expr::IsNotDistinctFrom { left, right } => {
                self.resolve_expression_is_not_distinct_from(*left, *right, schema, state)
                    .await
            }
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            } => {
                self.resolve_expression_similar_to(
                    *expr,
                    *pattern,
                    negated,
                    escape_char,
                    case_insensitive,
                    schema,
                    state,
                )
                .await
            }
        }
    }

    pub(super) async fn resolve_named_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        let mut results: Vec<NamedExpr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let named_expr = self
                .resolve_named_expression(expression, schema, state)
                .await?;
            results.push(named_expr);
        }
        Ok(results)
    }

    pub(super) async fn resolve_expression(
        &self,
        expressions: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        let NamedExpr { expr, .. } = self
            .resolve_named_expression(expressions, schema, state)
            .await?;
        Ok(expr)
    }

    pub(super) async fn resolve_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Expr>> {
        let mut results: Vec<expr::Expr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let expr = self.resolve_expression(expression, schema, state).await?;
            results.push(expr);
        }
        Ok(results)
    }

    pub(super) async fn resolve_expressions_and_names(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        let mut names: Vec<String> = Vec::with_capacity(expressions.len());
        let mut exprs: Vec<expr::Expr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let NamedExpr { name, expr, .. } = self
                .resolve_named_expression(expression, schema, state)
                .await?;
            names.push(name.one()?);
            exprs.push(expr);
        }
        Ok((names, exprs))
    }

    pub(super) async fn resolve_alias_expressions_and_names(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        let mut names: Vec<String> = Vec::with_capacity(expressions.len());
        let mut exprs: Vec<expr::Expr> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let named_expr = self
                .resolve_named_expression(expression, schema, state)
                .await?;
            let name = named_expr.name.clone().one()?;
            let expr = named_expr.into_alias_expr()?;
            names.push(name);
            exprs.push(expr);
        }
        Ok((names, exprs))
    }

    fn resolve_expression_literal(&self, literal: spec::Literal) -> PlanResult<NamedExpr> {
        let name = self.config.plan_formatter.literal_to_string(&literal)?;
        let literal = self.resolve_literal(literal)?;
        Ok(NamedExpr::new(vec![name], expr::Expr::Literal(literal)))
    }

    fn resolve_expression_attribute(
        &self,
        name: spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let candidates =
            self.resolve_expression_attribute_candidates(&name, plan_id, schema, state)?;
        if !candidates.is_empty() {
            if candidates.len() > 1 {
                // FIXME: This is a temporary hack to handle ambiguous attributes.
                debug!("ambiguous attribute: name: {name:?}, candidates: {candidates:?}");
            }
            let ((name, _, column), _candidates) = candidates.at_least_one()?;
            return Ok(NamedExpr::new(vec![name], expr::Expr::Column(column)));
        }
        let candidates = if let Some(schema) = state.get_outer_query_schema().cloned() {
            self.resolve_expression_attribute_candidates(&name, None, &schema, state)?
        } else {
            vec![]
        };
        if candidates.len() > 1 {
            return plan_err!("ambiguous outer attribute: {name:?}")?;
        }
        if !candidates.is_empty() {
            let (name, dt, column) = candidates.one()?;
            return Ok(NamedExpr::new(
                vec![name],
                expr::Expr::OuterReferenceColumn(dt, column),
            ));
        }
        plan_err!("cannot resolve attribute: {name:?}")?
    }

    fn resolve_expression_attribute_candidates(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(String, DataType, Column)>> {
        // TODO: handle qualifier and nested fields
        let name: Vec<String> = name.clone().into();
        let first = name
            .first()
            .ok_or_else(|| PlanError::invalid(format!("empty attribute: {:?}", name)))?;
        let last = name
            .last()
            .ok_or_else(|| PlanError::invalid(format!("empty attribute: {:?}", name)))?;
        let candidates = if let Some(plan_id) = plan_id {
            let field = state.get_resolved_field_name_in_plan(plan_id, first)?;
            schema
                .qualified_fields_with_unqualified_name(field)
                .iter()
                .map(|(qualifier, field)| {
                    (
                        last.clone(),
                        field.data_type().clone(),
                        Column::new(qualifier.cloned(), field.name()),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            schema
                .iter()
                .filter_map(|(qualifier, field)| {
                    if state
                        .get_field_name(field.name())
                        .is_ok_and(|f| f.eq_ignore_ascii_case(last))
                        && (name.len() == 1
                            || name.len() == 2
                                && qualifier.is_some_and(|q| q.table().eq_ignore_ascii_case(first)))
                    {
                        Some((
                            last.clone(),
                            field.data_type().clone(),
                            Column::new(qualifier.cloned(), field.name()),
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        Ok(candidates)
    }

    async fn resolve_expression_function(
        &self,
        function_name: String,
        arguments: Vec<spec::Expr>,
        is_distinct: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (argument_names, arguments) = self
            .resolve_alias_expressions_and_names(arguments, schema, state)
            .await?;

        // FIXME: is_user_defined_function is always false
        //  So, we need to check udf's before built-in functions.
        let func = if let Ok(udf) = self.ctx.udf(function_name.as_str()) {
            // TODO: UnresolvedPythonUDF will likely need to be accounted for as well
            //  once we integrate LakeSail Python UDF.
            let udf = if let Some(f) = udf.inner().as_any().downcast_ref::<UnresolvedPySparkUDF>() {
                let deterministic = f.deterministic()?;
                let function_definition = f.python_function_definition()?;
                let (output_type, eval_type, command, python_version) = match &function_definition {
                    spec::FunctionDefinition::PythonUdf {
                        output_type,
                        eval_type,
                        command,
                        python_version,
                    } => (output_type, eval_type, command, python_version),
                    _ => {
                        return Err(PlanError::invalid("UDF function type must be Python UDF"));
                    }
                };
                let output_type: DataType = self.resolve_data_type(output_type.clone())?;

                let python_function: PySparkUdfObject = deserialize_partial_pyspark_udf(
                    python_version,
                    command,
                    *eval_type,
                    arguments.len(),
                    &self.config.spark_udf_config,
                )
                .map_err(|e| {
                    PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
                })?;

                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

                let python_udf: PySparkUDF = PySparkUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    *eval_type,
                    python_function,
                    output_type,
                );

                Arc::new(ScalarUDF::from(python_udf))
            } else {
                udf
            };
            expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: udf,
                args: arguments,
            })
        } else if let Ok(func) = get_built_in_function(function_name.as_str()) {
            let function_context = FunctionContext::new(self.config.clone(), self.ctx);
            func(arguments.clone(), &function_context)?
        } else if let Ok(func) = get_built_in_aggregate_function(function_name.as_str()) {
            let agg_function_context = AggFunctionContext::new(is_distinct);
            func(arguments.clone(), agg_function_context)?
        } else {
            return Err(PlanError::unsupported(format!(
                "unknown function: {function_name}",
            )));
        };
        // TODO: udaf and udwf

        let name = self.config.plan_formatter.function_to_string(
            function_name.as_str(),
            argument_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], func))
    }

    async fn resolve_expression_alias(
        &self,
        expr: spec::Expr,
        name: Vec<spec::Identifier>,
        metadata: Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let name = name.into_iter().map(|x| x.into()).collect::<Vec<String>>();
        let expr = if let [n] = name.as_slice() {
            expr.alias(n)
        } else {
            expr
        };
        if let Some(metadata) = metadata {
            Ok(NamedExpr::new(name, expr).with_metadata(metadata))
        } else {
            Ok(NamedExpr::new(name, expr))
        }
    }

    async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let data_type = self.resolve_data_type(cast_to_type)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr = expr::Expr::Cast(expr::Cast {
            expr: Box::new(expr),
            data_type,
        });
        Ok(NamedExpr::new(name, expr))
    }

    async fn resolve_expression_sort_order(
        &self,
        sort: spec::SortOrder,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let sort = self.resolve_sort_order(sort, true, schema, state).await?;
        Ok(NamedExpr::new(vec![], sort.expr))
    }

    async fn resolve_expression_regex(
        &self,
        _col_name: String,
        _plan_id: Option<i64>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("unresolved regex"))
    }

    async fn resolve_expression_lambda_function(
        &self,
        _function: spec::Expr,
        _arguments: Vec<spec::UnresolvedNamedLambdaVariable>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("lambda function"))
    }

    async fn resolve_expression_window(
        &self,
        window_function: spec::Expr,
        partition_spec: Vec<spec::Expr>,
        order_spec: Vec<spec::SortOrder>,
        frame_spec: Option<spec::WindowFrame>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (function, function_name, argument_names, arguments, is_distinct) =
            match window_function {
                spec::Expr::UnresolvedFunction {
                    function_name,
                    arguments,
                    is_user_defined_function,
                    is_distinct,
                } => {
                    if is_user_defined_function {
                        return Err(PlanError::unsupported("user defined window function"));
                    }
                    if is_distinct {
                        return Err(PlanError::unsupported("distinct window function"));
                    }
                    let (argument_names, arguments) = self
                        .resolve_alias_expressions_and_names(arguments, schema, state)
                        .await?;
                    let function = get_built_in_window_function(function_name.as_str())?;
                    (
                        function,
                        function_name,
                        argument_names,
                        arguments,
                        is_distinct,
                    )
                }
                spec::Expr::CommonInlineUserDefinedFunction(function) => {
                    let spec::CommonInlineUserDefinedFunction {
                        function_name,
                        deterministic,
                        arguments,
                        function,
                    } = function;
                    let (argument_names, arguments) = self
                        .resolve_alias_expressions_and_names(arguments, schema, state)
                        .await?;
                    let input_types: Vec<DataType> = arguments
                        .iter()
                        .map(|arg| arg.get_type(schema))
                        .collect::<Result<Vec<DataType>, DataFusionError>>()?;

                    let (output_type, eval_type, command, python_version) = match function {
                        spec::FunctionDefinition::PythonUdf {
                            output_type,
                            eval_type,
                            command,
                            python_version,
                        } => (output_type, eval_type, command, python_version),
                        _ => {
                            return Err(PlanError::invalid(
                                "user-defined window function type must be Python UDF",
                            ));
                        }
                    };
                    let output_type = self.resolve_data_type(output_type)?;

                    let python_function: PySparkUdfObject = deserialize_partial_pyspark_udf(
                        &python_version,
                        &command,
                        eval_type,
                        arguments.len(),
                        &self.config.spark_udf_config,
                    )
                    .map_err(|e| {
                        PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
                    })?;

                    let function = match eval_type {
                        PySparkUdfType::GroupedAggPandas => {
                            let udaf = PySparkAggregateUDF::new(
                                function_name.to_owned(),
                                deterministic,
                                input_types,
                                output_type,
                                python_function,
                            );
                            let udaf = AggregateUDF::from(udaf);
                            expr::WindowFunctionDefinition::AggregateUDF(Arc::new(udaf))
                        }
                        _ => {
                            return Err(PlanError::invalid(
                                "invalid user-defined window function type",
                            ))
                        }
                    };
                    (function, function_name, argument_names, arguments, false)
                }
                _ => {
                    return Err(PlanError::invalid(format!(
                        "invalid window function expression: {:?}",
                        window_function
                    )));
                }
            };
        let partition_by = self
            .resolve_expressions(partition_spec, schema, state)
            .await?;
        // Spark treats literals as constants in ORDER BY window definition
        let order_by = self
            .resolve_sort_orders(order_spec, false, schema, state)
            .await?;
        let window_frame = if let Some(frame) = frame_spec {
            self.resolve_window_frame(frame, &order_by, schema)?
        } else {
            window_frame::WindowFrame::new(if order_by.is_empty() {
                None
            } else {
                // TODO: should we use strict ordering or not?
                Some(false)
            })
        };
        let window = expr::Expr::WindowFunction(expr::WindowFunction {
            fun: function,
            args: arguments,
            partition_by,
            order_by,
            window_frame,
            null_treatment: None,
        });
        let name = self.config.plan_formatter.function_to_string(
            function_name.as_str(),
            argument_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], window))
    }

    async fn resolve_expression_wildcard(
        &self,
        target: Option<spec::ObjectName>,
        wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let wildcard_options = self
            .resolve_wildcard_options(wildcard_options, schema, state)
            .await?;
        // FIXME: column reference is parsed as qualifier
        let expr = if let Some(target) = target {
            let target: Vec<String> = target.into();
            expr::Expr::Wildcard {
                qualifier: Some(target.join(".").into()),
                options: wildcard_options,
            }
        } else {
            expr::Expr::Wildcard {
                qualifier: None,
                options: wildcard_options,
            }
        };
        Ok(NamedExpr::new(vec!["*".to_string()], expr))
    }

    async fn resolve_wildcard_options(
        &self,
        wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::WildcardOptions> {
        use datafusion::sql::sqlparser::ast as df_ast;

        let ilike = wildcard_options
            .ilike_pattern
            .map(|x| df_ast::IlikeSelectItem { pattern: x });
        let exclude = wildcard_options
            .exclude_columns
            .map(|x| {
                let exclude = if x.len() > 1 {
                    df_ast::ExcludeSelectItem::Multiple(
                        x.into_iter().map(df_ast::Ident::new).collect(),
                    )
                } else if let Some(x) = x.into_iter().next() {
                    df_ast::ExcludeSelectItem::Single(df_ast::Ident::new(x))
                } else {
                    return Err(PlanError::invalid(
                        "exclude columns must have at least one column",
                    ));
                };
                Ok(exclude)
            })
            .transpose()?;
        let except = wildcard_options
            .except_columns
            .map(|x| {
                let except = if x.len() > 1 {
                    let mut deque = VecDeque::from(x);
                    let first_element = deque.pop_front().ok_or_else(|| {
                        PlanError::invalid("except columns must have at least one column")
                    })?;
                    let additional_elements = deque.into_iter().map(df_ast::Ident::new).collect();
                    df_ast::ExceptSelectItem {
                        first_element: df_ast::Ident::new(first_element),
                        additional_elements,
                    }
                } else if let Some(x) = x.into_iter().next() {
                    df_ast::ExceptSelectItem {
                        first_element: df_ast::Ident::new(x),
                        additional_elements: vec![],
                    }
                } else {
                    return Err(PlanError::invalid(
                        "except columns must have at least one column",
                    ));
                };
                Ok(except)
            })
            .transpose()?;
        let replace = match wildcard_options.replace_columns {
            Some(x) => {
                let mut items = Vec::with_capacity(x.len());
                let mut planned_expressions = Vec::with_capacity(x.len());
                for elem in x.into_iter() {
                    let expression = self
                        .resolve_expression(*elem.expression, schema, state)
                        .await?;
                    let item = df_ast::ReplaceSelectElement {
                        expr: expr_to_sql(&expression)?,
                        column_name: df_ast::Ident::new(elem.column_name),
                        as_keyword: elem.as_keyword,
                    };
                    items.push(item);
                    planned_expressions.push(expression);
                }
                Some(PlannedReplaceSelectItem {
                    items,
                    planned_expressions,
                })
            }
            None => None,
        };
        let rename = wildcard_options
            .rename_columns
            .map(|x| {
                let exclude = if x.len() > 1 {
                    df_ast::RenameSelectItem::Multiple(
                        x.into_iter()
                            .map(|x| df_ast::IdentWithAlias {
                                ident: df_ast::Ident::new(x.identifier),
                                alias: df_ast::Ident::new(x.alias),
                            })
                            .collect(),
                    )
                } else if let Some(x) = x.into_iter().next() {
                    df_ast::RenameSelectItem::Single(df_ast::IdentWithAlias {
                        ident: df_ast::Ident::new(x.identifier),
                        alias: df_ast::Ident::new(x.alias),
                    })
                } else {
                    return Err(PlanError::invalid(
                        "exclude columns must have at least one column",
                    ));
                };
                Ok(exclude)
            })
            .transpose()?;
        Ok(expr::WildcardOptions {
            ilike,
            exclude,
            except,
            replace,
            rename,
        })
    }

    async fn resolve_expression_extract_value(
        &self,
        child: spec::Expr,
        extraction: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let extraction = match extraction {
            spec::Expr::Literal(literal) => literal,
            _ => {
                return Err(PlanError::invalid("extraction must be a literal"));
            }
        };
        let extraction_name = self.config.plan_formatter.literal_to_string(&extraction)?;
        let extraction = self.resolve_literal(extraction)?;
        let NamedExpr { name, expr, .. } =
            self.resolve_named_expression(child, schema, state).await?;
        let name = if let expr::Expr::Column(column) = &expr {
            let data_type = schema
                .field_with_unqualified_name(&column.name)?
                .data_type();
            match data_type {
                DataType::Struct(_) => {
                    format!("{}.{}", name.one()?, extraction_name)
                }
                _ => {
                    format!("{}[{}]", name.one()?, extraction_name)
                }
            }
        } else {
            format!("{}[{}]", name.one()?, extraction_name)
        };
        Ok(NamedExpr::new(vec![name], expr.field(extraction)))
    }

    async fn resolve_expression_common_inline_udf(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // TODO: Function arg for if pyspark_udf or not.
        use sail_python_udf::cereal::pyspark_udf::{
            deserialize_partial_pyspark_udf, PySparkUdfObject,
        };
        use sail_python_udf::udf::pyspark_udf::PySparkUDF;

        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = function;

        let function_name: &str = function_name.as_str();
        let (argument_names, arguments) = self
            .resolve_alias_expressions_and_names(arguments, schema, state)
            .await?;
        let input_types: Vec<DataType> = arguments
            .iter()
            .map(|arg| arg.get_type(schema))
            .collect::<Result<Vec<DataType>, DataFusionError>>()?;

        let (output_type, eval_type, command, python_version) = match function {
            spec::FunctionDefinition::PythonUdf {
                output_type,
                eval_type,
                command,
                python_version,
            } => (output_type, eval_type, command, python_version),
            _ => {
                return Err(PlanError::invalid("UDF function type must be Python UDF"));
            }
        };
        let output_type = self.resolve_data_type(output_type)?;

        let python_function: PySparkUdfObject = deserialize_partial_pyspark_udf(
            &python_version,
            &command,
            eval_type,
            arguments.len(),
            &self.config.spark_udf_config,
        )
        .map_err(|e| PlanError::invalid(format!("Python UDF deserialization error: {:?}", e)))?;

        let func = match eval_type {
            PySparkUdfType::None
            | PySparkUdfType::Batched
            | PySparkUdfType::ArrowBatched
            | PySparkUdfType::ScalarPandas
            | PySparkUdfType::GroupedMapPandas
            | PySparkUdfType::WindowAggPandas
            | PySparkUdfType::ScalarPandasIter
            | PySparkUdfType::MapPandasIter
            | PySparkUdfType::CogroupedMapPandas
            | PySparkUdfType::MapArrowIter
            | PySparkUdfType::GroupedMapPandasWithState
            | PySparkUdfType::Table
            | PySparkUdfType::ArrowTable => {
                let udf = PySparkUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    eval_type,
                    python_function,
                    output_type,
                );
                expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(udf)),
                    args: arguments,
                })
            }
            PySparkUdfType::GroupedAggPandas => {
                let udaf = PySparkAggregateUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    output_type,
                    python_function,
                );
                expr::Expr::AggregateFunction(expr::AggregateFunction {
                    func: Arc::new(AggregateUDF::from(udaf)),
                    args: arguments,
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                })
            }
        };
        let name = self.config.plan_formatter.function_to_string(
            function_name,
            argument_names.iter().map(|x| x.as_str()).collect(),
            false,
        )?;
        Ok(NamedExpr::new(vec![name], func))
    }

    async fn resolve_expression_update_fields(
        &self,
        struct_expression: spec::Expr,
        field_name: spec::ObjectName,
        value_expression: Option<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let field_name: Vec<String> = field_name.into();
        let NamedExpr { name, expr, .. } = self
            .resolve_named_expression(struct_expression, schema, state)
            .await?;
        let name = if name.len() == 1 {
            name.one()?
        } else {
            let names = format!("({})", name.join(", "));
            return Err(PlanError::invalid(format!(
                "one name expected for expression, got: {names}"
            )));
        };

        let new_expr = if let Some(value_expression) = value_expression {
            let value_expr = self
                .resolve_expression(value_expression, schema, state)
                .await?;
            expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: Arc::new(ScalarUDF::from(UpdateStructField::new(field_name))),
                args: vec![expr, value_expr],
            })
        } else {
            expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: Arc::new(ScalarUDF::from(DropStructField::new(field_name))),
                args: vec![expr],
            })
        };
        Ok(NamedExpr::new(vec![name], new_expr))
    }

    async fn resolve_expression_named_lambda_variable(
        &self,
        _variable: spec::UnresolvedNamedLambdaVariable,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("named lambda variable"))
    }

    async fn resolve_expression_call_function(
        &self,
        _function_name: String,
        _arguments: Vec<spec::Expr>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("call function"))
    }

    async fn resolve_expression_placeholder(&self, placeholder: String) -> PlanResult<NamedExpr> {
        let name = placeholder.clone();
        let expr = expr::Expr::Placeholder(expr::Placeholder::new(placeholder, None));
        Ok(NamedExpr::new(vec![name], expr))
    }

    async fn resolve_expression_rollup(
        &self,
        rollup: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (names, expr) = self
            .resolve_expressions_and_names(rollup, schema, state)
            .await?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(expr::GroupingSet::Rollup(expr)),
        ))
    }

    async fn resolve_expression_cube(
        &self,
        cube: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let (names, expr) = self
            .resolve_expressions_and_names(cube, schema, state)
            .await?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(expr::GroupingSet::Cube(expr)),
        ))
    }

    async fn resolve_expression_grouping_sets(
        &self,
        grouping_sets: Vec<Vec<spec::Expr>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut name_map: HashMap<expr::Expr, String> = HashMap::new();
        let mut expr_sets: Vec<Vec<expr::Expr>> = Vec::with_capacity(grouping_sets.len());
        for grouping_set in grouping_sets {
            let mut expr_set = vec![];
            let exprs = self
                .resolve_named_expressions(grouping_set, schema, state)
                .await?;
            for NamedExpr { name, expr, .. } in exprs {
                let name = name.one()?;
                expr_set.push(expr.clone());
                name_map.insert(expr, name);
            }
            expr_sets.push(expr_set)
        }
        let grouping_sets = expr::GroupingSet::GroupingSets(expr_sets);
        let names = grouping_sets
            .distinct_expr()
            .into_iter()
            .map(|e| {
                name_map.get(e).cloned().ok_or_else(|| {
                    PlanError::invalid(format!("grouping set expression not found: {:?}", e))
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(NamedExpr::new(
            names,
            expr::Expr::GroupingSet(grouping_sets),
        ))
    }

    async fn resolve_expression_in_subquery(
        &self,
        expr: spec::Expr,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let in_subquery = if !negated {
            expr_fn::in_subquery(expr, Arc::new(subquery))
        } else {
            expr_fn::not_in_subquery(expr, Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], in_subquery))
    }

    async fn resolve_expression_scalar_subquery(
        &self,
        subquery: spec::QueryPlan,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        Ok(NamedExpr::new(
            vec!["subquery".to_string()],
            expr_fn::scalar_subquery(Arc::new(subquery)),
        ))
    }

    async fn resolve_expression_exists(
        &self,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let exists = if !negated {
            expr_fn::exists(Arc::new(subquery))
        } else {
            expr_fn::not_exists(Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["exists".to_string()], exists))
    }

    // TODO: Construct better names for the expression (e.g. a IN (b, c)) for all functions below.

    async fn resolve_expression_in_list(
        &self,
        expr: spec::Expr,
        list: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = Box::new(self.resolve_expression(expr, schema, state).await?);
        let list = self.resolve_expressions(list, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["in_list".to_string()],
            expr::Expr::InList(expr::InList::new(expr, list, negated)),
        ))
    }

    async fn resolve_expression_is_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_false".to_string()],
            expr::Expr::IsFalse(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_not_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_false".to_string()],
            expr::Expr::IsNotFalse(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_true".to_string()],
            expr::Expr::IsTrue(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_not_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_true".to_string()],
            expr::Expr::IsNotTrue(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_null".to_string()],
            expr::Expr::IsNull(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_not_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_null".to_string()],
            expr::Expr::IsNotNull(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_unknown".to_string()],
            expr::Expr::IsUnknown(Box::new(expr)),
        ))
    }

    async fn resolve_expression_is_not_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_unknown".to_string()],
            expr::Expr::IsNotUnknown(Box::new(expr)),
        ))
    }

    async fn resolve_expression_between(
        &self,
        expr: spec::Expr,
        negated: bool,
        low: spec::Expr,
        high: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let low = self.resolve_expression(low, schema, state).await?;
        let high = self.resolve_expression(high, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["between".to_string()],
            expr::Expr::Between(expr::Between::new(
                Box::new(expr),
                negated,
                Box::new(low),
                Box::new(high),
            )),
        ))
    }

    async fn resolve_expression_is_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_distinct_from".to_string()],
            expr::Expr::BinaryExpr(expr::BinaryExpr {
                left: Box::new(left),
                op: Operator::IsDistinctFrom,
                right: Box::new(right),
            }),
        ))
    }

    async fn resolve_expression_is_not_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_distinct_from".to_string()],
            expr::Expr::BinaryExpr(expr::BinaryExpr {
                left: Box::new(left),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(right),
            }),
        ))
    }

    async fn resolve_expression_similar_to(
        &self,
        expr: spec::Expr,
        pattern: spec::Expr,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let pattern = self.resolve_expression(pattern, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["similar_to".to_string()],
            expr::Expr::SimilarTo(expr::Like::new(
                negated,
                Box::new(expr),
                Box::new(pattern),
                escape_char,
                case_insensitive,
            )),
        ))
    }
}

enum WindowBoundaryKind {
    Lower,
    Upper,
}

enum WindowBoundaryOffset {
    PositiveInfinite,
    NegativeInfinite,
    PositiveFinite(u64),
    NegativeFinite(u64),
}

impl From<WindowBoundaryOffset> for window_frame::WindowFrameBound {
    fn from(offset: WindowBoundaryOffset) -> Self {
        match offset {
            WindowBoundaryOffset::PositiveInfinite => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
            }
            WindowBoundaryOffset::NegativeInfinite => {
                window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(None))
            }
            WindowBoundaryOffset::PositiveFinite(value) => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(Some(value)))
            }
            WindowBoundaryOffset::NegativeFinite(value) => {
                window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(Some(value)))
            }
        }
    }
}

impl From<u32> for WindowBoundaryOffset {
    fn from(value: u32) -> Self {
        Self::PositiveFinite(value as u64)
    }
}

impl From<i32> for WindowBoundaryOffset {
    fn from(value: i32) -> Self {
        if value < 0 {
            // We cast the value to `i64` before negation to avoid overflow.
            Self::NegativeFinite((-(value as i64)) as u64)
        } else {
            Self::PositiveFinite(value as u64)
        }
    }
}

impl From<u64> for WindowBoundaryOffset {
    fn from(value: u64) -> Self {
        Self::PositiveFinite(value)
    }
}

impl From<i64> for WindowBoundaryOffset {
    fn from(value: i64) -> Self {
        if value == i64::MIN {
            Self::NegativeInfinite
        } else if value < 0 {
            Self::NegativeFinite(-value as u64)
        } else {
            Self::PositiveFinite(value as u64)
        }
    }
}

struct WindowBoundaryFloatOffset<T>(T);

impl<T: Float + Display> TryFrom<WindowBoundaryFloatOffset<T>> for WindowBoundaryOffset {
    type Error = PlanError;

    fn try_from(value: WindowBoundaryFloatOffset<T>) -> PlanResult<Self> {
        let value = value.0;
        if value.is_infinite() {
            if value.is_sign_positive() {
                Ok(Self::PositiveInfinite)
            } else {
                Ok(Self::NegativeInfinite)
            }
        } else if value.is_sign_positive() {
            let v = num_traits::cast(value).ok_or_else(|| {
                PlanError::invalid(format!("invalid window boundary offset: {value}"))
            })?;
            Ok(Self::PositiveFinite(v))
        } else {
            let v = num_traits::cast(-value).ok_or_else(|| {
                PlanError::invalid(format!("invalid window boundary offset: {value}"))
            })?;
            Ok(Self::NegativeFinite(v))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::expr::{Alias, Expr};
    use datafusion_expr::{BinaryExpr, Operator};
    use sail_common::spec;

    use crate::config::PlanConfig;
    use crate::error::PlanResult;
    use crate::resolver::expression::NamedExpr;
    use crate::resolver::state::PlanResolverState;
    use crate::resolver::PlanResolver;

    #[tokio::test]
    async fn test_resolve_expression_with_name() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));

        async fn resolve(resolver: &PlanResolver<'_>, expr: spec::Expr) -> PlanResult<NamedExpr> {
            resolver
                .resolve_named_expression(
                    expr,
                    &Arc::new(DFSchema::empty()),
                    &mut PlanResolverState::new(),
                )
                .await
        }

        assert_eq!(
            resolve(
                &resolver,
                spec::Expr::UnresolvedFunction {
                    function_name: "not".to_string(),
                    arguments: vec![spec::Expr::Literal(spec::Literal::Boolean(true))],
                    is_distinct: false,
                    is_user_defined_function: false,
                }
            )
            .await?,
            NamedExpr {
                name: vec!["(NOT true)".to_string()],
                expr: Expr::Not(Box::new(Expr::Literal(ScalarValue::Boolean(Some(true))))),
                metadata: Default::default(),
            }
        );

        assert_eq!(
            resolve(
                &resolver,
                spec::Expr::Alias {
                    // This name "b" is overridden by the outer name "c".
                    expr: Box::new(spec::Expr::Alias {
                        // The resolver assigns a name (a human-readable string) for the function,
                        // and is then overridden by the explicitly specified outer name.
                        expr: Box::new(spec::Expr::UnresolvedFunction {
                            function_name: "+".to_string(),
                            arguments: vec![
                                spec::Expr::Alias {
                                    // The resolver assigns a name "1" for the literal,
                                    // and is then overridden by the explicitly specified name.
                                    expr: Box::new(spec::Expr::Literal(spec::Literal::Integer(1))),
                                    name: vec!["a".to_string().into()],
                                    metadata: None,
                                },
                                // The resolver assigns a name "2" for the literal.
                                spec::Expr::Literal(spec::Literal::Integer(2)),
                            ],
                            is_distinct: false,
                            is_user_defined_function: false,
                        }),
                        name: vec!["b".to_string().into()],
                        metadata: None,
                    }),
                    name: vec!["c".to_string().into()],
                    metadata: None,
                }
            )
            .await?,
            NamedExpr {
                name: vec!["c".to_string()],
                expr: Expr::Alias(Alias {
                    expr: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(Expr::Alias(Alias {
                                expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
                                name: "a".to_string(),
                                relation: None,
                            })),
                            op: Operator::Plus,
                            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(2)))),
                        })),
                        relation: None,
                        name: "b".to_string(),
                    })),
                    relation: None,
                    name: "c".to_string(),
                }),
                metadata: Default::default(),
            },
        );

        Ok(())
    }
}
