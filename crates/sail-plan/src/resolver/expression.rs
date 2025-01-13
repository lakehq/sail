use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display};
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions::core::get_field;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::{Column, DFSchemaRef, DataFusionError, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    col, expr, expr_fn, lit, window_frame, AggregateUDF, BinaryExpr, ExprSchemable, Operator,
    ScalarUDF,
};
use datafusion_functions_nested::expr_fn::array_element;
use num_traits::Float;
use sail_common::spec;
use sail_common::spec::PySparkUdfType;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::drop_struct_field::DropStructField;
use crate::extension::function::multi_expr::MultiExpr;
use crate::extension::function::table_input::TableInput;
use crate::extension::function::update_struct_field::UpdateStructField;
use crate::function::common::{AggFunctionContext, FunctionContext};
use crate::function::{
    get_built_in_aggregate_function, get_built_in_function, get_built_in_window_function,
};
use crate::resolver::function::PythonUdf;
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
                let info = state.get_field_info(column.name())?;
                Ok(Self::new(
                    vec![info.name().to_string()],
                    expr::Expr::Column(column),
                ))
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
                    spec::Literal::Int32 { value: Some(value) } => *value as usize,
                    spec::Literal::Int64 { value: Some(value) } => *value as usize,
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
        state: &mut PlanResolverState,
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
                self.resolve_window_boundary_offset(lower, WindowBoundaryKind::Lower, state)?,
                self.resolve_window_boundary_offset(upper, WindowBoundaryKind::Upper, state)?,
            ),
            WindowFrameUnits::Range => (
                self.resolve_window_boundary_value(
                    lower,
                    WindowBoundaryKind::Lower,
                    order_by,
                    schema,
                    state,
                )?,
                self.resolve_window_boundary_value(
                    upper,
                    WindowBoundaryKind::Upper,
                    order_by,
                    schema,
                    state,
                )?,
            ),
        };
        Ok(window_frame::WindowFrame::new_bounds(units, start, end))
    }

    fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        kind: WindowBoundaryKind,
        state: &mut PlanResolverState,
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
                let value = self.resolve_literal(value, state)?;
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
        state: &mut PlanResolverState,
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
                let value = self.resolve_literal(value, state)?;
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
            Expr::Literal(literal) => self.resolve_expression_literal(literal, state),
            Expr::UnresolvedAttribute { name, plan_id } => {
                self.resolve_expression_attribute(name, plan_id, schema, state)
            }
            Expr::UnresolvedFunction {
                function_name,
                arguments,
                ..
            } if function_name.to_lowercase() == "table" => {
                self.resolve_expression_table_function(arguments, state)
                    .await
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

    fn resolve_expression_literal(
        &self,
        literal: spec::Literal,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name = self.config.plan_formatter.literal_to_string(
            &literal,
            self.config.system_timezone.as_str(),
            &self.config.timestamp_type,
        )?;
        let literal = self.resolve_literal(literal, state)?;
        Ok(NamedExpr::new(vec![name], expr::Expr::Literal(literal)))
    }

    fn resolve_expression_attribute(
        &self,
        name: spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if let Some((name, expr)) =
            self.resolve_field_or_nested_field(&name, plan_id, schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        let Some(outer_schema) = state.get_outer_query_schema().cloned() else {
            return Err(PlanError::AnalysisError(format!(
                "cannot resolve attribute: {name:?}"
            )));
        };
        match self.resolve_outer_field(&name, &outer_schema, state)? {
            Some((name, expr)) => Ok(NamedExpr::new(vec![name], expr)),
            None => Err(PlanError::AnalysisError(format!(
                "cannot resolve attribute or outer attribute: {name:?}"
            ))),
        }
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let name: Vec<&str> = name.into();
        let candidates = Self::generate_qualified_nested_field_candidates(&name);
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                candidates
                    .iter()
                    .filter_map(|(q, name, inner)| {
                        if qualifier_matches(q.as_ref(), qualifier) && info.matches(name, plan_id) {
                            let expr = Self::resolve_nested_field(
                                col((qualifier, field)),
                                field.data_type(),
                                inner,
                            )?;
                            let name = inner.last().unwrap_or(name).to_string();
                            Some((name, expr))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous outer attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_outer_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let name: Vec<&str> = name.into();
        let candidates = Self::generate_qualified_field_candidates(&name);
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                candidates
                    .iter()
                    .filter(|(q, name)| {
                        qualifier_matches(q.as_ref(), qualifier) && info.matches(name, None)
                    })
                    .map(|(_, name)| {
                        (
                            name.to_string(),
                            expr::Expr::OuterReferenceColumn(
                                field.data_type().clone(),
                                Column::new(qualifier.cloned(), field.name()),
                            ),
                        )
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous outer attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_nested_field(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[&str],
    ) -> Option<expr::Expr> {
        match inner {
            [] => Some(expr),
            [name, remaining @ ..] => match data_type {
                DataType::Struct(fields) => fields
                    .iter()
                    .find(|x| x.name().eq_ignore_ascii_case(name))
                    .and_then(|field| {
                        let args = vec![expr, lit(field.name().to_string())];
                        let expr =
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                        Self::resolve_nested_field(expr, field.data_type(), remaining)
                    }),
                _ => None,
            },
        }
    }

    async fn resolve_expression_table_function(
        &self,
        arguments: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let query = match arguments.one() {
            Ok(spec::Expr::ScalarSubquery { subquery }) => *subquery,
            Ok(spec::Expr::UnresolvedAttribute {
                name,
                plan_id: None,
            }) => spec::QueryPlan::new(spec::QueryNode::Read {
                read_type: spec::ReadType::NamedTable(spec::ReadNamedTable {
                    name,
                    options: vec![],
                }),
                is_streaming: false,
            }),
            _ => {
                return Err(PlanError::invalid(
                    "expected a query or a table reference for table input",
                ));
            }
        };
        let plan = self.resolve_query_plan(query, state).await?;
        Ok(NamedExpr::new(
            vec!["table".to_string()],
            ScalarUDF::from(TableInput::new(Arc::new(plan))).call(vec![]),
        ))
    }

    async fn resolve_expression_function(
        &self,
        function_name: String,
        arguments: Vec<spec::Expr>,
        is_distinct: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        let canonical_function_name = function_name.to_ascii_lowercase();
        if let Ok(udf) = self.ctx.udf(&canonical_function_name) {
            if udf.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }

        let (argument_names, arguments) = self
            .resolve_expressions_and_names(arguments, schema, state)
            .await?;

        // FIXME: `is_user_defined_function` is always false,
        //   so we need to check UDFs before built-in functions.
        let func = if let Ok(udf) = self.ctx.udf(&canonical_function_name) {
            if let Some(f) = udf.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>() {
                let function = PythonUdf {
                    python_version: f.python_version().to_string(),
                    eval_type: f.eval_type(),
                    command: f.command().to_vec(),
                    output_type: f.output_type().clone(),
                };
                self.resolve_python_udf_expr(
                    function,
                    &function_name,
                    arguments,
                    &argument_names,
                    schema,
                    f.deterministic(),
                    state,
                )?
            } else {
                expr::Expr::ScalarFunction(ScalarFunction {
                    func: udf,
                    args: arguments,
                })
            }
        } else if let Ok(func) = get_built_in_function(&canonical_function_name) {
            let function_context =
                FunctionContext::new(self.config.clone(), self.ctx, &argument_names);
            func(arguments.clone(), &function_context)?
        } else if let Ok(func) = get_built_in_aggregate_function(&canonical_function_name) {
            let agg_function_context = AggFunctionContext::new(is_distinct);
            func(arguments.clone(), agg_function_context)?
        } else {
            return Err(PlanError::unsupported(format!(
                "unknown function: {function_name}",
            )));
        };

        let name = self.config.plan_formatter.function_to_string(
            &function_name,
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
        let data_type = self.resolve_data_type(&cast_to_type, state)?;
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
                    let canonical_function_name = function_name.to_ascii_lowercase();
                    let (argument_names, arguments) = self
                        .resolve_expressions_and_names(arguments, schema, state)
                        .await?;
                    let function = get_built_in_window_function(&canonical_function_name)?;
                    (
                        function,
                        function_name,
                        argument_names,
                        arguments,
                        is_distinct,
                    )
                }
                spec::Expr::CommonInlineUserDefinedFunction(function) => {
                    let mut scope = state.enter_config_scope();
                    let state = scope.state();
                    state.config_mut().arrow_allow_large_var_types = true;
                    let spec::CommonInlineUserDefinedFunction {
                        function_name,
                        deterministic,
                        arguments,
                        function,
                    } = function;
                    let (argument_names, arguments) = self
                        .resolve_expressions_and_names(arguments, schema, state)
                        .await?;
                    let input_types: Vec<DataType> = arguments
                        .iter()
                        .map(|arg| arg.get_type(schema))
                        .collect::<Result<Vec<DataType>, DataFusionError>>()?;
                    let function = self.resolve_python_udf(function, state)?;
                    let payload = PySparkUdfPayload::build(
                        &function.python_version,
                        &function.command,
                        function.eval_type,
                        &((0..arguments.len()).collect::<Vec<_>>()),
                        &self.config.pyspark_udf_config,
                    )?;
                    let function = match function.eval_type {
                        PySparkUdfType::GroupedAggPandas => {
                            let udaf = PySparkGroupAggregateUDF::new(
                                get_udf_name(&function_name, &payload),
                                payload,
                                deterministic,
                                argument_names.clone(),
                                input_types,
                                function.output_type,
                                self.config.pyspark_udf_config.clone(),
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
            self.resolve_window_frame(frame, &order_by, schema, state)?
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
        match target {
            Some(target) if wildcard_options == Default::default() => {
                self.resolve_wildcard_or_nested_field_wildcard(&target, schema, state)
            }
            _ => {
                let qualifier = target
                    .map(|x| self.resolve_table_reference(&x))
                    .transpose()?;
                let options = self
                    .resolve_wildcard_options(wildcard_options, schema, state)
                    .await?;
                Ok(NamedExpr::new(
                    vec!["*".to_string()],
                    expr::Expr::Wildcard {
                        qualifier,
                        options: Box::new(options),
                    },
                ))
            }
        }
    }

    fn resolve_wildcard_or_nested_field_wildcard(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name: Vec<&str> = name.into();
        let candidates = Self::generate_qualified_wildcard_candidates(&name)
            .into_iter()
            .flat_map(|(q, name)| match name {
                [] => {
                    if schema
                        .iter()
                        .any(|(qualifier, _)| qualifier_matches(q.as_ref(), qualifier))
                    {
                        vec![NamedExpr::new(
                            vec!["*".to_string()],
                            expr::Expr::Wildcard {
                                qualifier: q,
                                options: Default::default(),
                            },
                        )]
                    } else {
                        vec![]
                    }
                }
                [column, inner @ ..] => schema
                    .iter()
                    .filter_map(|(qualifier, field)| {
                        let Ok(info) = state.get_field_info(field.name()) else {
                            return None;
                        };
                        if qualifier_matches(q.as_ref(), qualifier) && info.matches(column, None) {
                            Self::resolve_nested_field_wildcard(
                                col((q.as_ref(), field)),
                                field.data_type(),
                                inner,
                            )
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous wildcard: {name:?}"
            )));
        }
        candidates
            .one()
            .map_err(|_| PlanError::AnalysisError(format!("cannot resolve wildcard: {name:?}")))
    }

    fn resolve_nested_field_wildcard(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[&str],
    ) -> Option<NamedExpr> {
        let DataType::Struct(fields) = data_type else {
            return None;
        };
        match inner {
            [] => {
                let (names, exprs) = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_string();
                        let args = vec![expr.clone(), lit(name.clone())];
                        (
                            name,
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args)),
                        )
                    })
                    .unzip();
                Some(NamedExpr::new(
                    names,
                    ScalarUDF::from(MultiExpr::new()).call(exprs),
                ))
            }
            [name, remaining @ ..] => fields
                .iter()
                .find(|x| x.name().eq_ignore_ascii_case(name))
                .and_then(|field| {
                    let args = vec![expr, lit(field.name().to_string())];
                    let expr =
                        expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                    Self::resolve_nested_field_wildcard(expr, field.data_type(), remaining)
                }),
        }
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
                Some(expr::PlannedReplaceSelectItem {
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
        let spec::Expr::Literal(extraction) = extraction else {
            return Err(PlanError::invalid("extraction must be a literal"));
        };
        let extraction_name = self.config.plan_formatter.literal_to_string(
            &extraction,
            self.config.system_timezone.as_str(),
            &self.config.timestamp_type,
        )?;
        let extraction = self.resolve_literal(extraction, state)?;
        let NamedExpr { name, expr, .. } =
            self.resolve_named_expression(child, schema, state).await?;
        let data_type = expr.get_type(schema)?;
        let name = match data_type {
            DataType::Struct(_) => {
                format!("{}.{}", name.one()?, extraction_name)
            }
            _ => {
                format!("{}[{}]", name.one()?, extraction_name)
            }
        };
        let expr = match data_type {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => array_element(
                expr,
                expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr::Expr::Literal(extraction)),
                    Operator::Plus,
                    Box::new(lit(1i64)),
                )),
            ),
            DataType::Struct(fields) => {
                let ScalarValue::Utf8(Some(name)) = extraction else {
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for struct: {extraction}"
                    )));
                };
                let Ok(name) = fields
                    .iter()
                    .filter(|x| x.name().eq_ignore_ascii_case(&name))
                    .map(|x| x.name().to_string())
                    .collect::<Vec<_>>()
                    .one()
                else {
                    return Err(PlanError::AnalysisError(format!(
                        "missing or ambiguous field: {name}"
                    )));
                };
                expr.field(name)
            }
            // TODO: support non-string map keys
            DataType::Map(_, _) => expr.field(extraction),
            _ => {
                return Err(PlanError::AnalysisError(format!(
                    "cannot extract value from data type: {data_type}"
                )))
            }
        };
        Ok(NamedExpr::new(vec![name], expr))
    }

    async fn resolve_expression_common_inline_udf(
        &self,
        function: spec::CommonInlineUserDefinedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = function;
        let (argument_names, arguments) = self
            .resolve_expressions_and_names(arguments, schema, state)
            .await?;
        let function = self.resolve_python_udf(function, state)?;
        let func = self.resolve_python_udf_expr(
            function,
            &function_name,
            arguments,
            &argument_names,
            schema,
            deterministic,
            state,
        )?;
        let name = self.config.plan_formatter.function_to_string(
            &function_name,
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
            ScalarUDF::from(UpdateStructField::new(field_name)).call(vec![expr, value_expr])
        } else {
            ScalarUDF::from(DropStructField::new(field_name)).call(vec![expr])
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
        function_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let function_name: Vec<String> = function_name.into();
        let Ok(function_name) = function_name.one() else {
            return Err(PlanError::todo("qualified function name"));
        };
        self.resolve_expression_function(function_name, arguments, false, schema, state)
            .await
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

        // DataFusion's BETWEEN operator has a bug, so we construct the expression manually.
        let greater_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr.clone()),
            Operator::GtEq,
            Box::new(low),
        ));
        let less_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr),
            Operator::LtEq,
            Box::new(high),
        ));
        let between_expr = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(greater_eq),
            Operator::And,
            Box::new(less_eq),
        ));
        let between_expr = if negated {
            expr::Expr::Not(Box::new(between_expr))
        } else {
            between_expr
        };
        Ok(NamedExpr::new(vec!["between".to_string()], between_expr))
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
            expr::Expr::BinaryExpr(BinaryExpr {
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
            expr::Expr::BinaryExpr(BinaryExpr {
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

    fn generate_qualified_field_candidates<'a>(
        name: &'a [&'a str],
    ) -> Vec<(Option<TableReference>, &'a str)> {
        match name {
            [n1] => vec![(None, *n1)],
            [n1, n2] => vec![(Some(TableReference::bare(*n1)), *n2)],
            [n1, n2, n3] => vec![(Some(TableReference::partial(*n1, *n2)), *n3)],
            [n1, n2, n3, n4] => vec![(Some(TableReference::full(*n1, *n2, *n3)), *n4)],
            _ => vec![],
        }
    }

    fn generate_qualified_nested_field_candidates<'a>(
        name: &'a [&'a str],
    ) -> Vec<(Option<TableReference>, &'a str, &'a [&'a str])> {
        let mut out = vec![];
        if let [n1, x @ ..] = name {
            out.push((None, *n1, x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::bare(*n1)), *n2, x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((Some(TableReference::partial(*n1, *n2)), *n3, x));
        }
        if let [n1, n2, n3, n4, x @ ..] = name {
            out.push((Some(TableReference::full(*n1, *n2, *n3)), *n4, x));
        }
        out
    }

    fn generate_qualified_wildcard_candidates<'a>(
        name: &'a [&'a str],
    ) -> Vec<(Option<TableReference>, &'a [&'a str])> {
        let mut out = vec![(None, name)];
        if let [n1, x @ ..] = name {
            out.push((Some(TableReference::bare(*n1)), x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::partial(*n1, *n2)), x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((Some(TableReference::full(*n1, *n2, *n3)), x));
        }
        out
    }
}

/// Returns whether the qualifier matches the target qualifier.
/// Identifiers are case-insensitive.
/// Note that the match is not symmetric, so please ensure the arguments are in the correct order.
fn qualifier_matches(qualifier: Option<&TableReference>, target: Option<&TableReference>) -> bool {
    let table_matches = |table: &str| {
        target
            .map(|x| x.table())
            .is_some_and(|x| x.eq_ignore_ascii_case(table))
    };
    let schema_matches = |schema: &str| {
        target
            .and_then(|x| x.schema())
            .is_some_and(|x| x.eq_ignore_ascii_case(schema))
    };
    let catalog_matches = |catalog: &str| {
        target
            .and_then(|x| x.catalog())
            .is_some_and(|x| x.eq_ignore_ascii_case(catalog))
    };
    match qualifier {
        Some(TableReference::Bare { table }) => table_matches(table),
        Some(TableReference::Partial { schema, table }) => {
            schema_matches(schema) && table_matches(table)
        }
        Some(TableReference::Full {
            catalog,
            schema,
            table,
        }) => catalog_matches(catalog) && schema_matches(schema) && table_matches(table),
        None => true,
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
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));

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
                    arguments: vec![spec::Expr::Literal(spec::Literal::Boolean {
                        value: Some(true)
                    })],
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
                                    expr: Box::new(spec::Expr::Literal(spec::Literal::Int32 {
                                        value: Some(1)
                                    })),
                                    name: vec!["a".to_string().into()],
                                    metadata: None,
                                },
                                // The resolver assigns a name "2" for the literal.
                                spec::Expr::Literal(spec::Literal::Int32 { value: Some(2) }),
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
