use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion_common::{plan_datafusion_err, plan_err, Column, DataFusionError};
use datafusion_expr::{expr, window_frame, ExprSchemable, ScalarUDF};
use framework_common::spec;
use framework_python::cereal::partial_pyspark_udf::{
    deserialize_partial_pyspark_udf, PartialPySparkUDF,
};
use framework_python::udf::pyspark_udf::PySparkUDF;
use framework_python::udf::unresolved_pyspark_udf::UnresolvedPySparkUDF;

use crate::error::{PlanError, PlanResult};
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
    pub metadata: HashMap<String, String>,
}

impl NamedExpr {
    pub fn new(name: Vec<String>, expr: expr::Expr) -> Self {
        Self {
            name,
            expr,
            metadata: HashMap::new(),
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

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn into_alias_expr(self) -> PlanResult<expr::Expr> {
        match &self.expr {
            // We should not add alias to expressions that will be rewritten in logical plans.
            // Otherwise, some logical plan optimizers may not work correctly.
            // TODO: This seems hacky. Is there a better way to handle this?
            expr::Expr::Wildcard { .. }
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
        Ok(expr::Expr::Alias(expr::Alias {
            expr: Box::new(self.expr),
            relation,
            name,
        }))
    }
}

impl PlanResolver<'_> {
    pub(super) fn resolve_sort_order(
        &self,
        sort: spec::SortOrder,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
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
        Ok(expr::Expr::Sort(expr::Sort {
            expr: Box::new(self.resolve_expression(*child, schema, state)?),
            asc,
            nulls_first,
        }))
    }

    pub(super) fn resolve_sort_orders(
        &self,
        sort: Vec<spec::SortOrder>,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Expr>> {
        sort.into_iter()
            .map(|x| self.resolve_sort_order(x, schema, state))
            .collect::<PlanResult<Vec<_>>>()
    }

    pub(super) fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        state: &mut PlanResolverState,
    ) -> PlanResult<window_frame::WindowFrame> {
        use spec::{WindowFrameBoundary, WindowFrameType};

        let spec::WindowFrame {
            frame_type,
            lower,
            upper,
        } = frame;

        let units = match frame_type {
            WindowFrameType::Undefined => return Err(PlanError::invalid("undefined frame type")),
            WindowFrameType::Row => window_frame::WindowFrameUnits::Rows,
            WindowFrameType::Range => window_frame::WindowFrameUnits::Range,
        };
        let start = match lower {
            WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
            WindowFrameBoundary::Unbounded => {
                window_frame::WindowFrameBound::Preceding(ScalarValue::UInt64(None))
            }
            WindowFrameBoundary::Value(value) => window_frame::WindowFrameBound::Preceding(
                self.resolve_window_boundary_value(*value, state)?,
            ),
        };
        let end = match upper {
            WindowFrameBoundary::CurrentRow => window_frame::WindowFrameBound::CurrentRow,
            WindowFrameBoundary::Unbounded => {
                window_frame::WindowFrameBound::Following(ScalarValue::UInt64(None))
            }
            WindowFrameBoundary::Value(value) => window_frame::WindowFrameBound::Following(
                self.resolve_window_boundary_value(*value, state)?,
            ),
        };
        Ok(window_frame::WindowFrame::new_bounds(units, start, end))
    }

    pub(super) fn resolve_window_boundary_value(
        &self,
        value: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let value = self.resolve_expression(value, &DFSchema::empty(), state)?;
        match value {
            expr::Expr::Literal(
                v @ (ScalarValue::UInt32(_)
                | ScalarValue::Int32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int64(_)),
            ) => Ok(v),
            _ => Err(PlanError::invalid(format!(
                "invalid window boundary value: {:?}",
                value
            ))),
        }
    }

    pub(super) fn resolve_named_expression(
        &self,
        expr: spec::Expr,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        use spec::Expr;

        match expr {
            Expr::Literal(literal) => {
                let name = self.config.plan_formatter.literal_to_string(&literal)?;
                let literal = self.resolve_literal(literal)?;
                Ok(NamedExpr::new(vec![name], expr::Expr::Literal(literal)))
            }
            Expr::UnresolvedAttribute { name, plan_id } => {
                let name: Vec<String> = name.into();
                let last = name
                    .last()
                    .ok_or_else(|| PlanError::invalid("empty attribute name"))?
                    .clone();
                Ok(NamedExpr::new(
                    vec![last],
                    self.resolve_attribute(name, plan_id, schema, state)?,
                ))
            }
            Expr::UnresolvedFunction {
                function_name,
                arguments,
                is_distinct,
                is_user_defined_function: _, // FIXME: is_user_defined_function is always false.
            } => {
                let (argument_names, arguments) =
                    self.resolve_alias_expressions_and_names(arguments, schema, state)?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

                // FIXME: is_user_defined_function is always false
                //  So, we need to check udf's before built-in functions.
                let func = if let Ok(udf) = self.ctx.udf(function_name.as_str()) {
                    // TODO: UnresolvedPythonUDF will likely need to be accounted for as well
                    //  once we integrate LakeSail Python UDF.
                    let udf = if let Some(f) =
                        udf.inner().as_any().downcast_ref::<UnresolvedPySparkUDF>()
                    {
                        let deterministic = f.deterministic()?;
                        let function_definition = f.python_function_definition()?;
                        let (output_type, eval_type, command, python_version) =
                            match &function_definition {
                                spec::FunctionDefinition::PythonUdf {
                                    output_type,
                                    eval_type,
                                    command,
                                    python_version,
                                } => (output_type, eval_type, command, python_version),
                                _ => {
                                    return Err(PlanError::invalid(
                                        "UDF function type must be Python UDF",
                                    ));
                                }
                            };
                        let output_type: DataType = self.resolve_data_type(output_type.clone())?;

                        let python_function: PartialPySparkUDF = deserialize_partial_pyspark_udf(
                            python_version,
                            command,
                            eval_type,
                            &(arguments.len() as i32),
                            &self.config.spark_udf_config,
                        )
                        .map_err(|e| {
                            PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
                        })?;

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
                    func(arguments.clone())?
                } else if let Ok(func) = get_built_in_aggregate_function(
                    function_name.as_str(),
                    arguments.clone(),
                    is_distinct,
                ) {
                    func
                } else {
                    return Err(PlanError::unsupported(format!(
                        "unknown function: {function_name}",
                    )));
                };
                // TODO: udaf and udwf

                let name = self.config.plan_formatter.function_to_string(
                    function_name.as_str(),
                    argument_names.iter().map(|x| x.as_str()).collect(),
                )?;
                Ok(NamedExpr::new(vec![name], func))
            }
            Expr::UnresolvedStar { target } => {
                // FIXME: column reference is parsed as qualifier
                let expr = if let Some(target) = target {
                    let target: Vec<String> = target.into();
                    expr::Expr::Wildcard {
                        qualifier: Some(target.join(".").into()),
                    }
                } else {
                    expr::Expr::Wildcard { qualifier: None }
                };
                Ok(NamedExpr::new(vec!["*".to_string()], expr))
            }
            Expr::Alias {
                expr,
                name,
                metadata,
            } => {
                let expr = self.resolve_expression(*expr, schema, state)?;
                let name = name.into_iter().map(|x| x.into()).collect();
                if let Some(metadata) = metadata {
                    Ok(NamedExpr::new(name, expr).with_metadata(metadata))
                } else {
                    Ok(NamedExpr::new(name, expr))
                }
            }
            Expr::Cast { expr, cast_to_type } => {
                let data_type = self.resolve_data_type(cast_to_type)?;
                let NamedExpr { expr, name, .. } =
                    self.resolve_named_expression(*expr, schema, state)?;
                let expr = expr::Expr::Cast(expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                });
                Ok(NamedExpr::new(name, expr))
            }
            Expr::UnresolvedRegex { .. } => Err(PlanError::todo("unresolved regex")),
            Expr::SortOrder(sort) => {
                let sort = self.resolve_sort_order(sort, schema, state);
                Ok(NamedExpr::new(vec![], sort?))
            }
            Expr::LambdaFunction { .. } => Err(PlanError::todo("lambda function")),
            Expr::Window {
                window_function,
                partition_spec,
                order_spec,
                frame_spec,
            } => {
                let (function_name, argument_names, arguments) = match *window_function {
                    Expr::UnresolvedFunction {
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
                        let (argument_names, arguments) =
                            self.resolve_alias_expressions_and_names(arguments, schema, state)?;
                        (function_name, argument_names, arguments)
                    }
                    Expr::CommonInlineUserDefinedFunction(_) => {
                        return Err(PlanError::unsupported(
                            "inline user defined window function",
                        ));
                    }
                    _ => {
                        return Err(PlanError::invalid(format!(
                            "invalid window function expression: {:?}",
                            window_function
                        )));
                    }
                };
                let partition_by = self.resolve_expressions(partition_spec, schema, state)?;
                let order_by = self.resolve_sort_orders(order_spec, schema, state)?;
                let window_frame = if let Some(frame) = frame_spec {
                    self.resolve_window_frame(frame, state)?
                } else {
                    window_frame::WindowFrame::new(None)
                };
                let window = expr::Expr::WindowFunction(expr::WindowFunction {
                    fun: get_built_in_window_function(function_name.as_str())?,
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment: None,
                });
                let name = self.config.plan_formatter.function_to_string(
                    function_name.as_str(),
                    argument_names.iter().map(|x| x.as_str()).collect(),
                )?;
                Ok(NamedExpr::new(vec![name], window))
            }
            Expr::UnresolvedExtractValue { child, extraction } => {
                let extraction = match *extraction {
                    Expr::Literal(literal) => literal,
                    _ => {
                        return Err(PlanError::invalid("extraction must be a literal"));
                    }
                };
                let extraction_name = self.config.plan_formatter.literal_to_string(&extraction)?;
                let extraction = self.resolve_literal(extraction)?;
                let NamedExpr { name, expr, .. } =
                    self.resolve_named_expression(*child, schema, state)?;
                let name = format!("{}[{}]", name.one()?, extraction_name);
                Ok(NamedExpr::new(vec![name], expr.field(extraction)))
            }
            Expr::UpdateFields { .. } => Err(PlanError::todo("update fields")),
            Expr::UnresolvedNamedLambdaVariable(_) => {
                Err(PlanError::todo("unresolved named lambda variable"))
            }
            Expr::CommonInlineUserDefinedFunction(function) => {
                // TODO: Function arg for if pyspark_udf or not.
                use framework_python::cereal::partial_pyspark_udf::{
                    deserialize_partial_pyspark_udf, PartialPySparkUDF,
                };
                use framework_python::udf::pyspark_udf::PySparkUDF;

                let spec::CommonInlineUserDefinedFunction {
                    function_name,
                    deterministic,
                    arguments,
                    function,
                } = function;

                let function_name: &str = function_name.as_str();
                let (argument_names, arguments) =
                    self.resolve_alias_expressions_and_names(arguments, schema, state)?;
                let input_types: Vec<DataType> = arguments
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<DataType>, DataFusionError>>(
                )?;

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

                let python_function: PartialPySparkUDF = deserialize_partial_pyspark_udf(
                    &python_version,
                    &command,
                    &eval_type,
                    &(arguments.len() as i32),
                    &self.config.spark_udf_config,
                )
                .map_err(|e| {
                    PlanError::invalid(format!("Python UDF deserialization error: {:?}", e))
                })?;

                let python_udf: PySparkUDF = PySparkUDF::new(
                    function_name.to_owned(),
                    deterministic,
                    input_types,
                    eval_type,
                    python_function,
                    output_type,
                );
                let name = self.config.plan_formatter.function_to_string(
                    function_name,
                    argument_names.iter().map(|x| x.as_str()).collect(),
                )?;
                let func = expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func: Arc::new(ScalarUDF::from(python_udf)),
                    args: arguments,
                });
                Ok(NamedExpr::new(vec![name], func))
            }
            Expr::CallFunction { .. } => Err(PlanError::todo("call function")),
            Expr::Placeholder(placeholder) => {
                let name = placeholder.clone();
                let expr = expr::Expr::Placeholder(expr::Placeholder::new(placeholder, None));
                Ok(NamedExpr::new(vec![name], expr))
            }
        }
    }

    pub(super) fn resolve_named_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        expressions
            .into_iter()
            .map(|x| self.resolve_named_expression(x, schema, state))
            .collect::<PlanResult<Vec<_>>>()
    }

    pub(super) fn resolve_expression(
        &self,
        expressions: spec::Expr,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        let NamedExpr { expr, .. } = self.resolve_named_expression(expressions, schema, state)?;
        Ok(expr)
    }

    pub(super) fn resolve_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Expr>> {
        expressions
            .into_iter()
            .map(|x| self.resolve_expression(x, schema, state))
            .collect::<PlanResult<Vec<_>>>()
    }

    pub(super) fn resolve_alias_expressions_and_names(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        Ok(expressions
            .into_iter()
            .map(|x| {
                let expr = self.resolve_named_expression(x, schema, state)?;
                let name = expr.name.clone().one()?;
                let expr = expr.into_alias_expr()?;
                Ok((name, expr))
            })
            .collect::<PlanResult<Vec<(String, expr::Expr)>>>()?
            .into_iter()
            .unzip())
    }

    fn resolve_attribute(
        &self,
        name: Vec<String>,
        plan_id: Option<i64>,
        schema: &DFSchema,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        // TODO: handle qualifier and nested fields
        let first = name
            .first()
            .ok_or_else(|| plan_datafusion_err!("empty attribute: {:?}", name))?;
        let column = if let Some(plan_id) = plan_id {
            let field = state.get_resolved_field_name_in_plan(plan_id, first)?;
            schema.columns_with_unqualified_name(field)
        } else {
            schema
                .iter()
                .filter_map(|(qualifier, field)| {
                    if state.get_field_name(field.name()).is_ok_and(|f| f == first) {
                        Some(Column::new(qualifier.cloned(), field.name()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        if column.len() > 1 {
            return plan_err!("ambiguous attribute: {:?}", name)?;
        }
        let column = column
            .one()
            .map_err(|_| plan_datafusion_err!("cannot resolve attribute: {:?}", name))?;
        Ok(expr::Expr::Column(column))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::expr::{Alias, Expr};
    use datafusion_expr::{BinaryExpr, Operator};
    use framework_common::spec;

    use crate::config::PlanConfig;
    use crate::error::PlanResult;
    use crate::resolver::expression::NamedExpr;
    use crate::resolver::state::PlanResolverState;
    use crate::resolver::PlanResolver;

    #[test]
    fn test_resolve_expression_with_name() -> PlanResult<()> {
        let ctx = SessionContext::default();
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
        let resolve = |expr| {
            resolver.resolve_named_expression(
                expr,
                &DFSchema::empty(),
                &mut PlanResolverState::new(),
            )
        };

        assert_eq!(
            resolve(spec::Expr::UnresolvedFunction {
                function_name: "not".to_string(),
                arguments: vec![spec::Expr::Literal(spec::Literal::Boolean(true))],
                is_distinct: false,
                is_user_defined_function: false,
            })?,
            NamedExpr {
                name: vec!["(NOT true)".to_string()],
                expr: Expr::Not(Box::new(Expr::Alias(Alias {
                    expr: Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    name: "true".to_string(),
                    relation: None,
                }))),
                metadata: Default::default(),
            }
        );

        assert_eq!(
            resolve(spec::Expr::Alias {
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
            })?,
            NamedExpr {
                name: vec!["c".to_string()],
                expr: Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
                        name: "a".to_string(),
                        relation: None,
                    })),
                    op: Operator::Plus,
                    right: Box::new(Expr::Alias(Alias {
                        expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(2)))),
                        name: "2".to_string(),
                        relation: None,
                    })),
                }),
                metadata: Default::default(),
            },
        );

        Ok(())
    }
}
