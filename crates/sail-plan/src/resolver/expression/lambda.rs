use datafusion_common::DFSchemaRef;
use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_common::datatype::FieldExt;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::{Lambda, LambdaVariable};
use datafusion_expr::{ExprSchemable, ValueOrLambda, expr};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_python_udf::get_udf_display_name;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::{
    get_lambda_parameters, lambda_argument_positions, wrapped_lambda_param_count,
};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

/// Returns whether an expression contains a subquery anywhere in its tree
/// (scalar subquery, `IN (subquery)`, or `EXISTS (subquery)`).
fn expr_contains_subquery(expr: &expr::Expr) -> PlanResult<bool> {
    let mut found = false;
    expr.apply(|e| {
        Ok(match e {
            expr::Expr::ScalarSubquery(_) | expr::Expr::InSubquery(_) | expr::Expr::Exists(_) => {
                found = true;
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}

/// Returns the name of the first Python UDF found anywhere in an expression's
/// tree, if any. Spark rejects Python UDFs inside a higher-order function's
/// lambda because its evaluators cannot drive the Python worker per element.
fn expr_python_udf_name(expr: &expr::Expr) -> PlanResult<Option<String>> {
    let mut found = None;
    expr.apply(|e| {
        Ok(match e {
            // A resolved scalar Python UDF is a scalar function whose inner impl is
            // a `PySparkUDF` (see `resolve_python_udf_expr`). Unlike the shadowing
            // gate, this only rejects Python UDFs: a non-Python user function
            // resolves to a plain `ScalarFunction` indistinguishable from a
            // built-in here, so it cannot be detected at this point (Spark also
            // rejects SQL UDFs in a lambda, but that would need a check earlier,
            // before resolution).
            expr::Expr::ScalarFunction(function)
                if function.func.inner().downcast_ref::<PySparkUDF>().is_some() =>
            {
                found = Some(get_udf_display_name(function.func.name()).to_string());
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(found)
}

/// Collects the names of every lambda variable referenced anywhere in an
/// expression's tree.
fn referenced_lambda_variables(expr: &expr::Expr) -> PlanResult<std::collections::HashSet<String>> {
    let mut names = std::collections::HashSet::new();
    expr.apply(|e| {
        if let expr::Expr::LambdaVariable(variable) = e {
            names.insert(variable.name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(names)
}

pub(super) fn is_spec_lambda_argument(argument: &spec::Expr) -> bool {
    match argument {
        spec::Expr::LambdaFunction { .. } => true,
        spec::Expr::Alias { expr, .. } => is_spec_lambda_argument(expr),
        _ => false,
    }
}

fn take_spec_lambda_argument(
    argument: spec::Expr,
) -> Option<(spec::Expr, Vec<spec::UnresolvedNamedLambdaVariable>)> {
    // TODO: Do we need to preserve any information from the original argument?
    match argument {
        spec::Expr::LambdaFunction {
            function,
            arguments,
        } => Some((*function, arguments)),
        spec::Expr::Alias { expr, .. } => take_spec_lambda_argument(*expr),
        _ => None,
    }
}

impl PlanResolver<'_> {
    /// Resolves the arguments of a built-in higher-order function.
    ///
    /// The value (non-lambda) arguments are resolved first so that the lambda
    /// parameters can be typed from their fields, mirroring DataFusion's lambda
    /// planning. Lambda bodies are then resolved with typed lambda variables in
    /// scope, which lets type-dispatching function builders (e.g. `size`) see
    /// the real parameter types.
    pub(super) async fn resolve_higher_order_function_arguments(
        &self,
        function_name: &str,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        enum Slot {
            Resolved(NamedExpr),
            Lambda(spec::Expr, Vec<spec::UnresolvedNamedLambdaVariable>),
            /// A plain expression sitting in a lambda position, to be wrapped in
            /// a lambda that declares the parameters the function expects and
            /// references none of them.
            WrappedLambda(spec::Expr),
        }

        let lambda_positions = lambda_argument_positions(function_name, arguments.len());

        let mut slots: Vec<Slot> = Vec::with_capacity(arguments.len());
        for (position, argument) in arguments.into_iter().enumerate() {
            if is_spec_lambda_argument(&argument) {
                let Some((function, arguments)) = take_spec_lambda_argument(argument) else {
                    return Err(PlanError::internal(
                        "lambda argument predicate and extraction disagreed",
                    ));
                };
                slots.push(Slot::Lambda(function, arguments));
            } else if lambda_positions.contains(&position) {
                slots.push(Slot::WrappedLambda(argument));
            } else {
                slots.push(Slot::Resolved(
                    self.resolve_named_expression(argument, schema, state)
                        .await?,
                ));
            }
        }

        let fields = slots
            .iter()
            .map(|slot| {
                Ok(match slot {
                    Slot::Resolved(named) => ValueOrLambda::Value(named.expr.to_field(schema)?.1),
                    Slot::Lambda(..) | Slot::WrappedLambda(..) => ValueOrLambda::Lambda(None),
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut lambda_params = get_lambda_parameters(function_name, &fields)?.into_iter();

        let mut names: Vec<String> = Vec::with_capacity(slots.len());
        let mut exprs: Vec<expr::Expr> = Vec::with_capacity(slots.len());
        for slot in slots {
            let NamedExpr { name, expr, .. } = match slot {
                Slot::Resolved(named) => named,
                Slot::Lambda(function, arguments) => {
                    let param_fields = lambda_params.next().ok_or_else(|| {
                        PlanError::internal(format!(
                            "missing lambda parameters for a lambda argument of {function_name}"
                        ))
                    })?;
                    self.resolve_expression_lambda_function(
                        function,
                        arguments,
                        Some(&param_fields),
                        schema,
                        state,
                    )
                    .await?
                }
                Slot::WrappedLambda(expression) => {
                    let param_fields = lambda_params.next().ok_or_else(|| {
                        PlanError::internal(format!(
                            "missing lambda parameters for a lambda argument of {function_name}"
                        ))
                    })?;
                    // The body is resolved outside of a lambda scope, so nothing
                    // in it can bind to the parameters declared below. They exist
                    // only to satisfy the arity the function expects and to give
                    // the evaluation batch its row count.
                    let body = self
                        .resolve_named_expression(expression, schema, state)
                        .await?;
                    // Only declare the parameters Spark's wrapping declares (a
                    // single element parameter for the element-wise functions),
                    // not every optional parameter the function supports. The body
                    // references none of them, so any extra parameter (e.g.
                    // `transform`'s index) would only force DataFusion to
                    // materialize an unused per-element column.
                    let param_count = wrapped_lambda_param_count(function_name, param_fields.len());
                    // Generated placeholder parameter names. Spark's hidden lambda
                    // parameters use expression identity and never participate in
                    // name resolution; DataFusion resolves lambda variables by name,
                    // so a placeholder must not match any variable the body captures,
                    // or evaluation would rebind it. `transform(array(1), x ->
                    // transform(array(2), x))` is the adversarial case: the inner
                    // body captures the outer `x`, so the placeholder avoids it.
                    // The names are generated locally rather than through the plan's
                    // hidden-field registry — lambda parameters are not plan columns
                    // (the real-lambda path registers none either), so registering
                    // them would only leak a field entry for every rejected candidate.
                    let captured = referenced_lambda_variables(&body.expr)?;
                    let mut params: Vec<String> = Vec::with_capacity(param_count);
                    let mut n = 0;
                    while params.len() < param_count {
                        let candidate = format!("__wrapped_lambda_param_{n}");
                        n += 1;
                        if !captured.contains(&candidate) {
                            params.push(candidate);
                        }
                    }
                    // Spark renders each hidden parameter as a nameless
                    // `namedlambdavariable()` in the derived column name (the
                    // placeholder's internal name never surfaces), e.g.
                    // `exists(a, lambdafunction(true, namedlambdavariable()))`.
                    let placeholders = vec!["namedlambdavariable()"; params.len()].join(", ");
                    let name = format!("lambdafunction({}, {placeholders})", body.name.clone().one()?);
                    NamedExpr::new(
                        vec![name],
                        expr::Expr::Lambda(Lambda::new(params, body.expr)),
                    )
                }
            };
            names.push(name.one()?);
            exprs.push(expr);
        }
        Ok((names, exprs))
    }

    /// Rejects the expressions Spark forbids anywhere in a higher-order function
    /// call: subquery expressions (SPARK-47509) and Python UDFs inside a lambda
    /// body. Spark applies both to the whole `HigherOrderFunction` node, so this
    /// is called on every argument of the call regardless of arity — including
    /// forms like 1-arg `array_sort(a)` that never enter
    /// `resolve_higher_order_function_arguments`.
    pub(super) fn reject_disallowed_higher_order_arguments(
        &self,
        exprs: &[expr::Expr],
    ) -> PlanResult<()> {
        let reject_subquery = !self.config.allow_subquery_in_higher_order_functions;
        for expr in exprs {
            // The guard is on by default, controlled by
            // `spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions`.
            // `Expr::Lambda` recurses only into its body and is not itself a
            // subquery variant, so checking the whole expression covers a lambda's
            // body as well as every plain argument.
            if reject_subquery && expr_contains_subquery(expr)? {
                return Err(PlanError::AnalysisError(
                    "Subquery expressions are not supported within higher-order functions. \
                     Please remove all subquery expressions from higher-order functions \
                     and then try the query again."
                        .to_string(),
                ));
            }
            // Spark rejects Python UDFs inside a higher-order function lambda
            // (`UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF`, checked in
            // `CheckAnalysis`), because the evaluator cannot drive the Python
            // worker per element.
            if let expr::Expr::Lambda(lambda) = expr
                && let Some(name) = expr_python_udf_name(&lambda.body)?
            {
                // Spark quotes the full SQL of the UDF call here (e.g.
                // `"plus_one(lambda x#11)"`); the exact form embeds a runtime
                // expression id we cannot reproduce, so we quote the UDF name.
                return Err(PlanError::AnalysisError(format!(
                    "Lambda function with Python UDF \"{name}\" in a higher order function."
                )));
            }
        }
        Ok(())
    }

    pub(super) async fn resolve_expression_lambda_function(
        &self,
        function: spec::Expr,
        arguments: Vec<spec::UnresolvedNamedLambdaVariable>,
        param_fields: Option<&[FieldRef]>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let params: Vec<String> = arguments
            .into_iter()
            .map(|variable| {
                <Vec<String>>::from(variable.name)
                    .one()
                    .map_err(|_| PlanError::invalid("multi-part lambda function parameter name"))
            })
            .collect::<PlanResult<_>>()?;
        // Spark rejects duplicate lambda parameter names case-insensitively
        // (DUPLICATE_ARG_NAMES). DataFusion's `all_unique` is case-sensitive and
        // lambda parameter lookup here is case-insensitive, so check explicitly.
        let mut seen = std::collections::HashSet::new();
        for param in &params {
            if !seen.insert(param.to_ascii_lowercase()) {
                return Err(PlanError::AnalysisError(format!(
                    "the lambda function has duplicate arguments `{param}`"
                )));
            }
        }
        let frame: Vec<(String, Option<FieldRef>)> = match param_fields {
            Some(fields) => {
                if params.len() > fields.len() {
                    return Err(PlanError::AnalysisError(format!(
                        "the lambda function declares {} parameters ({}) but only {} are supported",
                        params.len(),
                        params.join(", "),
                        fields.len()
                    )));
                }
                params
                    .iter()
                    .zip(fields)
                    .map(|(param, field)| {
                        (
                            param.clone(),
                            Some(FieldRef::clone(field).renamed(param.as_str())),
                        )
                    })
                    .collect()
            }
            None => params.iter().map(|param| (param.clone(), None)).collect(),
        };
        let body = {
            let mut scope = state.enter_lambda_scope(frame);
            self.resolve_named_expression(function, schema, scope.state())
                .await?
        };
        let name = format!(
            "lambdafunction({}, {})",
            body.name.clone().one()?,
            params.join(", ")
        );
        Ok(NamedExpr::new(
            vec![name],
            expr::Expr::Lambda(Lambda::new(params, body.expr)),
        ))
    }

    pub(super) async fn resolve_expression_named_lambda_variable(
        &self,
        variable: spec::UnresolvedNamedLambdaVariable,
        _schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name = <Vec<String>>::from(variable.name)
            .one()
            .map_err(|_| PlanError::invalid("multi-part lambda variable name"))?;
        let (declared, field) = state
            .resolve_lambda_parameter(&name)
            .map(|(param, field)| (param.to_string(), field.cloned()))
            .ok_or_else(|| {
                if state.in_lambda_scope() {
                    PlanError::AnalysisError(format!("unknown lambda parameter `{name}`"))
                } else {
                    PlanError::AnalysisError(format!(
                        "cannot resolve lambda variable `{name}` outside of a lambda function"
                    ))
                }
            })?;
        Ok(NamedExpr::new(
            vec![declared.clone()],
            expr::Expr::LambdaVariable(LambdaVariable::new(declared, field)),
        ))
    }
}
