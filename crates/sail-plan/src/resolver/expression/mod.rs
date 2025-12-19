use std::fmt::Debug;

use async_recursion::async_recursion;
use datafusion_common::DFSchemaRef;
use datafusion_expr::expr;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

mod attribute;
mod cast;
mod function;
mod grouping;
mod lambda;
mod literal;
mod misc;
mod predicate;
mod sort;
mod subquery;
mod udf;
mod wildcard;
mod window;

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
        let metadata = match &expr {
            expr::Expr::Alias(alias) => alias
                .metadata
                .as_ref()
                .map(|x| {
                    x.inner()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                })
                .unwrap_or(vec![]),
            _ => vec![],
        };
        Self {
            name,
            expr,
            metadata,
        }
    }

    pub fn try_from_alias_expr(expr: expr::Expr) -> PlanResult<Self> {
        match expr {
            expr::Expr::Alias(alias) => Ok(Self {
                name: vec![alias.name],
                expr: *alias.expr,
                metadata: alias
                    .metadata
                    .map(|x| x.to_hashmap().into_iter().collect())
                    .unwrap_or(vec![]),
            }),
            _ => Err(PlanError::invalid(
                "alias expected to create named expression",
            )),
        }
    }

    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self.metadata = metadata;
        self
    }
}

impl PlanResolver<'_> {
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
            Expr::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column,
            } => {
                self.resolve_expression_attribute(name, plan_id, is_metadata_column, schema, state)
            }
            Expr::UnresolvedFunction(function) => {
                self.resolve_expression_function(function, schema, state)
                    .await
            }
            Expr::UnresolvedStar {
                target,
                plan_id,
                wildcard_options,
            } => {
                self.resolve_expression_wildcard(target, plan_id, wildcard_options, schema, state)
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
            Expr::Cast {
                expr,
                cast_to_type,
                rename,
                is_try,
            } => {
                self.resolve_expression_cast(*expr, cast_to_type, rename, is_try, schema, state)
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
                window,
            } => {
                self.resolve_expression_window(*window_function, window, schema, state)
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
            Expr::Table { expr } => self.resolve_expression_table(*expr, state).await,
            Expr::UnresolvedDate { value } => self.resolve_expression_date(value, state),
            Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => self.resolve_expression_timestamp(value, timestamp_type, state),
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr::expr::{Alias, Expr};
    use datafusion_expr::{BinaryExpr, Operator};
    use sail_common::spec;
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::session::PlanService;

    use crate::catalog::SparkCatalogObjectDisplay;
    use crate::config::PlanConfig;
    use crate::error::PlanResult;
    use crate::formatter::SparkPlanFormatter;
    use crate::resolver::expression::NamedExpr;
    use crate::resolver::state::PlanResolverState;
    use crate::resolver::PlanResolver;

    #[tokio::test]
    async fn test_resolve_expression_with_name() -> PlanResult<()> {
        let mut state = SessionStateBuilder::new().build();
        state.config_mut().set_extension(Arc::new(PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        )));
        let ctx = SessionContext::new_with_state(state);
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
                spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                    function_name: spec::ObjectName::bare("not"),
                    arguments: vec![spec::Expr::Literal(spec::Literal::Boolean {
                        value: Some(true)
                    })],
                    named_arguments: vec![],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                    ignore_nulls: None,
                    filter: None,
                    order_by: None,
                })
            )
            .await?,
            NamedExpr {
                name: vec!["(NOT true)".to_string()],
                expr: Expr::Not(Box::new(Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                    None
                ))),
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
                        expr: Box::new(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                            function_name: spec::ObjectName::bare("+"),
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
                            named_arguments: vec![],
                            is_distinct: false,
                            is_user_defined_function: false,
                            is_internal: None,
                            ignore_nulls: None,
                            filter: None,
                            order_by: None
                        })),
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
                                expr: Box::new(Expr::Literal(ScalarValue::Int32(Some(1)), None)),
                                name: "a".to_string(),
                                relation: None,
                                metadata: None,
                            })),
                            op: Operator::Plus,
                            right: Box::new(Expr::Literal(ScalarValue::Int32(Some(2)), None)),
                        })),
                        relation: None,
                        name: "b".to_string(),
                        metadata: None,
                    })),
                    relation: None,
                    name: "c".to_string(),
                    metadata: None,
                }),
                metadata: Default::default(),
            },
        );

        Ok(())
    }
}
