use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Column, DFSchema, DFSchemaRef, TableReference};
use datafusion_expr::expr::{Alias, FieldMetadata, ScalarFunction};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Projection};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::qualifier_matches;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::tree::exists::ExistsRewriter;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

/// Resolved expressions and the stable fields used by later aliases in the list.
pub(super) struct LateralExpressions {
    pub expressions: Vec<NamedExpr>,
    pub columns: Vec<Column>,
    pub schema: DFSchemaRef,
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_project(
        &self,
        input: Option<spec::QueryPlan>,
        expr: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = match input {
            Some(x) => self.resolve_query_plan_with_hidden_fields(x, state).await?,
            None => self.resolve_query_empty(true)?,
        };
        let (input, expr) = self.resolve_projection_list(input, expr, state).await?;
        let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let has_aggregate = expr.iter().any(|e| {
            e.expr
                .exists(|e| match e {
                    Expr::AggregateFunction(_) => Ok(true),
                    _ => Ok(false),
                })
                .unwrap_or(false)
        });
        if has_aggregate {
            self.rewrite_aggregate(input, expr, vec![], None, false, state)
        } else {
            let (input, expr) = self.rewrite_projection::<ExistsRewriter>(input, expr, state)?;
            let expr = self.rewrite_named_expressions(expr, state)?;
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
    }

    /// Resolve aliases left to right, with input attributes taking precedence.
    /// Scalar aliases are materialized so nondeterministic expressions are evaluated once.
    pub(super) async fn resolve_projection_list(
        &self,
        mut input: LogicalPlan,
        expressions: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        let schema = Arc::clone(input.schema());
        if let Ok(exprs) = self
            .resolve_named_expressions(expressions.clone(), &schema, state)
            .await
        {
            return Ok((input, exprs));
        }
        let LateralExpressions {
            mut expressions,
            columns,
            schema: _,
        } = self
            .resolve_lateral_expressions(expressions, &input, state)
            .await?;
        let aggregates = datafusion_expr::utils::find_aggregate_exprs(
            &expressions
                .iter()
                .map(|named| named.expr.clone())
                .collect::<Vec<_>>(),
        );
        // Resolve once: resolving a scalar subquery again gives its fields new
        // identities, so its enclosing aggregate would no longer match here.
        if !aggregates.is_empty() {
            input = self.rewrite_aggregate(
                input,
                aggregates
                    .iter()
                    .map(|expr| NamedExpr::new(vec![String::new()], expr.clone()))
                    .collect(),
                vec![],
                None,
                false,
                state,
            )?;
            let aggregate_columns = input.schema().columns();
            for named in &mut expressions {
                named.expr = named
                    .expr
                    .clone()
                    .transform_down(|expr| {
                        Ok(
                            match aggregates.iter().position(|aggregate| aggregate == &expr) {
                                Some(index) => {
                                    Transformed::yes(Expr::Column(aggregate_columns[index].clone()))
                                }
                                None => Transformed::no(expr),
                            },
                        )
                    })?
                    .data;
            }
        }
        self.materialize_lateral_expressions(input, expressions, columns, state)
    }

    /// Resolve dependencies using typed columns, retaining each expression for
    /// materialization instead of resolving subqueries twice or inlining aliases.
    #[expect(deprecated)]
    pub(super) async fn resolve_lateral_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        input: &LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LateralExpressions> {
        let mut schema = Arc::clone(input.schema());
        let mut aliases = vec![];
        let mut resolved = vec![];
        let mut columns = vec![];
        for expression in expressions {
            let named = {
                let mut scope =
                    state.enter_aggregate_scope(crate::resolver::state::AggregateState::Lateral {
                        projections: aliases.clone(),
                    });
                self.resolve_named_expression(expression, &schema, scope.state())
                    .await?
            };
            let expanded = if matches!(named.expr, Expr::Wildcard { .. }) {
                self.rewrite_wildcard(input.clone(), vec![named], state)?.1
            } else {
                vec![named]
            };
            for named in expanded {
                if named.name.len() != 1 {
                    return Err(PlanError::analysis(
                        "unsupported lateral alias with generator",
                    ));
                }
                for aggregate in
                    datafusion_expr::utils::find_aggregate_exprs(std::slice::from_ref(&named.expr))
                {
                    if aggregate
                        .column_refs()
                        .iter()
                        .any(|column| columns.contains(*column))
                    {
                        return Err(PlanError::analysis(
                            "lateral column alias in aggregate function is not supported",
                        ));
                    }
                }
                let (qualifier, name) = if let Expr::Column(column) = &named.expr {
                    (
                        column.relation.clone(),
                        state.register_identity_field(String::new(), column.name())?,
                    )
                } else {
                    (None, state.register_field_name(""))
                };
                let (_, field) = named.expr.to_field(&schema)?;
                let mut fields = schema
                    .iter()
                    .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                    .collect::<Vec<_>>();
                fields.push((
                    qualifier.clone(),
                    Arc::new(field.as_ref().clone().with_name(&name)),
                ));
                schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    schema.metadata().clone(),
                )?);
                let column = Column::new(qualifier, name);
                aliases.push(NamedExpr {
                    expr: Expr::Column(column.clone()),
                    ..named.clone()
                });
                columns.push(column);
                resolved.push(named);
            }
        }
        Ok(LateralExpressions {
            expressions: resolved,
            columns,
            schema,
        })
    }

    /// Stage aliases after their input (including aggregation) has been computed.
    pub(super) fn materialize_lateral_expressions(
        &self,
        mut input: LogicalPlan,
        expressions: Vec<NamedExpr>,
        columns: Vec<Column>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        let mut resolved = vec![];
        for (named, column) in expressions.into_iter().zip(columns) {
            // A grouping key may already have materialized this alias below
            // the aggregate and carried it into the aggregate output.
            if input.schema().has_column(&column)
                && named.expr.clone().unalias() == Expr::Column(column.clone())
            {
                resolved.push(named);
                continue;
            }
            let (next, named) =
                self.rewrite_projection::<MonotonicIdRewriter>(input, vec![named], state)?;
            let (next, named) =
                self.rewrite_projection::<SparkPartitionIdRewriter>(next, named, state)?;
            let (next, named) = self.rewrite_projection::<ExplodeRewriter>(next, named, state)?;
            let (next, named) = self.rewrite_projection::<WindowRewriter>(next, named, state)?;
            let (next, named) = self.rewrite_projection::<ExistsRewriter>(next, named, state)?;
            input = next;
            let named = named.one()?;
            let mut exprs = input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            exprs.push(
                named
                    .expr
                    .alias_qualified(column.relation.clone(), &column.name),
            );
            input = LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(input))?);
            resolved.push(NamedExpr {
                expr: Expr::Column(column),
                ..named
            });
        }
        Ok((input, resolved))
    }

    pub(super) fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        fn to_named_expr(
            expr: Expr,
            qualified: bool,
            state: &PlanResolverState,
        ) -> PlanResult<Option<NamedExpr>> {
            let Expr::Column(column) = expr else {
                return Err(PlanError::invalid(
                    "column expected for expanded wildcard expression",
                ));
            };
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() && !qualified {
                return Ok(None);
            }
            Ok(Some(NamedExpr::new(
                vec![info.name().to_string()],
                Expr::Column(column),
            )))
        }

        fn expand_outer_wildcard(
            qualifier_filter: Option<&TableReference>,
            input_schema: &DFSchemaRef,
            state: &PlanResolverState,
            projected: &mut Vec<NamedExpr>,
        ) -> PlanResult<()> {
            if qualifier_filter.is_none() && !input_schema.fields().is_empty() {
                return Ok(());
            }

            if let Some(expected) = qualifier_filter {
                let qualifier_in_input = input_schema
                    .iter()
                    .any(|(q, _)| q.is_some_and(|q| *q == *expected));
                if qualifier_in_input {
                    return Ok(());
                }
            }

            if let Some(outer_schema) = state.get_outer_query_schema() {
                for (qualifier, field) in outer_schema.iter() {
                    if let Some(expected) = qualifier_filter
                        && qualifier.is_none_or(|q| expected != q)
                    {
                        continue;
                    }
                    let info = state.get_field_info(field.name())?;
                    if info.is_hidden() {
                        continue;
                    }
                    let outer_col = Expr::OuterReferenceColumn(
                        field.clone(),
                        Column::new(qualifier.cloned(), field.name()),
                    );
                    projected.push(NamedExpr::new(vec![info.name().to_string()], outer_col));
                }
            }
            Ok(())
        }

        let schema = input.schema();
        let mut projected = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            // FIXME: wildcard options do not take into account opaque field IDs
            match expr {
                #[expect(deprecated)]
                Expr::Wildcard {
                    qualifier: None,
                    options,
                } => {
                    for e in expand_wildcard(schema, &input, Some(&options))? {
                        projected.extend(to_named_expr(e, false, state)?)
                    }
                    expand_outer_wildcard(None, schema, state, &mut projected)?;
                }
                #[expect(deprecated)]
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    let parts = state
                        .relation_name(&qualifier)
                        .into_iter()
                        .map(str::to_string)
                        .collect();
                    let qualifiers = schema
                        .iter()
                        .filter_map(|(q, _)| q)
                        .filter(|q| {
                            qualifier_matches(
                                Some(&parts),
                                Some(q),
                                self.config.case_sensitive,
                                state,
                            )
                        })
                        .cloned()
                        .collect::<std::collections::HashSet<_>>();
                    if !qualifiers.is_empty() {
                        let expanded = qualifiers
                            .iter()
                            .map(|qualifier| {
                                expand_qualified_wildcard(qualifier, schema, Some(&options))
                            })
                            .collect::<datafusion_common::Result<Vec<_>>>()?
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>();
                        // Walk the schema to preserve source column order.
                        for (q, field) in schema.iter() {
                            if expanded.contains(&Expr::Column(Column::from((q, field)))) {
                                projected.extend(to_named_expr(
                                    Expr::Column(Column::from((q, field))),
                                    true,
                                    state,
                                )?);
                            }
                        }
                    } else {
                        let before = projected.len();
                        expand_outer_wildcard(Some(&qualifier), schema, state, &mut projected)?;
                        if projected.len() == before {
                            return Err(PlanError::analysis(format!(
                                "cannot resolve wildcard: {qualifier}.*"
                            )));
                        }
                    }
                }
                _ => projected.push(NamedExpr {
                    name,
                    expr: columnize_expr(normalize_col(expr, &input)?, &input)?,
                    metadata,
                }),
            }
        }
        Ok((input, projected))
    }

    pub(super) fn rewrite_projection<'s, T>(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &'s mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)>
    where
        T: PlanRewriter<'s> + TreeNodeRewriter<Node = Expr>,
    {
        let mut rewriter = T::new_from_plan(input, state);
        let expr = expr
            .into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = e;
                Ok(NamedExpr {
                    name,
                    expr: expr.rewrite(&mut rewriter)?.data,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expr))
    }

    /// Materialize partition-dependent functions before evaluating an operator's expressions.
    pub(super) fn rewrite_partition_expressions(
        &self,
        input: LogicalPlan,
        expressions: Vec<Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<Expr>)> {
        let mut rewriter = MonotonicIdRewriter::new_from_plan(input, state);
        let expressions = expressions
            .into_iter()
            .map(|expr| Ok(expr.rewrite(&mut rewriter)?.data))
            .collect::<PlanResult<Vec<_>>>()?;
        let input = rewriter.into_plan();
        let mut rewriter = SparkPartitionIdRewriter::new_from_plan(input, state);
        let expressions = expressions
            .into_iter()
            .map(|expr| Ok(expr.rewrite(&mut rewriter)?.data))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expressions))
    }

    pub(super) fn rewrite_multi_expr(&self, expr: Vec<NamedExpr>) -> PlanResult<Vec<NamedExpr>> {
        let mut out = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            match expr {
                Expr::ScalarFunction(ScalarFunction { func, args }) => {
                    if func.inner().is::<MultiExpr>() {
                        // The metadata from the original expression are ignored.
                        if name.len() == args.len() {
                            for (name, arg) in name.into_iter().zip(args) {
                                out.push(NamedExpr::new(vec![name], arg));
                            }
                        } else {
                            for arg in args {
                                out.push(NamedExpr::try_from_alias_expr(arg)?);
                            }
                        }
                    } else {
                        out.push(NamedExpr {
                            name,
                            expr: func.call(args),
                            metadata,
                        });
                    }
                }
                _ => {
                    out.push(NamedExpr {
                        name,
                        expr,
                        metadata,
                    });
                }
            };
        }
        Ok(out)
    }

    /// Rewrite named expressions to DataFusion expressions.
    /// A field is registered for each name.
    /// If the expression is a column expression, all plan IDs for the column are registered for the field.
    /// This means the column must refer to a **registered field** of the input plan. Otherwise, the column must be wrapped with an alias.
    pub(super) fn rewrite_named_expressions(
        &self,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = e;
                let name = if name.len() == 1 {
                    name.one()?
                } else {
                    let names = format!("({})", name.join(", "));
                    return Err(PlanError::invalid(format!(
                        "one name expected for expression, got: {names}"
                    )));
                };
                let field_id = if let Expr::Column(Column { name: field_id, .. }) = &expr {
                    state.register_identity_field(name, field_id)?
                } else {
                    state.register_field_name(name)
                };
                let qualifier = if let Expr::Column(column) = &expr {
                    column
                        .relation
                        .clone()
                        .filter(|relation| relation.table() != datafusion_expr::UNNAMED_TABLE)
                } else {
                    None
                };
                if !metadata.is_empty() {
                    let metadata_map: HashMap<String, String> = metadata.into_iter().collect();
                    let field_metadata = Some(FieldMetadata::from(metadata_map));
                    Ok(expr.alias_with_metadata(field_id, field_metadata))
                } else {
                    Ok(expr.alias_qualified(qualifier, field_id))
                }
            })
            .collect()
    }

    /// Builds a projection whose expressions are mostly aliased input columns.
    /// An aliased column keeps its input field, so reuse it instead of looking up
    /// every column in the input schema, which is quadratic in the width.
    pub(super) fn projection_reusing_input_fields(
        expr: Vec<Expr>,
        input: LogicalPlan,
    ) -> PlanResult<LogicalPlan> {
        let schema = input.schema();
        // Let DataFusion project the functional dependencies when there are any.
        if !schema.functional_dependencies().is_empty() {
            return Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(input),
            )?));
        }
        // Field IDs are unique, so a column almost always names a single field.
        // Leave duplicate names to DataFusion's lookup.
        let mut indices = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            indices
                .entry(field.name().as_str())
                .and_modify(|x| *x = None)
                .or_insert(Some(index));
        }
        let fields = expr
            .iter()
            .map(|expr| {
                if let Expr::Alias(Alias {
                    expr: inner,
                    relation,
                    name,
                    metadata: None,
                }) = expr
                    && let Expr::Column(column) = inner.as_ref()
                    && let Some(Some(index)) = indices.get(column.name.as_str())
                    && let (qualifier, field) = schema.qualified_field(*index)
                    && qualifier == column.relation.as_ref()
                {
                    let field = field.as_ref().clone().with_name(name);
                    Ok((relation.clone(), Arc::new(field)))
                } else {
                    expr.to_field(schema)
                }
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )?);
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            expr,
            Arc::new(input),
            schema,
        )?))
    }
}
