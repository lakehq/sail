use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{Column, DFSchema, DFSchemaRef, TableReference};
use datafusion_expr::expr::{Alias, FieldMetadata, ScalarFunction};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Projection};
use sail_common::spec;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::tree::exists::ExistsRewriter;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

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
        let schema = input.schema();
        let expr = self.resolve_named_expressions(expr, schema, state).await?;
        let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_window_projection(input, expr, state)?;
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

    pub(super) fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        fn to_named_expr(expr: Expr, state: &PlanResolverState) -> PlanResult<Option<NamedExpr>> {
            let Expr::Column(column) = expr else {
                return Err(PlanError::invalid(
                    "column expected for expanded wildcard expression",
                ));
            };
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
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
                        projected.extend(to_named_expr(e, state)?)
                    }
                    expand_outer_wildcard(None, schema, state, &mut projected)?;
                }
                #[expect(deprecated)]
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    let qualifier_in_input = schema
                        .iter()
                        .any(|(q, _)| q.is_some_and(|q| *q == qualifier));
                    if qualifier_in_input {
                        for e in expand_qualified_wildcard(&qualifier, schema, Some(&options))? {
                            projected.extend(to_named_expr(e, state)?)
                        }
                    }
                    expand_outer_wildcard(Some(&qualifier), schema, state, &mut projected)?;
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

    /// Materialize metadata from this SELECT layer before lowering its windows.
    pub(super) fn rewrite_window_projection(
        &self,
        mut input: LogicalPlan,
        mut expressions: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        let mut has_window = false;
        for named in &expressions {
            if named
                .expr
                .exists(|expr| Ok(matches!(expr, Expr::WindowFunction(_))))?
            {
                has_window = true;
                break;
            }
        }
        if !has_window {
            return Ok((input, expressions));
        }

        let mut metadata: Vec<(Expr, String)> = Vec::new();
        for named in &mut expressions {
            let mut extract_metadata = |expr: Expr| {
                let Expr::ScalarFunction(function) = &expr else {
                    return Ok(Transformed::no(expr));
                };
                if !is_input_file_metadata_function(&function.func) {
                    return Ok(Transformed::no(expr));
                }
                let id = match metadata.iter().find(|(candidate, _)| candidate == &expr) {
                    Some((_, id)) => id.clone(),
                    None => {
                        let id = state.next_field_id();
                        metadata.push((expr, id.clone()));
                        id
                    }
                };
                // Internal IDs are not registered user fields. An alias prevents
                // rewrite_named_expressions from treating them as identity fields.
                Ok(Transformed::yes(
                    Expr::Column(Column::new_unqualified(&id)).alias(id),
                ))
            };
            let expr = named.expr.clone();
            named.expr = if expr.exists(|expr| Ok(matches!(expr, Expr::WindowFunction(_))))? {
                // Metadata combined with a window result stays above that window.
                // Only its arguments and keys are evaluated in the input context.
                expr.transform_up(|expr| {
                    if matches!(expr, Expr::WindowFunction(_)) {
                        expr.transform_up(&mut extract_metadata)
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })?
                .data
            } else {
                // Regular siblings include nested lambda bodies, but Expr traversal
                // never enters independent subquery plans.
                expr.transform_up(&mut extract_metadata)?.data
            };
        }
        if !metadata.is_empty() {
            let projection = input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .chain(metadata.into_iter().map(|(expr, id)| expr.alias(id)))
                .collect();
            input = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
        }
        self.rewrite_projection::<WindowRewriter>(input, expressions, state)
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
                if !metadata.is_empty() {
                    let metadata_map: HashMap<String, String> = metadata.into_iter().collect();
                    let field_metadata = Some(FieldMetadata::from(metadata_map));
                    Ok(expr.alias_with_metadata(field_id, field_metadata))
                } else {
                    Ok(expr.alias(field_id))
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

#[cfg(test)]
mod tests {
    use datafusion::functions::expr_fn::{coalesce, input_file_name};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::functions_window::row_number::row_number_udwf;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::expr::{Lambda, WindowFunction};
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, col, lit, scalar_subquery};
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};

    use super::*;
    use crate::config::PlanConfig;

    fn input(state: &mut PlanResolverState) -> PlanResult<LogicalPlan> {
        let id = state.register_field_name("id");
        Ok(LogicalPlanBuilder::empty(false)
            .project(vec![lit(1_i64).alias(id)])?
            .build()?)
    }

    fn has_metadata(expr: &Expr) -> datafusion_common::Result<bool> {
        expr.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
            if is_input_file_metadata_function(&function.func)))
        })
    }

    #[test]
    fn materializes_window_siblings_arguments_and_keys_once() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let input = input(&mut state)?;
        let name = coalesce(vec![input_file_name(), lit("")]);
        let start = ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]);
        let length = ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]);
        let mut window = WindowFunction::new(sum_udaf(), vec![start.clone()]);
        window.params.partition_by = vec![name.clone()];
        window.params.order_by = vec![length.clone().sort(true, true)];
        let expressions = vec![
            NamedExpr::new(vec!["total".into()], Expr::WindowFunction(Box::new(window))),
            NamedExpr::new(vec!["name".into()], name.clone())
                .with_metadata(vec![("comment".into(), "file".into())]),
            NamedExpr::new(vec!["start".into()], start),
            NamedExpr::new(vec!["length".into()], length + lit(1_i64)),
            NamedExpr::new(vec!["again".into()], name),
        ];
        let original = expressions.clone();
        let (plan, expressions) =
            resolver.rewrite_window_projection(input.clone(), expressions, &mut state)?;
        for (actual, expected) in expressions.iter().zip(&original) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.metadata, expected.metadata);
            assert!(!has_metadata(&actual.expr)?);
        }
        let LogicalPlan::Window(window) = &plan else {
            return Err(PlanError::internal("expected window"));
        };
        assert!(!has_metadata(&window.window_expr[0])?);
        let LogicalPlan::Projection(projection) = window.input.as_ref() else {
            return Err(PlanError::internal("expected metadata projection"));
        };
        assert_eq!(projection.input.as_ref(), &input);
        assert_eq!(projection.expr.len(), input.schema().fields().len() + 3);
        let projected = resolver.rewrite_named_expressions(expressions, &mut state)?;
        let result = Projection::try_new(projected, Arc::new(plan))?;
        assert_eq!(result.schema.fields().len(), original.len());
        assert_eq!(
            result.schema.field(1).metadata().get("comment"),
            Some(&"file".to_string())
        );
        Ok(())
    }

    #[test]
    fn keeps_metadata_combined_with_window_results_above_window() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        for metadata in [
            input_file_name(),
            ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]),
            ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]),
        ] {
            for metadata_in_keys in [false, true] {
                let mut state = PlanResolverState::new();
                let input = input(&mut state)?;
                let mut function = WindowFunction::new(row_number_udwf(), vec![]);
                if metadata_in_keys {
                    function.params.partition_by = vec![metadata.clone()];
                }
                let combined = metadata
                    .clone()
                    .is_not_null()
                    .and(Expr::WindowFunction(Box::new(function)).gt(lit(0_u64)));
                let (plan, expressions) = resolver.rewrite_window_projection(
                    input.clone(),
                    vec![NamedExpr::new(vec!["combined".into()], combined)],
                    &mut state,
                )?;
                assert!(has_metadata(&expressions[0].expr)?);
                let LogicalPlan::Window(window) = plan else {
                    return Err(PlanError::internal("expected window"));
                };
                assert!(!has_metadata(&window.window_expr[0])?);
                if metadata_in_keys {
                    let LogicalPlan::Projection(projection) = window.input.as_ref() else {
                        return Err(PlanError::internal("expected metadata projection"));
                    };
                    assert_eq!(projection.input.as_ref(), &input);
                    assert_eq!(projection.expr.len(), input.schema().fields().len() + 1);
                } else {
                    assert_eq!(window.input.as_ref(), &input);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn traverses_nested_lambdas_but_not_subquery_plans() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let input = input(&mut state)?;
        let nested = Expr::Lambda(Lambda::new(
            vec!["x".into()],
            Expr::Lambda(Lambda::new(vec!["y".into()], input_file_name())),
        ));
        let subquery = scalar_subquery(Arc::new(
            LogicalPlanBuilder::empty(false)
                .project(vec![
                    ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]),
                ])?
                .build()?,
        ));
        let expressions = vec![
            NamedExpr::new(
                vec!["rn".into()],
                Expr::WindowFunction(Box::new(WindowFunction::new(row_number_udwf(), vec![]))),
            ),
            NamedExpr::new(vec!["nested".into()], nested),
            NamedExpr::new(vec!["subquery".into()], subquery.clone()),
        ];
        let (plan, expressions) =
            resolver.rewrite_window_projection(input, expressions, &mut state)?;
        assert!(!has_metadata(&expressions[1].expr)?);
        assert_eq!(expressions[2].expr, subquery);
        let LogicalPlan::Window(window) = plan else {
            return Err(PlanError::internal("expected window"));
        };
        let LogicalPlan::Projection(projection) = window.input.as_ref() else {
            return Err(PlanError::internal("expected metadata projection"));
        };
        assert_eq!(projection.expr.len(), 2);
        Ok(())
    }

    #[test]
    fn projection_after_window_keeps_its_original_layer() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let input = input(&mut state)?;
        let (window, _) = resolver.rewrite_window_projection(
            input,
            vec![NamedExpr::new(
                vec!["rn".into()],
                Expr::WindowFunction(Box::new(WindowFunction::new(row_number_udwf(), vec![]))),
            )],
            &mut state,
        )?;
        let expressions = vec![NamedExpr::new(vec!["name".into()], input_file_name())];
        let (plan, actual) =
            resolver.rewrite_window_projection(window.clone(), expressions.clone(), &mut state)?;
        assert_eq!(plan, window);
        assert_eq!(actual, expressions);
        Ok(())
    }

    #[test]
    fn aggregate_window_projection_stays_above_aggregation() -> PlanResult<()> {
        let context = SessionContext::new();
        let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::new()?));
        let mut state = PlanResolverState::new();
        let input = input(&mut state)?;
        let id = input.schema().field(0).name().clone();
        let aggregate = LogicalPlanBuilder::from(input)
            .aggregate(vec![col(id)], Vec::<Expr>::new())?
            .build()?;
        let expressions = vec![
            NamedExpr::new(
                vec!["rn".into()],
                Expr::WindowFunction(Box::new(WindowFunction::new(row_number_udwf(), vec![]))),
            ),
            NamedExpr::new(vec!["name".into()], input_file_name()),
        ];
        let (plan, _) =
            resolver.rewrite_window_projection(aggregate.clone(), expressions, &mut state)?;
        let LogicalPlan::Window(window) = plan else {
            return Err(PlanError::internal("expected window"));
        };
        let LogicalPlan::Projection(projection) = window.input.as_ref() else {
            return Err(PlanError::internal("expected metadata projection"));
        };
        assert_eq!(projection.input.as_ref(), &aggregate);
        Ok(())
    }
}
