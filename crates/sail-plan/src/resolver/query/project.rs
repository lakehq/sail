use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion_common::Column;
use datafusion_expr::expr::{FieldMetadata, ScalarFunction};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{lit, Expr, ExprSchemable, LogicalPlan, Projection};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::PlanResolver;

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
            let schema = input.schema();
            let expr = self.rewrite_named_expressions(expr, schema, state)?;
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
                #[allow(deprecated)]
                Expr::Wildcard {
                    qualifier: None,
                    options,
                } => {
                    for e in expand_wildcard(schema, &input, Some(&options))? {
                        projected.extend(to_named_expr(e, state)?)
                    }
                }
                #[allow(deprecated)]
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    for e in expand_qualified_wildcard(&qualifier, schema, Some(&options))? {
                        projected.extend(to_named_expr(e, state)?)
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
                    if func.inner().as_any().is::<MultiExpr>() {
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
        schema: &Arc<datafusion_common::DFSchema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = e;
                if name.len() == 1 {
                    let name = name.one()?;
                    // Check if this is a single-field struct from json_tuple that needs unpacking
                    // json_tuple returns structs with fields named c0, c1, c2, etc.
                    let data_type = expr.get_type(schema)?;
                    if let DataType::Struct(fields) = data_type {
                        if fields.len() == 1 && fields[0].name() == "c0" {
                            // Unpack single-field struct from json_tuple
                            let field = &fields[0];
                            let args = vec![expr.clone(), lit(field.name().to_string())];
                            let field_expr = Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                            let field_id = state.register_field_name(name);
                            return Ok(vec![field_expr.alias(field_id)]);
                        }
                    }
                    // Normal case - single name
                    let plan_ids = if let Expr::Column(Column { name: field_id, .. }) = &expr {
                        let info = state.get_field_info(field_id)?;
                        info.plan_ids()
                    } else {
                        vec![]
                    };
                    let field_id = state.register_field_name(name);
                    for plan_id in plan_ids {
                        state.register_plan_id_for_field(&field_id, plan_id)?;
                    }
                    if !metadata.is_empty() {
                        let metadata_map: HashMap<String, String> = metadata.into_iter().collect();
                        let field_metadata = Some(FieldMetadata::from(metadata_map));
                        Ok(vec![expr.alias_with_metadata(field_id, field_metadata)])
                    } else {
                        Ok(vec![expr.alias(field_id)])
                    }
                } else {
                    // Multiple names: unpack struct fields
                    let data_type = expr.get_type(schema)?;
                    if let DataType::Struct(fields) = data_type {
                        if fields.len() != name.len() {
                            return Err(PlanError::invalid(format!(
                                "number of aliases ({}) does not match number of struct fields ({})",
                                name.len(),
                                fields.len()
                            )));
                        }
                        let mut result = Vec::with_capacity(name.len());
                        for (i, alias_name) in name.into_iter().enumerate() {
                            // Use get_field to extract struct field by name (c0, c1, c2, etc.)
                            let field = &fields[i];
                            let args = vec![expr.clone(), lit(field.name().to_string())];
                            let field_expr = Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                            let field_id = state.register_field_name(alias_name);
                            result.push(field_expr.alias(field_id));
                        }
                        Ok(result)
                    } else {
                        let names = format!("({})", name.join(", "));
                        Err(PlanError::invalid(format!(
                            "multiple aliases {} require a struct type, got: {}",
                            names, data_type
                        )))
                    }
                }
            })
            .collect::<PlanResult<Vec<Vec<Expr>>>>()
            .map(|v| v.into_iter().flatten().collect())
    }
}
