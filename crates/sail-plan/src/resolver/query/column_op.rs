use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::Column;
use datafusion_expr::{cast, col, lit, Expr, ExprSchemable, LogicalPlan, Projection};
use indexmap::IndexMap;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_to_df(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        if columns.len() != schema.fields().len() {
            return Err(PlanError::invalid(format!(
                "number of column names ({}) does not match number of columns ({})",
                columns.len(),
                schema.fields().len()
            )));
        }
        let expr = schema
            .columns()
            .into_iter()
            .zip(columns.into_iter())
            .map(|(col, name)| NamedExpr::new(vec![name.into()], Expr::Column(col)))
            .collect();
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    pub(super) async fn resolve_query_to_schema(
        &self,
        input: spec::QueryPlan,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let target_schema = self.resolve_schema(schema, state)?;
        let input_names = Self::get_field_names(input.schema(), state)?;
        let mut projected_exprs = Vec::new();
        for target_field in target_schema.fields() {
            let target_name = target_field.name();
            let input_idx = input_names
                .iter()
                .position(|input_name| input_name.eq_ignore_ascii_case(target_name))
                .ok_or_else(|| {
                    PlanError::invalid(format!("field not found in input schema: {target_name}"))
                })?;
            let (input_qualifier, input_field) = input.schema().qualified_field(input_idx);
            let expr = Expr::Column(Column::from((input_qualifier, input_field)));
            let expr = if input_field.data_type() == target_field.data_type() {
                expr
            } else {
                expr.cast_to(target_field.data_type(), &input.schema())?
                    .alias_qualified(input_qualifier.cloned(), input_field.name())
            };
            projected_exprs.push(expr);
        }
        let projected_plan =
            LogicalPlan::Projection(Projection::try_new(projected_exprs, Arc::new(input))?);
        Ok(projected_plan)
    }

    pub(super) async fn resolve_query_with_columns_renamed(
        &self,
        input: spec::QueryPlan,
        rename_columns_map: Vec<(spec::Identifier, spec::Identifier)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;

        let mut inverse_map: HashMap<String, HashSet<String>> = HashMap::new();
        for (from, to) in rename_columns_map
            .iter()
            .map(|(a, b)| (a.as_ref().to_string(), b.as_ref().to_string()))
        {
            let from_froms = inverse_map.remove(&from).unwrap_or_default(); //.unwrap_or_else(|| HashSet::new());
            let to_froms = inverse_map.entry(to.clone()).or_default();
            to_froms.extend(from_froms);
            to_froms.insert(from);
        }

        let rename_columns_map: HashMap<String, String> = inverse_map
            .into_iter()
            .flat_map(|(to, froms)| froms.into_iter().map(move |from| (from, to.clone())))
            .collect();
        let schema = input.schema();
        let expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name();
                match rename_columns_map.get(name) {
                    Some(n) => Ok(NamedExpr::new(vec![n.clone()], Expr::Column(column))),
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    pub(super) async fn resolve_query_drop(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Expr>,
        column_names: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let excluded = columns
            .into_iter()
            .filter_map(|col| {
                let spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id,
                    is_metadata_column: false,
                } = col
                else {
                    return Some(Err(PlanError::invalid("expecting column to drop")));
                };
                let name: Vec<String> = name.into();
                let Ok(name) = name.one() else {
                    // Ignore nested names since they cannot match a column name.
                    // This is not an error in Spark.
                    return None;
                };
                // An error is returned when there are ambiguous columns.
                self.resolve_optional_column(schema, &name, plan_id, state)
                    .transpose()
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let excluded = excluded
            .into_iter()
            .chain(column_names.into_iter().flat_map(|name| {
                let name: String = name.into();
                // The excluded column names are allow to refer to ambiguous columns,
                // so we just check the column name here.
                self.resolve_column_candidates(schema, &name, None, state)
                    .into_iter()
            }))
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = schema
            .columns()
            .into_iter()
            .filter(|column| !excluded.contains(column))
            .map(Expr::Column)
            .collect();
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    pub(super) async fn resolve_query_with_columns(
        &self,
        input: spec::QueryPlan,
        aliases: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        // We use `IndexMap` to ensure the result schema has a deterministic column order.
        let mut aliases: IndexMap<String, (Expr, bool, Vec<_>)> = async {
            let mut results = IndexMap::new();
            for alias in aliases {
                let (name, expr, metadata) = match alias {
                    spec::Expr::Alias {
                        name,
                        expr,
                        metadata,
                    } => {
                        let name = name
                            .one()
                            .map_err(|_| PlanError::invalid("multi-alias for column"))?;
                        (name, *expr, metadata.unwrap_or(Vec::new()))
                    }
                    _ => return Err(PlanError::invalid("alias expression expected for column")),
                };
                let expr = self.resolve_expression(expr, schema, state).await?;
                results.insert(name.into(), (expr, false, metadata));
            }
            Ok(results) as PlanResult<_>
        }
        .await?;
        let mut expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name();
                match aliases.get_mut(name) {
                    Some((e, exists, metadata)) => {
                        *exists = true;
                        if !metadata.is_empty() {
                            Ok(NamedExpr::new(vec![name.to_string()], e.clone())
                                .with_metadata(metadata.clone()))
                        } else {
                            Ok(NamedExpr::new(vec![name.to_string()], e.clone()))
                        }
                    }
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        for (name, (e, exists, metadata)) in &aliases {
            if !exists {
                if !metadata.is_empty() {
                    expr.push(
                        NamedExpr::new(vec![name.clone()], e.clone())
                            .with_metadata(metadata.clone()),
                    );
                } else {
                    expr.push(NamedExpr::new(vec![name.clone()], e.clone()));
                }
            }
        }
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    pub(super) async fn resolve_query_replace(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        replacements: Vec<spec::Replacement>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let cols_to_change: Vec<String> = columns
            .into_iter()
            .map(|ident| ident.as_ref().to_ascii_lowercase())
            .collect();
        let replacements: Vec<(Expr, Expr)> = replacements
            .into_iter()
            .map(|r| {
                Ok((
                    lit(self.resolve_literal(r.old_value, state)?),
                    lit(self.resolve_literal(r.new_value, state)?),
                ))
            })
            .collect::<PlanResult<_>>()?;

        let existing_cols_info = schema
            .iter()
            .map(|(qualifier, field)| {
                let field_info = state.get_field_info(field.name())?;
                Ok::<_, PlanError>((
                    col((qualifier, field)),
                    field.data_type(),
                    field_info.name().to_ascii_lowercase(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let existing_cols_set: HashSet<_> =
            existing_cols_info.iter().map(|(_, _, name)| name).collect();

        if let Some(missing_colname) = cols_to_change
            .iter()
            .find(|col| !existing_cols_set.contains(*col))
        {
            let existing_cols = existing_cols_info
                .iter()
                .map(|(_, _, name)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");

            return Err(PlanError::AnalysisError(format!(
                "Cannot resolve column name \"{}\" among ({})",
                missing_colname, existing_cols
            )));
        }

        let cols_to_change_set: HashSet<_> = cols_to_change.iter().collect();

        let replace_exprs = existing_cols_info
            .into_iter()
            .map(|(column_expr, column_type, column_name)| {
                let expr = if cols_to_change.is_empty() || cols_to_change_set.contains(&column_name)
                {
                    let when_then_expr = replacements
                        .iter()
                        .filter(|(old, _new)| {
                            old.get_type(schema).is_ok_and(|old_type| {
                                old_type.is_null()
                                    || (old_type.is_numeric() && column_type.is_numeric())
                                    || (old_type == *column_type)
                            })
                        })
                        .map(|(old, new)| {
                            let old = cast(old.clone(), column_type.clone());
                            let new = cast(new.clone(), column_type.clone());
                            (Box::new(column_expr.clone().eq(old)), Box::new(new))
                        })
                        .collect::<Vec<_>>();

                    if when_then_expr.is_empty() {
                        column_expr
                    } else {
                        Expr::Case(datafusion_expr::Case {
                            expr: None,
                            when_then_expr,
                            else_expr: Some(Box::new(column_expr)),
                        })
                    }
                } else {
                    column_expr
                };
                Ok(NamedExpr::new(vec![column_name], expr))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            self.rewrite_named_expressions(replace_exprs, state)?,
            Arc::new(input),
        )?))
    }
}
