use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::{Column, DFSchemaRef, ScalarValue};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, Projection, SubqueryAlias, cast, col, lit,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::{
    unresolved_column_fields_error, unresolved_column_name_error,
};
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

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
            .zip(columns)
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
            let mut matches = input_names
                .iter()
                .enumerate()
                .filter(|(_, input_name)| self.match_identifier(input_name, target_name));
            let Some((input_idx, _)) = matches.next() else {
                // A target field that matches no input column is filled with NULL when it is
                // nullable, and is only rejected otherwise.
                if !target_field.is_nullable() {
                    let candidates = input_names.iter().map(|x| x.as_str()).collect::<Vec<_>>();
                    return Err(unresolved_column_name_error(
                        &spec::ObjectName::bare(target_name.as_str()),
                        &candidates,
                    ));
                }
                let field_id = state.register_field_name(target_name.clone());
                projected_exprs.push(
                    cast(lit(ScalarValue::Null), target_field.data_type().clone()).alias(field_id),
                );
                continue;
            };
            if matches.next().is_some() {
                return Err(PlanError::AnalysisError(format!(
                    "[AMBIGUOUS_COLUMN_OR_FIELD] Column or field `{}` is ambiguous and has {} \
                     matches.",
                    target_name.replace('`', "``"),
                    2 + matches.count()
                )));
            }
            let (input_qualifier, input_field) = input.schema().qualified_field(input_idx);
            let expr = Expr::Column(Column::from((input_qualifier, input_field)));
            let expr = if input_field.data_type() == target_field.data_type() {
                expr
            } else {
                expr.cast_to(target_field.data_type(), &input.schema())?
            };
            // The column takes the name of the target field rather than the one it matched.
            let field_id = state.register_field_name(target_name.clone());
            projected_exprs.push(expr.alias(field_id));
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
        let columns = input.schema().columns();
        let mut names = Self::get_field_names(input.schema(), state)?;
        // Each rename is applied to the output of the previous one. A name that matches no
        // column is ignored.
        for (from, to) in rename_columns_map {
            let (from, to) = (from.as_ref(), to.as_ref());
            for name in names.iter_mut() {
                if self.match_identifier(name, from) {
                    *name = to.to_string();
                }
            }
        }
        let expr = columns
            .into_iter()
            .zip(names)
            .map(|(column, name)| NamedExpr::new(vec![name], Expr::Column(column)))
            .collect::<Vec<_>>();
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
                // so we just check the column name here. The name is matched with the resolver
                // alone, unlike an attribute reference, which Spark also looks up in a map
                // keyed by the lowercased name.
                self.resolve_column_candidates_by_resolver(schema, &name, state)
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
        // `AliasEntry` is `(name, resolved_expr, explicit_metadata)` where `explicit_metadata`
        // is `Some(meta)` when the user explicitly provided metadata via `withMetadata`, and
        // `None` when no metadata was specified on the alias.
        type AliasEntry = (String, Expr, Option<Vec<(String, String)>>);

        let input = self.resolve_query_plan(input, state).await?;
        // If the input is a SubqueryAlias, save the alias and re-apply it after building the
        // projection. A Projection node strips qualifiers from its output schema, so without
        // re-wrapping, subsequent operations could no longer reference columns by the qualified name.
        let input_alias = match &input {
            LogicalPlan::SubqueryAlias(sa) => Some(sa.alias.clone()),
            _ => None,
        };
        let schema = input.schema();
        // The alias names are collected first so that duplicates are rejected before the
        // expressions are resolved, which is the order in which Spark reports the errors.
        let aliases = aliases
            .into_iter()
            .map(|alias| match alias {
                spec::Expr::Alias {
                    name,
                    expr,
                    metadata,
                } => {
                    let name: String = name
                        .one()
                        .map_err(|_| PlanError::invalid("multi-alias for column"))?
                        .into();
                    Ok((name, *expr, metadata))
                }
                _ => Err(PlanError::invalid("alias expression expected for column")),
            })
            .collect::<PlanResult<Vec<_>>>()?;
        // Names that differ only in case are duplicates, and the first one in alphabetical
        // order is reported.
        let mut folded = aliases
            .iter()
            .map(|(name, _, _)| self.fold_identifier(name))
            .collect::<Vec<_>>();
        folded.sort();
        let duplicate = folded.windows(2).find_map(|names| match names {
            [a, b] if a == b => Some(a),
            _ => None,
        });
        if let Some(name) = duplicate {
            let name = name.replace('`', "``");
            return Err(PlanError::AnalysisError(format!(
                "[COLUMN_ALREADY_EXISTS] The column `{name}` already exists. \
                 Choose another name or rename the existing column."
            )));
        }
        let aliases = {
            let mut results: Vec<AliasEntry> = Vec::with_capacity(aliases.len());
            for (name, expr, metadata) in aliases {
                let expr = self.resolve_expression(expr, schema, state).await?;
                results.push((name, expr, metadata));
            }
            results
        };
        let names = Self::get_field_names(schema, state)?;
        // An alias is appended only when it matches no existing column, which is not the same as
        // the alias not having replaced one: when two aliases match the same column, the first
        // one replaces it and the other one is discarded instead of being appended.
        let matched = aliases
            .iter()
            .map(|(name, ..)| {
                names
                    .iter()
                    .any(|column| self.match_identifier(name, column))
            })
            .collect::<Vec<_>>();
        let mut expr = schema
            .columns()
            .into_iter()
            .zip(names)
            .map(|(column, name)| {
                // The alias name replaces the name of the column that it matches.
                match aliases
                    .iter()
                    .find(|(alias, ..)| self.match_identifier(alias, &name))
                {
                    Some((alias, expr, metadata)) => {
                        self.added_column(alias, expr, metadata, schema)
                    }
                    None => Ok(NamedExpr::new(vec![name], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        for ((name, e, metadata), matched) in aliases.iter().zip(matched) {
            if !matched {
                expr.push(self.added_column(name, e, metadata, schema)?);
            }
        }
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;
        let result = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?);
        if let Some(alias) = input_alias {
            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(result),
                alias,
            )?))
        } else {
            Ok(result)
        }
    }

    /// Builds the named expression for a column added or replaced by `withColumn`.
    /// Spark always sets explicit metadata for such columns, defaulting to empty metadata, so the
    /// metadata of the expression is never inherited. Empty metadata is only attached when there
    /// is something to override, since metadata that the physical schema does not have would
    /// otherwise make it differ from the logical one.
    fn added_column(
        &self,
        name: &str,
        expr: &Expr,
        metadata: &Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
    ) -> PlanResult<NamedExpr> {
        let named = NamedExpr::new(vec![name.to_string()], expr.clone());
        let metadata = match metadata {
            Some(metadata) if !metadata.is_empty() => metadata.clone(),
            _ if expr.metadata(schema)?.is_empty() => return Ok(named),
            _ => vec![(spec::SPARK_METADATA_JSON_KEY.to_string(), "{}".to_string())],
        };
        Ok(named.with_metadata(metadata))
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
            .map(|ident| ident.as_ref().to_string())
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
                    field_info.name().to_string(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // The column name is resolved as an attribute reference, so an ambiguous name is an error.
        // Only a column whose name matches exactly is replaced, though, because the resolver
        // renames the attribute to the requested name, so an attribute resolved from a name that
        // differs in case is no longer equal to the one in the output of the plan.
        for name in &cols_to_change {
            if self
                .resolve_optional_column(schema, name, None, state)?
                .is_none()
            {
                let candidates = existing_cols_info
                    .iter()
                    .map(|(_, _, name)| name.as_str())
                    .collect::<Vec<_>>();
                let object = spec::ObjectName::parse_attribute(name)
                    .unwrap_or_else(|| spec::ObjectName::bare(name.as_str()));
                return Err(unresolved_column_fields_error(&object, &candidates));
            }
        }

        let cols_to_change_set: HashSet<&str> =
            cols_to_change.iter().map(|name| name.as_str()).collect();

        let replace_exprs = existing_cols_info
            .into_iter()
            .map(|(column_expr, column_type, column_name)| {
                let expr = if cols_to_change.is_empty()
                    || cols_to_change_set.contains(column_name.as_str())
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
