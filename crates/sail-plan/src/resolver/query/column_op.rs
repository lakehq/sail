use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::{Column, DFSchemaRef, ExprSchema, ScalarValue};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, Projection, SubqueryAlias, cast, col, lit,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_attribute_name;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::{
    invalid_attribute_name_error, quote_identifier_name, replace_nested_column_error,
    unresolved_column_fields_error, unresolved_column_name_error, utf16_key,
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
        let expr = self.rewrite_named_expressions(expr, schema, state)?;
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
            // The reconciliation refuses to narrow a column that can be null to a field that
            // cannot, and it does so before it looks at the type.
            //
            // TODO: Spark applies the same rule to the fields of a struct it walks into, naming
            // the whole path, while a nested target is rebuilt here with a single cast. See
            // `test_to_schema_rejects_a_nested_field_narrowed_to_non_nullable`.
            if input_field.is_nullable() && !target_field.is_nullable() {
                return Err(PlanError::AnalysisError(format!(
                    "[NULLABLE_COLUMN_OR_FIELD] Column or field {} is nullable while it's \
                     required to be non-nullable.",
                    quote_identifier_name(target_name)
                )));
            }
            let column = Expr::Column(Column::from((input_qualifier, input_field)));
            let expr = if input_field.data_type() == target_field.data_type() {
                column
            } else {
                column.cast_to(target_field.data_type(), &input.schema())?
            };
            // The column takes the name of the target field but keeps its plan IDs, so a
            // `df["col"]` reference still resolves on the output.
            //
            // TODO: Spark drops that identity for a column it rebuilds, and withholding the IDs is
            // not the fix either. See `test_to_schema_drops_the_identity_of_a_column_it_has_to_cast`.
            let plan_ids = state.get_field_info(input_field.name())?.plan_ids();
            let field_id = state.register_field_name(target_name.clone());
            for plan_id in plan_ids {
                state.register_plan_id_for_field(&field_id, plan_id)?;
            }
            // An attribute the reconciliation leaves alone keeps the qualifier it was read
            // through, so a reference such as `df.alias("t").to(schema).select("t.a")` still
            // resolves. A cast or a rebuilt container is a new expression instead.
            let qualifier = match &expr {
                Expr::Column(_) if !input_field.data_type().is_nested() => input_qualifier.cloned(),
                _ => None,
            };
            projected_exprs.push(expr.alias_qualified(qualifier, field_id));
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
        let expr = self.rewrite_named_expressions(expr, input.schema(), state)?;
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
        // order is reported. Spark sorts them with `sortBy`, which is `String.compareTo`, so the
        // order is the one of the UTF-16 code units and not that of the UTF-8 bytes.
        let mut folded = aliases
            .iter()
            .map(|(name, _, _)| self.fold_identifier(name))
            .collect::<Vec<_>>();
        folded.sort_by_cached_key(|x| utf16_key(Some(x.as_str())));
        let duplicate = folded.windows(2).find_map(|names| match names {
            [a, b] if a == b => Some(a),
            _ => None,
        });
        if let Some(name) = duplicate {
            return Err(PlanError::AnalysisError(format!(
                "[COLUMN_ALREADY_EXISTS] The column {} already exists. \
                 Choose another name or rename the existing column.",
                quote_identifier_name(name)
            )));
        }
        let names = Self::get_field_names(schema, state)?;
        // A column takes the first alias that matches it, so an alias that another one already
        // matched is discarded. It is discarded before its expression is resolved, which is what
        // makes an expression that cannot be resolved harmless there.
        let selected = names
            .iter()
            .filter_map(|column| {
                aliases
                    .iter()
                    .position(|(name, ..)| self.match_identifier(name, column))
            })
            .collect::<HashSet<_>>();
        let aliases = {
            let mut results: Vec<AliasEntry> = Vec::with_capacity(aliases.len());
            for (index, (name, expr, metadata)) in aliases.into_iter().enumerate() {
                if !selected.contains(&index)
                    && names
                        .iter()
                        .any(|column| self.match_identifier(&name, column))
                {
                    continue;
                }
                let expr = self.resolve_expression(expr, schema, state).await?;
                results.push((name, expr, metadata));
            }
            results
        };
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
                //
                // TODO: replacing in place takes the column out of the output, and Sail has no
                // equivalent of Spark's missing-attribute pull-up for `Filter`, only for `Sort`.
                // See `test_a_filter_by_a_replaced_column_reads_the_original`.
                match aliases
                    .iter()
                    .find(|(alias, ..)| self.match_identifier(alias, &name))
                {
                    Some((alias, expr, metadata)) => {
                        Self::added_column(alias, expr, metadata, schema)
                    }
                    None => NamedExpr::new(vec![name], Expr::Column(column)),
                }
            })
            .collect::<Vec<_>>();
        for ((name, e, metadata), matched) in aliases.iter().zip(matched) {
            if !matched {
                expr.push(Self::added_column(name, e, metadata, schema));
            }
        }
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, input.schema(), state)?;
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
    fn added_column(
        name: &str,
        expr: &Expr,
        metadata: &Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
    ) -> NamedExpr {
        let named = NamedExpr::new(vec![name.to_string()], expr.clone());
        if let Some(metadata) = metadata
            && !metadata.is_empty()
        {
            return named.with_metadata(metadata.clone());
        }
        // A column `withColumn` builds is a new one rather than the one it replaces, and Spark
        // gives it explicit metadata, empty unless the caller asked for some, so the metadata of
        // the expression is never inherited. The empty override is only attached when the
        // expression has Spark metadata to hide, since an override of its own keeps a projection
        // from being merged into the one below it.
        let inherited = expr.metadata(schema).unwrap_or_default();
        let overridden = inherited
            .inner()
            .get(spec::SPARK_METADATA_JSON_KEY)
            .is_some_and(|x| x != "{}");
        if !overridden {
            return named;
        }
        named.with_metadata(vec![(
            spec::SPARK_METADATA_JSON_KEY.to_string(),
            "{}".to_string(),
        )])
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
        let resolved_names = cols_to_change
            .iter()
            .map(|name| {
                // The name is parsed before it is looked up, so a malformed one is a syntax error
                // rather than a column that could not be found.
                let object =
                    parse_attribute_name(name).ok_or_else(|| invalid_attribute_name_error(name))?;
                let [leading, rest @ ..] = object.parts() else {
                    return Err(invalid_attribute_name_error(name));
                };
                let column = self.resolve_optional_column(schema, leading.as_ref(), None, state)?;
                // Only a top-level column can be replaced, so a name that walks into one is
                // rejected on its own condition. A walk that does not resolve falls through, since
                // reporting the missing field is a separate gap.
                if let Some(column) = &column
                    && !rest.is_empty()
                    && let Ok(field) = schema.field_from_column(column)
                    && self
                        .resolve_potentially_nested_field(
                            Expr::Column(column.clone()),
                            field.data_type(),
                            rest,
                        )?
                        .is_some()
                {
                    return Err(replace_nested_column_error(&object));
                }
                if column.is_none() || !rest.is_empty() {
                    let candidates = existing_cols_info
                        .iter()
                        .map(|(_, _, name)| name.as_str())
                        .collect::<Vec<_>>();
                    return Err(unresolved_column_fields_error(&object, &candidates));
                }
                Ok(leading.as_ref().to_string())
            })
            .collect::<PlanResult<Vec<_>>>()?;

        let cols_to_change_set: HashSet<&str> =
            resolved_names.iter().map(|name| name.as_str()).collect();

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
            self.rewrite_named_expressions(replace_exprs, input.schema(), state)?,
            Arc::new(input),
        )?))
    }
}
