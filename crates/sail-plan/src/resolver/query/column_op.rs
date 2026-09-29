use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::TreeNode;
use datafusion_common::{Column, DFSchemaRef};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Projection, cast, col, lit};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::explode::Explode;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_sql_analyzer::parser::parse_attribute_name;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::{
    invalid_attribute_name_error, quote_identifier_name, replace_nested_column_error,
    unresolved_column_fields_error, utf16_key,
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
        Self::rename_query_output(input, columns, state)
    }

    /// TODO: this reads every target field out of the input by name, which diverges from Spark's
    ///   `Project.reorderFields` in several ways: a nullable target field the input lacks is
    ///   filled with NULL rather than refused, the name is matched by the resolver instead of
    ///   ignoring the case of ASCII letters only, a name that matches two columns is
    ///   `AMBIGUOUS_COLUMN_OR_FIELD`, a nested field is reordered by name as well, and a column
    ///   that is not cast keeps its qualifier and its plan id. Its own change is PR #2610, which
    ///   carries the measurements and the tests, so this one leaves the behavior as it is.
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
        let columns = input.schema().columns();
        let mut names = Self::get_field_names(input.schema(), state)?;
        // Each rename is applied to the output of the previous one. A name that matches no
        // column is ignored.
        let mut renamed = vec![false; names.len()];
        for (from, to) in rename_columns_map {
            let (from, to) = (from.as_ref(), to.as_ref());
            for (name, renamed) in names.iter_mut().zip(renamed.iter_mut()) {
                if self.match_identifier(name, from) {
                    *name = to.to_string();
                    *renamed = true;
                }
            }
        }
        // A column a rename matched becomes an alias, which is an attribute of its own and has no
        // qualifier; every other column is passed on as it was and keeps the one it had
        // (`UnresolvedStarWithColumnsRenames.expandStar`). A rename that matches the name the
        // column already has still builds the alias, so what decides this is whether the name was
        // matched and not whether it changed -- `rewrite_named_expressions`'s own same-name check
        // cannot tell the two apart, so the root and the plan ids are carried over here instead,
        // only for the columns a rename did not match.
        let expr = columns
            .iter()
            .zip(names)
            .zip(renamed)
            .map(|((column, name), renamed)| -> PlanResult<Expr> {
                let field_id = state.register_field_name(name);
                if !renamed {
                    let info = state.get_field_info(&column.name)?;
                    let plan_ids = info.plan_ids().collect::<Vec<_>>();
                    for plan_id in plan_ids {
                        state.register_plan_id_for_field(&field_id, plan_id)?;
                    }
                    state.register_root_for_field(&field_id, &column.name)?;
                }
                let alias = Expr::Column(column.clone()).alias(field_id);
                Ok(if renamed {
                    alias
                } else if let Expr::Alias(alias) = alias {
                    Expr::Alias(datafusion_expr::expr::Alias {
                        relation: column.relation.clone(),
                        ..alias
                    })
                } else {
                    alias
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
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

        // A key that a `USING` join or a natural join hid is still reachable by its qualified
        // name while the new expression is resolved, and stays out of the output
        // (`LogicalPlan.metadataOutput`), so the input is resolved with its hidden fields and
        // only the visible ones are projected.
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
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
        // A hidden field is reachable by name but is not a column of the input: no alias
        // replaces one and none of them is passed through.
        let mut visible = Vec::new();
        for column in schema.columns() {
            let info = state.get_field_info(&column.name)?;
            if !info.is_hidden() {
                let name = info.name().to_string();
                visible.push((column, name));
            }
        }
        let names = visible
            .iter()
            .map(|(_, name)| name.clone())
            .collect::<Vec<_>>();
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
                // One name cannot take the whole list of columns a star stands for
                // (`ResolveStar`, `invalidStarUsageError`).
                if matches!(expr, spec::Expr::UnresolvedStar { .. }) {
                    return Err(PlanError::AnalysisError(
                        "[INVALID_USAGE_OF_STAR_OR_REGEX] Invalid usage of '*' in expression \
                         `alias`."
                            .to_string(),
                    ));
                }
                let resolved = self.resolve_named_expression(expr, schema, state).await?;
                // Spark's ExtractGenerator permits a generator only at the root of a
                // projected expression, after removing aliases. Check before rewriting
                // generators, which would otherwise hide their original placement.
                let contains_generator = |expr: &Expr| {
                    expr.exists(|node| {
                        Ok(matches!(node, Expr::ScalarFunction(function)
                            if function.func.inner().is::<Explode>()))
                    })
                };
                let mut root = &resolved.expr;
                while let Expr::Alias(alias) = root {
                    root = &alias.expr;
                }
                let nested = match root {
                    Expr::ScalarFunction(function) if function.func.inner().is::<Explode>() => {
                        let mut nested = false;
                        for argument in &function.args {
                            if contains_generator(argument)? {
                                nested = true;
                                break;
                            }
                        }
                        nested
                    }
                    other => contains_generator(other)?,
                };
                if nested {
                    // TODO: Spark names the expression the generator sits in, rendered as SQL
                    //   (`"(explode(array(a, b)) + 1)"`), where this names the column instead.
                    //   `select` accepts the same expression rather than refusing it, since the
                    //   check lives here and not where a projection is built.
                    return Err(PlanError::AnalysisError(format!(
                        "[UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS] The generator is not \
                         supported: nested in expressions \"{}\".",
                        resolved.name.join(", ")
                    )));
                }
                results.push((name, resolved.expr, metadata));
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
        // A column the input passes through keeps its qualifier, and every other output is a new
        // name, which has none, however it is built (`Alias.qualifier`). So the qualifier is
        // decided here, where a column that is only passed through is told apart from one the
        // operation builds, rather than read off the expression, which a copy of a column and a
        // column itself leave looking the same.
        let mut qualifiers = Vec::with_capacity(visible.len());
        let mut expr = Vec::with_capacity(visible.len());
        for (column, name) in visible {
            // The alias name replaces the name of the column that it matches, and a `Filter`
            // (or `Sort`, etc.) over this projection can still recover the replaced column:
            // `MissingInputBoundaries`/`add_missing_inputs` pull it back up from the input it
            // was read from.
            match aliases
                .iter()
                .find(|(alias, ..)| self.match_identifier(alias, &name))
            {
                Some((alias, e, metadata)) => {
                    qualifiers.push(None);
                    expr.push(Self::added_column(alias, e, metadata, schema));
                }
                None => {
                    qualifiers.push(column.relation.clone());
                    expr.push(NamedExpr::new(vec![name], Expr::Column(column)));
                }
            }
        }
        for ((name, e, metadata), matched) in aliases.iter().zip(matched) {
            if !matched {
                qualifiers.push(None);
                expr.push(Self::added_column(name, e, metadata, schema));
            }
        }
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        // One name per column the generator outputs, and `withColumn` gives exactly one
        // (`GeneratorResolution.makeGeneratorOutput`).
        for named in &expr {
            if let Expr::ScalarFunction(function) = &named.expr
                && function.func.inner().is::<MultiExpr>()
                && named.name.len() != function.args.len()
            {
                return Err(PlanError::AnalysisError(format!(
                    "[UDTF_ALIAS_NUMBER_MISMATCH] The number of aliases supplied in the AS clause \
                     does not match the number of columns output by the UDTF. Expected {} \
                     aliases, but got {}. Please ensure that the number of aliases provided \
                     matches the number of columns output by the UDTF.",
                    function.args.len(),
                    named.name.join(",")
                )));
            }
        }
        let expr = self.rewrite_multi_expr(expr)?;
        // An aggregate turns the projection into an aggregation without grouping, as it does for
        // `select`, so the columns passed through are refused there unless they are aggregated.
        // Every column it outputs is then an aggregate rather than a column of the input, so none
        // of them keeps a qualifier either.
        if Self::contains_aggregate(&expr) {
            return self.rewrite_aggregate(input, expr, vec![], None, false, state);
        }
        qualifiers.resize(expr.len(), None);
        let expr = self.rewrite_named_expressions(expr, input.schema(), state)?;
        let expr = expr
            .into_iter()
            .zip(qualifiers)
            .map(|(e, qualifier)| match (e, qualifier) {
                (Expr::Alias(e), Some(relation)) => Expr::Alias(datafusion_expr::expr::Alias {
                    relation: Some(relation),
                    ..e
                }),
                (e, _) => e,
            })
            .collect::<Vec<_>>();
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
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
        // A qualified name is resolved before the fields a join hid are removed, so that it can
        // still reach a key the join hid.
        let hidden = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = self.remove_hidden_fields(hidden.clone(), state)?;
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
                    field.name().to_string(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // The column name is resolved as an attribute reference, so an ambiguous name is an error.
        // Spark then keeps the output attributes the resolved ones are EQUAL to
        // (`DataFrameNaFunctions.replace0`), and that equality takes both the identity of the
        // attribute and its name. The identity is what picks one side of a join for `l.a`. The name
        // is why a name that differs in case replaces nothing: the resolver renames the attribute
        // to the requested name, so it is no longer equal to the one in the output of the plan.
        // Each name therefore yields the identity of the column it resolves to, and only when the
        // requested name is exactly the name of that column.
        let resolved_names = cols_to_change
            .iter()
            .map(|name| {
                // The name is parsed before it is looked up, so a malformed one is a syntax error
                // rather than a column that could not be found.
                let object =
                    parse_attribute_name(name).ok_or_else(|| invalid_attribute_name_error(name))?;
                let unresolved = || {
                    let candidates = existing_cols_info
                        .iter()
                        .map(|(_, _, name, _)| name.as_str())
                        .collect::<Vec<_>>();
                    unresolved_column_fields_error(&object, &candidates)
                };
                // The same column selected twice is one attribute under two outputs, so every
                // one of them is replaced, with the same equality: a name that differs in case
                // from the column replaces none of its outputs.
                if let Some(ids) = self.resolve_repeated_column(&object, &input, state)? {
                    let requested = object.parts().last().map(|x| x.as_ref());
                    let mut exact = Vec::new();
                    for id in ids {
                        if Some(state.get_field_info(&id)?.name()) == requested {
                            exact.push(id);
                        }
                    }
                    return Ok(exact);
                }
                let [leading, rest @ ..] = object.parts() else {
                    return Err(invalid_attribute_name_error(name));
                };
                if rest.is_empty() {
                    let Some(column) =
                        self.resolve_optional_column(schema, leading.as_ref(), None, state)?
                    else {
                        return Err(unresolved());
                    };
                    let exact = state.get_field_info(column.name())?.name() == leading.as_ref();
                    return Ok(if exact {
                        vec![column.name().to_string()]
                    } else {
                        vec![]
                    });
                }
                // A longer name is resolved in full before anything decides what to do with it,
                // so the leading part is tried as a qualifier before it is tried as a column. Only
                // a top-level column can be replaced, so a name that reaches anything else is
                // rejected on its own condition.
                match self.resolve_column_reference(&object, hidden.schema(), state)? {
                    Some((_, Expr::Column(column))) => {
                        let info = state.get_field_info(column.name())?;
                        // A key a join hid is not in the output `replace0` goes over, so it is
                        // reached but nothing is replaced.
                        let requested = rest.last().map(|x| x.as_ref()).unwrap_or_default();
                        Ok(if !info.is_hidden() && info.name() == requested {
                            vec![column.name().to_string()]
                        } else {
                            vec![]
                        })
                    }
                    Some(_) => Err(replace_nested_column_error(&object)),
                    None => Err(unresolved()),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;

        let cols_to_change_set: HashSet<&str> = resolved_names
            .iter()
            .flatten()
            .map(|id| id.as_str())
            .collect();

        let replace_exprs = existing_cols_info
            .into_iter()
            .map(|(column_expr, column_type, column_name, column_id)| {
                let expr = if cols_to_change.is_empty()
                    || cols_to_change_set.contains(column_id.as_str())
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
