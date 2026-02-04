use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{col, expr, lit, ScalarUDF};
use datafusion_functions::core::get_field;
use regex::Regex;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::attribute::qualifier_matches;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_wildcard(
        &self,
        target: Option<spec::ObjectName>,
        plan_id: Option<i64>,
        wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if plan_id.is_some() {
            return Err(PlanError::todo("wildcard with plan ID"));
        }
        if wildcard_options == Default::default() {
            return match target {
                Some(target) => {
                    self.resolve_wildcard_or_nested_field_wildcard(&target, schema, state)
                }
                None => Ok(NamedExpr::new(
                    vec!["*".to_string()],
                    #[allow(deprecated)]
                    expr::Expr::Wildcard {
                        qualifier: None,
                        options: Default::default(),
                    },
                )),
            };
        }
        self.resolve_expression_wildcard_with_options(target, wildcard_options, schema, state)
            .await
    }

    async fn resolve_expression_wildcard_with_options(
        &self,
        target: Option<spec::ObjectName>,
        wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // Expand wildcard options into an explicit `MultiExpr`.
        let qualifier = target
            .as_ref()
            .map(|x| self.resolve_table_reference(x))
            .transpose()?;

        // Start from visible columns that match the (optional) qualifier.
        let mut items: Vec<(String, expr::Expr)> = vec![];
        for (q, field) in schema.iter() {
            if !qualifier_matches(qualifier.as_ref(), q) {
                continue;
            }
            let info = state.get_field_info(field.name())?;
            if info.is_hidden() {
                continue;
            }
            items.push((info.name().to_string(), col((q, field))));
        }

        // Apply ILIKE filter (case-insensitive, `%`/`_` wildcards).
        if let Some(pattern) = wildcard_options.ilike_pattern {
            let re = Self::ilike_pattern_to_regex(&pattern)?;
            items.retain(|(name, _)| re.is_match(name));
        }

        // Apply EXCLUDE / EXCEPT (both treated as "drop columns" here).
        let mut excluded: HashSet<String> = HashSet::new();
        if let Some(cols) = wildcard_options.exclude_columns {
            excluded.extend(cols.into_iter().map(|x| x.as_ref().to_ascii_lowercase()));
        }
        if let Some(cols) = wildcard_options.except_columns {
            excluded.extend(cols.into_iter().map(|x| x.as_ref().to_ascii_lowercase()));
        }
        if !excluded.is_empty() {
            items.retain(|(name, _)| !excluded.contains(&name.to_ascii_lowercase()));
        }

        // Apply REPLACE: replace the expression for an existing output column.
        if let Some(replacements) = wildcard_options.replace_columns {
            // Map lower-case output name -> index for O(1) replacement lookup.
            let mut index: HashMap<String, usize> = HashMap::with_capacity(items.len());
            for (i, (name, _)) in items.iter().enumerate() {
                index.insert(name.to_ascii_lowercase(), i);
            }
            for elem in replacements {
                let target_name = elem.column_name.as_ref();
                let Some(i) = index.get(&target_name.to_ascii_lowercase()).copied() else {
                    return Err(PlanError::invalid(format!(
                        "cannot replace column '{target_name}': no such column in wildcard expansion"
                    )));
                };
                let expression = self
                    .resolve_expression(*elem.expression, schema, state)
                    .await?;
                items[i] = (target_name.to_string(), expression);
            }
        }

        // Apply RENAME.
        if let Some(renames) = wildcard_options.rename_columns {
            let mut index: HashMap<String, usize> = HashMap::with_capacity(items.len());
            for (i, (name, _)) in items.iter().enumerate() {
                index.insert(name.to_ascii_lowercase(), i);
            }
            for elem in renames {
                let from = elem.identifier.as_ref();
                let to = elem.alias.as_ref();
                let Some(i) = index.get(&from.to_ascii_lowercase()).copied() else {
                    return Err(PlanError::invalid(format!(
                        "cannot rename column '{from}': no such column in wildcard expansion"
                    )));
                };
                items[i].0 = to.to_string();
            }
        }

        // Validate output names are unique after transformations.
        let mut seen: HashSet<String> = HashSet::with_capacity(items.len());
        for (name, _) in &items {
            let key = name.to_ascii_lowercase();
            if !seen.insert(key) {
                return Err(PlanError::invalid(format!(
                    "wildcard expansion produced duplicate column name '{name}'"
                )));
            }
        }

        let (names, exprs): (Vec<_>, Vec<_>) = items.into_iter().unzip();
        Ok(NamedExpr::new(
            names,
            ScalarUDF::from(MultiExpr::new()).call(exprs),
        ))
    }

    fn ilike_pattern_to_regex(pattern: &str) -> PlanResult<Regex> {
        // ILIKE is case-insensitive LIKE with `%` (any string) and `_` (any char).
        // We keep it simple here (no explicit ESCAPE handling).
        let mut out = String::with_capacity(pattern.len() + 8);
        out.push_str("(?i)^");
        for ch in pattern.chars() {
            match ch {
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                _ => out.push_str(&regex::escape(&ch.to_string())),
            }
        }
        out.push('$');
        Regex::new(&out).map_err(|e| PlanError::invalid(format!("invalid ILIKE pattern: {e}")))
    }

    fn resolve_wildcard_or_nested_field_wildcard(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        for (q, remaining) in Self::generate_qualified_wildcard_candidates(name.parts()) {
            if remaining.is_empty()
                && schema
                    .iter()
                    .any(|(qualifier, _)| qualifier_matches(q.as_ref(), qualifier))
            {
                return Ok(NamedExpr::new(
                    vec!["*".to_string()],
                    #[allow(deprecated)]
                    expr::Expr::Wildcard {
                        qualifier: q,
                        options: Default::default(),
                    },
                ));
            }
        }

        let candidates = Self::generate_qualified_wildcard_candidates(name.parts())
            .into_iter()
            .flat_map(|(q, name)| match name {
                [] => vec![],
                [column, inner @ ..] => schema
                    .iter()
                    .filter_map(|(qualifier, field)| {
                        let Ok(info) = state.get_field_info(field.name()) else {
                            return None;
                        };
                        if qualifier_matches(q.as_ref(), qualifier)
                            && info.matches(column.as_ref(), None)
                        {
                            Self::resolve_nested_field_wildcard(
                                col((q.as_ref(), field)),
                                field.data_type(),
                                inner,
                            )
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        candidates
            .one()
            .map_err(|_| PlanError::AnalysisError(format!("cannot resolve wildcard: {name:?}")))
    }

    fn resolve_nested_field_wildcard<T: AsRef<str>>(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
    ) -> Option<NamedExpr> {
        let DataType::Struct(fields) = data_type else {
            return None;
        };
        match inner {
            [] => {
                let (names, exprs) = fields
                    .iter()
                    .map(|field| {
                        let name = field.name().to_string();
                        let args = vec![expr.clone(), lit(name.clone())];
                        (
                            name,
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args)),
                        )
                    })
                    .unzip();
                Some(NamedExpr::new(
                    names,
                    ScalarUDF::from(MultiExpr::new()).call(exprs),
                ))
            }
            [name, remaining @ ..] => fields
                .iter()
                .find(|x| x.name().eq_ignore_ascii_case(name.as_ref()))
                .and_then(|field| {
                    let args = vec![expr, lit(field.name().to_string())];
                    let expr =
                        expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                    Self::resolve_nested_field_wildcard(expr, field.data_type(), remaining)
                }),
        }
    }

    fn generate_qualified_wildcard_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &[T])> {
        let mut out = vec![(None, name)];
        if let [n1, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::partial(n1.as_ref(), n2.as_ref())), x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                x,
            ));
        }
        out
    }
}
