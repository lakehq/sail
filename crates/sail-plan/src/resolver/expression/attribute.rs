use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::{LambdaVariable, ScalarFunction};
use datafusion_expr::{ScalarUDF, UNNAMED_TABLE, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array_struct_field::ArrayStructField;
use sail_sql_analyzer::parser::parse_attribute_name;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

/// Builds the error Spark reports when a name matches more than one attribute.
/// A name that carries a plan ID comes from a DataFrame column object, and Spark reports it
/// with a different error condition than a name written in a query
/// (`QueryCompilationErrors.ambiguousColumnReferences` vs `ambiguousReferenceError`).
fn ambiguous_attribute_error(
    name: &spec::ObjectName,
    plan_id: Option<i64>,
    references: Vec<Vec<String>>,
) -> PlanError {
    if plan_id.is_some() {
        // The condition has four sentences in the catalog, and `ErrorClassesJSONReader` joins
        // them with a newline rather than with a space, so the message is written on four lines.
        return PlanError::AnalysisError(format!(
            "[AMBIGUOUS_COLUMN_REFERENCE] Column \"{}\" is ambiguous. It's because you joined \
             several DataFrame together, and some of these DataFrames are the same.\n\
             This column points to one of the DataFrames but Spark is unable to figure out \
             which one.\n\
             Please alias the DataFrames with different names via `DataFrame.alias` before \
             joining them,\n\
             and specify the column using qualified name, e.g. \
             `df.alias(\"a\").join(df.alias(\"b\"), col(\"a.id\") > col(\"b.id\"))`.",
            pretty_attribute(name)
        ));
    }
    let mut references = references
        .iter()
        .map(|x| quote_identifier_parts(x.iter().map(|x| x.as_str())))
        .collect::<Vec<_>>();
    references.sort_by_cached_key(|x| utf16_key(Some(x.as_str())));
    PlanError::AnalysisError(format!(
        "[AMBIGUOUS_REFERENCE] Reference {} is ambiguous, could be: [{}].",
        quote_identifier(name),
        references.join(", ")
    ))
}

/// Renders an object name the way Spark's `toSQLId` does.
fn quote_identifier(name: &spec::ObjectName) -> String {
    quote_identifier_parts(name.parts().iter().map(|x| x.as_ref()))
}

/// The parts of a qualifier, as the user wrote them. The reference is already split, so the parts
/// are read from it rather than from its rendering: splitting that on dots would cut a single part
/// that contains one, naming a qualifier nobody wrote.
///
/// A relation the user did not name carries DataFusion's placeholder qualifier, while the matching
/// attribute in Spark has no qualifier at all, so it contributes nothing.
pub(crate) fn qualifier_parts(relation: Option<&TableReference>) -> Vec<String> {
    match relation {
        Some(relation) if relation.table() != UNNAMED_TABLE => relation
            .catalog()
            .into_iter()
            .chain(relation.schema())
            .chain(std::iter::once(relation.table()))
            .map(|x| x.to_string())
            .collect(),
        _ => vec![],
    }
}

fn quote_identifier_parts<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    parts
        .map(quote_identifier_part)
        .collect::<Vec<_>>()
        .join(".")
}

/// Renders a name that reaches a message as a single string the way Spark's
/// `toSQLId(parts: String)` does, which parses the name before quoting each part
/// (`org.apache.spark.sql.errors.DataTypeErrorsBase#toSQLId`). A part of the name that contains a dot is therefore reported
/// as several quoted parts, and a name that is already quoted keeps its back quotes single. A
/// name the parser rejects is quoted whole, since its syntax has an error condition of its own.
pub(in crate::resolver) fn quote_identifier_name(name: &str) -> String {
    match parse_attribute_name(name) {
        Some(object) => quote_identifier(&object),
        None => quote_identifier_part(name),
    }
}

/// Quotes one part of an identifier as Spark's `QuotingUtils.quoteIdentifier` does, doubling the
/// back quotes it contains.
pub(crate) fn quote_identifier_part(part: &str) -> String {
    format!("`{}`", part.replace('`', "``"))
}

/// Quotes one part unless it is a plain identifier, as `QuotingUtils.quoteIfNeeded` does. This is
/// the rendering `UnresolvedAttribute.sql` uses, which is the name the suggestion is ordered by.
fn quote_if_needed(part: &str) -> String {
    let mut characters = part.chars();
    let plain = matches!(characters.next(), Some(x) if x.is_ascii_alphabetic() || x == '_')
        && characters.all(|x| x.is_ascii_alphanumeric() || x == '_');
    if plain {
        part.to_string()
    } else {
        quote_identifier_part(part)
    }
}

/// Renders an attribute the way `UnresolvedAttribute.name` does, which is what reaches the
/// message through `toSQLExpr`. Only a part that contains a dot is quoted, since that is the one
/// case where joining the parts would be ambiguous, and the back quotes it contains are not
/// doubled. This is a different rule from the fully quoted form used for a column name.
fn pretty_attribute(name: &spec::ObjectName) -> String {
    name.parts()
        .iter()
        .map(|x| {
            let part = x.as_ref();
            if part.contains('.') {
                format!("`{part}`")
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The sort key of a name, as a Java string compares: by UTF-16 code unit rather than by
/// character, which orders a name outside the BMP before one in the high part of it.
pub(in crate::resolver) fn utf16_key(name: Option<&str>) -> Option<Vec<u16>> {
    name.map(|x| x.encode_utf16().collect())
}

/// The edit distance Spark orders the suggested names by
/// (`org.apache.commons.text.similarity.LevenshteinDistance`). It walks a Java string, so the
/// units it counts are UTF-16 code units and a character outside the BMP counts as two.
fn edit_distance(left: &str, right: &str) -> usize {
    let right = right.encode_utf16().collect::<Vec<_>>();
    let mut row = (0..=right.len()).collect::<Vec<_>>();
    for (i, left_char) in left.encode_utf16().enumerate() {
        let mut previous = row[0];
        row[0] = i + 1;
        for (j, right_char) in right.iter().enumerate() {
            let substitution = previous + usize::from(left_char != *right_char);
            previous = row[j + 1];
            row[j + 1] = substitution.min(row[j] + 1).min(row[j + 1] + 1);
        }
    }
    row[right.len()]
}

/// Orders the names Spark suggests for an unresolved column, as
/// `StringUtils.orderSuggestedIdentifiersBySimilarity` does. A qualifier that every candidate
/// shares is stripped, since it is not what tells them apart.
///
/// The base the distance is measured against is passed in rather than derived from the name,
/// because Spark measures against a different string per call site: the analyzer renders the name
/// first, while `Project.reorderFields` uses the raw field name.
fn order_candidates_by_similarity(
    name: &spec::ObjectName,
    base: &str,
    candidates: Vec<Vec<String>>,
) -> Vec<String> {
    let parts = name.parts().len();
    let shared = |depth: usize| {
        let mut prefixes = candidates
            .iter()
            .map(|x| &x[..x.len().saturating_sub(depth)]);
        let first = prefixes.next();
        first.is_some_and(|first| prefixes.all(|x| x == first))
    };
    let stripped = if parts == 1 && shared(1) {
        1
    } else if parts <= 2 && shared(2) {
        2
    } else {
        usize::MAX
    };
    // The caller orders the candidates the way the analyzer reads them, since the sort by
    // distance below is stable and so never undoes that order.
    let mut candidates = candidates
        .into_iter()
        .map(|parts| {
            let start = parts.len().saturating_sub(stripped);
            quote_identifier_parts(parts[start..].iter().map(|x| x.as_str()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|x| edit_distance(x, base));
    candidates
}

/// Builds the error Spark reports when a name resolves to no column. The suggestion lists the
/// first few columns in scope, and its absence selects the other sub-condition, as
/// `QueryCompilationErrors.unresolvedColumnError` does.
pub(in crate::resolver) fn unresolved_column_error(
    name: &spec::ObjectName,
    schema: &DFSchemaRef,
    state: &PlanResolverState,
) -> PlanError {
    let mut candidates = schema
        .columns()
        .into_iter()
        .filter_map(|column| {
            let info = state.get_field_info(column.name()).ok()?;
            if info.is_hidden() {
                return None;
            }
            // The placeholder qualifier of a relation that has no name is not part of the name
            // of the column, so it must not reach the suggestion.
            let mut parts = qualifier_parts(column.relation.as_ref());
            parts.push(info.name().to_string());
            Some(parts)
        })
        .collect::<Vec<_>>();
    // The candidates of the schema reach the analyzer through `AttributeSet.toSeq`, which sorts
    // them by name.
    candidates.sort_by_cached_key(|parts| utf16_key(parts.last().map(|x| x.as_str())));
    // Inside a `HAVING` clause the aggregate expressions are names of their own, and they come
    // BEFORE the columns rather than through that sorted set, so one of them wins a tie in
    // distance. The grouping expressions are the columns themselves, which are already there.
    let grouping = state
        .get_grouping_for_having()
        .iter()
        .flat_map(|x| x.name.iter())
        .collect::<Vec<_>>();
    // The filter that carries the `HAVING` reads the OUTPUT of the aggregate, so a column of its
    // input that the aggregate does not carry through is not a name there and must not reach the
    // suggestion. Outside a `HAVING` the schema is the input itself and every column is a name.
    let candidates = if state.get_projections_for_having().is_empty() {
        candidates
    } else {
        candidates
            .into_iter()
            .filter(|parts| parts.last().is_some_and(|x| grouping.contains(&x)))
            .collect::<Vec<_>>()
    };
    let candidates = state
        .get_projections_for_having()
        .iter()
        .filter_map(|x| match x.name.as_slice() {
            [name] if !grouping.contains(&name) => Some(vec![name.clone()]),
            _ => None,
        })
        .chain(candidates)
        .fold(Vec::new(), |mut out, parts| {
            if !out.contains(&parts) {
                out.push(parts);
            }
            out
        });
    // The analyzer measures the distance against `a.sql`, the name rendered with `quoteIfNeeded`.
    let base = name
        .parts()
        .iter()
        .map(|x| quote_if_needed(x.as_ref()))
        .collect::<Vec<_>>()
        .join(".");
    let proposal = order_candidates_by_similarity(name, &base, candidates)
        .into_iter()
        .take(5)
        .collect::<Vec<_>>();
    let name = quote_identifier(name);
    if proposal.is_empty() {
        PlanError::AnalysisError(format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name {name} cannot be resolved."
        ))
    } else {
        PlanError::AnalysisError(format!(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
             name {name} cannot be resolved. Did you mean one of the following? [{}].",
            proposal.join(", ")
        ))
    }
}

/// Builds the unresolved column error for a name matched against a flat list of column names.
pub(in crate::resolver) fn unresolved_column_name_error(
    name: &spec::ObjectName,
    candidates: &[&str],
) -> PlanError {
    let candidates = candidates
        .iter()
        .map(|x| vec![x.to_string()])
        .collect::<Vec<_>>();
    // `Project.reorderFields` measures the distance against the raw field name instead, so the
    // back quotes of a name that would need them do not count towards it.
    let base = name
        .parts()
        .iter()
        .map(|x| x.as_ref())
        .collect::<Vec<&str>>()
        .join(".");
    let proposal = order_candidates_by_similarity(name, &base, candidates)
        .into_iter()
        .take(5)
        .collect::<Vec<_>>();
    let name = quote_identifier(name);
    if proposal.is_empty() {
        PlanError::AnalysisError(format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name {name} cannot be resolved."
        ))
    } else {
        PlanError::AnalysisError(format!(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
             name {name} cannot be resolved. Did you mean one of the following? [{}].",
            proposal.join(", ")
        ))
    }
}

/// Builds the unresolved column error that `Dataset.resolve` reports. Unlike the suggestion of a
/// name written in a query, this one lists every field of the schema, in order and untruncated.
pub(in crate::resolver) fn unresolved_column_fields_error<T: AsRef<str>>(
    name: &spec::ObjectName,
    fields: &[T],
) -> PlanError {
    // Unlike the suggestion of a name written in a query, this one has no sub-condition to fall
    // back to: it reports an empty list rather than the other condition.
    let name = quote_identifier(name);
    let proposal = fields
        .iter()
        .map(|x| quote_identifier_name(x.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");
    PlanError::AnalysisError(format!(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name \
         {name} cannot be resolved. Did you mean one of the following? [{proposal}]."
    ))
}

/// Builds the error Spark reports for a name that the attribute parser rejects. The name is the
/// one the user wrote, since there is nothing to quote it against yet.
pub(in crate::resolver) fn invalid_attribute_name_error(name: &str) -> PlanError {
    PlanError::AnalysisError(format!(
        "[INVALID_ATTRIBUTE_NAME_SYNTAX] Syntax error in the attribute name: {name}. Check that \
         backticks appear in pairs, a quoted string is a complete name part and use a backtick \
         only inside quoted name parts."
    ))
}

/// Builds the error Spark reports when `replace` is given a name that walks into a column.
pub(in crate::resolver) fn replace_nested_column_error(name: &spec::ObjectName) -> PlanError {
    PlanError::AnalysisError(format!(
        "[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN] The feature is not supported: The replace \
         function does not support nested column {}.",
        quote_identifier(name)
    ))
}

impl PlanResolver<'_> {
    pub(super) fn resolve_expression_attribute(
        &self,
        name: spec::ObjectName,
        plan_id: Option<i64>,
        is_metadata_column: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if is_metadata_column {
            return Err(PlanError::todo("resolve metadata column"));
        }
        // Lambda parameters shadow columns inside a lambda function body. SQL lambda
        // bodies reference parameters as plain attributes, so the lambda scope stack
        // is consulted first. A `plan_id` indicates an explicit DataFrame column
        // reference, which never refers to a lambda parameter.
        if plan_id.is_none()
            && let [first, rest @ ..] = name.parts()
            && let Some((declared, field)) = state
                .resolve_lambda_parameter(first.as_ref(), |a, b| self.match_lambda_parameter(a, b))
                .map(|(param, field)| (param.to_string(), field.cloned()))
        {
            let display = rest
                .last()
                .map(|x| x.as_ref())
                .unwrap_or(declared.as_str())
                .to_string();
            let mut expr = expr::Expr::LambdaVariable(LambdaVariable::new(declared, field));
            for part in rest {
                expr = expr::Expr::ScalarFunction(ScalarFunction::new_udf(
                    get_field(),
                    vec![expr, lit(part.as_ref().to_string())],
                ));
            }
            return Ok(NamedExpr::new(vec![display], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_grouping_for_having(), true)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_having(), true)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_field_or_nested_field(&name, plan_id, schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping(), false)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) = self.resolve_hidden_field(&name, plan_id, schema, state)? {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        // A name that carries a plan ID comes from a DataFrame column object, which Spark
        // reports on its own error condition instead of the one for a name in a query.
        if plan_id.is_some() {
            return Err(PlanError::AnalysisError(format!(
                "[CANNOT_RESOLVE_DATAFRAME_COLUMN] Cannot resolve dataframe column \"{}\". \
                 It's probably because of illegal references like `df1.select(df2.col(\"a\"))`.",
                pretty_attribute(&name)
            )));
        }
        let Some(outer_schema) = state.get_outer_query_schema().cloned() else {
            return Err(unresolved_column_error(&name, schema, state));
        };
        match self.resolve_outer_field(&name, &outer_schema, state)? {
            Some((name, expr)) => Ok(NamedExpr::new(vec![name], expr)),
            None => Err(unresolved_column_error(&name, schema, state)),
        }
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        // The analyzer tries the interpretations of the name from the longest qualifier down and
        // stops at the first one whose attribute matches anything, so a qualifier wins over a
        // struct that is named like it, and the struct field is never considered. Once the
        // attribute has matched, a part that names no field of what it reached is a missing field
        // rather than a name that did not resolve.
        for (qualifier_name, field_name, inner) in
            Self::generate_qualified_nested_field_candidates(name.parts())
                .into_iter()
                .rev()
        {
            let matched = schema
                .iter()
                .filter(|(qualifier, field)| {
                    let Ok(info) = state.get_field_info(field.name()) else {
                        return false;
                    };
                    !info.is_hidden()
                        && self.match_attribute_qualifier(qualifier_name.as_ref(), *qualifier)
                        && self.match_field(info, field_name.as_ref(), plan_id)
                })
                .collect::<Vec<_>>();
            let [(qualifier, field)] = matched.as_slice() else {
                if matched.is_empty() {
                    continue;
                }
                let references = matched
                    .iter()
                    .map(|(qualifier, _)| {
                        // A plan that the user did not name carries DataFusion's placeholder
                        // qualifier, while the matching attribute in Spark has no qualifier at
                        // all, so it must not reach the reference list.
                        let mut reference = qualifier_parts(*qualifier);
                        reference.push(field_name.as_ref().to_string());
                        reference
                    })
                    .collect();
                return Err(ambiguous_attribute_error(name, plan_id, references));
            };
            let column = col((*qualifier, *field));
            let Some(expr) =
                self.resolve_potentially_nested_field(column, field.data_type(), inner)?
            else {
                // Spark renders the base with `toSQLExpr`, which prints the name the user
                // wrote rather than the one the attribute resolved to.
                let base = field_name.as_ref().to_string();
                return match self.missing_struct_field_error(&base, field.data_type(), inner)? {
                    Some(error) => Err(error),
                    // The interpretation reached something this resolver cannot walk into, which
                    // is reported as a name that did not resolve, the way it always was.
                    None => Ok(None),
                };
            };
            let display = inner.last().unwrap_or(field_name).as_ref().to_string();
            return Ok(Some((display, expr)));
        }
        Ok(None)
    }

    /// The error for a part of the name that does not name a field of what it reached, as Spark
    /// reports it once the attribute itself has matched: a name that is not a field of the struct
    /// is a missing field, and a base that is not a complex type at all is a different error.
    fn missing_struct_field_error<T: AsRef<str>>(
        &self,
        base: &str,
        data_type: &DataType,
        inner: &[T],
    ) -> PlanResult<Option<PlanError>> {
        let mut base = base.to_string();
        let mut data_type = data_type.clone();
        for part in inner {
            let fields = match &data_type {
                DataType::Struct(fields) => fields.clone(),
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => match field.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => return self.invalid_extract_base_error(&base, &data_type).map(Some),
                },
                // A map is a complex type that Spark walks into by key. Sail does not reach it
                // through a dotted name, and that gap is reported the way it always was.
                DataType::Map(..) => return Ok(None),
                _ => return self.invalid_extract_base_error(&base, &data_type).map(Some),
            };
            match self.resolve_struct_field(&fields, part.as_ref()) {
                Ok(Some(field)) => {
                    // The base of the next step is the whole path walked so far, as Spark
                    // prints it, and not just the name of the field that was reached.
                    base = format!("{base}.{}", part.as_ref());
                    data_type = field.data_type().clone();
                }
                _ => {
                    let names = fields
                        .iter()
                        .map(|x| x.name().to_string())
                        .collect::<Vec<_>>();
                    return Ok(Some(Self::field_not_found_error(part.as_ref(), &names)));
                }
            }
        }
        Ok(None)
    }

    /// The error for a name that walks into something that is not a complex type.
    fn invalid_extract_base_error(
        &self,
        base: &str,
        data_type: &DataType,
    ) -> PlanResult<PlanError> {
        let rendered = self.spark_type_name(data_type)?;
        Ok(PlanError::AnalysisError(format!(
            "[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from \"{base}\". \
             Need a complex type [STRUCT, ARRAY, MAP] but got \"{rendered}\"."
        )))
    }

    fn field_not_found_error(name: &str, fields: &[String]) -> PlanError {
        let fields = fields
            .iter()
            .map(|x| quote_identifier_part(x))
            .collect::<Vec<_>>()
            .join(", ");
        PlanError::AnalysisError(format!(
            "[FIELD_NOT_FOUND] No such struct field {} in {}.",
            quote_identifier_part(name),
            fields
        ))
    }

    /// Resolves a name against the expressions of an aggregate.
    ///
    /// A name that matches more than one of them is an ambiguous reference when the aggregate has
    /// already been built and the name reads its output, which is what a `HAVING` does. While the
    /// grouping expressions are still being resolved there is no output to be ambiguous about:
    /// Spark keeps the first match there (`ResolveReferencesInAggregate.resolveGroupByAlias` uses
    /// `find`) and reports the query later on its own condition.
    ///
    /// TODO: keep the first match for the grouping instead of rejecting it, which needs the
    /// condition Spark reports afterwards. See `test_a_repeated_alias_in_a_group_by`.
    fn resolve_aggregate_field(
        &self,
        name: &spec::ObjectName,
        expressions: &[NamedExpr],
        ambiguity_is_a_reference: bool,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [name] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = expressions
            .iter()
            .filter_map(|expr| {
                let NamedExpr {
                    name: agg, expr, ..
                } = expr;
                match agg.as_slice() {
                    // The alias is looked up with the rule for an attribute reference rather than
                    // with the resolver alone, so a name that only the resolver would match, such
                    // as `ıd` against `Id`, does not resolve.
                    [agg] if self.match_attribute(agg, name.as_ref()) => {
                        Some((name.as_ref().to_string(), expr.clone()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            if ambiguity_is_a_reference {
                let references = vec![vec![name.as_ref().to_string()]; candidates.len()];
                return Err(ambiguous_attribute_error(
                    &spec::ObjectName::bare(name.as_ref()),
                    None,
                    references,
                ));
            }
            return Err(PlanError::AnalysisError(format!(
                "ambiguous aggregate expression: `{}`",
                name.as_ref()
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_hidden_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        // A hidden field is reachable by its name alone, and the key of a join also through the
        // qualifier of the side it came from, which is what tells `l.k` from `r.k` once the join
        // has merged them into one column.
        let (written, identifier) = match name.parts() {
            [identifier] => (None, identifier),
            [qualifier, identifier] => (Some(TableReference::bare(qualifier.as_ref())), identifier),
            _ => return Ok(None),
        };
        let mut candidates = schema
            .iter()
            .filter_map(|(qualifier, field)| {
                if !self.match_attribute_qualifier(written.as_ref(), qualifier) {
                    return None;
                }
                let Ok(info) = state.get_field_info(field.name()) else {
                    return None;
                };
                if !info.is_hidden() {
                    return None;
                }
                if self.match_field(info, identifier.as_ref(), plan_id) {
                    let mut reference = qualifier_parts(qualifier);
                    reference.push(identifier.as_ref().to_string());
                    Some((
                        reference,
                        identifier.as_ref().to_string(),
                        expr::Expr::Column(Column::new_unqualified(field.name())),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            let references = candidates
                .into_iter()
                .map(|(reference, _, _)| reference)
                .collect();
            return Err(ambiguous_attribute_error(name, plan_id, references));
        }
        Ok(candidates.pop().map(|(_, name, expr)| (name, expr)))
    }

    fn resolve_outer_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let candidates = Self::generate_qualified_field_candidates(name.parts());
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                if info.is_hidden() {
                    return vec![];
                }
                candidates
                    .iter()
                    .filter(|(q, name)| {
                        self.match_attribute_qualifier(q.as_ref(), qualifier)
                            && self.match_field(info, name.as_ref(), None)
                    })
                    .map(|(_, name)| {
                        (
                            name.as_ref().to_string(),
                            expr::Expr::OuterReferenceColumn(
                                field.clone(),
                                Column::new(qualifier.cloned(), field.name()),
                            ),
                        )
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            let references = candidates
                .iter()
                .map(|(reference, expr)| match expr {
                    expr::Expr::OuterReferenceColumn(_, column) => {
                        let mut parts = qualifier_parts(column.relation.as_ref());
                        parts.push(reference.clone());
                        parts
                    }
                    _ => vec![reference.clone()],
                })
                .collect();
            return Err(ambiguous_attribute_error(name, None, references));
        }
        Ok(candidates.pop())
    }

    pub(in crate::resolver) fn resolve_potentially_nested_field<T: AsRef<str>>(
        &self,
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
    ) -> PlanResult<Option<expr::Expr>> {
        match inner {
            [] => Ok(Some(expr)),
            [name, remaining @ ..] => match data_type {
                DataType::Struct(fields) => {
                    let Some(field) = self.resolve_struct_field(fields, name.as_ref())? else {
                        return Ok(None);
                    };
                    let args = vec![expr, lit(field.name().to_string())];
                    let expr =
                        expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                    self.resolve_potentially_nested_field(expr, field.data_type(), remaining)
                }
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => {
                    let DataType::Struct(fields) = field.data_type() else {
                        return Ok(None);
                    };
                    let Some(child) = self.resolve_struct_field(fields, name.as_ref())? else {
                        return Ok(None);
                    };
                    let expr = ScalarUDF::from(ArrayStructField::new())
                        .call(vec![expr, lit(child.name().to_string())]);
                    let item = Arc::new(Field::new_list_field(
                        child.data_type().clone(),
                        field.is_nullable() || child.is_nullable(),
                    ));
                    let data_type = match data_type {
                        DataType::List(_) => DataType::List(item),
                        DataType::LargeList(_) => DataType::LargeList(item),
                        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(item, *size),
                        _ => unreachable!("list data type matched above"),
                    };
                    self.resolve_potentially_nested_field(expr, &data_type, remaining)
                }
                _ => Ok(None),
            },
        }
    }

    fn generate_qualified_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T)> {
        match name {
            [n1] => vec![(None, n1)],
            [n1, n2] => vec![(Some(TableReference::bare(n1.as_ref())), n2)],
            [n1, n2, n3] => vec![(Some(TableReference::partial(n1.as_ref(), n2.as_ref())), n3)],
            [n1, n2, n3, n4] => vec![(
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
            )],
            _ => vec![],
        }
    }

    fn generate_qualified_nested_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T, &[T])> {
        let mut out = vec![];
        if let [n1, x @ ..] = name {
            out.push((None, n1, x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), n2, x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::partial(n1.as_ref(), n2.as_ref())),
                n3,
                x,
            ));
        }
        if let [n1, n2, n3, n4, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
                x,
            ));
        }
        out
    }
}
