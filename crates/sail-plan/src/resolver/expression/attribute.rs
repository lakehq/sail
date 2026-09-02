use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::{LambdaVariable, ScalarFunction};
use datafusion_expr::{ScalarUDF, UNNAMED_TABLE, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array_struct_field::ArrayStructField;

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
    references: Vec<String>,
) -> PlanError {
    if plan_id.is_some() {
        return PlanError::AnalysisError(format!(
            "[AMBIGUOUS_COLUMN_REFERENCE] Column \"{}\" is ambiguous. It's because you joined \
             several DataFrame together, and some of these DataFrames are the same. This column \
             points to one of the DataFrames but Spark is unable to figure out which one. \
             Please alias the DataFrames with different names via `DataFrame.alias` before \
             joining them, and specify the column using qualified name.",
            name.parts()
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>()
                .join(".")
        ));
    }
    let mut references = references
        .iter()
        .map(|x| quote_identifier_parts(x.split('.')))
        .collect::<Vec<_>>();
    references.sort();
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

fn quote_identifier_parts<'a>(parts: impl Iterator<Item = &'a str>) -> String {
    parts
        .map(|x| format!("`{x}`"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Builds the error Spark reports when a name resolves to no column. The suggestion lists the
/// first few columns in scope, and its absence selects the other sub-condition, as
/// `QueryCompilationErrors.unresolvedColumnError` does.
fn unresolved_column_error(
    name: &spec::ObjectName,
    schema: &DFSchemaRef,
    state: &PlanResolverState,
) -> PlanError {
    let proposal = schema
        .columns()
        .into_iter()
        .filter_map(|column| {
            let info = state.get_field_info(column.name()).ok()?;
            if info.is_hidden() {
                return None;
            }
            Some(match column.relation {
                Some(relation) => format!("`{relation}`.`{}`", info.name()),
                None => format!("`{}`", info.name()),
            })
        })
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
            self.resolve_aggregate_field(&name, state.get_grouping_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_field_or_nested_field(&name, plan_id, schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping())?
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
                name.parts()
                    .iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>()
                    .join(".")
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
        let candidates = Self::generate_qualified_nested_field_candidates(name.parts());
        let candidates = schema
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
                    .filter_map(|(q, name, inner)| {
                        if self.match_attribute_qualifier(q.as_ref(), qualifier)
                            && self.match_field(info, name.as_ref(), plan_id)
                        {
                            let expr = match self.resolve_potentially_nested_field(
                                col((qualifier, field)),
                                field.data_type(),
                                inner,
                            ) {
                                Ok(Some(expr)) => expr,
                                Ok(None) => return None,
                                Err(e) => return Some(Err(e)),
                            };
                            // A plan that the user did not name carries DataFusion's placeholder
                            // qualifier, while the matching attribute in Spark has no qualifier
                            // at all, so it must not reach the reference list.
                            let reference = match qualifier {
                                Some(qualifier) if qualifier.table() != UNNAMED_TABLE => {
                                    format!("{qualifier}.{}", info.name())
                                }
                                _ => info.name().to_string(),
                            };
                            let name = inner.last().unwrap_or(name).as_ref().to_string();
                            Some(Ok((reference, name, expr)))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        // A field that cannot be extracted only fails the interpretation that reaches it, since
        // another one may still resolve the name. The error is reported only when no
        // interpretation succeeded, which is the order in which Spark resolves the name.
        let (mut candidates, errors): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(Result::is_ok);
        if candidates.is_empty() {
            if let Some(error) = errors.into_iter().next() {
                error?;
            }
            return Ok(None);
        }
        if candidates.len() > 1 {
            let references = candidates
                .into_iter()
                .filter_map(|x| x.ok().map(|(reference, _, _)| reference))
                .collect();
            return Err(ambiguous_attribute_error(name, plan_id, references));
        }
        candidates
            .pop()
            .map(|x| x.map(|(_, name, expr)| (name, expr)))
            .transpose()
    }

    fn resolve_aggregate_field(
        &self,
        name: &spec::ObjectName,
        expressions: &[NamedExpr],
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
                    [agg] if agg.eq_ignore_ascii_case(name.as_ref()) => {
                        Some((name.as_ref().to_string(), expr.clone()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous aggregate expression: {name:?}"
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
        let [identifier] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = schema
            .iter()
            .filter_map(|(qualifier, field)| {
                if qualifier.is_some() {
                    return None;
                }
                let Ok(info) = state.get_field_info(field.name()) else {
                    return None;
                };
                if !info.is_hidden() {
                    return None;
                }
                if self.match_field(info, identifier.as_ref(), plan_id) {
                    Some((
                        info.name().to_string(),
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
            return Err(PlanError::AnalysisError(format!(
                "ambiguous outer attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_potentially_nested_field<T: AsRef<str>>(
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
