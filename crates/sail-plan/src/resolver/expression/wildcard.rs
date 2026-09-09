use std::collections::VecDeque;

use arrow::datatypes::DataType;
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::sql::{Ident, ObjectName, ObjectNamePart};
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{EmptyRelation, Expr, LogicalPlan, ScalarUDF, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::attribute::qualifier_matches;
use crate::resolver::state::PlanResolverState;

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
        match target {
            Some(target) if wildcard_options == Default::default() => {
                self.resolve_wildcard_or_nested_field_wildcard(&target, schema, state)
            }
            _ => {
                let qualifier = target
                    .map(|x| self.resolve_table_reference(&x))
                    .transpose()?;
                let options = self
                    .resolve_wildcard_options(wildcard_options, schema, state)
                    .await?;
                self.expand_schema_wildcard(qualifier.as_ref(), &options, schema, state)
            }
        }
    }

    fn resolve_wildcard_or_nested_field_wildcard(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        for (q, remaining) in Self::generate_qualified_wildcard_candidates(name.parts()) {
            if remaining.is_empty() {
                let in_input = schema
                    .iter()
                    .any(|(qualifier, _)| qualifier_matches(q.as_ref(), qualifier));
                let in_outer = state.get_outer_query_schema().is_some_and(|outer_schema| {
                    outer_schema
                        .iter()
                        .any(|(qualifier, _)| qualifier_matches(q.as_ref(), qualifier))
                });
                if in_input || in_outer {
                    return self.expand_schema_wildcard(
                        q.as_ref(),
                        &Default::default(),
                        schema,
                        state,
                    );
                }
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

    fn expand_schema_wildcard(
        &self,
        qualifier: Option<&TableReference>,
        options: &expr::WildcardOptions,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let in_input = match qualifier {
            Some(expected) => schema.iter().any(|(q, _)| q == Some(expected)),
            None => !schema.fields().is_empty(),
        };
        // Sail represents duplicate USING join keys as hidden fields, so wildcard
        // expansion only needs the schema and field visibility from the resolver.
        // FIXME: wildcard options do not take into account opaque field IDs.
        let expanded = match qualifier {
            Some(qualifier) if in_input => {
                expand_qualified_wildcard(qualifier, schema, Some(options))?
            }
            Some(_) => vec![],
            None => expand_wildcard(
                schema,
                &LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: schema.clone(),
                }),
                Some(options),
            )?,
        };
        let mut names = vec![];
        let mut expressions = vec![];
        for expression in expanded {
            let Expr::Column(column) = expression else {
                return Err(PlanError::invalid(
                    "column expected for expanded wildcard expression",
                ));
            };
            let info = state.get_field_info(column.name())?;
            if !info.is_hidden() {
                names.push(info.name().to_string());
                expressions.push(Expr::Column(column));
            }
        }
        if !in_input && let Some(outer_schema) = state.get_outer_query_schema() {
            for (q, field) in outer_schema.iter() {
                if qualifier.is_some_and(|expected| q != Some(expected)) {
                    continue;
                }
                let info = state.get_field_info(field.name())?;
                if !info.is_hidden() {
                    names.push(info.name().to_string());
                    expressions.push(Expr::OuterReferenceColumn(
                        field.clone(),
                        Column::new(q.cloned(), field.name()),
                    ));
                }
            }
        }
        Ok(NamedExpr::new(
            names,
            ScalarUDF::from(MultiExpr::new()).call(expressions),
        ))
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

    async fn resolve_wildcard_options(
        &self,
        wildcard_options: spec::WildcardOptions,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::WildcardOptions> {
        fn make_ident(value: impl Into<String>) -> Ident {
            Ident {
                value: value.into(),
                quote_style: None,
                span: String::new(),
            }
        }

        fn make_object_name(value: impl Into<String>) -> ObjectName {
            ObjectName(vec![ObjectNamePart::Identifier(make_ident(value))])
        }

        let ilike = wildcard_options
            .ilike_pattern
            .map(|x| expr::IlikeSelectItem { pattern: x });
        let exclude = wildcard_options
            .exclude_columns
            .map(|x| {
                let exclude = if x.len() > 1 {
                    expr::ExcludeSelectItem::Multiple(x.into_iter().map(make_object_name).collect())
                } else if let Some(x) = x.into_iter().next() {
                    expr::ExcludeSelectItem::Single(make_object_name(x))
                } else {
                    return Err(PlanError::invalid(
                        "exclude columns must have at least one column",
                    ));
                };
                Ok(exclude)
            })
            .transpose()?;
        let except = wildcard_options
            .except_columns
            .map(|x| {
                let except = if x.len() > 1 {
                    let mut deque = VecDeque::from(x);
                    let first_element = deque.pop_front().ok_or_else(|| {
                        PlanError::invalid("except columns must have at least one column")
                    })?;
                    let additional_elements = deque.into_iter().map(make_ident).collect();
                    expr::ExceptSelectItem {
                        first_element: make_ident(first_element),
                        additional_elements,
                    }
                } else if let Some(x) = x.into_iter().next() {
                    expr::ExceptSelectItem {
                        first_element: make_ident(x),
                        additional_elements: vec![],
                    }
                } else {
                    return Err(PlanError::invalid(
                        "except columns must have at least one column",
                    ));
                };
                Ok(except)
            })
            .transpose()?;
        let replace = match wildcard_options.replace_columns {
            Some(x) => {
                let mut items = Vec::with_capacity(x.len());
                let mut planned_expressions = Vec::with_capacity(x.len());
                for elem in x.into_iter() {
                    let expression = self
                        .resolve_expression(*elem.expression, schema, state)
                        .await?;
                    let item = expr::ReplaceSelectElement {
                        expr: expression.to_string(),
                        column_name: make_ident(elem.column_name),
                        as_keyword: elem.as_keyword,
                    };
                    items.push(item);
                    planned_expressions.push(expression);
                }
                Some(expr::PlannedReplaceSelectItem {
                    items,
                    planned_expressions,
                })
            }
            None => None,
        };
        let rename = wildcard_options
            .rename_columns
            .map(|x| {
                let rename = if x.len() > 1 {
                    expr::RenameSelectItem::Multiple(
                        x.into_iter()
                            .map(|x| {
                                format!(
                                    "{} AS {}",
                                    String::from(x.identifier),
                                    String::from(x.alias)
                                )
                            })
                            .collect(),
                    )
                } else if let Some(x) = x.into_iter().next() {
                    expr::RenameSelectItem::Single(format!(
                        "{} AS {}",
                        String::from(x.identifier),
                        String::from(x.alias)
                    ))
                } else {
                    return Err(PlanError::invalid(
                        "exclude columns must have at least one column",
                    ));
                };
                Ok(rename)
            })
            .transpose()?;
        Ok(expr::WildcardOptions {
            ilike,
            exclude,
            except,
            replace,
            rename,
        })
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
