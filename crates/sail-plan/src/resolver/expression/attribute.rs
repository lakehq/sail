use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::{LambdaVariable, ScalarFunction};
use datafusion_expr::{ScalarUDF, col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array_struct_field::ArrayStructField;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunctionInput};
use crate::function::get_built_in_function;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

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
                .resolve_lambda_parameter(first.as_ref())
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
        let local_schema = &state.get_local_schema(schema);
        if let Some((name, expr)) =
            self.resolve_field_or_nested_field(&name, plan_id, local_schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_hidden_field(&name, plan_id, local_schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        // Spark resolves literal function names (e.g. `current_date`) that the output does
        // not have to the functions, before missing attributes and outer references.
        if plan_id.is_none() && Self::is_literal_function_name(&name) {
            let function_name = name.parts()[0].as_ref().to_ascii_lowercase();
            if function_name == "grouping__id" {
                // TODO: Resolve the Hive grouping ID name to Spark's grouping aggregate.
                return Err(PlanError::analysis(format!(
                    "attribute {name:?} is missing from the schema: cannot resolve attribute"
                )));
            }
            let function_name = match function_name.as_str() {
                "user" | "session_user" => "current_user",
                name => name,
            };
            let expr = get_built_in_function(function_name)?(ScalarFunctionInput {
                arguments: vec![],
                function_context: FunctionContextInput {
                    argument_display_names: &[],
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            })?;
            let name = match function_name {
                "current_time" => "current_time(6)".to_string(),
                name => format!("{name}()"),
            };
            return Ok(NamedExpr::new(vec![name], expr));
        }
        let missing_input_schemas = state
            .get_missing_input_schemas(schema)
            .unwrap_or(&[])
            .to_vec();
        // A projected struct with an invalid nested path shadows any older struct
        // of the same name. Only an absent root can be recovered from a descendant.
        if !missing_input_schemas.is_empty()
            && self
                .missing_input_attribute_root_fails(&name, plan_id, local_schema, state)
                .is_some()
        {
            return Err(PlanError::analysis(format!(
                "attribute {name:?} is missing from the schema: cannot resolve attribute"
            )));
        }
        // Spark discards all tentative bindings to a descendant output where resolution
        // fails and continues with deeper outputs, then outer references.
        // TODO: Spark resolves again in later analyzer iterations, so a name bound only by
        // a discarded output can still bind there once the failing name resolves deeper.
        // DataFrame column references (plan IDs) are never discarded in Spark either.
        for (index, schema) in missing_input_schemas.iter().enumerate().skip(1) {
            if let Some((name, expr)) = self
                .resolve_field_or_nested_field(&name, plan_id, schema, state)
                .inspect_err(|_| state.discard_missing_input_schema(index))?
            {
                return Ok(NamedExpr::new(vec![name], expr));
            }
            if let Some(fails) =
                self.missing_input_attribute_root_fails(&name, plan_id, schema, state)
            {
                // Spark keeps the bindings to this output if the nested field can be
                // extracted without failing (a map value or an array item).
                // TODO: Extract map values and array items by a dotted name as Spark does.
                if fails {
                    state.discard_missing_input_schema(index);
                }
                return Err(PlanError::analysis(format!(
                    "attribute {name:?} is missing from the schema: cannot resolve attribute"
                )));
            }
            if let Some((name, expr)) = self
                .resolve_hidden_field(&name, plan_id, schema, state)
                .inspect_err(|_| state.discard_missing_input_schema(index))?
            {
                return Ok(NamedExpr::new(vec![name], expr));
            }
        }
        let Some(outer_schema) = state.get_outer_query_schema().cloned() else {
            return Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute"
            )));
        };
        match self.resolve_outer_field(&name, &outer_schema, state)? {
            Some((name, expr)) => Ok(NamedExpr::new(vec![name], expr)),
            None => Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute or outer attribute"
            ))),
        }
    }

    /// Returns whether extracting the nested field fails for every attribute root in the
    /// schema that the name refers to, or `None` if the schema has no such root.
    fn missing_input_attribute_root_fails(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> Option<bool> {
        Self::generate_qualified_nested_field_candidates(name.parts())
            .iter()
            .rev()
            .find_map(|(q, root, inner)| {
                schema
                    .iter()
                    .filter(|(qualifier, field)| {
                        qualifier_matches(q.as_ref(), *qualifier, self.config.case_sensitive)
                            && state.get_field_info(field.name()).is_ok_and(|info| {
                                !info.is_hidden()
                                    && info.matches(root.as_ref(), plan_id)
                                    && (!self.config.case_sensitive || info.name() == root.as_ref())
                            })
                    })
                    .map(|(_, field)| self.nested_field_extraction_fails(field.data_type(), inner))
                    .reduce(|a, b| a && b)
            })
    }

    /// Returns whether Spark fails to extract the nested field from a value of the data type.
    /// Map values and array items resolve before coercion, but extracting a later
    /// field can still fail for the resulting type.
    // TODO: Spark 4.2 propagates NullType through extraction via `applyOrNull`.
    // Support that behavior without changing Spark 3.5–4.1 missing-input recovery.
    fn nested_field_extraction_fails<T: AsRef<str>>(
        &self,
        data_type: &DataType,
        inner: &[T],
    ) -> bool {
        let [name, remaining @ ..] = inner else {
            return false;
        };
        match data_type {
            DataType::Struct(fields) => {
                match find_struct_field(fields, name.as_ref(), self.config.case_sensitive) {
                    Ok(Some(field)) => {
                        self.nested_field_extraction_fails(field.data_type(), remaining)
                    }
                    _ => true,
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => match field.data_type() {
                DataType::Struct(fields) => {
                    match find_struct_field(fields, name.as_ref(), self.config.case_sensitive) {
                        Ok(Some(child)) => {
                            let item = Field::new_list_field(child.data_type().clone(), true);
                            self.nested_field_extraction_fails(
                                &DataType::List(Arc::new(item)),
                                remaining,
                            )
                        }
                        _ => true,
                    }
                }
                _ => self.nested_field_extraction_fails(field.data_type(), remaining),
            },
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type()
                    && let Some(value) = fields.get(1)
                {
                    self.nested_field_extraction_fails(value.data_type(), remaining)
                } else {
                    false
                }
            }
            _ => true,
        }
    }

    fn is_literal_function_name(name: &spec::ObjectName) -> bool {
        // The names in Spark's `LiteralFunctionResolution`.
        const LITERAL_FUNCTION_NAMES: [&str; 7] = [
            "current_date",
            "current_timestamp",
            "current_time",
            "current_user",
            "user",
            "session_user",
            "grouping__id",
        ];
        matches!(name.parts(), [part] if LITERAL_FUNCTION_NAMES
            .iter()
            .any(|x| part.as_ref().eq_ignore_ascii_case(x)))
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let candidates = Self::generate_qualified_nested_field_candidates(name.parts());
        // Spark chooses the most-qualified matching root before extracting nested
        // fields, using its configured resolver for both the qualifier and root name.
        // A missing field in that root cannot select a less-qualified root.
        for (q, root, inner) in candidates.iter().rev() {
            // Reuse the matching fields for extraction rather than scanning the
            // schema again after choosing a qualifier. Stop even if extraction is
            // unsupported: a matching root still shadows less-qualified roots.
            let mut fields = schema
                .iter()
                .filter(|(qualifier, field)| {
                    qualifier_matches(q.as_ref(), *qualifier, self.config.case_sensitive)
                        && state.get_field_info(field.name()).is_ok_and(|info| {
                            !info.is_hidden()
                                && info.matches(root.as_ref(), plan_id)
                                && (!self.config.case_sensitive || info.name() == root.as_ref())
                        })
                })
                .peekable();
            if fields.peek().is_none() {
                continue;
            }
            let mut resolved = fields
                .filter_map(|(qualifier, field)| {
                    match self.resolve_potentially_nested_field(
                        col((qualifier, field)),
                        field.data_type(),
                        inner,
                    ) {
                        Ok(Some(expr)) => {
                            let name = inner.last().unwrap_or(root).as_ref().to_string();
                            Some(Ok((name, expr)))
                        }
                        Ok(None) => self
                            .nested_field_extraction_fails(field.data_type(), inner)
                            .then(|| {
                                Err(PlanError::analysis(format!(
                                    "attribute {name:?} is missing from the schema: cannot resolve attribute"
                                )))
                            }),
                        Err(error) => Some(Err(error)),
                    }
                })
                .collect::<PlanResult<Vec<_>>>()?;
            if resolved.len() > 1 {
                return Err(PlanError::AnalysisError(format!(
                    "ambiguous attribute: {name:?}"
                )));
            }
            return Ok(resolved.pop());
        }
        Ok(None)
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
        let [name] = name.parts() else {
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
                if info.matches(name.as_ref(), plan_id) {
                    Some((
                        name.as_ref().to_string(),
                        expr::Expr::Column(Column::new_unqualified(field.name())),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
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
                        qualifier_matches(q.as_ref(), qualifier, self.config.case_sensitive)
                            && info.matches(name.as_ref(), None)
                            && (!self.config.case_sensitive || info.name() == name.as_ref())
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
                    let Some(field) =
                        find_struct_field(fields, name.as_ref(), self.config.case_sensitive)?
                    else {
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
                    let Some(child) =
                        find_struct_field(fields, name.as_ref(), self.config.case_sensitive)?
                    else {
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

/// Returns whether the qualifier matches the target qualifier.
/// Note that the match is not symmetric, so please ensure the arguments are in the correct order.
pub(super) fn qualifier_matches(
    qualifier: Option<&TableReference>,
    target: Option<&TableReference>,
    case_sensitive: bool,
) -> bool {
    let names_equal = |left: &str, right: &str| {
        if case_sensitive {
            left == right
        } else {
            left.eq_ignore_ascii_case(right)
        }
    };
    let table_matches = |table: &str| {
        target
            .map(|x| x.table())
            .is_some_and(|x| names_equal(x, table))
    };
    let schema_matches = |schema: &str| {
        target
            .and_then(|x| x.schema())
            .is_some_and(|x| names_equal(x, schema))
    };
    let catalog_matches = |catalog: &str| {
        target
            .and_then(|x| x.catalog())
            .is_some_and(|x| names_equal(x, catalog))
    };
    match qualifier {
        Some(TableReference::Bare { table }) => table_matches(table),
        Some(TableReference::Partial { schema, table }) => {
            schema_matches(schema) && table_matches(table)
        }
        Some(TableReference::Full {
            catalog,
            schema,
            table,
        }) => catalog_matches(catalog) && schema_matches(schema) && table_matches(table),
        None => true,
    }
}

/// Returns the struct field selected by Spark's configured name resolver.
/// More than one match is an ambiguous reference.
fn find_struct_field<'a>(
    fields: &'a Fields,
    name: &str,
    case_sensitive: bool,
) -> PlanResult<Option<&'a FieldRef>> {
    let mut matches = fields.iter().filter(|x| {
        if case_sensitive {
            x.name() == name
        } else {
            x.name().eq_ignore_ascii_case(name)
        }
    });
    let field = matches.next();
    if matches.next().is_some() {
        return Err(PlanError::AnalysisError(format!(
            "ambiguous reference to the field: {name}"
        )));
    }
    Ok(field)
}
