use std::sync::Arc;

use datafusion_common::arrow::datatypes::{FieldRef, Fields};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::UNNAMED_TABLE;
use sail_common::spec;
use sail_common::utils::string::{equals_ignore_case, to_lowercase};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::attribute::{
    quote_identifier_name, quote_identifier_part, unresolved_column_fields_error,
};
use crate::resolver::state::{FieldInfo, PlanResolverState};

impl PlanResolver<'_> {
    /// Matches an identifier against another one the way the Spark analyzer resolver does.
    pub(super) fn match_identifier(&self, a: &str, b: &str) -> bool {
        if self.config.case_sensitive {
            a == b
        } else {
            equals_ignore_case(a, b)
        }
    }

    /// Matches an identifier against an attribute name. Spark looks up an attribute in a map
    /// keyed by the lowercased name and then filters the candidates with the resolver, so the
    /// name must match both ways. This is stricter than the resolver alone, which folds through
    /// the simple case mappings while lowercasing uses the full ones.
    pub(super) fn match_attribute(&self, a: &str, b: &str) -> bool {
        if self.config.case_sensitive {
            return a == b;
        }
        // Identifiers are overwhelmingly ASCII, where lowercasing and the resolver agree with
        // each other, so the folded names only have to be built beyond ASCII. This matters
        // because the comparison runs once per schema field for every name being resolved.
        if a.is_ascii() && b.is_ascii() {
            return a.eq_ignore_ascii_case(b);
        }
        self.fold_identifier(a) == self.fold_identifier(b) && self.match_identifier(a, b)
    }

    /// Matches a field against an attribute name, and checks that the field belongs to the plan
    /// when a plan ID is given.
    pub(super) fn match_field(&self, info: &FieldInfo, name: &str, plan_id: Option<i64>) -> bool {
        self.match_attribute(info.name(), name) && info.has_plan_id(plan_id)
    }

    /// Matches a lambda parameter name against a reference to it. Spark canonicalizes lambda
    /// variable names by lowercasing them instead of using the resolver, so this is the same rule
    /// that detects duplicate names.
    pub(super) fn match_lambda_parameter(&self, a: &str, b: &str) -> bool {
        self.fold_identifier(a) == self.fold_identifier(b)
    }

    /// Matches a qualifier against the qualifier of a field when resolving an attribute reference.
    /// Spark looks up the attribute in a map keyed by the lowercased qualifier and name, and then
    /// filters the candidates with the resolver, so the qualifier is matched the same way as the
    /// name.
    pub(super) fn match_attribute_qualifier(
        &self,
        qualifier: Option<&TableReference>,
        target: Option<&TableReference>,
    ) -> bool {
        self.match_qualifier(qualifier, target, |a, b| self.match_attribute(a, b))
    }

    /// Matches a qualifier against the qualifier of a field when expanding a wildcard. Spark
    /// matches the target of a wildcard with the resolver alone, unlike an attribute reference.
    pub(super) fn match_wildcard_qualifier(
        &self,
        qualifier: Option<&TableReference>,
        target: Option<&TableReference>,
    ) -> bool {
        self.match_qualifier(qualifier, target, |a, b| self.match_identifier(a, b))
    }

    /// Returns whether the qualifier matches the target qualifier.
    /// Note that the match is not symmetric, so please ensure the arguments are in the correct
    /// order: the qualifier may name fewer parts than the target.
    fn match_qualifier(
        &self,
        qualifier: Option<&TableReference>,
        target: Option<&TableReference>,
        matches: impl Fn(&str, &str) -> bool,
    ) -> bool {
        let table_matches =
            |table: &str| target.map(|x| x.table()).is_some_and(|x| matches(x, table));
        let schema_matches = |schema: &str| {
            target
                .and_then(|x| x.schema())
                .is_some_and(|x| matches(x, schema))
        };
        let catalog_matches = |catalog: &str| {
            target
                .and_then(|x| x.catalog())
                .is_some_and(|x| matches(x, catalog))
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

    /// Finds the struct field with the given name. The name is matched with the resolver, and
    /// more than one match is an error, which is why the field cannot simply be looked up.
    /// A field that matches no name is not an error here, since the caller may be considering
    /// more than one candidate for the same expression.
    pub(super) fn resolve_struct_field<'a>(
        &self,
        fields: &'a Fields,
        name: &str,
    ) -> PlanResult<Option<&'a FieldRef>> {
        let mut matched = fields
            .iter()
            .filter(|field| self.match_identifier(field.name(), name));
        let Some(field) = matched.next() else {
            return Ok(None);
        };
        let count = 1 + matched.count();
        if count > 1 {
            return Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field {}. \
                 It appears {count} times in the schema.",
                quote_identifier_name(name)
            )));
        }
        Ok(Some(field))
    }

    /// Folds an identifier so that duplicates can be detected. Spark lowercases the name here
    /// instead of using the resolver, so this is deliberately not [`Self::match_identifier`].
    pub(super) fn fold_identifier(&self, name: &str) -> String {
        if self.config.case_sensitive {
            name.to_string()
        } else {
            to_lowercase(name)
        }
    }

    pub(super) fn resolve_table_reference(
        &self,
        name: &spec::ObjectName,
    ) -> PlanResult<TableReference> {
        let names = name.parts();
        match names {
            [a] => Ok(TableReference::Bare {
                table: Arc::from(a.as_ref()),
            }),
            [a, b] => Ok(TableReference::Partial {
                schema: Arc::from(a.as_ref()),
                table: Arc::from(b.as_ref()),
            }),
            [a, b, c] => Ok(TableReference::Full {
                catalog: Arc::from(a.as_ref()),
                schema: Arc::from(b.as_ref()),
                table: Arc::from(c.as_ref()),
            }),
            _ => Err(PlanError::invalid(format!("table reference: {names:?}"))),
        }
    }

    pub(super) fn resolve_column_candidates(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        plan_id: Option<i64>,
        state: &PlanResolverState,
    ) -> Vec<Column> {
        self.column_candidates(schema, state, |info| self.match_field(info, name, plan_id))
    }

    /// Returns the columns whose name matches the resolver, which is what Spark uses for the
    /// operations that select the output columns by name, such as `drop`. This is more permissive
    /// than [`Self::resolve_column_candidates`], which resolves an attribute reference.
    pub(super) fn resolve_column_candidates_by_resolver(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> Vec<Column> {
        self.column_candidates(schema, state, |info| {
            self.match_identifier(info.name(), name)
        })
    }

    /// Returns the columns whose name matches the resolver, and fails when the name matches none.
    /// Spark keeps every match rather than rejecting an ambiguous name for the operations that
    /// select the output columns by name.
    pub(super) fn resolve_columns_by_resolver(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<Column>> {
        let columns = self.resolve_column_candidates_by_resolver(schema, name, state);
        if columns.is_empty() {
            return Err(PlanError::AnalysisError(format!(
                "[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column name \"{name}\" \
                 among ({}).",
                Self::get_field_names(schema, state)?.join(", ")
            )));
        }
        Ok(columns)
    }

    fn column_candidates(
        &self,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
        matches: impl Fn(&FieldInfo) -> bool,
    ) -> Vec<Column> {
        schema
            .iter()
            .filter(|(_, field)| {
                state
                    .get_field_info(field.name())
                    .is_ok_and(|info| !info.is_hidden() && matches(info))
            })
            .map(|x| x.into())
            .collect()
    }

    pub(super) fn resolve_optional_column(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        plan_id: Option<i64>,
        state: &PlanResolverState,
    ) -> PlanResult<Option<Column>> {
        let columns = self.resolve_column_candidates(schema, name, plan_id, state);
        if columns.len() > 1 {
            let mut references = columns
                .iter()
                .map(|x| match &x.relation {
                    Some(relation) if relation.table() != UNNAMED_TABLE => {
                        let qualifier = relation.to_string();
                        let parts = qualifier.split('.').chain(std::iter::once(name));
                        parts
                            .map(quote_identifier_part)
                            .collect::<Vec<_>>()
                            .join(".")
                    }
                    _ => quote_identifier_part(name),
                })
                .collect::<Vec<_>>();
            references.sort();
            return Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE] Reference {} is ambiguous, could be: [{}].",
                quote_identifier_part(name),
                references.join(", ")
            )));
        }
        if columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(columns.one()?.clone()))
        }
    }

    pub(super) fn resolve_one_column(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> PlanResult<Column> {
        if let Some(column) = self.resolve_optional_column(schema, name, None, state)? {
            return Ok(column);
        }
        // The name is the one the user wrote, so it is split the way a column reference is
        // before it is reported.
        let object =
            spec::ObjectName::parse_attribute(name).unwrap_or_else(|| spec::ObjectName::bare(name));
        Err(unresolved_column_fields_error(
            &object,
            &Self::get_field_names(schema, state)?,
        ))
    }

    pub(super) fn resolve_columns<T: AsRef<str>>(
        &self,
        schema: &DFSchemaRef,
        names: &[T],
        state: &PlanResolverState,
    ) -> PlanResult<Vec<Column>> {
        names
            .iter()
            .map(|name| self.resolve_one_column(schema, name.as_ref(), state))
            .collect::<PlanResult<Vec<Column>>>()
    }

    /// Returns the user-visible field names for a resolved schema. A hidden field is not part of
    /// the output a plan reports, so it never reaches a name that Spark suggests either.
    pub(super) fn get_field_names(
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<String>> {
        schema
            .fields()
            .iter()
            .filter_map(|field| match state.get_field_info(field.name()) {
                Ok(info) if info.is_hidden() => None,
                Ok(info) => Some(Ok(info.name().to_string())),
                Err(e) => Some(Err(e)),
            })
            .collect::<PlanResult<Vec<_>>>()
    }
}
