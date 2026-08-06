use std::sync::Arc;

use datafusion_common::{Column, DFSchemaRef, TableReference};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::{FieldInfo, PlanResolverState};

/// Returns the simple uppercase mapping of a character, the way `Character.toUpperCase` does.
/// [`char::to_uppercase`] returns the full mapping, which may expand to several characters
/// (e.g. `ß` becomes `SS`), while Java leaves the character unchanged when it has no simple
/// mapping.
fn simple_uppercase(c: char) -> char {
    let mut chars = c.to_uppercase();
    match (chars.next(), chars.next()) {
        (Some(x), None) => x,
        _ => c,
    }
}

/// Returns the simple lowercase mapping of a character, the way `Character.toLowerCase` does.
/// See [`simple_uppercase`] for why the full mapping cannot be used.
fn simple_lowercase(c: char) -> char {
    // `İ` is the only character whose unconditional full lowercase mapping expands to more than
    // one character (`i` followed by a combining dot above) even though it does have a simple
    // mapping, so the rule below would otherwise leave it unchanged.
    if c == 'İ' {
        return 'i';
    }
    let mut chars = c.to_lowercase();
    match (chars.next(), chars.next()) {
        (Some(x), None) => x,
        _ => c,
    }
}

/// Compares two strings the way `String.equalsIgnoreCase` does, so that identifiers are folded
/// beyond ASCII. Note that this is not the same as comparing the lowercased strings, since the
/// case mappings of a character are not always symmetric.
/// The result can still differ from the JVM for the characters whose case mappings were added
/// after the Unicode version that the JVM implements, since the mappings come from the Rust
/// standard library here.
pub(super) fn equals_ignore_case(a: &str, b: &str) -> bool {
    let mut a = a.chars();
    let mut b = b.chars();
    loop {
        match (a.next(), b.next()) {
            (None, None) => return true,
            (Some(x), Some(y)) => {
                if x == y {
                    continue;
                }
                let (x, y) = (simple_uppercase(x), simple_uppercase(y));
                if x == y || simple_lowercase(x) == simple_lowercase(y) {
                    continue;
                }
                return false;
            }
            _ => return false,
        }
    }
}

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
        let table_matches = |table: &str| target.map(|x| x.table()).is_some_and(|x| matches(x, table));
        let schema_matches =
            |schema: &str| target.and_then(|x| x.schema()).is_some_and(|x| matches(x, schema));
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

    /// Folds an identifier so that duplicates can be detected. Spark lowercases the name here
    /// instead of using the resolver, so this is deliberately not [`Self::match_identifier`].
    pub(super) fn fold_identifier(&self, name: &str) -> String {
        if self.config.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
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
            return Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE] Reference {name} is ambiguous, found: {} matches",
                columns.len()
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
            Ok(column)
        } else {
            Err(PlanError::AnalysisError(format!(
                "[UNRESOLVED_COLUMN] Cannot find column {name}"
            )))
        }
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

    /// Formats column names the way Spark lists the candidates of an unresolved column error.
    pub(super) fn format_column_candidates<T: AsRef<str>>(names: &[T]) -> String {
        names
            .iter()
            .map(|name| format!("`{}`", name.as_ref().replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Returns the user-visible field names for a resolved schema.
    pub(super) fn get_field_names(
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<String>> {
        schema
            .fields()
            .iter()
            .map(|field| Ok(state.get_field_info(field.name())?.name().to_string()))
            .collect::<PlanResult<Vec<_>>>()
    }
}
