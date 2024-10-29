use std::sync::Arc;

use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion_common::{Column, DFSchema, DFSchemaRef, SchemaReference, TableReference};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::{FieldName, PlanResolverState};
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

impl PlanResolver<'_> {
    pub(super) fn resolve_schema_reference(
        &self,
        name: &spec::ObjectName,
    ) -> PlanResult<SchemaReference> {
        let names: Vec<&str> = name.into();
        match names[..] {
            [a] => Ok(SchemaReference::Bare {
                schema: Arc::from(a),
            }),
            [a, b] => Ok(SchemaReference::Full {
                catalog: Arc::from(a),
                schema: Arc::from(b),
            }),
            _ => Err(PlanError::invalid(format!("schema reference: {:?}", names))),
        }
    }

    pub(super) fn resolve_table_reference(
        &self,
        name: &spec::ObjectName,
    ) -> PlanResult<TableReference> {
        let names: Vec<&str> = name.into();
        match names[..] {
            [a] => Ok(TableReference::Bare {
                table: Arc::from(a),
            }),
            [a, b] => Ok(TableReference::Partial {
                schema: Arc::from(a),
                table: Arc::from(b),
            }),
            [a, b, c] => Ok(TableReference::Full {
                catalog: Arc::from(a),
                schema: Arc::from(b),
                table: Arc::from(c),
            }),
            _ => Err(PlanError::invalid(format!("table reference: {:?}", names))),
        }
    }

    pub(super) async fn resolve_table_schema(
        &self,
        table_reference: &TableReference,
        table_provider: &Arc<dyn TableProvider>,
        columns: &[spec::Identifier],
    ) -> PlanResult<SchemaRef> {
        let columns: Vec<&str> = columns.iter().map(|c| c.into()).collect();
        let schema = table_provider.schema();
        if columns.is_empty() {
            Ok(schema)
        } else {
            let df_schema =
                DFSchema::try_from_qualified_schema(table_reference.clone(), schema.as_ref())?;
            let fields = columns
                .into_iter()
                .map(|c| {
                    let column_index = df_schema
                        .index_of_column_by_name(None, c)
                        .ok_or_else(|| PlanError::invalid(format!("Column {c} not found")))?;
                    Ok(schema.field(column_index).clone())
                })
                .collect::<PlanResult<Vec<_>>>()?;
            Ok(SchemaRef::new(Schema::new(Fields::from(fields))))
        }
    }

    pub(super) fn get_resolved_columns(
        &self,
        schema: &DFSchemaRef,
        unresolved: Vec<&str>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Column>> {
        unresolved
            .iter()
            .map(|unresolved_name| self.get_resolved_column(schema, unresolved_name, state))
            .collect::<PlanResult<Vec<Column>>>()
    }

    pub(super) fn maybe_get_resolved_column(
        &self,
        schema: &DFSchemaRef,
        unresolved: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<Column>> {
        let cols = schema.columns();
        let matches: Vec<_> = cols
            .iter()
            .filter(|column| {
                state
                    .get_field_name(column.name())
                    .ok()
                    .map_or(false, |n| n.eq_ignore_ascii_case(unresolved))
            })
            .collect();
        if matches.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE] Reference `{unresolved}` is ambiguous, found: {} matches",
                matches.len()
            )));
        }
        if matches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(matches.one()?.clone()))
        }
    }

    pub(super) fn get_resolved_column(
        &self,
        schema: &DFSchemaRef,
        unresolved: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<Column> {
        let resolved = self.maybe_get_resolved_column(schema, unresolved, state)?;
        if let Some(column) = resolved {
            Ok(column)
        } else {
            Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE] Reference `{unresolved}` is ambiguous, found: 0 matches"
            )))
        }
    }

    #[allow(dead_code)]
    pub(super) fn get_unresolved_field_index(
        &self,
        field_names: &[FieldName],
        unresolved: &str,
    ) -> PlanResult<usize> {
        let idx = field_names
            .iter()
            .position(|field_name| field_name.eq_ignore_ascii_case(unresolved))
            .ok_or_else(|| {
                PlanError::AnalysisError(format!(
                    "[AMBIGUOUS_REFERENCE] Reference `{unresolved}` is ambiguous, found: 0 matches"
                ))
            })?;
        Ok(idx)
    }

    #[allow(dead_code)]
    pub(super) fn get_unresolved_schema_index(
        &self,
        schema: &DFSchemaRef,
        unresolved: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<usize> {
        let field_names = state.get_field_names(schema.inner())?;
        self.get_unresolved_field_index(&field_names, unresolved)
    }

    #[allow(dead_code)]
    pub(super) fn get_unresolved_schema_indices(
        &self,
        schema: &DFSchemaRef,
        unresolved: Vec<&str>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<usize>> {
        let field_names = state.get_field_names(schema.inner())?;
        unresolved
            .into_iter()
            .map(|unresolved_name| self.get_unresolved_field_index(&field_names, unresolved_name))
            .collect::<PlanResult<Vec<usize>>>()
    }
}
