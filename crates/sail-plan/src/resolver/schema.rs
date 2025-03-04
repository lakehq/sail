use std::sync::Arc;

use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::{Column, DFSchemaRef, SchemaReference, TableReference};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

impl PlanResolver<'_> {
    pub(super) fn resolve_schema_reference(
        &self,
        name: &spec::ObjectName,
    ) -> PlanResult<SchemaReference> {
        let names = name.parts();
        match names {
            [a] => Ok(SchemaReference::Bare {
                schema: Arc::from(a.as_ref()),
            }),
            [a, b] => Ok(SchemaReference::Full {
                catalog: Arc::from(a.as_ref()),
                schema: Arc::from(b.as_ref()),
            }),
            _ => Err(PlanError::invalid(format!("schema reference: {:?}", names))),
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
            _ => Err(PlanError::invalid(format!("table reference: {:?}", names))),
        }
    }

    pub(super) async fn resolve_schema_projection<T: AsRef<str>>(
        &self,
        schema: SchemaRef,
        columns: &[T],
    ) -> PlanResult<SchemaRef> {
        let fields = columns
            .iter()
            .map(|column| {
                let column = column.as_ref();
                let matches = schema
                    .fields()
                    .iter()
                    .filter(|f| f.name().eq_ignore_ascii_case(column))
                    .collect::<Vec<_>>();
                if matches.is_empty() {
                    Err(PlanError::invalid(format!("column {column} not found")))
                } else {
                    matches
                        .one()
                        .map_err(|_| PlanError::invalid(format!("ambiguous column {column}")))
                        .cloned()
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(SchemaRef::new(Schema::new(Fields::from(fields))))
    }

    pub(super) fn resolve_column_candidates(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        plan_id: Option<i64>,
        state: &PlanResolverState,
    ) -> Vec<Column> {
        schema
            .iter()
            .filter(|(_, field)| {
                state
                    .get_field_info(field.name())
                    .is_ok_and(|info| !info.is_hidden() && info.matches(name, plan_id))
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
