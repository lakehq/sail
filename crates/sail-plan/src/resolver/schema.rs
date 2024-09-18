use std::sync::Arc;

use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion_common::{DFSchema, SchemaReference, TableReference};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

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
}
