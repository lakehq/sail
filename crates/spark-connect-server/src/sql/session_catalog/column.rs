use std::sync::Arc;

use crate::error::SparkResult;
use crate::sql::session_catalog::table::{get_catalog_table, CatalogTable};
use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

#[derive(Debug, Clone)]
pub(crate) struct CatalogColumn {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_partition: bool,
    pub(crate) is_bucket: bool,
}

impl CatalogColumn {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("is_partition", DataType::Boolean, false),
            Field::new("is_bucket", DataType::Boolean, false),
        ]))
    }
}

pub(crate) fn create_catalog_column_memtable(columns: Vec<CatalogColumn>) -> SparkResult<MemTable> {
    let schema_ref = CatalogColumn::schema();

    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(columns.len());
    let mut data_types: Vec<String> = Vec::with_capacity(columns.len());
    let mut nullables: Vec<bool> = Vec::with_capacity(columns.len());
    let mut is_partitions: Vec<bool> = Vec::with_capacity(columns.len());
    let mut is_buckets: Vec<bool> = Vec::with_capacity(columns.len());

    for column in columns {
        names.push(column.name);
        descriptions.push(column.description);
        data_types.push(column.data_type);
        nullables.push(column.nullable);
        is_partitions.push(column.is_partition);
        is_buckets.push(column.is_bucket);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(BooleanArray::from(nullables)),
            Arc::new(BooleanArray::from(is_partitions)),
            Arc::new(BooleanArray::from(is_buckets)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}

pub(crate) async fn list_catalog_table_columns(
    catalog_pattern: &String,
    database_pattern: &String,
    table_name: &String,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogColumn>> {
    let mut catalog_table_columns: Vec<CatalogColumn> = Vec::new();
    let catalog_table: Vec<CatalogTable> =
        get_catalog_table(&table_name, &catalog_pattern, &database_pattern, &ctx).await?;

    if catalog_table.is_empty() {
        return Ok(catalog_table_columns);
    }

    let catalog_table: &CatalogTable = &catalog_table[0];
    if let Some(catalog_name) = &catalog_table.catalog {
        if let Some(catalog) = &ctx.catalog(catalog_name) {
            if let Some(schema) = &catalog.schema(&catalog_table.namespace.as_ref().unwrap()[0]) {
                if let Ok(Some(table)) = &schema.table(&catalog_table.name).await {
                    for column in table.schema().fields() {
                        catalog_table_columns.push(CatalogColumn {
                            name: column.name().clone(),
                            description: None, // TODO: Add actual description if available
                            // TODO: needs to be sql data type e.g. "int"
                            data_type: column.data_type().to_string(),
                            nullable: column.is_nullable(),
                            is_partition: false, // TODO: Add actual is_partition if available
                            is_bucket: false,    // TODO: Add actual is_bucket if available
                        });
                    }
                }
            }
        }
    }

    Ok(catalog_table_columns)
}
