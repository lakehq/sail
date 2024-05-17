use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogTable {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) table_type: String,
    pub(crate) is_temporary: bool,
}

impl CatalogTable {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("is_temporary", DataType::Boolean, false),
        ]))
    }
}

pub(crate) fn create_catalog_table_memtable(tables: Vec<CatalogTable>) -> SparkResult<MemTable> {
    let schema_ref = CatalogTable::schema();

    let mut names: Vec<String> = Vec::with_capacity(tables.len());
    let mut catalogs: Vec<Option<String>> = Vec::with_capacity(tables.len());
    let mut namespaces: Vec<Option<String>> = Vec::with_capacity(tables.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(tables.len());
    let mut table_types: Vec<String> = Vec::with_capacity(tables.len());
    let mut is_temporary: Vec<bool> = Vec::with_capacity(tables.len());

    for table in tables {
        names.push(table.name);
        catalogs.push(table.catalog);
        namespaces.push(table.namespace);
        descriptions.push(table.description);
        table_types.push(table.table_type);
        is_temporary.push(table.is_temporary);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(table_types)),
            Arc::new(BooleanArray::from(is_temporary)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
