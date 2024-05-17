use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{
    BooleanArray, GenericListBuilder, GenericStringBuilder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogTable {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) table_type: String,
    pub(crate) is_temporary: bool,
}

impl CatalogTable {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new_list(
                "namespace",
                Field::new_list_field(DataType::Utf8, true),
                true,
            ),
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
    let mut namespaces_builder: GenericListBuilder<i32, GenericStringBuilder<i32>> =
        ListBuilder::with_capacity(StringBuilder::new(), tables.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(tables.len());
    let mut table_types: Vec<String> = Vec::with_capacity(tables.len());
    let mut is_temporaries: Vec<bool> = Vec::with_capacity(tables.len());

    for table in tables {
        names.push(table.name);
        catalogs.push(table.catalog);
        match table.namespace {
            Some(namespace_list) => {
                for namespace in namespace_list {
                    namespaces_builder.values().append_value(&namespace);
                }
                namespaces_builder.append(true);
            }
            None => namespaces_builder.append(false),
        }
        descriptions.push(table.description);
        table_types.push(table.table_type);
        is_temporaries.push(table.is_temporary);
    }
    let namespaces = namespaces_builder.finish();

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(namespaces),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(table_types)),
            Arc::new(BooleanArray::from(is_temporaries)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
