use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogFunction {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) class_name: String,
    pub(crate) is_temporary: bool,
}

impl CatalogFunction {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("class_name", DataType::Utf8, false),
            Field::new("is_temporary", DataType::Boolean, false),
        ]))
    }
}

pub(crate) fn create_catalog_function_memtable(
    functions: Vec<CatalogFunction>,
) -> SparkResult<MemTable> {
    let schema_ref = CatalogFunction::schema();

    let mut names: Vec<String> = Vec::with_capacity(functions.len());
    let mut catalogs: Vec<Option<String>> = Vec::with_capacity(functions.len());
    let mut namespaces: Vec<Option<String>> = Vec::with_capacity(functions.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(functions.len());
    let mut class_names: Vec<String> = Vec::with_capacity(functions.len());
    let mut is_temporaries: Vec<bool> = Vec::with_capacity(functions.len());

    for function in functions {
        names.push(function.name);
        catalogs.push(function.catalog);
        namespaces.push(function.namespace);
        descriptions.push(function.description);
        class_names.push(function.class_name);
        is_temporaries.push(function.is_temporary);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(namespaces)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(class_names)),
            Arc::new(BooleanArray::from(is_temporaries)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
