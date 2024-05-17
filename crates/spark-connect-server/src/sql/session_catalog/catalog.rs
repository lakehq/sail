use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{
    BooleanArray, GenericListBuilder, GenericStringBuilder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
}

impl CatalogMetadata {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
        ]))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogDatabase {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) description: Option<String>,
    // TODO: location_uri should technically not be nullable
    pub(crate) location_uri: Option<String>,
}

impl CatalogDatabase {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            // TODO: location_uri should technically not be nullable
            Field::new("location_uri", DataType::Utf8, true),
        ]))
    }
}

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

pub(crate) fn create_catalog_metadata_memtable(
    catalogs: Vec<CatalogMetadata>,
) -> SparkResult<MemTable> {
    let schema_ref = CatalogMetadata::schema();

    let mut names: Vec<String> = Vec::with_capacity(catalogs.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(catalogs.len());

    for catalog in catalogs {
        names.push(catalog.name);
        descriptions.push(catalog.description);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(descriptions)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}

pub(crate) fn create_catalog_database_memtable(
    databases: Vec<CatalogDatabase>,
) -> SparkResult<MemTable> {
    let schema_ref = CatalogDatabase::schema();

    let mut names: Vec<String> = Vec::with_capacity(databases.len());
    let mut catalogs: Vec<Option<String>> = Vec::with_capacity(databases.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(databases.len());
    let mut location_uris: Vec<Option<String>> = Vec::with_capacity(databases.len());

    for db in databases {
        names.push(db.name);
        catalogs.push(db.catalog);
        descriptions.push(db.description);
        location_uris.push(db.location_uri);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(location_uris)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
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
