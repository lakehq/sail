use std::collections::HashMap;
use std::sync::Arc;

use crate::error::SparkResult;
use crate::sql::session_catalog::catalog::list_catalogs;
use crate::sql::utils::{build_schema_reference, filter_pattern};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::SchemaReference;

#[derive(Debug, Clone)]
pub(crate) struct CatalogDatabase {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) location_uri: Option<String>,
}

impl CatalogDatabase {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("location_uri", DataType::Utf8, true),
        ]))
    }
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

pub(crate) fn list_catalog_databases(
    catalog_pattern: Option<&str>,
    database_pattern: Option<&str>,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogDatabase>> {
    let catalogs: HashMap<String, Arc<dyn CatalogProvider>> = list_catalogs(catalog_pattern, &ctx)?;
    let catalog_databases: Vec<CatalogDatabase> = catalogs
        .iter()
        .flat_map(|(catalog_name, catalog)| {
            catalog
                .schema_names()
                .iter()
                .filter_map(|schema_name| {
                    let filtered_names: Vec<String> =
                        filter_pattern(vec![&schema_name], database_pattern);
                    if filtered_names.is_empty() {
                        None
                    } else {
                        Some(CatalogDatabase {
                            name: filtered_names[0].clone(),
                            catalog: Some(catalog_name.clone()),
                            description: None, // TODO: Add actual description if available
                            location_uri: None, // TODO: Add actual location URI if available
                        })
                    }
                })
                .collect::<Vec<CatalogDatabase>>()
        })
        .collect();
    Ok(catalog_databases)
}

pub(crate) fn get_catalog_database(
    db_name: &str,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogDatabase>> {
    let (catalog_name, db_name) = parse_optional_db_name_with_defaults(
        Some(&db_name),
        &ctx.state().config().options().catalog.default_catalog,
        &db_name,
    )?;
    list_catalog_databases(Some(&catalog_name), Some(&db_name), &ctx)
}

pub(crate) fn parse_optional_db_name_with_defaults(
    db_name: Option<&str>,
    default_catalog: &str,
    default_database: &str,
) -> SparkResult<(String, String)> {
    let (catalog_name, database_name) = match db_name {
        Some(db_name) => {
            let schema_reference: SchemaReference = build_schema_reference(&db_name)?;
            match schema_reference {
                SchemaReference::Bare { schema } => {
                    (default_catalog.to_string(), schema.to_string())
                }
                SchemaReference::Full { catalog, schema } => {
                    (catalog.to_string(), schema.to_string())
                }
            }
        }
        None => (default_catalog.to_string(), default_database.to_string()),
    };
    Ok((catalog_name, database_name))
}
