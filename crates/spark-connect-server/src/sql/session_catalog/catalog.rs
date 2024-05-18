use std::collections::HashMap;
use std::sync::Arc;

use crate::error::SparkResult;
use crate::sql::utils::filter_pattern;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

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

pub(crate) fn list_catalogs(
    pattern: Option<&String>,
    ctx: &SessionContext,
) -> SparkResult<HashMap<String, Arc<dyn CatalogProvider>>> {
    let catalog_list: &Arc<dyn CatalogProviderList> = &ctx.state().catalog_list();
    let catalogs: HashMap<String, Arc<dyn CatalogProvider>> = catalog_list
        .catalog_names()
        .iter()
        .filter_map(|catalog_name| {
            catalog_list.catalog(catalog_name).and_then(|catalog| {
                let filtered_names = filter_pattern(&vec![catalog_name.to_string()], pattern);
                if filtered_names.is_empty() {
                    None
                } else {
                    Some((catalog_name.to_string(), catalog))
                }
            })
        })
        .collect();
    Ok(catalogs)
}

pub(crate) fn list_catalogs_metadata(
    pattern: Option<&String>,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogMetadata>> {
    let catalogs: HashMap<String, Arc<dyn CatalogProvider>> = list_catalogs(pattern, &ctx)?;
    let catalogs_metadata: Vec<CatalogMetadata> = catalogs
        .iter()
        .map(|(catalog_name, _catalog)| CatalogMetadata {
            name: catalog_name.clone(),
            description: None, // TODO: Add actual description if available
        })
        .collect();
    Ok(catalogs_metadata)
}
