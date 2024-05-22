use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    BooleanArray, GenericListBuilder, GenericStringBuilder, ListBuilder, RecordBatch, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::TableReference;
use datafusion_expr::TableType;

use crate::error::SparkResult;
use crate::sql::session_catalog::database::{list_catalog_databases, CatalogDatabase};
use crate::sql::utils::filter_pattern;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogTableType {
    EXTERNAL,
    MANAGED,
    VIEW,
    TEMPORARY,
}

impl CatalogTableType {
    fn as_str(&self) -> &str {
        match self {
            CatalogTableType::EXTERNAL => "EXTERNAL",
            CatalogTableType::MANAGED => "MANAGED",
            CatalogTableType::VIEW => "VIEW",
            CatalogTableType::TEMPORARY => "TEMPORARY",
        }
    }
}

impl fmt::Display for CatalogTableType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub(crate) fn table_type_to_catalog_table_type(table_type: TableType) -> CatalogTableType {
    match table_type {
        TableType::Base => CatalogTableType::MANAGED, // TODO: Could also be EXTERNAL
        TableType::Temporary => CatalogTableType::TEMPORARY,
        TableType::View => CatalogTableType::TEMPORARY,
    }
    // TODO: handle_execute_create_dataframe_view
    //  is currently creating a View table from DataFrame.
    //  Unsure if this would be considered a Temporary View Table or not.
    //  Spark's expectation is that a Temporary View Table is created.
}

pub(crate) fn catalog_table_type_to_table_type(catalog_table_type: CatalogTableType) -> TableType {
    match catalog_table_type {
        CatalogTableType::EXTERNAL => TableType::Base,
        CatalogTableType::MANAGED => TableType::Base,
        CatalogTableType::VIEW => TableType::View,
        CatalogTableType::TEMPORARY => TableType::Temporary,
    }
}

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

pub(crate) async fn list_catalog_tables(
    catalog_pattern: Option<&str>,
    database_pattern: Option<&str>,
    table_pattern: Option<&str>,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogTable>> {
    let catalog_databases: Vec<CatalogDatabase> =
        list_catalog_databases(catalog_pattern, database_pattern, ctx)?;
    let state = &ctx.state();
    let default_catalog_name = state.config().options().catalog.default_catalog.as_str();
    let default_database_name = state.config().options().catalog.default_schema.as_str();
    let mut includes_default_database = false;
    let mut catalog_tables: Vec<CatalogTable> = Vec::new();
    for db in catalog_databases {
        if let Some(catalog_name) = &db.catalog {
            if let Some(catalog) = ctx.catalog(catalog_name) {
                if let Some(schema) = catalog.schema(&db.name) {
                    if catalog_name == default_catalog_name && &db.name == default_database_name {
                        includes_default_database = true;
                    }
                    catalog_tables.extend(
                        list_catalog_tables_in_schema(
                            &schema,
                            &catalog_name,
                            &db.name,
                            table_pattern,
                            None,
                        )
                        .await?,
                    );
                }
            }
        }
    }
    // Spark Temp View Tables are not associated with a catalog or database
    if !includes_default_database {
        let catalog_databases: Vec<CatalogDatabase> = list_catalog_databases(
            Some(&default_catalog_name),
            Some(default_database_name),
            ctx,
        )?;
        for db in catalog_databases {
            if let Some(catalog_name) = &db.catalog {
                if let Some(catalog) = ctx.catalog(catalog_name) {
                    if let Some(schema) = catalog.schema(&db.name) {
                        catalog_tables.extend(
                            list_catalog_tables_in_schema(
                                &schema,
                                &catalog_name,
                                &db.name,
                                table_pattern,
                                Some(TableType::Temporary),
                            )
                            .await?,
                        );
                        catalog_tables.extend(
                            list_catalog_tables_in_schema(
                                &schema,
                                &catalog_name,
                                &db.name,
                                table_pattern,
                                Some(TableType::View),
                            )
                            .await?,
                        );
                    }
                }
            }
        }
    }
    Ok(catalog_tables)
}

pub(crate) async fn list_catalog_tables_in_schema(
    schema: &Arc<dyn SchemaProvider>,
    catalog_name: &str,
    db_name: &str,
    table_pattern: Option<&str>,
    table_type_filter: Option<TableType>,
) -> SparkResult<Vec<CatalogTable>> {
    let mut catalog_tables: Vec<CatalogTable> = Vec::new();
    for table_name in schema.table_names() {
        let filtered_table_names: Vec<String> = filter_pattern(vec![&table_name], table_pattern);
        if !filtered_table_names.is_empty() {
            if let Ok(Some(table)) = schema.table(&filtered_table_names[0]).await {
                let table_type: TableType = table.table_type();
                let cat_table_type: CatalogTableType = table_type_to_catalog_table_type(table_type);
                match table_type_filter {
                    Some(filter) => {
                        if filter != table_type {
                            continue;
                        }
                    }
                    None => {}
                }
                let table_schema = table.schema();
                catalog_tables.push(CatalogTable {
                    name: filtered_table_names[0].clone(),
                    // DataFrame Temp Views in Spark Session do not have a Catalog or Namespace
                    catalog: if cat_table_type == CatalogTableType::TEMPORARY
                        || cat_table_type == CatalogTableType::VIEW
                    {
                        None
                    } else {
                        Some(catalog_name.to_string())
                    },
                    namespace: if cat_table_type == CatalogTableType::TEMPORARY
                        || cat_table_type == CatalogTableType::VIEW
                    {
                        None
                    } else {
                        Some(vec![db_name.to_string()])
                    },
                    description: table_schema
                        .metadata()
                        .get("description")
                        .map(|description| description.clone()),
                    table_type: cat_table_type.to_string(),
                    is_temporary: cat_table_type == CatalogTableType::TEMPORARY,
                });
            }
        }
    }
    Ok(catalog_tables)
}

pub(crate) async fn get_catalog_table(
    table_name: &str,
    catalog_pattern: &str,
    database_pattern: &str,
    ctx: &SessionContext,
) -> SparkResult<Vec<CatalogTable>> {
    let table_ref = TableReference::from(table_name);
    let table_name = table_ref.table();
    let database_pattern = table_ref
        .schema()
        .map_or(database_pattern.clone(), |schema| schema);
    let catalog_pattern = table_ref
        .catalog()
        .map_or(catalog_pattern.clone(), |catalog| catalog);
    Ok(list_catalog_tables(
        Some(&catalog_pattern),
        Some(&database_pattern),
        Some(&table_name),
        &ctx,
    )
    .await?)
}
