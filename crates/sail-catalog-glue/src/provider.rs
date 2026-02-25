use std::collections::HashMap;

use aws_config::BehaviorVersion;
use aws_sdk_glue::config::Region;
use aws_sdk_glue::types::{
    StorageDescriptor, TableInput, ViewDefinitionInput, ViewRepresentationInput,
};
use aws_sdk_glue::Client;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewColumnOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};
use tokio::sync::OnceCell;

use crate::data_type::{arrow_to_glue_type, glue_type_to_arrow};
use crate::format::GlueStorageFormat;
use crate::{hive, iceberg};

/// Configuration for AWS Glue Data Catalog.
#[derive(Debug, Clone, Default)]
pub struct GlueCatalogConfig {
    /// AWS region (e.g., "us-east-1"). If not set, uses default from credential chain.
    pub region: Option<String>,
    /// Custom endpoint URL (optional). Useful for VPC endpoints or local development.
    pub endpoint_url: Option<String>,
}

/// An AWS Glue Data Catalog provider.
pub struct GlueCatalogProvider {
    name: String,
    config: GlueCatalogConfig,
    client: OnceCell<Client>,
}

impl GlueCatalogProvider {
    pub fn new(name: String, config: GlueCatalogConfig) -> Self {
        Self {
            name,
            config,
            client: OnceCell::new(),
        }
    }

    pub(crate) async fn get_client(&self) -> CatalogResult<&Client> {
        self.client
            .get_or_try_init(|| async {
                let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

                if let Some(region) = &self.config.region {
                    config_loader = config_loader.region(Region::new(region.clone()));
                }

                if let Some(endpoint) = &self.config.endpoint_url {
                    config_loader = config_loader.endpoint_url(endpoint);
                }

                let sdk_config = config_loader.load().await;
                Ok(Client::new(&sdk_config))
            })
            .await
    }

    fn database_to_status(
        &self,
        db: &aws_sdk_glue::types::Database,
    ) -> CatalogResult<DatabaseStatus> {
        let name = db.name();
        let properties: Vec<(String, String)> = db
            .parameters()
            .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        Ok(DatabaseStatus {
            catalog: self.name.clone(),
            database: vec![name.to_string()],
            comment: db.description().map(|s| s.to_string()),
            location: db.location_uri().map(|s| s.to_string()),
            properties,
        })
    }

    fn table_to_status(
        &self,
        database: &Namespace,
        table: &aws_sdk_glue::types::Table,
    ) -> CatalogResult<TableStatus> {
        let table_name = table.name().to_string();
        let comment = table.description().map(|s| s.to_string());

        // Get storage descriptor info
        let storage = table.storage_descriptor();

        // Extract location
        let location = storage.and_then(|sd| sd.location()).map(|s| s.to_string());

        // Detect format from serde info
        let format = storage
            .and_then(|sd| sd.serde_info())
            .and_then(|si| si.serialization_library())
            .map(|lib| GlueStorageFormat::detect_format_from_serde(Some(lib)))
            .unwrap_or_else(|| "unknown".to_string());

        // Extract columns from storage descriptor
        let mut columns: Vec<TableColumnStatus> = storage
            .map(|sd| sd.columns())
            .unwrap_or_default()
            .iter()
            .filter_map(|col| {
                let name = col.name().to_string();
                let type_str = col.r#type()?;
                let data_type = glue_type_to_arrow(type_str).ok()?;
                Some(TableColumnStatus {
                    name,
                    data_type,
                    nullable: true, // Glue doesn't track nullability
                    comment: col.comment().map(|s| s.to_string()),
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                })
            })
            .collect();

        // Extract partition keys
        let partition_keys: Vec<String> = table
            .partition_keys()
            .iter()
            .map(|pk| pk.name().to_string())
            .collect();

        // Add partition columns
        for pk in table.partition_keys() {
            if let Some(type_str) = pk.r#type() {
                if let Ok(data_type) = glue_type_to_arrow(type_str) {
                    columns.push(TableColumnStatus {
                        name: pk.name().to_string(),
                        data_type,
                        nullable: true,
                        comment: pk.comment().map(|s| s.to_string()),
                        default: None,
                        generated_always_as: None,
                        is_partition: true,
                        is_bucket: false,
                        is_cluster: false,
                    });
                }
            }
        }

        // Extract properties
        let properties: Vec<(String, String)> = table
            .parameters()
            .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        Ok(TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: table_name,
            kind: TableKind::Table {
                columns,
                comment,
                constraints: vec![],
                location,
                format,
                partition_by: partition_keys,
                sort_by: vec![],
                bucket_by: None,
                options: vec![],
                properties,
            },
        })
    }

    fn view_to_status(
        &self,
        database: &Namespace,
        table: &aws_sdk_glue::types::Table,
    ) -> CatalogResult<TableStatus> {
        let view_name = table.name().to_string();
        let comment = table.description().map(|s| s.to_string());

        let definition = table
            .view_definition()
            .and_then(|vd| vd.representations().first())
            .and_then(|rep| rep.view_original_text())
            .ok_or_else(|| CatalogError::External("View has no definition".to_string()))?
            .to_string();

        let storage = table.storage_descriptor();
        let columns: Vec<TableColumnStatus> = storage
            .map(|sd| sd.columns())
            .unwrap_or_default()
            .iter()
            .filter_map(|col| {
                let name = col.name().to_string();
                let type_str = col.r#type()?;
                let data_type = glue_type_to_arrow(type_str).ok()?;
                Some(TableColumnStatus {
                    name,
                    data_type,
                    nullable: true,
                    comment: col.comment().map(|s| s.to_string()),
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                })
            })
            .collect();

        let properties: Vec<(String, String)> = table
            .parameters()
            .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        Ok(TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: view_name,
            kind: TableKind::View {
                definition,
                columns,
                comment,
                properties,
            },
        })
    }

    /// Builds Glue columns from CreateViewColumnOptions.
    fn build_view_columns(
        columns: Vec<CreateViewColumnOptions>,
    ) -> CatalogResult<Vec<aws_sdk_glue::types::Column>> {
        columns
            .into_iter()
            .map(|col| {
                let glue_type = arrow_to_glue_type(&col.data_type)?;
                aws_sdk_glue::types::Column::builder()
                    .name(&col.name)
                    .r#type(glue_type)
                    .set_comment(col.comment.clone())
                    .build()
                    .map_err(|e| CatalogError::External(format!("Failed to build column: {e}")))
            })
            .collect()
    }

    /// Builds a Glue TableInput for a view.
    fn build_view_input(
        view_name: &str,
        columns: Vec<aws_sdk_glue::types::Column>,
        definition: &str,
        comment: Option<&str>,
        properties: Vec<(String, String)>,
    ) -> CatalogResult<TableInput> {
        let parameters: Option<HashMap<String, String>> = if properties.is_empty() {
            None
        } else {
            Some(properties.into_iter().collect())
        };

        let storage_descriptor = StorageDescriptor::builder()
            .set_columns(Some(columns))
            .build();

        let view_representation = ViewRepresentationInput::builder()
            .view_original_text(definition)
            .build();

        let view_definition = ViewDefinitionInput::builder()
            .representations(view_representation)
            .build();

        let mut builder = TableInput::builder()
            .name(view_name)
            .table_type("VIRTUAL_VIEW")
            .view_definition(view_definition)
            .storage_descriptor(storage_descriptor);

        if let Some(desc) = comment {
            builder = builder.description(desc);
        }

        if let Some(params) = parameters {
            builder = builder.set_parameters(Some(params));
        }

        builder
            .build()
            .map_err(|e| CatalogError::InvalidArgument(format!("Failed to build view input: {e}")))
    }
}

#[async_trait::async_trait]
impl CatalogProvider for GlueCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;

        let parameters: Option<HashMap<String, String>> = if properties.is_empty() {
            None
        } else {
            Some(properties.into_iter().collect())
        };

        let mut db_input = aws_sdk_glue::types::DatabaseInput::builder().name(&db_name);

        if let Some(desc) = &comment {
            db_input = db_input.description(desc);
        }
        if let Some(loc) = &location {
            db_input = db_input.location_uri(loc);
        }
        if let Some(params) = parameters {
            db_input = db_input.set_parameters(Some(params));
        }

        let db_input = db_input.build().map_err(|e| {
            CatalogError::InvalidArgument(format!("Failed to build database input: {e}"))
        })?;

        let result = client
            .create_database()
            .database_input(db_input)
            .send()
            .await;

        match result {
            Ok(_) => self.get_database(database).await,
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_already_exists_exception() {
                    if if_not_exists {
                        self.get_database(database).await
                    } else {
                        Err(CatalogError::AlreadyExists("database", db_name))
                    }
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to create database: {service_err}"
                    )))
                }
            }
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let result = client.get_database().name(&db_name).send().await;

        match result {
            Ok(output) => {
                let db = output.database().ok_or_else(|| {
                    CatalogError::External("Database response is empty".to_string())
                })?;
                self.database_to_status(db)
            }
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() {
                    Err(CatalogError::NotFound("database", db_name))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to get database: {service_err}"
                    )))
                }
            }
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let client = self.get_client().await?;

        let mut databases = Vec::new();
        let mut paginator = client.get_databases().into_paginator().send();

        while let Some(page) = paginator
            .next()
            .await
            .transpose()
            .map_err(|e| CatalogError::External(format!("Failed to list databases: {e}")))?
        {
            for db in page.database_list() {
                let status = self.database_to_status(db)?;
                if let Some(p) = prefix {
                    let db_namespace = Namespace::try_from(status.database.clone())
                        .map_err(|e| CatalogError::External(format!("Invalid namespace: {e}")))?;
                    if !p.is_parent_of(&db_namespace) && p != &db_namespace {
                        continue;
                    }
                }
                databases.push(status);
            }
        }

        Ok(databases)
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let DropDatabaseOptions {
            if_exists,
            cascade: _, // Glue requires database to be empty; cascade not directly supported
        } = options;

        let result = client.delete_database().name(&db_name).send().await;

        match result {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() {
                    if if_exists {
                        Ok(())
                    } else {
                        Err(CatalogError::NotFound("database", db_name))
                    }
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to delete database: {service_err}"
                    )))
                }
            }
        }
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let client = self.get_client().await?;
        let format_lower = options.format.to_lowercase();

        if format_lower == "iceberg" {
            iceberg::create_iceberg_table(self, client, database, table, options).await
        } else {
            hive::create_hive_table(self, client, database, table, options).await
        }
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let result = client
            .get_table()
            .database_name(&db_name)
            .name(table)
            .send()
            .await;

        match result {
            Ok(output) => {
                let tbl = output
                    .table()
                    .ok_or_else(|| CatalogError::External("Table response is empty".to_string()))?;

                // Reject views - they should be accessed via get_view
                if matches!(tbl.table_type(), Some(t) if t == "VIRTUAL_VIEW") {
                    return Err(CatalogError::NotFound("table", table.to_string()));
                }

                self.table_to_status(database, tbl)
            }
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() {
                    Err(CatalogError::NotFound("table", table.to_string()))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to get table: {service_err}"
                    )))
                }
            }
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let mut tables = Vec::new();
        let mut paginator = client
            .get_tables()
            .database_name(&db_name)
            .into_paginator()
            .send();

        while let Some(page) = paginator
            .next()
            .await
            .transpose()
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?
        {
            for tbl in page.table_list() {
                // Filter out views - they should be listed via list_views
                if tbl.table_type().unwrap_or_default() == "VIRTUAL_VIEW" {
                    continue;
                }
                tables.push(self.table_to_status(database, tbl)?);
            }
        }

        Ok(tables)
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let DropTableOptions { if_exists, purge } = options;

        if purge {
            return Err(CatalogError::NotSupported(
                "AWS Glue catalog does not support PURGE".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let db_name = database.to_string();

        let result = client
            .delete_table()
            .database_name(&db_name)
            .name(table)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() && if_exists {
                    Ok(())
                } else if service_err.is_entity_not_found_exception() {
                    Err(CatalogError::NotFound("table", table.to_string()))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to drop table: {service_err}"
                    )))
                }
            }
        }
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties,
        } = options;

        if replace {
            return Err(CatalogError::NotSupported(
                "AWS Glue catalog does not support REPLACE for views".to_string(),
            ));
        }

        let glue_columns = Self::build_view_columns(columns)?;
        let view_input = Self::build_view_input(
            view,
            glue_columns,
            &definition,
            comment.as_deref(),
            properties,
        )?;

        let result = client
            .create_table()
            .database_name(&db_name)
            .table_input(view_input)
            .send()
            .await;

        match result {
            Ok(_) => self.get_view(database, view).await,
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_already_exists_exception() {
                    if if_not_exists {
                        self.get_view(database, view).await
                    } else {
                        Err(CatalogError::AlreadyExists("view", view.to_string()))
                    }
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to create view: {service_err}"
                    )))
                }
            }
        }
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let result = client
            .get_table()
            .database_name(&db_name)
            .name(view)
            .send()
            .await;

        match result {
            Ok(output) => {
                let tbl = output
                    .table()
                    .ok_or_else(|| CatalogError::External("View response is empty".to_string()))?;

                let table_type = tbl.table_type().unwrap_or_default();
                if table_type != "VIRTUAL_VIEW" {
                    return Err(CatalogError::NotFound("view", view.to_string()));
                }

                self.view_to_status(database, tbl)
            }
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() {
                    Err(CatalogError::NotFound("view", view.to_string()))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to get view: {service_err}"
                    )))
                }
            }
        }
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let client = self.get_client().await?;
        let db_name = database.to_string();

        let mut views = Vec::new();
        let mut paginator = client
            .get_tables()
            .database_name(&db_name)
            .into_paginator()
            .send();

        while let Some(page) = paginator
            .next()
            .await
            .transpose()
            .map_err(|e| CatalogError::External(format!("Failed to list views: {e}")))?
        {
            for tbl in page.table_list() {
                if tbl.table_type().unwrap_or_default() == "VIRTUAL_VIEW" {
                    views.push(self.view_to_status(database, tbl)?);
                }
            }
        }

        Ok(views)
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        let DropViewOptions { if_exists } = options;

        let client = self.get_client().await?;
        let db_name = database.to_string();

        let result = client
            .delete_table()
            .database_name(&db_name)
            .name(view)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                if service_err.is_entity_not_found_exception() && if_exists {
                    Ok(())
                } else if service_err.is_entity_not_found_exception() {
                    Err(CatalogError::NotFound("view", view.to_string()))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to drop view: {service_err}"
                    )))
                }
            }
        }
    }
}
