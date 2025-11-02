use std::collections::HashMap;
use std::sync::OnceLock;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
    DatabaseStatus, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    TableStatus,
};

use crate::unity::types;
use crate::unity::Client;

pub const UNITY_CATALOG_PROP_URI: &str = "uri";

#[derive(Clone, Debug)]
pub struct UnityCatalogConfig {
    uri: String,
    props: HashMap<String, String>,
}

/// Provider for Unity Catalog
pub struct UnityCatalogProvider {
    name: String,
    catalog_config: UnityCatalogConfig,
    client: OnceLock<Client>,
}

impl UnityCatalogProvider {
    pub fn new(name: String, props: HashMap<String, String>) -> Self {
        let default_uri = "http://localhost:8080/api/2.1/unity-catalog".to_string();
        let catalog_config = UnityCatalogConfig {
            uri: props
                .get(UNITY_CATALOG_PROP_URI)
                .cloned()
                .unwrap_or(default_uri),
            props: props
                .into_iter()
                .filter(|(k, _)| k != UNITY_CATALOG_PROP_URI)
                .collect(),
        };

        Self {
            name,
            catalog_config,
            client: OnceLock::new(),
        }
    }

    fn get_client(&self) -> &Client {
        self.client.get_or_init(|| {
            let client = reqwest::Client::new();
            Client::new_with_client(&self.catalog_config.uri, client)
        })
    }

    fn namespace_to_full_name(&self, namespace: &Namespace) -> CatalogResult<String> {
        let parts: Vec<String> = namespace.clone().into();
        match parts.len() {
            0 => Err(CatalogError::InvalidArgument(
                "Namespace cannot be empty".to_string(),
            )),
            1 => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog requires catalog.schema format, got only: {}",
                parts[0]
            ))),
            2 => Ok(format!("{}.{}", parts[0], parts[1])),
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog only supports 2-level namespaces (catalog.schema), got {} levels",
                parts.len()
            ))),
        }
    }

    fn extract_catalog_and_schema(&self, namespace: &Namespace) -> CatalogResult<(String, String)> {
        let parts: Vec<String> = namespace.clone().into();
        match parts.len() {
            2 => Ok((parts[0].clone(), parts[1].clone())),
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog requires catalog.schema format, got {} parts",
                parts.len()
            ))),
        }
    }

    fn schema_info_to_database_status(&self, schema_info: types::SchemaInfo) -> DatabaseStatus {
        let catalog_name = schema_info.catalog_name.unwrap_or_default();
        let schema_name = schema_info.name.unwrap_or_default();
        let database = vec![catalog_name, schema_name];

        let comment = schema_info.comment;

        let properties_map: HashMap<String, String> = schema_info
            .properties
            .map(|p| {
                let inner: HashMap<String, String> = p.into();
                inner
            })
            .unwrap_or_default();

        let properties: Vec<(String, String)> = properties_map.into_iter().collect();

        DatabaseStatus {
            catalog: self.name.clone(),
            database,
            comment,
            location: None,
            properties,
        }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for UnityCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let (catalog_name, schema_name) = self.extract_catalog_and_schema(database)?;
        let client = self.get_client();

        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location: _,
            properties,
        } = options;

        let mut props: HashMap<String, String> = properties.into_iter().collect();
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }

        let request = types::CreateSchema::builder()
            .catalog_name(catalog_name.clone())
            .name(schema_name.clone())
            .comment(props.get("comment").cloned())
            .properties(if props.is_empty() {
                None
            } else {
                Some(types::SecurablePropertiesMap::from(props))
            });

        let result = client.create_schema().body(request).send().await;

        match result {
            Ok(response) => {
                let schema_info = response.into_inner();
                Ok(self.schema_info_to_database_status(schema_info))
            }
            Err(progenitor_client::Error::ErrorResponse(response))
                if response.status().as_u16() == 409 && if_not_exists =>
            {
                self.get_database(database).await
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to create schema: {e}"
            ))),
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let full_name = self.namespace_to_full_name(database)?;
        let client = self.get_client();

        let result = client.get_schema().full_name(&full_name).send().await;

        match result {
            Ok(response) => {
                let schema_info = response.into_inner();
                Ok(self.schema_info_to_database_status(schema_info))
            }
            Err(progenitor_client::Error::ErrorResponse(response))
                if response.status().as_u16() == 404 =>
            {
                Err(CatalogError::NotFound("schema", full_name))
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to get schema: {e}"
            ))),
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let catalog_name = match prefix {
            None => {
                return Err(CatalogError::InvalidArgument(
                    "Unity Catalog requires catalog name for listing schemas".to_string(),
                ));
            }
            Some(ns) => {
                let parts: Vec<String> = ns.clone().into();
                if parts.len() != 1 {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Unity Catalog list_databases expects single catalog name, got {} parts",
                        parts.len()
                    )));
                }
                parts[0].clone()
            }
        };

        let client = self.get_client();
        let result = client
            .list_schemas()
            .catalog_name(&catalog_name)
            .send()
            .await;

        match result {
            Ok(response) => {
                let list_response = response.into_inner();
                Ok(list_response
                    .schemas
                    .into_iter()
                    .map(|schema_info| self.schema_info_to_database_status(schema_info))
                    .collect())
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to list schemas: {e}"
            ))),
        }
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let full_name = self.namespace_to_full_name(database)?;
        let client = self.get_client();

        let DropDatabaseOptions {
            if_exists,
            cascade,
        } = options;

        let result = client
            .delete_schema()
            .full_name(&full_name)
            .force(cascade)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(progenitor_client::Error::ErrorResponse(response))
                if response.status().as_u16() == 404 && if_exists =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to drop schema: {e}"
            ))),
        }
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        todo!()
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        todo!()
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        todo!()
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        todo!()
    }
}
