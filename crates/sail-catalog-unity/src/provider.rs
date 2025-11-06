// CHECK HERE
#![allow(clippy::todo, dead_code, unused_imports, unused_variables)]

use std::collections::HashMap;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions, DatabaseStatus,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace, TableStatus,
};
use sail_catalog::utils::{get_property, quote_name_if_needed};
use tokio::sync::OnceCell;

use crate::unity::{types, Client};

pub const UNITY_CATALOG_PROP_URI: &str = "uri";

const DEFAULT_URI: &str = "http://localhost:8080/api/2.1/unity-catalog";

#[derive(Clone, Debug)]
pub struct UnityCatalogConfig {
    default_catalog: String,
    uri: String,
    props: HashMap<String, String>, // CHECK HERE
}

/// Provider for Unity Catalog
pub struct UnityCatalogProvider {
    name: String,
    catalog_config: UnityCatalogConfig,
    client: OnceCell<Client>,
}

impl UnityCatalogProvider {
    pub fn new(name: String, default_catalog: String, props: HashMap<String, String>) -> Self {
        let catalog_config = UnityCatalogConfig {
            default_catalog: quote_name_if_needed(&default_catalog),
            uri: props
                .get(UNITY_CATALOG_PROP_URI)
                .cloned()
                .unwrap_or(DEFAULT_URI.to_string()),
            props: props
                .into_iter()
                .filter(|(k, _)| k != UNITY_CATALOG_PROP_URI)
                .collect(),
        };

        Self {
            name,
            catalog_config,
            client: OnceCell::new(),
        }
    }

    async fn get_client(&self) -> CatalogResult<&Client> {
        let client = self
            .client
            .get_or_try_init(|| async { Ok(Client::new(&self.catalog_config.uri)) })
            .await?;
        Ok(client)
    }

    fn get_catalog_and_schema_name(&self, namespace: &Namespace) -> (String, String) {
        match namespace.tail.len() {
            0 => (
                self.catalog_config.default_catalog.to_string(),
                namespace.head_to_string(),
            ),
            _ => (namespace.head_to_string(), namespace.tail_to_string()),
        }
    }

    fn namespace_to_full_name(&self, namespace: &Namespace) -> String {
        match namespace.tail.len() {
            0 => {
                format!(
                    "{}.{}",
                    self.catalog_config.default_catalog,
                    namespace.head_to_string()
                )
            }
            _ => namespace.to_string(),
        }
    }

    fn schema_info_to_database_status(
        &self,
        schema_info: types::SchemaInfo,
        catalog_name: &str,
        schema_name: Option<&str>,
    ) -> DatabaseStatus {
        let catalog_name = schema_info.catalog_name.unwrap_or(catalog_name.to_string());
        let schema_name = if let Some(schema_name) = schema_name {
            schema_info.name.unwrap_or(schema_name.to_string())
        } else {
            schema_info.name.unwrap_or_default()
        };
        let database = vec![catalog_name.to_string(), schema_name];

        let mut properties: HashMap<String, String> =
            schema_info.properties.map(|p| p.0).unwrap_or_default();
        if let Some(created_at) = schema_info.created_at {
            if !properties.contains_key("created_at") {
                properties.insert("created_at".to_string(), created_at.to_string());
            }
        }
        if let Some(created_by) = schema_info.created_by {
            if !properties.contains_key("created_by") {
                properties.insert("created_by".to_string(), created_by);
            }
        }
        if let Some(owner) = schema_info.owner {
            if !properties.contains_key("owner") {
                properties.insert("owner".to_string(), owner);
            }
        }
        if let Some(schema_id) = schema_info.schema_id {
            if !properties.contains_key("schema_id") {
                properties.insert("schema_id".to_string(), schema_id);
            }
        }
        if let Some(updated_at) = schema_info.updated_at {
            if !properties.contains_key("updated_at") {
                properties.insert("updated_at".to_string(), updated_at.to_string());
            }
        }
        if let Some(updated_by) = schema_info.updated_by {
            if !properties.contains_key("updated_by") {
                properties.insert("updated_by".to_string(), updated_by);
            }
        }

        DatabaseStatus {
            catalog: catalog_name,
            database,
            comment: schema_info.comment,
            location: get_property(&properties, "location"),
            properties: properties.into_iter().collect(),
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
        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let mut props: HashMap<String, String> = properties.into_iter().collect();
        if let Some(c) = &comment {
            props.insert("comment".to_string(), c.to_string());
        }
        if let Some(l) = location {
            props.insert("location".to_string(), l);
        }

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);

        let request = types::CreateSchema::builder()
            .catalog_name(&catalog_name)
            .name(&schema_name)
            .comment(comment)
            .properties(if props.is_empty() {
                None
            } else {
                Some(types::SecurablePropertiesMap::from(props))
            });

        let result = client.create_schema().body(request).send().await;

        match result {
            Ok(response) => {
                let schema_info = response.into_inner();
                Ok(self.schema_info_to_database_status(
                    schema_info,
                    &catalog_name,
                    Some(&schema_name),
                ))
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
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
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);
        let full_name = self.namespace_to_full_name(database);
        let result = client.get_schema().full_name(&full_name).send().await;

        match result {
            Ok(response) => {
                let schema_info = response.into_inner();
                Ok(self.schema_info_to_database_status(
                    schema_info,
                    &catalog_name,
                    Some(&schema_name),
                ))
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
                if response.status().as_u16() == 404 =>
            {
                Err(CatalogError::NotFound("schema", full_name))
            }
            Err(e) => Err(CatalogError::External(format!("Failed to get schema: {e}"))),
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let catalog_name = prefix
            .map(|namespace| namespace.to_string())
            .unwrap_or(self.catalog_config.default_catalog.to_string());
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
                    .map(|schema_info| {
                        self.schema_info_to_database_status(schema_info, &catalog_name, None)
                    })
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
        let DropDatabaseOptions { if_exists, cascade } = options;

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let full_name = self.namespace_to_full_name(database);

        let result = client
            .delete_schema()
            .full_name(&full_name)
            .force(cascade)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(progenitor_client::Error::UnexpectedResponse(response))
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
