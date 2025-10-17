use std::collections::HashMap;
use std::sync::Arc;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DatabaseStatus, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace, TableColumnStatus, TableKind, TableStatus,
};
use sail_common::runtime::RuntimeHandle;
use sail_iceberg::{arrow_type_to_iceberg, iceberg_type_to_arrow, NestedField};

use crate::apis::configuration::Configuration;
use crate::apis::{self, Api, ApiClient};

/// Provider for Apache Iceberg REST Catalog.
pub struct IcebergRestCatalogProvider {
    name: String,
    client: ApiClient,
    prefix: String,
    runtime: RuntimeHandle, // CHECK HERE: ADD SECONDARY RUNTIME LOGIC BEFORE MERGING
}

impl IcebergRestCatalogProvider {
    pub fn new(
        name: String,
        prefix: String,
        configuration: Arc<Configuration>,
        runtime: RuntimeHandle,
    ) -> Self {
        let client = ApiClient::new(configuration);

        Self {
            name,
            client,
            prefix,
            runtime,
        }
    }

    fn load_table_result_to_status(
        &self,
        table_name: &str,
        database: &Namespace,
        result: &crate::models::LoadTableResult,
    ) -> CatalogResult<TableStatus> {
        let metadata = &result.metadata;

        let current_schema = if let Some(schemas) = &metadata.schemas {
            let schema_id = metadata.current_schema_id.unwrap_or(0);
            schemas
                .iter()
                .find(|s| s.schema_id == Some(schema_id))
                .or_else(|| schemas.first())
        } else {
            None
        };

        let columns = if let Some(schema) = current_schema {
            let mut cols = Vec::new();
            for field in &schema.fields {
                let data_type = iceberg_type_to_arrow(&field.field_type).map_err(|e| {
                    CatalogError::External(format!(
                        "Failed to convert Iceberg type to Arrow type for field '{}': {e}",
                        field.name
                    ))
                })?;
                cols.push(TableColumnStatus {
                    name: field.name.clone(),
                    data_type,
                    nullable: !field.required,
                    comment: field.doc.clone(),
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                });
            }
            cols
        } else {
            Vec::new()
        };

        let comment = metadata
            .properties
            .as_ref()
            .and_then(|p| p.get("comment"))
            .cloned();

        let properties: Vec<_> = metadata
            .properties
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| k != "comment")
            .collect();

        Ok(TableStatus {
            name: table_name.to_string(),
            kind: TableKind::Table {
                catalog: self.name.clone(),
                database: database.clone().into(),
                columns,
                comment,
                constraints: Vec::new(),
                location: metadata.location.clone(),
                format: "iceberg".to_string(),
                partition_by: Vec::new(),
                sort_by: Vec::new(),
                bucket_by: None,
                options: Vec::new(),
                properties,
            },
        })
    }

    fn load_view_result_to_status(
        &self,
        view_name: &str,
        database: &Namespace,
        result: &crate::models::LoadViewResult,
    ) -> CatalogResult<TableStatus> {
        let metadata = &result.metadata;

        let current_version = metadata
            .versions
            .iter()
            .find(|v| v.version_id == metadata.current_version_id);

        let current_schema = if let Some(version) = current_version {
            metadata
                .schemas
                .iter()
                .find(|s| s.schema_id == Some(version.schema_id))
        } else {
            metadata.schemas.first()
        };

        let columns = if let Some(schema) = current_schema {
            let mut cols = Vec::new();
            for field in &schema.fields {
                let data_type = iceberg_type_to_arrow(&field.field_type).map_err(|e| {
                    CatalogError::External(format!(
                        "Failed to convert Iceberg type to Arrow type for field '{}': {e}",
                        field.name
                    ))
                })?;
                cols.push(TableColumnStatus {
                    name: field.name.clone(),
                    data_type,
                    nullable: !field.required,
                    comment: field.doc.clone(),
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                });
            }
            cols
        } else {
            Vec::new()
        };

        let definition = current_version
            .and_then(|v| v.representations.first())
            .map(|r| r.sql.clone())
            .unwrap_or_default();

        let comment = metadata
            .properties
            .as_ref()
            .and_then(|p| p.get("comment"))
            .cloned();

        let properties: Vec<_> = metadata
            .properties
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| k != "comment")
            .collect();

        Ok(TableStatus {
            name: view_name.to_string(),
            kind: TableKind::View {
                catalog: self.name.clone(),
                database: database.clone().into(),
                definition,
                columns,
                comment,
                properties,
            },
        })
    }
}

#[async_trait::async_trait]
impl CatalogProvider for IcebergRestCatalogProvider {
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

        if if_not_exists {
            let api = self.client.catalog_api_api();

            if api
                .namespace_exists(&self.prefix, &database.to_string())
                .await
                .is_ok()
            {
                return self.get_database(database).await;
            }
        }

        let mut props = HashMap::new();
        for (k, v) in properties {
            props.insert(k, v);
        }
        if let Some(c) = &comment {
            props.insert("comment".to_string(), c.clone());
        }
        if let Some(l) = location {
            props.insert("location".to_string(), l);
        }

        let request = crate::models::CreateNamespaceRequest {
            namespace: database.clone().into(),
            properties: if props.is_empty() { None } else { Some(props) },
        };

        let api = self.client.catalog_api_api();
        let result = api
            .create_namespace(&self.prefix, request)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create namespace: {}", e)))?;

        let comment = result
            .properties
            .as_ref()
            .and_then(|p| p.get("comment"))
            .cloned();
        let location = result
            .properties
            .as_ref()
            .and_then(|p| p.get("location"))
            .cloned();
        let properties: Vec<_> = result
            .properties
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| k != "comment" && k != "location")
            .collect();

        Ok(DatabaseStatus {
            catalog: self.name.clone(),
            database: database.clone().into(),
            comment,
            location,
            properties,
        })
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let DropDatabaseOptions {
            if_exists,
            cascade: _,
        } = options;
        let api = self.client.catalog_api_api();
        match api
            .drop_namespace(&self.prefix, &database.to_string())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to drop namespace: {e}"
                    )))
                }
            }
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let api = self.client.catalog_api_api();
        let result = api
            .load_namespace_metadata(&self.prefix, &database.to_string())
            .await
            .map_err(|e| CatalogError::NotFound("database", format!("{database}: {e}")))?;

        let comment = result
            .properties
            .as_ref()
            .and_then(|p| p.get("comment"))
            .cloned();
        let location = result
            .properties
            .as_ref()
            .and_then(|p| p.get("location"))
            .cloned();
        let properties: Vec<_> = result
            .properties
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| k != "comment" && k != "location")
            .collect();

        Ok(DatabaseStatus {
            catalog: self.name.clone(),
            database: database.clone().into(),
            comment,
            location,
            properties,
        })
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let parent = prefix.map(|namespace| namespace.to_string());

        let api = self.client.catalog_api_api();
        let result = api
            .list_namespaces(&self.prefix, None, None, parent.as_deref())
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list namespaces: {}", e)))?;

        let namespaces = result.namespaces.unwrap_or_default();
        let mut statuses = Vec::new();

        for namespace in namespaces {
            let ns: Namespace = namespace.try_into().map_err(|e| {
                CatalogError::External(format!("Invalid namespace from catalog: {e}"))
            })?;
            let namespace_str = ns.to_string();
            if let Ok(meta) = api
                .load_namespace_metadata(&self.prefix, &namespace_str)
                .await
            {
                let comment = meta
                    .properties
                    .as_ref()
                    .and_then(|p| p.get("comment"))
                    .cloned();
                let location = meta
                    .properties
                    .as_ref()
                    .and_then(|p| p.get("location"))
                    .cloned();
                let properties: Vec<_> = meta
                    .properties
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(k, _)| k != "comment" && k != "location")
                    .collect();

                statuses.push(DatabaseStatus {
                    catalog: self.name.clone(),
                    database: ns.into(),
                    comment,
                    location,
                    properties,
                });
            }
        }

        Ok(statuses)
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let CreateTableOptions {
            columns,
            comment,
            constraints: _,
            location,
            format: _,
            partition_by: _,
            sort_by: _,
            bucket_by: _,
            if_not_exists,
            replace: _,
            options: _,
            properties,
        } = options;

        if if_not_exists {
            if let Ok(existing) = self.get_table(database, table).await {
                return Ok(existing);
            }
        }

        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let field_type = arrow_type_to_iceberg(&col.data_type).map_err(|e| {
                CatalogError::External(format!(
                    "Failed to convert Arrow type to Iceberg type for column '{}': {e}",
                    col.name
                ))
            })?;
            let mut field =
                NestedField::new(idx as i32, col.name.clone(), field_type, !col.nullable);
            if let Some(comment_text) = &col.comment {
                field = field.with_doc(comment_text);
            }
            fields.push(Arc::new(field));
        }

        let schema = crate::models::Schema {
            r#type: crate::models::schema::Type::Struct,
            fields,
            schema_id: None,
            identifier_field_ids: None,
        };

        let mut props = HashMap::new();
        for (k, v) in properties {
            props.insert(k, v);
        }
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }

        let request = crate::models::CreateTableRequest {
            name: table.to_string(),
            location,
            schema: Box::new(schema),
            partition_spec: None,
            write_order: None,
            stage_create: None,
            properties: if props.is_empty() { None } else { Some(props) },
        };

        let api = self.client.catalog_api_api();
        let result = api
            .create_table(&self.prefix, &database.to_string(), request, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create table: {}", e)))?;

        self.load_table_result_to_status(table, database, &result)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let api = self.client.catalog_api_api();
        let result = api
            .load_table(&self.prefix, &database.to_string(), table, None, None, None)
            .await
            .map_err(|_e| CatalogError::NotFound("table", format!("{}.{}", database, table)))?;
        self.load_table_result_to_status(table, database, &result)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let api = self.client.catalog_api_api();
        let result = api
            .list_tables(&self.prefix, &database.to_string(), None, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {}", e)))?;

        let identifiers = result.identifiers.unwrap_or_default();
        let mut tables = Vec::new();

        for identifier in identifiers {
            if let Ok(table_status) = self.get_table(database, &identifier.name).await {
                tables.push(table_status);
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
        // In Spark, the `DROP TABLE ... PURGE` SQL statement deletes data if the table
        // is managed by the Hive metastore. `PURGE` is ignored if the table is external.
        // In Sail, all tables are external, so we ignore the `purge` option.
        let DropTableOptions { if_exists, purge } = options;
        let api = self.client.catalog_api_api();
        match api
            .drop_table(&self.prefix, &database.to_string(), table, Some(purge))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::External(format!("Failed to drop table: {e}")))
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
        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace: _,
            comment,
            properties,
        } = options;

        if if_not_exists {
            if let Ok(existing) = self.get_view(database, view).await {
                return Ok(existing);
            }
        }

        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let field_type = arrow_type_to_iceberg(&col.data_type).map_err(|e| {
                CatalogError::External(format!(
                    "Failed to convert Arrow type to Iceberg type for column '{}': {e}",
                    col.name
                ))
            })?;
            let mut field =
                NestedField::new(idx as i32, col.name.clone(), field_type, !col.nullable);
            if let Some(comment_text) = &col.comment {
                field = field.with_doc(comment_text);
            }
            fields.push(Arc::new(field));
        }

        let schema = crate::models::Schema {
            r#type: crate::models::schema::Type::Struct,
            fields,
            schema_id: None,
            identifier_field_ids: None,
        };

        let sql_representation = crate::models::SqlViewRepresentation {
            r#type: "sql".to_string(),
            sql: definition,
            dialect: "spark".to_string(),
        };

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let view_version = crate::models::ViewVersion {
            version_id: 1,
            timestamp_ms,
            schema_id: 0,
            summary: HashMap::new(),
            representations: vec![sql_representation],
            default_catalog: None,
            default_namespace: database.clone().into(),
        };

        let mut props = HashMap::new();
        for (k, v) in properties {
            props.insert(k, v);
        }
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }

        let request = crate::models::CreateViewRequest {
            name: view.to_string(),
            location: None,
            schema: Box::new(schema),
            view_version: Box::new(view_version),
            properties: props,
        };

        let api = self.client.catalog_api_api();
        let result = api
            .create_view(&self.prefix, &database.to_string(), request)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create view: {}", e)))?;

        self.load_view_result_to_status(view, database, &result)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let api = self.client.catalog_api_api();
        let result = api
            .load_view(&self.prefix, &database.to_string(), view)
            .await
            .map_err(|_e| CatalogError::NotFound("view", format!("{database}.{view}")))?;
        self.load_view_result_to_status(view, database, &result)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let api = self.client.catalog_api_api();
        let result = api
            .list_views(&self.prefix, &database.to_string(), None, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list views: {}", e)))?;
        let identifiers = result.identifiers.unwrap_or_default();
        let mut views = Vec::new();
        for identifier in identifiers {
            if let Ok(view_status) = self.get_view(database, &identifier.name).await {
                views.push(view_status);
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
        let api = self.client.catalog_api_api();
        match api
            .drop_view(&self.prefix, &database.to_string(), view)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::External(format!("Failed to drop view: {e}")))
                }
            }
        }
    }
}
