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

    // CHECK HERE
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

    // CHECK HERE
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

    // CHECK HERE
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
            Err(apis::Error::ResponseError(apis::ResponseContent { status, .. }))
                if status == 404 && if_exists =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to drop namespace: {e}"
            ))),
        }
    }

    // CHECK HERE
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

        let result = self
            .client
            .catalog_api_api()
            .list_namespaces(&self.prefix, None, None, parent.as_deref())
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list namespaces: {}", e)))?;
        let catalog = &self.name;
        Ok(result
            .namespaces
            .unwrap_or_default()
            .into_iter()
            .map(|namespace| DatabaseStatus {
                catalog: catalog.clone(),
                database: namespace,
                comment: None,
                location: None,
                properties: Vec::new(),
            })
            .collect())
    }

    // CHECK HERE
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

    // CHECK HERE
    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let api = self.client.catalog_api_api();
        let result = api
            .load_table(&self.prefix, &database.to_string(), table, None, None, None)
            .await
            .map_err(|_e| CatalogError::NotFound("table", format!("{database}.{table}")))?;
        self.load_table_result_to_status(table, database, &result)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let result = self
            .client
            .catalog_api_api()
            .list_tables(&self.prefix, &database.to_string(), None, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?;
        let catalog = &self.name;
        Ok(result
            .identifiers
            .unwrap_or_default()
            .into_iter()
            .map(|identifier| TableStatus {
                name: identifier.name,
                kind: TableKind::Table {
                    catalog: catalog.clone(),
                    database: identifier.namespace,
                    columns: Vec::new(),
                    comment: None,
                    constraints: Vec::new(),
                    location: None,
                    format: "iceberg".to_string(),
                    partition_by: Vec::new(),
                    sort_by: Vec::new(),
                    bucket_by: None,
                    options: Vec::new(),
                    properties: Vec::new(),
                },
            })
            .collect())
    }

    // CHECK HERE, IS THE COMMENT IN DROP TABLE ABOUT PURGE CORRECT FOR ICEBERG?
    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let DropTableOptions { if_exists, purge } = options;
        let api = self.client.catalog_api_api();
        match api
            .drop_table(&self.prefix, &database.to_string(), table, Some(purge))
            .await
        {
            Ok(_) => Ok(()),
            Err(apis::Error::ResponseError(apis::ResponseContent { status, .. }))
                if status == 404 && if_exists =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!("Failed to drop table: {e}"))),
        }
    }

    // CHECK HERE
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

    // CHECK HERE
    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let api = self.client.catalog_api_api();
        let result = api
            .load_view(&self.prefix, &database.to_string(), view)
            .await
            .map_err(|_e| CatalogError::NotFound("view", format!("{database}.{view}")))?;
        self.load_view_result_to_status(view, database, &result)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let result = self
            .client
            .catalog_api_api()
            .list_views(&self.prefix, &database.to_string(), None, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list views: {}", e)))?;
        let catalog = &self.name;
        Ok(result
            .identifiers
            .unwrap_or_default()
            .into_iter()
            .map(|identifier| TableStatus {
                name: identifier.name,
                kind: TableKind::View {
                    catalog: catalog.clone(),
                    database: identifier.namespace,
                    definition: String::new(),
                    columns: Vec::new(),
                    comment: None,
                    properties: Vec::new(),
                },
            })
            .collect())
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
            Err(apis::Error::ResponseError(apis::ResponseContent { status, .. }))
                if status == 404 && if_exists =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!("Failed to drop view: {e}"))),
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeManager;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    struct TestContext {
        server: MockServer,
        catalog: IcebergRestCatalogProvider,
        prefix: String,
    }

    impl TestContext {
        async fn new(prefix: Option<&str>) -> Self {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/v1/config"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "overrides": {
                        "warehouse": "s3://iceberg-catalog"
                    },
                    "defaults": {}
                })))
                .mount(&server)
                .await;

            let prefix_str = prefix.unwrap_or("");
            let runtime = RuntimeHandle::new(tokio::runtime::Handle::current(), None);
            let config = Arc::new(Configuration {
                base_path: server.uri(),
                user_agent: None,
                client: reqwest::Client::new(),
                basic_auth: None,
                oauth_access_token: None,
                bearer_access_token: None,
                api_key: None,
            });
            let catalog = IcebergRestCatalogProvider::new(
                "test_catalog".to_string(),
                prefix_str.to_string(),
                config,
                runtime,
            );

            Self {
                server,
                catalog,
                prefix: prefix_str.to_string(),
            }
        }

        fn path(&self, suffix: &str) -> String {
            if self.prefix.is_empty() {
                format!("/v1/{}", suffix)
            } else {
                format!("/v1/{}{}", self.prefix, suffix)
            }
        }

        async fn mock_get_json(&self, path_str: &str, response: serde_json::Value) {
            Mock::given(method("GET"))
                .and(path(path_str))
                .respond_with(ResponseTemplate::new(200).set_body_json(response))
                .mount(&self.server)
                .await;
        }

        async fn mock_post_json(&self, path_str: &str, response: serde_json::Value) {
            Mock::given(method("POST"))
                .and(path(path_str))
                .respond_with(ResponseTemplate::new(200).set_body_json(response))
                .mount(&self.server)
                .await;
        }

        async fn mock_delete(&self, path_str: &str) {
            Mock::given(method("DELETE"))
                .and(path(path_str))
                .respond_with(ResponseTemplate::new(204))
                .mount(&self.server)
                .await;
        }

        async fn mock_delete_404(&self, path_str: &str, error_type: &str, message: &str) {
            Mock::given(method("DELETE"))
                .and(path(path_str))
                .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "error": {
                        "message": message,
                        "type": error_type,
                        "code": 404
                    }
                })))
                .mount(&self.server)
                .await;
        }
    }

    async fn test_list_namespace_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;

        ctx.mock_get_json(
            &ctx.path("/namespaces"),
            serde_json::json!({
                "namespaces": [
                    ["ns1", "ns11"],
                    ["ns2"]
                ]
            }),
        )
        .await;

        let databases = ctx.catalog.list_databases(None).await.unwrap();

        assert_eq!(databases.len(), 2);
        assert_eq!(
            databases[0].database,
            vec!["ns1".to_string(), "ns11".to_string()]
        );
        assert_eq!(databases[1].database, vec!["ns2".to_string()]);
    }

    #[tokio::test]
    async fn test_list_namespace() {
        test_list_namespace_impl(None).await;
        test_list_namespace_impl(Some("test")).await;
    }

    async fn test_list_namespace_parent_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;
        let ns_path = ctx.path("/namespaces");

        Mock::given(method("GET"))
            .and(path(ns_path.as_str()))
            .and(query_param_is_missing("parent"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": [
                    ["accounting"],
                    ["engineering"]
                ]
            })))
            .mount(&ctx.server)
            .await;

        Mock::given(method("GET"))
            .and(path(ns_path.as_str()))
            .and(query_param("parent", "accounting"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": [
                    ["accounting", "tax"],
                    ["accounting", "payroll"]
                ]
            })))
            .mount(&ctx.server)
            .await;

        Mock::given(method("GET"))
            .and(path(ns_path.as_str()))
            .and(query_param("parent", "engineering"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespaces": [
                    ["engineering", "backend"],
                    ["engineering", "frontend"]
                ]
            })))
            .mount(&ctx.server)
            .await;

        let top_level = ctx.catalog.list_databases(None).await.unwrap();
        assert_eq!(top_level.len(), 2);
        assert_eq!(top_level[0].database, vec!["accounting".to_string()]);
        assert_eq!(top_level[1].database, vec!["engineering".to_string()]);

        let accounting_prefix = Namespace::try_from(vec!["accounting".to_string()]).unwrap();
        let accounting_children = ctx
            .catalog
            .list_databases(Some(&accounting_prefix))
            .await
            .unwrap();
        assert_eq!(accounting_children.len(), 2);
        assert_eq!(
            accounting_children[0].database,
            vec!["accounting".to_string(), "tax".to_string()]
        );
        assert_eq!(
            accounting_children[1].database,
            vec!["accounting".to_string(), "payroll".to_string()]
        );

        let engineering_prefix = Namespace::try_from(vec!["engineering".to_string()]).unwrap();
        let engineering_children = ctx
            .catalog
            .list_databases(Some(&engineering_prefix))
            .await
            .unwrap();
        assert_eq!(engineering_children.len(), 2);
        assert_eq!(
            engineering_children[0].database,
            vec!["engineering".to_string(), "backend".to_string()]
        );
        assert_eq!(
            engineering_children[1].database,
            vec!["engineering".to_string(), "frontend".to_string()]
        );
    }

    #[tokio::test]
    async fn test_list_namespace_parent() {
        test_list_namespace_parent_impl(None).await;
        test_list_namespace_parent_impl(Some("test")).await;
    }

    async fn test_list_tables_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "table2"
                    }
                ]
            }),
        )
        .await;

        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();
        let tables = ctx.catalog.list_tables(&namespace).await.unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "table1");
        assert_eq!(tables[1].name, "table2");

        assert!(matches!(tables[0].kind, TableKind::Table { .. }));
        assert!(matches!(tables[1].kind, TableKind::Table { .. }));
    }

    #[tokio::test]
    async fn test_list_tables() {
        test_list_tables_impl(None).await;
        test_list_tables_impl(Some("test")).await;
    }

    async fn test_list_views_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/views"),
            serde_json::json!({
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "view1"
                    },
                    {
                        "namespace": ["ns1"],
                        "name": "view2"
                    }
                ]
            }),
        )
        .await;

        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();
        let views = ctx.catalog.list_views(&namespace).await.unwrap();

        assert_eq!(views.len(), 2);
        assert_eq!(views[0].name, "view1");
        assert_eq!(views[1].name, "view2");

        assert!(matches!(views[0].kind, TableKind::View { .. }));
        assert!(matches!(views[1].kind, TableKind::View { .. }));
    }

    #[tokio::test]
    async fn test_list_views() {
        test_list_views_impl(None).await;
        test_list_views_impl(Some("test")).await;
    }

    async fn test_drop_database_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;

        ctx.mock_delete(&ctx.path("/namespaces/db1")).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: false,
                },
            )
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/db2"),
            "NoSuchNamespaceException",
            "The given namespace does not exist",
        )
        .await;
        let namespace = Namespace::try_from(vec!["db2".to_string()]).unwrap();
        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: true,
                    cascade: false,
                },
            )
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/db3"),
            "NoSuchNamespaceException",
            "The given namespace does not exist",
        )
        .await;
        let namespace = Namespace::try_from(vec!["db3".to_string()]).unwrap();
        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: false,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_database() {
        test_drop_database_impl(None).await;
        test_drop_database_impl(Some("test")).await;
    }

    async fn test_drop_table_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1/tables/table1").as_str()))
            .and(query_param("purgeRequested", "true"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&ctx.server)
            .await;
        let result = ctx
            .catalog
            .drop_table(
                &namespace,
                "table1",
                DropTableOptions {
                    if_exists: false,
                    purge: true,
                },
            )
            .await;
        assert!(result.is_ok());

        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1/tables/table2").as_str()))
            .and(query_param("purgeRequested", "false"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&ctx.server)
            .await;
        let result = ctx
            .catalog
            .drop_table(
                &namespace,
                "table2",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/tables/table3"),
            "NoSuchTableException",
            "The given table does not exist",
        )
        .await;
        let result = ctx
            .catalog
            .drop_table(
                &namespace,
                "table3",
                DropTableOptions {
                    if_exists: true,
                    purge: false,
                },
            )
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/tables/table3"),
            "NoSuchTableException",
            "The given table does not exist",
        )
        .await;
        let result = ctx
            .catalog
            .drop_table(
                &namespace,
                "table3",
                DropTableOptions {
                    if_exists: true,
                    purge: true,
                },
            )
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/tables/table4"),
            "NoSuchTableException",
            "The given table does not exist",
        )
        .await;
        let result = ctx
            .catalog
            .drop_table(
                &namespace,
                "table4",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_table() {
        test_drop_table_impl(None).await;
        test_drop_table_impl(Some("test")).await;
    }

    async fn test_drop_view_impl(prefix: Option<&str>) {
        let ctx = TestContext::new(prefix).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_delete(&ctx.path("/namespaces/ns1/views/view1"))
            .await;
        let result = ctx
            .catalog
            .drop_view(&namespace, "view1", DropViewOptions { if_exists: false })
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/views/view2"),
            "NoSuchViewException",
            "The given view does not exist",
        )
        .await;
        let result = ctx
            .catalog
            .drop_view(&namespace, "view2", DropViewOptions { if_exists: true })
            .await;
        assert!(result.is_ok());

        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/views/view3"),
            "NoSuchViewException",
            "The given view does not exist",
        )
        .await;
        let result = ctx
            .catalog
            .drop_view(&namespace, "view3", DropViewOptions { if_exists: false })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_view() {
        test_drop_view_impl(None).await;
        test_drop_view_impl(Some("test")).await;
    }
}
