use std::collections::HashMap;
use std::sync::Arc;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DatabaseStatus, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace, TableColumnStatus, TableKind, TableStatus,
};
use sail_common::runtime::RuntimeHandle;

use crate::apis::configuration::Configuration;
use crate::apis::{self, Api, ApiClient};

/// Provider for Apache Iceberg REST Catalog.
pub struct IcebergRestCatalogProvider {
    name: String,
    client: ApiClient,
    prefix: String,
    runtime: RuntimeHandle, // CHECK HERE: ADD SECONDARY RUNTIME LOGIC BEFORE MERGIN
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

    // Convert an Iceberg namespace to a URL-encoded string
    fn namespace_to_url_param(&self, namespace: &[String]) -> String {
        // CHECK HERE: validate this is true
        namespace.join("\u{001F}") // Use unit separator as per Iceberg spec
    }

    // Convert Arrow DataType to Iceberg Type
    fn datatype_to_iceberg(
        &self,
        data_type: &datafusion::arrow::datatypes::DataType,
    ) -> CatalogResult<crate::types::Type> {
        use datafusion::arrow::datatypes::DataType;

        use crate::types::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};

        match data_type {
            DataType::Boolean => Ok(Type::Primitive(PrimitiveType::Boolean)),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => Ok(Type::Primitive(PrimitiveType::Int)),
            DataType::Int64 | DataType::UInt64 => Ok(Type::Primitive(PrimitiveType::Long)),
            DataType::Float32 => Ok(Type::Primitive(PrimitiveType::Float)),
            DataType::Float64 => Ok(Type::Primitive(PrimitiveType::Double)),
            DataType::Decimal128(precision, scale) => Ok(Type::Primitive(PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: *scale as u32,
            })),
            DataType::Date32 | DataType::Date64 => Ok(Type::Primitive(PrimitiveType::Date)),
            DataType::Time64(_) => Ok(Type::Primitive(PrimitiveType::Time)),
            DataType::Timestamp(unit, None) => match unit {
                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                    Ok(Type::Primitive(PrimitiveType::Timestamp))
                }
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    Ok(Type::Primitive(PrimitiveType::TimestampNs))
                }
                _ => Err(CatalogError::External(format!(
                    "Unsupported timestamp unit: {:?}",
                    unit
                ))),
            },
            DataType::Timestamp(unit, Some(_)) => match unit {
                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                    Ok(Type::Primitive(PrimitiveType::Timestamptz))
                }
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    Ok(Type::Primitive(PrimitiveType::TimestamptzNs))
                }
                _ => Err(CatalogError::External(format!(
                    "Unsupported timestamp unit: {:?}",
                    unit
                ))),
            },
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::Primitive(PrimitiveType::String)),
            DataType::Binary | DataType::LargeBinary => Ok(Type::Primitive(PrimitiveType::Binary)),
            DataType::FixedSizeBinary(size) => {
                Ok(Type::Primitive(PrimitiveType::Fixed(*size as u64)))
            }
            DataType::List(field) | DataType::LargeList(field) => {
                let element_type = self.datatype_to_iceberg(field.data_type())?;
                let element_field = Arc::new(NestedField::list_element(
                    0, // Field ID will be assigned by Iceberg catalog
                    element_type,
                    field.is_nullable(),
                ));
                Ok(Type::List(ListType::new(element_field)))
            }
            DataType::Map(field, _sorted) => {
                if let DataType::Struct(fields) = field.data_type() {
                    if fields.len() == 2 {
                        let key_type = self.datatype_to_iceberg(fields[0].data_type())?;
                        let value_type = self.datatype_to_iceberg(fields[1].data_type())?;
                        let key_field = Arc::new(NestedField::map_key_element(0, key_type));
                        let value_field = Arc::new(NestedField::map_value_element(
                            0,
                            value_type,
                            fields[1].is_nullable(),
                        ));
                        return Ok(Type::Map(MapType::new(key_field, value_field)));
                    }
                }
                Err(CatalogError::External(
                    "Invalid map type structure".to_string(),
                ))
            }
            DataType::Struct(fields) => {
                let mut nested_fields = Vec::new();
                for (idx, field) in fields.iter().enumerate() {
                    let field_type = self.datatype_to_iceberg(field.data_type())?;
                    nested_fields.push(Arc::new(NestedField::new(
                        idx as i32, // Field ID will be assigned by Iceberg catalog
                        field.name().clone(),
                        field_type,
                        !field.is_nullable(),
                    )));
                }
                Ok(Type::Struct(StructType::new(nested_fields)))
            }
            _ => Err(CatalogError::External(format!(
                "Unsupported Arrow type: {:?}",
                data_type
            ))),
        }
    }

    // Convert Iceberg Type to Arrow DataType
    fn iceberg_to_datatype(
        &self,
        iceberg_type: &crate::types::Type,
    ) -> CatalogResult<datafusion::arrow::datatypes::DataType> {
        use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

        use crate::types::{PrimitiveType, Type};

        match iceberg_type {
            Type::Primitive(prim) => match prim {
                PrimitiveType::Boolean => Ok(DataType::Boolean),
                PrimitiveType::Int => Ok(DataType::Int32),
                PrimitiveType::Long => Ok(DataType::Int64),
                PrimitiveType::Float => Ok(DataType::Float32),
                PrimitiveType::Double => Ok(DataType::Float64),
                PrimitiveType::Decimal { precision, scale } => {
                    Ok(DataType::Decimal128(*precision as u8, *scale as i8))
                }
                PrimitiveType::Date => Ok(DataType::Date32),
                PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
                PrimitiveType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                PrimitiveType::Timestamptz => Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )),
                PrimitiveType::TimestampNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                PrimitiveType::TimestamptzNs => Ok(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )),
                PrimitiveType::String => Ok(DataType::Utf8),
                PrimitiveType::Uuid => Ok(DataType::FixedSizeBinary(16)),
                PrimitiveType::Fixed(size) => Ok(DataType::FixedSizeBinary(*size as i32)),
                PrimitiveType::Binary => Ok(DataType::Binary),
            },
            Type::Struct(struct_type) => {
                let mut fields = Vec::new();
                for nested_field in struct_type.fields() {
                    let data_type = self.iceberg_to_datatype(&nested_field.field_type)?;
                    fields.push(Field::new(
                        nested_field.name.clone(),
                        data_type,
                        !nested_field.required,
                    ));
                }
                Ok(DataType::Struct(fields.into()))
            }
            Type::List(list_type) => {
                let element_type = self.iceberg_to_datatype(&list_type.element_field.field_type)?;
                Ok(DataType::List(Arc::new(Field::new(
                    "element",
                    element_type,
                    !list_type.element_field.required,
                ))))
            }
            Type::Map(map_type) => {
                let key_type = self.iceberg_to_datatype(&map_type.key_field.field_type)?;
                let value_type = self.iceberg_to_datatype(&map_type.value_field.field_type)?;
                let entries = DataType::Struct(
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", value_type, !map_type.value_field.required),
                    ]
                    .into(),
                );
                Ok(DataType::Map(
                    Arc::new(Field::new("entries", entries, false)),
                    false,
                ))
            }
        }
    }

    // Convert LoadTableResult to TableStatus
    fn load_table_result_to_status(
        &self,
        table_name: &str,
        database: &Namespace,
        result: &crate::models::LoadTableResult,
    ) -> CatalogResult<TableStatus> {
        let metadata = &result.metadata;

        // Get the current schema
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
                let data_type = self.iceberg_to_datatype(&field.field_type)?;
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

    // Convert LoadViewResult to TableStatus
    fn load_view_result_to_status(
        &self,
        view_name: &str,
        database: &Namespace,
        result: &crate::models::LoadViewResult,
    ) -> CatalogResult<TableStatus> {
        let metadata = &result.metadata;

        // Get the current version
        let current_version = metadata
            .versions
            .iter()
            .find(|v| v.version_id == metadata.current_version_id);

        // Get the current schema
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
                let data_type = self.iceberg_to_datatype(&field.field_type)?;
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

        // Extract SQL definition from view version
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

        let namespace: Vec<String> = database.clone().into();

        if if_not_exists {
            let api = self.client.catalog_api_api();
            let namespace_param = self.namespace_to_url_param(&namespace);
            if api
                .namespace_exists(&self.prefix, &namespace_param)
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
            namespace: namespace.clone(),
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

        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        match api.drop_namespace(&self.prefix, &namespace_param).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to drop namespace: {}",
                        e
                    )))
                }
            }
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        let result = api
            .load_namespace_metadata(&self.prefix, &namespace_param)
            .await
            .map_err(|_e| CatalogError::NotFound("database", database.to_string()))?;

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
        let parent = prefix.map(|p| self.namespace_to_url_param(&self.namespace_to_iceberg(p)));

        let api = self.client.catalog_api_api();
        let result = api
            .list_namespaces(&self.prefix, None, None, parent.as_deref())
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list namespaces: {}", e)))?;

        let namespaces = result.namespaces.unwrap_or_default();
        let mut statuses = Vec::new();

        for namespace in namespaces {
            let namespace_str = self.namespace_to_url_param(&namespace);
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

                let ns = Namespace::try_from(&namespace[..])
                    .map_err(|e| CatalogError::External(format!("Invalid namespace: {}", e)))?;

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

        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        // Check if table exists when if_not_exists is set
        if if_not_exists {
            if let Ok(existing) = self.get_table(database, table).await {
                return Ok(existing);
            }
        }

        // Convert columns to Iceberg schema
        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let field_type = self.datatype_to_iceberg(&col.data_type)?;
            let mut field = crate::types::NestedField::new(
                idx as i32,
                col.name.clone(),
                field_type,
                !col.nullable,
            );
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

        // Build properties
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
            .create_table(&self.prefix, &namespace_param, request, None)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create table: {}", e)))?;

        // Convert LoadTableResult to TableStatus
        self.load_table_result_to_status(table, database, &result)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        let result = api
            .load_table(&self.prefix, &namespace_param, table, None, None, None)
            .await
            .map_err(|_e| CatalogError::NotFound("table", format!("{}.{}", database, table)))?;

        self.load_table_result_to_status(table, database, &result)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        let result = api
            .list_tables(&self.prefix, &namespace_param, None, None)
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

        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        match api
            .drop_table(&self.prefix, &namespace_param, table, Some(purge))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to drop table: {}",
                        e
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
        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace: _,
            comment,
            properties,
        } = options;

        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        // Check if view exists when if_not_exists is set
        if if_not_exists {
            if let Ok(existing) = self.get_view(database, view).await {
                return Ok(existing);
            }
        }

        // Convert columns to Iceberg schema
        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let field_type = self.datatype_to_iceberg(&col.data_type)?;
            let mut field = crate::types::NestedField::new(
                idx as i32,
                col.name.clone(),
                field_type,
                !col.nullable,
            );
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

        // Build view version with SQL representation
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
            default_namespace: namespace.clone(),
        };

        // Build properties
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
            .create_view(&self.prefix, &namespace_param, request)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create view: {}", e)))?;

        self.load_view_result_to_status(view, database, &result)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        let result = api
            .load_view(&self.prefix, &namespace_param, view)
            .await
            .map_err(|_e| CatalogError::NotFound("view", format!("{}.{}", database, view)))?;

        self.load_view_result_to_status(view, database, &result)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let namespace: Vec<String> = database.clone().into();
        let namespace_param = self.namespace_to_url_param(&namespace);

        let api = self.client.catalog_api_api();
        let result = api
            .list_views(&self.prefix, &namespace_param, None, None)
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
                    Err(CatalogError::External(format!("Failed to drop view: {e}",)))
                }
            }
        }
    }
}
