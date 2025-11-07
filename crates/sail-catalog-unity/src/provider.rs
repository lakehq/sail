// CHECK HERE
#![allow(unused_imports)]

use std::collections::HashMap;
use std::str::FromStr;

use arrow::datatypes::{DataType, Field, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions, DatabaseStatus,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace, TableColumnStatus,
    TableKind, TableStatus,
};
use sail_catalog::utils::{get_property, quote_name_if_needed};
use tokio::sync::OnceCell;

use crate::config::UnityCatalogConfigKey;
use crate::unity::{types, Client};

pub const UNITY_CATALOG_PROP_URI: &str = "uri";

const DEFAULT_URI: &str = "http://localhost:8080/api/2.1/unity-catalog";

struct UnityColumnType {
    type_text: String,
    type_json: serde_json::Value,
    type_name: types::ColumnTypeName,
}

// CHECK HERE
#[allow(dead_code)]
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

    fn get_full_schema_name(&self, namespace: &Namespace) -> String {
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

    fn get_full_table_name(&self, database: &Namespace, table: &str) -> String {
        format!(
            "{}.{}",
            self.get_full_schema_name(database),
            quote_name_if_needed(table)
        )
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

    // https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table
    fn datatype_to_unity_type(data_type: &DataType) -> CatalogResult<UnityColumnType> {
        // Docs additionally list the following types which we do not handle:
        //  CHAR | VARIANT | GEOMETRY | GEOGRAPHY | TABLE_TYPE
        //  https://docs.databricks.com/api/workspace/tables/create#columns-type_name
        match data_type {
            DataType::Null => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Null.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Null.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Null,
            }),
            DataType::Boolean => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Boolean.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Boolean.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Boolean,
            }),
            DataType::Int8 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Byte.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Byte.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Byte,
            }),
            DataType::Int16 | DataType::UInt8 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Short.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Short.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Short,
            }),
            DataType::Int32 | DataType::UInt16 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Int.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Int.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Int,
            }),
            DataType::Int64 | DataType::UInt32 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Long.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Long.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Long,
            }),
            DataType::Float32 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Float.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Float.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Float,
            }),
            DataType::Float64 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Double.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Double.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Double,
            }),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Timestamp.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Timestamp.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Timestamp,
            }),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::TimestampNtz
                    .to_string()
                    .to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::TimestampNtz
                        .to_string()
                        .to_lowercase(),
                ),
                type_name: types::ColumnTypeName::TimestampNtz,
            }),
            DataType::Date32 => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Date.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Date.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Date,
            }),
            DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::Binary.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::Binary.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::Binary,
            }),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(UnityColumnType {
                type_text: types::ColumnTypeName::String.to_string().to_lowercase(),
                type_json: serde_json::Value::String(
                    types::ColumnTypeName::String.to_string().to_lowercase(),
                ),
                type_name: types::ColumnTypeName::String,
            }),
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale) => {
                let type_str = format!("decimal({},{})", precision, scale);
                Ok(UnityColumnType {
                    type_text: type_str.clone(),
                    type_json: serde_json::Value::String(type_str),
                    type_name: types::ColumnTypeName::Decimal,
                })
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    let type_str = format!("decimal({},{})", precision, scale);
                    Ok(UnityColumnType {
                        type_text: type_str.clone(),
                        type_json: serde_json::Value::String(type_str),
                        type_name: types::ColumnTypeName::Decimal,
                    })
                } else {
                    Err(CatalogError::External(format!(
                        "Decimal with precision > {} and scale > {} is not supported in Unity Catalog",
                        DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE
                    )))
                }
            }
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => {
                // CHECK HERE
                // https://github.com/unitycatalog/unitycatalog/blob/fc76ab52aa03550d6577eca7be90004a5d2913b8/ai/core/tests/core/test_utils.py#L184
                let field_type = Self::datatype_to_unity_type(field.data_type())?;
                let type_text = format!("array<{}>", field_type.type_text);
                let mut metadata_map = serde_json::Map::new();
                for (k, v) in field.metadata() {
                    metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
                let type_json = serde_json::json!({
                    "type": serde_json::json!({
                        "type": "array",
                        "elementType": field_type.type_json,
                        "containsNull": field.is_nullable()
                    }),
                    "metadata": metadata_map
                });
                Ok(UnityColumnType {
                    type_text,
                    type_json,
                    type_name: types::ColumnTypeName::Array,
                })
            }
            DataType::Map(field, _) => {
                // CHECK HERE
                // https://github.com/unitycatalog/unitycatalog/blob/fc76ab52aa03550d6577eca7be90004a5d2913b8/ai/core/tests/core/test_utils.py#L287
                if let DataType::Struct(fields) = field.data_type() {
                    if fields.len() == 2 {
                        let key_type = Self::datatype_to_unity_type(fields[0].data_type())?;
                        let value_type = Self::datatype_to_unity_type(fields[1].data_type())?;
                        let type_text =
                            format!("map<{},{}>", key_type.type_text, value_type.type_text);
                        let mut metadata_map = serde_json::Map::new();
                        for (k, v) in field.metadata() {
                            metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
                        }
                        let type_json = serde_json::json!({
                            "type": serde_json::json!({
                                "type": "map",
                                "keyType": key_type.type_json,
                                "valueType": value_type.type_json,
                                "valueContainsNull": fields[1].is_nullable()
                            }),
                            "metadata": metadata_map
                        });
                        Ok(UnityColumnType {
                            type_text,
                            type_json,
                            type_name: types::ColumnTypeName::Map,
                        })
                    } else {
                        Err(CatalogError::External(format!(
                            "Map type struct must have exactly two fields, found {fields:?}"
                        )))
                    }
                } else {
                    Err(CatalogError::External(format!(
                        "Map type must be a struct with key and value fields, found {field:?}"
                    )))
                }
            }
            DataType::Struct(fields) => {
                // CHECK HERE
                // https://github.com/unitycatalog/unitycatalog/blob/fc76ab52aa03550d6577eca7be90004a5d2913b8/ai/core/tests/core/test_utils.py#L162
                let mut type_text_parts = Vec::new();
                let mut json_fields = Vec::new();
                for field in fields.iter() {
                    let field_type = Self::datatype_to_unity_type(field.data_type())?;
                    type_text_parts.push(format!("{}:{}", field.name(), field_type.type_text));
                    let mut metadata_map = serde_json::Map::new();
                    for (k, v) in field.metadata() {
                        metadata_map.insert(k.clone(), serde_json::Value::String(v.clone()));
                    }
                    json_fields.push(serde_json::json!({
                        "name": field.name(),
                        "type": field_type.type_json,
                        "nullable": field.is_nullable(),
                        "metadata": metadata_map,
                    }));
                }
                let type_text = format!("struct<{}>", type_text_parts.join(","));
                let type_json = serde_json::json!({
                    "type": serde_json::json!({
                        "type": "struct",
                        "fields": json_fields
                    }),
                });
                Ok(UnityColumnType {
                    type_text,
                    type_json,
                    type_name: types::ColumnTypeName::Struct,
                })
            }
            DataType::UInt64
            | DataType::Float16
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _) => Err(CatalogError::External(format!(
                "{data_type:?} type is not supported in Unity Catalog",
            ))),
        }
    }

    // fn parse_unity_type_text(type_text: &str) -> CatalogResult<DataType> {
    //     let type_upper = type_text.to_uppercase();
    //     match type_upper.as_str() {
    //         "BOOLEAN" => Ok(DataType::Boolean),
    //         "BYTE" | "TINYINT" => Ok(DataType::Int8),
    //         "SHORT" | "SMALLINT" => Ok(DataType::Int16),
    //         "INT" | "INTEGER" => Ok(DataType::Int32),
    //         "LONG" | "BIGINT" => Ok(DataType::Int64),
    //         "FLOAT" | "REAL" => Ok(DataType::Float32),
    //         "DOUBLE" => Ok(DataType::Float64),
    //         "DATE" => Ok(DataType::Date32),
    //         "TIMESTAMP" => Ok(DataType::Timestamp(
    //             TimeUnit::Microsecond,
    //             Some("UTC".into()),
    //         )),
    //         "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
    //         "STRING" | "VARCHAR" | "CHAR" => Ok(DataType::Utf8),
    //         "BINARY" => Ok(DataType::Binary),
    //         "NULL" => Ok(DataType::Null),
    //         s if s.starts_with("DECIMAL(") => {
    //             let inner = s.strip_prefix("DECIMAL(").and_then(|s| s.strip_suffix(")"));
    //             if let Some(inner) = inner {
    //                 let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    //                 if parts.len() == 2 {
    //                     let precision: u8 = parts[0].parse().map_err(|_| {
    //                         CatalogError::External(format!(
    //                             "Invalid DECIMAL precision: {}",
    //                             parts[0]
    //                         ))
    //                     })?;
    //                     let scale: i8 = parts[1].parse().map_err(|_| {
    //                         CatalogError::External(format!("Invalid DECIMAL scale: {}", parts[1]))
    //                     })?;
    //                     return Ok(DataType::Decimal128(precision, scale));
    //                 }
    //             }
    //             Err(CatalogError::External(format!(
    //                 "Invalid DECIMAL type: {type_text}",
    //             )))
    //         }
    //         s if s.starts_with("ARRAY<") => {
    //             let inner = s.strip_prefix("ARRAY<").and_then(|s| s.strip_suffix(">"));
    //             if let Some(inner) = inner {
    //                 let element_type = Self::parse_unity_type_text(inner)?;
    //                 return Ok(DataType::List(std::sync::Arc::new(Field::new(
    //                     "element",
    //                     element_type,
    //                     true,
    //                 ))));
    //             }
    //             Ok(DataType::List(std::sync::Arc::new(Field::new(
    //                 "element",
    //                 DataType::Utf8,
    //                 true,
    //             ))))
    //         }
    //         _ => Ok(DataType::Utf8),
    //     }
    // }

    fn table_info_to_table_status(
        &self,
        table_info: types::TableInfo,
        catalog_name: &str,
        schema_name: &str,
    ) -> CatalogResult<TableStatus> {
        let types::TableInfo {
            catalog_name: table_catalog_name,
            columns,
            comment,
            created_at,
            created_by,
            data_source_format,
            name,
            owner,
            properties,
            schema_name: table_schema_name,
            storage_location,
            table_id,
            table_type,
            updated_at,
            updated_by,
        } = table_info;

        let name = name.unwrap_or_default();
        let catalog = table_catalog_name.unwrap_or(catalog_name.to_string());
        let schema = table_schema_name.unwrap_or(schema_name.to_string());
        let database = vec![catalog.clone(), schema];

        let mut partition_by: Vec<String> = vec![];
        let columns = columns
            .into_iter()
            .map(|col| {
                let types::ColumnInfo {
                    comment,
                    name,
                    nullable,
                    partition_index,
                    position: _,
                    type_interval_type: _,
                    type_json: _,
                    type_name: _,
                    type_precision: _,
                    type_scale: _,
                    type_text: _,
                } = col;
                if partition_index.is_some() {
                    if let Some(col_name) = &name {
                        partition_by.push(col_name.to_string());
                    }
                }
                Ok(TableColumnStatus {
                    name: name.unwrap_or_default(),
                    data_type: DataType::Null, // CHECK HERE THIS IS PLACEHOLDER
                    nullable,
                    comment,
                    default: None,
                    generated_always_as: None,
                    is_partition: partition_index.is_some(),
                    is_bucket: false,
                    is_cluster: false,
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;

        let format = data_source_format
            .map(|f| f.to_string().to_lowercase())
            .unwrap_or_else(|| "delta".to_string());

        let mut properties: HashMap<String, String> = properties.map(|p| p.0).unwrap_or_default();

        let comment = comment.or(get_property(&properties, "comment"));

        let options: Vec<_> = properties
            .extract_if(|k, _| k.trim().to_lowercase().starts_with("options."))
            .map(|(k, v)| {
                let trimmed = k.trim().to_string();
                let stripped =
                    if trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("options.") {
                        trimmed[8..].to_string()
                    } else {
                        trimmed
                    };
                (stripped, v)
            })
            .collect();

        if let Some(created_at) = created_at {
            properties.insert("created_at".to_string(), created_at.to_string());
        }
        if let Some(created_by) = created_by {
            properties.insert("created_by".to_string(), created_by);
        }
        if let Some(owner) = owner {
            properties.insert("owner".to_string(), owner);
        }
        if let Some(table_id) = table_id {
            properties.insert("table_id".to_string(), table_id);
        }
        if let Some(updated_at) = updated_at {
            properties.insert("updated_at".to_string(), updated_at.to_string());
        }
        if let Some(updated_by) = updated_by {
            properties.insert("updated_by".to_string(), updated_by);
        }
        if let Some(table_type) = table_type {
            properties.insert("table_type".to_string(), table_type.to_string());
        }

        let properties: Vec<_> = properties.into_iter().collect();

        Ok(TableStatus {
            name,
            kind: TableKind::Table {
                catalog,
                database,
                columns,
                comment,
                constraints: vec![],
                location: storage_location,
                format,
                partition_by,
                sort_by: vec![],
                bucket_by: None,
                options,
                properties,
            },
        })
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
        let full_name = self.get_full_schema_name(database);
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

        let full_name = self.get_full_schema_name(database);

        let result = client
            .delete_schema()
            .full_name(&full_name)
            .force(cascade)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(progenitor_client::Error::InvalidResponsePayload(bytes, _))
                if bytes.as_ref() == b"200 OK" =>
            {
                Ok(())
            }
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
        // Only external table creation is supported according to the Unity Catalog OpenAPI spec.
        let CreateTableOptions {
            columns,
            comment,
            constraints,
            location,
            format,
            partition_by,
            sort_by,
            bucket_by,
            if_not_exists,
            replace,
            options,
            properties,
        } = options;

        if replace {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support REPLACE option".to_string(),
            ));
        }

        if !sort_by.is_empty() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support SORT BY option".to_string(),
            ));
        }

        if bucket_by.is_some() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support BUCKET BY option".to_string(),
            ));
        }

        if !constraints.is_empty() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support CONSTRAINT option".to_string(),
            ));
        }

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);

        let data_source_format = types::DataSourceFormat::from_str(&format.trim().to_uppercase())
            .map_err(|e| {
            CatalogError::InvalidArgument(format!("Invalid data source format: {e}"))
        })?;

        let unity_columns: Vec<types::ColumnInfo> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let unity_type = Self::datatype_to_unity_type(&col.data_type)?;
                let (type_precision, type_scale) = match &col.data_type {
                    DataType::Decimal32(precision, scale)
                    | DataType::Decimal64(precision, scale)
                    | DataType::Decimal128(precision, scale)
                    | DataType::Decimal256(precision, scale) => {
                        (Some(*precision as i32), Some(*scale as i32))
                    }
                    _ => (None, None),
                };
                let partition_index = partition_by
                    .iter()
                    .position(|p| p.trim().to_lowercase() == col.name.trim().to_lowercase())
                    .map(|i| i as i32);
                Ok(types::ColumnInfo {
                    comment: col.comment.clone(),
                    name: Some(col.name.clone()),
                    nullable: col.nullable,
                    partition_index,
                    position: Some(idx as i32),
                    type_interval_type: None, // FIXME: Handle interval types
                    type_json: Some(unity_type.type_json.to_string()),
                    type_name: Some(unity_type.type_name),
                    type_precision,
                    type_scale,
                    type_text: Some(unity_type.type_text),
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;

        let mut props = HashMap::new();
        // TODO: Is this correct for options?
        for (k, v) in options {
            props.insert(format!("options.{k}"), v);
        }
        if let Some(c) = &comment {
            props.insert("comment".to_string(), c.to_string());
        }
        for (k, v) in properties {
            props.insert(k, v);
        }

        let request = types::CreateTable::builder()
            .name(table)
            .catalog_name(&catalog_name)
            .schema_name(&schema_name)
            .table_type(types::TableType::External)
            .data_source_format(data_source_format)
            .columns(unity_columns)
            .storage_location(location.ok_or_else(|| {
                CatalogError::External(
                    "Storage location is required for Unity Catalog tables".to_string(),
                )
            })?)
            .comment(comment)
            .properties(if props.is_empty() {
                None
            } else {
                Some(types::SecurablePropertiesMap::from(props))
            });

        let result = client.create_table().body(request).send().await;

        match result {
            Ok(response) => {
                let table_info = response.into_inner();
                eprintln!("CHECK HERE CREATE TABLE table_info: {table_info:?}");
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
                if response.status().as_u16() == 409 && if_not_exists =>
            {
                self.get_table(database, table).await
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to create table: {e}"
            ))),
        }
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);
        let full_name = format!("{}.{}.{}", catalog_name, schema_name, table);

        let result = client.get_table().full_name(&full_name).send().await;

        match result {
            Ok(response) => {
                let table_info = response.into_inner();
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
                if response.status().as_u16() == 404 =>
            {
                Err(CatalogError::NotFound("table", full_name))
            }
            Err(e) => Err(CatalogError::External(format!("Failed to get table: {e}"))),
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);

        let result = client
            .list_tables()
            .catalog_name(&catalog_name)
            .schema_name(&schema_name)
            .send()
            .await;

        match result {
            Ok(response) => {
                let list_response = response.into_inner();
                list_response
                    .tables
                    .into_iter()
                    .map(|table_info| {
                        self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
                    })
                    .collect::<CatalogResult<Vec<_>>>()
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to list tables: {e}"
            ))),
        }
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
                "Open source Unity Catalog does not support PURGE option".to_string(),
            ));
        }

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let full_name = self.get_full_table_name(database, table);

        let result = client.delete_table().full_name(&full_name).send().await;

        match result {
            Ok(_) => Ok(()),
            Err(progenitor_client::Error::InvalidResponsePayload(bytes, _))
                if bytes.as_ref() == b"200 OK" =>
            {
                Ok(())
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
                if response.status().as_u16() == 404 && if_exists =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!("Failed to drop table: {e}"))),
        }
    }

    async fn create_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "Open source Unity Catalog does not support creating views".to_string(),
        ))
    }

    async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "Open source Unity Catalog does not support getting views".to_string(),
        ))
    }

    async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Err(CatalogError::NotSupported(
            "Open source Unity Catalog does not support listing views".to_string(),
        ))
    }

    async fn drop_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: DropViewOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "Open source Unity Catalog does not support dropping views".to_string(),
        ))
    }
}
