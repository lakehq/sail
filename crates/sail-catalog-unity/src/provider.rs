// CHECK HERE
#![allow(unused_imports)]

use std::collections::HashMap;
use std::str::FromStr;

use datafusion::arrow::datatypes::{
    DataType, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
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

    // CHECK HERE
    fn datatype_to_type_text(data_type: &DataType) -> CatalogResult<String> {
        // Docs additionally list the following types which we do not handle:
        //  CHAR | VARIANT | GEOMETRY | GEOGRAPHY | TABLE_TYPE
        //  https://docs.databricks.com/api/workspace/tables/create#columns-type_name

        // DataType::List(field)
        //     | DataType::LargeList(field)
        //     | DataType::FixedSizeList(field, _) => {
        //     format!("ARRAY<{}>", Self::datatype_to_type_text(field.data_type()))
        // }
        // DataType::Struct(fields) => {
        //     let field_strs: Vec<String> = fields
        //         .iter()
        //         .map(|f| {
        //             format!(
        //                 "{}: {}",
        //                 f.name(),
        //                 Self::datatype_to_type_text(f.data_type())
        //             )
        //         })
        //         .collect();
        //     format!("STRUCT<{}>", field_strs.join(", "))
        // }

        match data_type {
            DataType::Null => Ok("NULL".to_string()),
            DataType::Boolean => Ok("BOOLEAN".to_string()),
            DataType::Int8 => Ok("BYTE".to_string()),
            DataType::Int16 | DataType::UInt8 => Ok("SHORT".to_string()),
            DataType::Int32 | DataType::UInt16 => Ok("INT".to_string()),
            DataType::Int64 | DataType::UInt32 => Ok("LONG".to_string()),
            DataType::Float32 => Ok("FLOAT".to_string()),
            DataType::Float64 => Ok("DOUBLE".to_string()),
            DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMP".to_string()),
            DataType::Timestamp(_, None) => Ok("TIMESTAMP_NTZ".to_string()),
            DataType::Date32 => Ok("DATE".to_string()),
            DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView => Ok("BINARY".to_string()),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("STRING".to_string()),
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field) => {
                // CHECK HERE
                Ok(format!(
                    "ARRAY<{}>",
                    Self::datatype_to_type_text(field.data_type())?
                ))
            }
            DataType::Struct(fields) => {
                // CHECK HERE
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|f| {
                        Ok(format!(
                            "{}: {}",
                            f.name(),
                            Self::datatype_to_type_text(f.data_type())?
                        ))
                    })
                    .collect::<CatalogResult<Vec<_>>>()?;
                Ok(format!("STRUCT<{}>", field_strs.join(", ")))
            }
            DataType::Decimal32(precision, scale)
            | DataType::Decimal64(precision, scale)
            | DataType::Decimal128(precision, scale) => {
                Ok(format!("DECIMAL({precision}, {scale})"))
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    Ok(format!("DECIMAL({precision}, {scale})"))
                } else {
                    Err(CatalogError::External(format!(
                        "Decimal with precision > {DECIMAL128_MAX_PRECISION} and scale > {DECIMAL128_MAX_SCALE} is not supported in Unity Catalog"
                    )))
                }
            }
            DataType::Map(field, _) => {
                // CHECK HERE
                if let DataType::Struct(fields) = field.data_type() {
                    if fields.len() == 2 {
                        let key_type = Self::datatype_to_type_text(fields[0].data_type())?;
                        let value_type = Self::datatype_to_type_text(fields[1].data_type())?;
                        return Ok(format!("MAP<{key_type}, {value_type}>"));
                    }
                }
                Ok("MAP<STRING, STRING>".to_string())
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
                "{data_type:?} type is not supported in Unity Catalog"
            ))),
        }
    }

    fn datatype_to_column_type_name(data_type: &DataType) -> CatalogResult<types::ColumnTypeName> {
        // Docs additionally list the following types which we do not handle:
        //  CHAR | VARIANT | GEOMETRY | GEOGRAPHY | TABLE_TYPE
        //  https://docs.databricks.com/api/workspace/tables/create#columns-type_name
        match data_type {
            DataType::Null => Ok(types::ColumnTypeName::Null),
            DataType::Boolean => Ok(types::ColumnTypeName::Boolean),
            DataType::Int8 => Ok(types::ColumnTypeName::Byte),
            DataType::Int16 | DataType::UInt8 => Ok(types::ColumnTypeName::Short),
            DataType::Int32 | DataType::UInt16 => Ok(types::ColumnTypeName::Int),
            DataType::Int64 | DataType::UInt32 => Ok(types::ColumnTypeName::Long),
            DataType::Float32 => Ok(types::ColumnTypeName::Float),
            DataType::Float64 => Ok(types::ColumnTypeName::Double),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                Ok(types::ColumnTypeName::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(types::ColumnTypeName::TimestampNtz)
            }
            DataType::Date32 => Ok(types::ColumnTypeName::Date),
            DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView => Ok(types::ColumnTypeName::Binary),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(types::ColumnTypeName::String)
            }
            DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_) => Ok(types::ColumnTypeName::Array),
            DataType::Struct(_) => Ok(types::ColumnTypeName::Struct),
            DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) => {
                Ok(types::ColumnTypeName::Decimal)
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    Ok(types::ColumnTypeName::Decimal)
                } else {
                    Err(CatalogError::External(format!(
                        "Decimal with precision > {DECIMAL128_MAX_PRECISION} and scale > {DECIMAL128_MAX_SCALE} is not supported in Unity Catalog"
                    )))
                }
            }
            DataType::Map(_, _) => Ok(types::ColumnTypeName::Map),
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
                "{data_type:?} type is not supported in Unity Catalog"
            ))),
        }
    }

    fn column_info_to_column_status(
        column: types::ColumnInfo,
        partition_columns: &[String],
    ) -> CatalogResult<TableColumnStatus> {
        let name = column.name.unwrap_or_default();
        let type_text = column.type_text.unwrap_or_else(|| "STRING".to_string());
        let data_type = Self::parse_unity_type_text(&type_text)?;
        let nullable = column.nullable;
        let comment = column.comment;

        Ok(TableColumnStatus {
            name: name.clone(),
            data_type,
            nullable,
            comment,
            default: None,
            generated_always_as: None,
            is_partition: partition_columns.contains(&name),
            is_bucket: false,
            is_cluster: false,
        })
    }

    fn parse_unity_type_text(type_text: &str) -> CatalogResult<DataType> {
        let type_upper = type_text.to_uppercase();
        match type_upper.as_str() {
            "BOOLEAN" => Ok(DataType::Boolean),
            "BYTE" | "TINYINT" => Ok(DataType::Int8),
            "SHORT" | "SMALLINT" => Ok(DataType::Int16),
            "INT" | "INTEGER" => Ok(DataType::Int32),
            "LONG" | "BIGINT" => Ok(DataType::Int64),
            "FLOAT" | "REAL" => Ok(DataType::Float32),
            "DOUBLE" => Ok(DataType::Float64),
            "DATE" => Ok(DataType::Date32),
            "TIMESTAMP" => Ok(DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            "STRING" | "VARCHAR" | "CHAR" => Ok(DataType::Utf8),
            "BINARY" => Ok(DataType::Binary),
            "NULL" => Ok(DataType::Null),
            s if s.starts_with("DECIMAL(") => {
                let inner = s.strip_prefix("DECIMAL(").and_then(|s| s.strip_suffix(")"));
                if let Some(inner) = inner {
                    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
                    if parts.len() == 2 {
                        let precision: u8 = parts[0].parse().map_err(|_| {
                            CatalogError::External(format!(
                                "Invalid DECIMAL precision: {}",
                                parts[0]
                            ))
                        })?;
                        let scale: i8 = parts[1].parse().map_err(|_| {
                            CatalogError::External(format!("Invalid DECIMAL scale: {}", parts[1]))
                        })?;
                        return Ok(DataType::Decimal128(precision, scale));
                    }
                }
                Err(CatalogError::External(format!(
                    "Invalid DECIMAL type: {}",
                    type_text
                )))
            }
            s if s.starts_with("ARRAY<") => {
                let inner = s.strip_prefix("ARRAY<").and_then(|s| s.strip_suffix(">"));
                if let Some(inner) = inner {
                    let element_type = Self::parse_unity_type_text(inner)?;
                    return Ok(DataType::List(std::sync::Arc::new(
                        datafusion::arrow::datatypes::Field::new("element", element_type, true),
                    )));
                }
                Ok(DataType::List(std::sync::Arc::new(
                    datafusion::arrow::datatypes::Field::new("element", DataType::Utf8, true),
                )))
            }
            _ => Ok(DataType::Utf8),
        }
    }

    fn table_info_to_table_status(
        &self,
        table_info: types::TableInfo,
        catalog_name: &str,
        schema_name: &str,
    ) -> CatalogResult<TableStatus> {
        let name = table_info.name.unwrap_or_default();
        let catalog = table_info.catalog_name.unwrap_or(catalog_name.to_string());
        let schema = table_info.schema_name.unwrap_or(schema_name.to_string());
        let database = vec![catalog.clone(), schema];

        let columns = table_info
            .columns
            .into_iter()
            .map(|col| Self::column_info_to_column_status(col, &[]))
            .collect::<CatalogResult<Vec<_>>>()?;

        let format = table_info
            .data_source_format
            .map(|f| f.to_string().to_lowercase())
            .unwrap_or_else(|| "delta".to_string());

        let mut properties: HashMap<String, String> =
            table_info.properties.map(|p| p.0).unwrap_or_default();

        if let Some(created_at) = table_info.created_at {
            properties.insert("created_at".to_string(), created_at.to_string());
        }
        if let Some(created_by) = table_info.created_by {
            properties.insert("created_by".to_string(), created_by);
        }
        if let Some(owner) = table_info.owner {
            properties.insert("owner".to_string(), owner);
        }
        if let Some(table_id) = table_info.table_id {
            properties.insert("table_id".to_string(), table_id);
        }
        if let Some(updated_at) = table_info.updated_at {
            properties.insert("updated_at".to_string(), updated_at.to_string());
        }
        if let Some(updated_by) = table_info.updated_by {
            properties.insert("updated_by".to_string(), updated_by);
        }

        Ok(TableStatus {
            name,
            kind: TableKind::Table {
                catalog,
                database,
                columns,
                comment: table_info.comment,
                constraints: vec![],
                location: table_info.storage_location,
                format,
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                options: vec![],
                properties: properties.into_iter().collect(),
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
            options: table_options,
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
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database);

        let data_source_format = match format.trim().to_uppercase().as_str() {
            "DELTA" => types::DataSourceFormat::Delta,
            "CSV" => types::DataSourceFormat::Csv,
            "JSON" => types::DataSourceFormat::Json,
            "AVRO" => types::DataSourceFormat::Avro,
            "PARQUET" => types::DataSourceFormat::Parquet,
            "ORC" => types::DataSourceFormat::Orc,
            "TEXT" => types::DataSourceFormat::Text,
            "UNITY_CATALOG" => types::DataSourceFormat::UnityCatalog,
            "DELTASHARING" => types::DataSourceFormat::Deltasharing,
            "DATABRICKS_FORMAT" => types::DataSourceFormat::DatabricksFormat,
            "MYSQL_FORMAT" => types::DataSourceFormat::MysqlFormat,
            "ORACLE_FORMAT" => types::DataSourceFormat::OracleFormat,
            "POSTGRESQL_FORMAT" => types::DataSourceFormat::PostgresqlFormat,
            "REDSHIFT_FORMAT" => types::DataSourceFormat::RedshiftFormat,
            "SNOWFLAKE_FORMAT" => types::DataSourceFormat::SnowflakeFormat,
            "SQLDW_FORMAT" => types::DataSourceFormat::SqldwFormat,
            "SQLSERVER_FORMAT" => types::DataSourceFormat::SqlserverFormat,
            "SALESFORCE_FORMAT" => types::DataSourceFormat::SalesforceFormat,
            "SALESFORCE_DATA_CLOUD_FORMAT" => types::DataSourceFormat::SalesforceDataCloudFormat,
            "TERADATA_FORMAT" => types::DataSourceFormat::TeradataFormat,
            "BIGQUERY_FORMAT" => types::DataSourceFormat::BigqueryFormat,
            "NETSUITE_FORMAT" => types::DataSourceFormat::NetsuiteFormat,
            "WORKDAY_RAAS_FORMAT" => types::DataSourceFormat::WorkdayRaasFormat,
            "MONGODB_FORMAT" => types::DataSourceFormat::MongodbFormat,
            "HIVE" => types::DataSourceFormat::Hive,
            "VECTOR_INDEX_FORMAT" => types::DataSourceFormat::VectorIndexFormat,
            "DATABRICKS_ROW_STORE_FORMAT" => types::DataSourceFormat::DatabricksRowStoreFormat,
            "DELTA_UNIFORM_HUDI" => types::DataSourceFormat::DeltaUniformHudi,
            "DELTA_UNIFORM_ICEBERG" => types::DataSourceFormat::DeltaUniformIceberg,
            "ICEBERG" => types::DataSourceFormat::Iceberg,
            _ => {
                return Err(CatalogError::External(format!(
                    "Unsupported data source format: {format}",
                )))
            }
        };

        let unity_columns: Vec<types::ColumnInfo> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let type_text = Self::datatype_to_type_text(&col.data_type)?;
                let type_name = Self::datatype_to_column_type_name(&col.data_type)?;
                let (type_precision, type_scale) = match &col.data_type {
                    DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                        (Some(*p as i32), Some(*s as i32))
                    }
                    _ => (None, None),
                };

                Ok(types::ColumnInfo {
                    comment: col.comment.clone(),
                    name: Some(col.name.clone()),
                    nullable: col.nullable,
                    partition_index: None,
                    position: Some(idx as i32),
                    type_interval_type: None,
                    type_json: Some(serde_json::json!({"type": type_text}).to_string()),
                    type_name: Some(type_name),
                    type_precision,
                    type_scale,
                    type_text: Some(type_text),
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;

        let mut props: HashMap<String, String> = properties.into_iter().collect();
        for (k, v) in table_options {
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
