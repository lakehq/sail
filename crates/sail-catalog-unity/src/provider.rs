// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::datatypes::DataType;
use sail_catalog::credentials::CatalogCredentials;
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaRatifiedCommit, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest,
    LakehouseCreateMaterialization, LakehouseCreatePlan, LakehouseCreateRequest,
};
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableMetadataRequirement,
    CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions,
    LakeSourceCreateMetadataMode, Namespace, PartitionTransform,
};
use sail_catalog::utils::{get_property, quote_names_if_needed, quote_namespace_if_needed};
use sail_common::utils::http::SAIL_USER_AGENT;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY, unity_table_id_value,
};
use sail_common_datafusion::catalog::{
    DatabaseStatus, TableColumnStatus, TableKind, TableStatus, identity_partition_fields,
};

use crate::data_type::{
    data_type_to_unity_type, unity_struct_field_type_json, unity_type_to_data_type,
};
use crate::r#gen::ApiClient;
use crate::{delta_gen, r#gen};

pub(crate) const DEFAULT_URI: &str = "http://localhost:8080/api/2.1/unity-catalog";

/// The highest version of the Unity Catalog Delta REST API supported by the client.
const DELTA_API_PROTOCOL_VERSIONS: &str = "1.0";

/// The Unity Catalog API used for catalog-managed Delta commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeltaCommitApi {
    /// The Delta REST API (`/delta/v1`).
    V1,
    /// The legacy API (`/delta/preview/commits`) for servers without the Delta REST API.
    Preview,
}

/// Provider for Unity Catalog
pub struct UnityCatalogProvider {
    name: String,
    options: UnityCatalogOptions,
    http_client: reqwest::Client,
    /// The negotiated Delta commit API for each catalog.
    delta_commit_apis: Mutex<HashMap<String, DeltaCommitApi>>,
}

#[derive(Debug, Clone)]
pub struct UnityCatalogOptions {
    pub default_catalog: String,
    pub uri: String,
    pub credentials: Arc<dyn CatalogCredentials>,
}

impl UnityCatalogProvider {
    pub fn new(name: String, options: UnityCatalogOptions) -> CatalogResult<Self> {
        let http_client = reqwest::Client::new();
        Ok(Self {
            name,
            options,
            http_client,
            delta_commit_apis: Mutex::new(HashMap::new()),
        })
    }

    async fn get_client(&self) -> CatalogResult<ApiClient> {
        Ok(ApiClient::new(
            self.options.uri.clone(),
            self.http_client.clone(),
            self.get_request_headers().await?,
        ))
    }

    async fn get_delta_client(&self) -> CatalogResult<delta_gen::ApiClient> {
        Ok(delta_gen::ApiClient::new(
            self.options.uri.clone(),
            self.http_client.clone(),
            self.get_request_headers().await?,
        ))
    }

    async fn get_request_headers(&self) -> CatalogResult<reqwest::header::HeaderMap> {
        let mut headers = reqwest::header::HeaderMap::new();

        if let Some(token) = self.options.credentials.retrieve().await? {
            let header = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| {
                    CatalogError::External(format!("Failed to create header value from token: {e}"))
                })?;
            headers.insert(reqwest::header::AUTHORIZATION, header);
        }

        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_static(SAIL_USER_AGENT),
        );

        Ok(headers)
    }

    /// Determines which API to use for catalog-managed Delta commits in the catalog.
    ///
    /// The Delta REST API is used if the server answers its configuration endpoint
    /// successfully, whatever the response body is.
    /// The legacy API is used if the server does not have the endpoint or does not
    /// support the protocol version of the client. Other failures are returned as errors
    /// and are not remembered.
    async fn get_delta_commit_api(
        &self,
        client: &delta_gen::ApiClient,
        catalog_name: &str,
    ) -> CatalogResult<DeltaCommitApi> {
        if let Some(api) = self.delta_commit_api_cache()?.get(catalog_name).copied() {
            return Ok(api);
        }
        let api = match client
            .get_config(
                catalog_name.to_string(),
                DELTA_API_PROTOCOL_VERSIONS.to_string(),
            )
            .await
        {
            Ok(_) => DeltaCommitApi::V1,
            Err(e) if e.status().is_some_and(|status| status.is_success()) => DeltaCommitApi::V1,
            Err(e)
                if matches!(
                    e.status(),
                    Some(
                        reqwest::StatusCode::NOT_FOUND
                            | reqwest::StatusCode::NOT_IMPLEMENTED
                            | reqwest::StatusCode::BAD_REQUEST
                    )
                ) =>
            {
                DeltaCommitApi::Preview
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED) => {
                return Err(CatalogError::Unauthorized(format!(
                    "Unity Catalog Delta API configuration unauthorized: {e}"
                )));
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::FORBIDDEN) => {
                return Err(CatalogError::Forbidden(format!(
                    "Unity Catalog Delta API configuration forbidden: {e}"
                )));
            }
            Err(e) => {
                return Err(CatalogError::External(format!(
                    "Failed to get Delta API configuration from Unity Catalog: {e}"
                )));
            }
        };
        self.delta_commit_api_cache()?
            .insert(catalog_name.to_string(), api);
        Ok(api)
    }

    fn delta_commit_api_cache(
        &self,
    ) -> CatalogResult<MutexGuard<'_, HashMap<String, DeltaCommitApi>>> {
        self.delta_commit_apis.lock().map_err(|e| {
            CatalogError::Internal(format!("Unity Catalog Delta commit API cache: {e}"))
        })
    }

    fn get_catalog_and_schema_name(
        &self,
        namespace: &Namespace,
    ) -> CatalogResult<(String, String)> {
        match namespace.tail.as_slice() {
            [] => Ok((
                self.options.default_catalog.to_string(),
                namespace.head.to_string(),
            )),
            [x] => Ok((namespace.head.to_string(), x.to_string())),
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog does not support multi-level schema name: {}",
                quote_namespace_if_needed(namespace)
            ))),
        }
    }

    async fn create_managed_staging_location(
        &self,
        client: &ApiClient,
        catalog_name: &str,
        schema_name: &str,
        table: &str,
    ) -> CatalogResult<String> {
        let request = r#gen::CreateStagingTable {
            name: table.to_string(),
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        };
        let response = match client.create_staging_table(request).await {
            Ok(response) => response,
            Err(e) => {
                return Err(CatalogError::External(format!(
                    "Failed to create staging table: {e}"
                )));
            }
        };
        response.inner.staging_location.ok_or_else(|| {
            CatalogError::External(
                "Unity Catalog staging table response is missing a staging location".to_string(),
            )
        })
    }

    /// Returns full name of an object for the REST API (`catalog.schema.table`).
    ///
    /// Names are joined without SQL quoting. Unity Catalog does not parse backticks in
    /// full names, so `` main.`a-b` `` would refer to a schema named `` `a-b` `` instead of `a-b`.
    fn full_name(names: &[&str]) -> String {
        names.join(".")
    }

    fn schema_info_to_database_status(
        &self,
        schema_info: r#gen::SchemaInfo,
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

        let mut properties: HashMap<String, String> = schema_info
            .properties
            .map(|p| p.0.into_iter().collect())
            .unwrap_or_default();
        if let Some(created_at) = schema_info.created_at
            && !properties.contains_key("created_at")
        {
            properties.insert("created_at".to_string(), created_at.to_string());
        }
        if let Some(created_by) = schema_info.created_by
            && !properties.contains_key("created_by")
        {
            properties.insert("created_by".to_string(), created_by);
        }
        if let Some(owner) = schema_info.owner
            && !properties.contains_key("owner")
        {
            properties.insert("owner".to_string(), owner);
        }
        if let Some(schema_id) = schema_info.schema_id
            && !properties.contains_key("schema_id")
        {
            properties.insert("schema_id".to_string(), schema_id);
        }
        if let Some(updated_at) = schema_info.updated_at
            && !properties.contains_key("updated_at")
        {
            properties.insert("updated_at".to_string(), updated_at.to_string());
        }
        if let Some(updated_by) = schema_info.updated_by
            && !properties.contains_key("updated_by")
        {
            properties.insert("updated_by".to_string(), updated_by);
        }

        DatabaseStatus {
            catalog: self.name.clone(),
            database,
            comment: schema_info.comment,
            location: get_property(&properties, "location"),
            properties: properties.into_iter().collect(),
        }
    }

    fn table_info_to_table_status(
        &self,
        table_info: r#gen::TableInfo,
        catalog_name: &str,
        schema_name: &str,
    ) -> CatalogResult<TableStatus> {
        let r#gen::TableInfo {
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
            ..
        } = table_info;

        let name = name.unwrap_or_default();
        let catalog = table_catalog_name.unwrap_or(catalog_name.to_string());
        let schema = table_schema_name.unwrap_or(schema_name.to_string());
        let database = vec![catalog.clone(), schema];

        let mut partition_by: Vec<String> = vec![];
        // If all partition indices are 0, treat the table as unpartitioned.
        // This can happen in the Unity Catalog endpoint in OneLake.
        let columns = columns.unwrap_or_default();
        let ignore_partition_index =
            !columns.is_empty() && columns.iter().all(|col| col.partition_index == Some(0));
        let columns = columns
            .into_iter()
            .map(|col| {
                let r#gen::ColumnInfo {
                    comment,
                    name,
                    nullable,
                    partition_index,
                    position: _,
                    type_interval_type,
                    type_json,
                    type_name,
                    type_precision,
                    type_scale,
                    type_text,
                } = col;
                let partition_index = if ignore_partition_index {
                    None
                } else {
                    partition_index
                };
                if partition_index.is_some()
                    && let Some(col_name) = &name
                {
                    partition_by.push(col_name.to_string());
                }
                Ok(TableColumnStatus {
                    metadata: Default::default(),
                    name: name.unwrap_or_default(),
                    data_type: unity_type_to_data_type(
                        type_name.map(|type_name| *type_name),
                        type_json,
                        type_text,
                        type_precision,
                        type_scale,
                        type_interval_type,
                    )?,
                    nullable: nullable.unwrap_or(true),
                    comment,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                    is_partition: partition_index.is_some(),
                    is_bucket: false,
                    is_cluster: false,
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;

        let format = data_source_format
            .map(|f| f.to_string().to_lowercase())
            .unwrap_or_else(|| "delta".to_string());

        let mut properties: HashMap<String, String> = properties
            .map(|p| p.0.into_iter().collect())
            .unwrap_or_default();

        let comment = comment.or(get_property(&properties, "comment"));

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
            properties.insert(
                DELTA_UNITY_TABLE_ID_LEGACY_KEY.to_string(),
                table_id.clone(),
            );
            properties.insert(DELTA_UNITY_TABLE_ID_KEY.to_string(), table_id);
        }
        if let Some(updated_at) = updated_at {
            properties.insert("updated_at".to_string(), updated_at.to_string());
        }
        if let Some(updated_by) = updated_by {
            properties.insert("updated_by".to_string(), updated_by);
        }
        let is_external = table_type
            .as_ref()
            .is_none_or(|table_type| matches!(&**table_type, r#gen::TableType::External));
        if let Some(table_type) = table_type {
            properties.insert("table_type".to_string(), table_type.to_string());
        }

        let properties: Vec<_> = properties.into_iter().collect();

        Ok(TableStatus {
            catalog: Some(self.name.clone()),
            database,
            name,
            kind: TableKind::Table {
                columns,
                comment,
                constraints: vec![],
                location: storage_location,
                format,
                partition_by: identity_partition_fields(&partition_by),
                sort_by: vec![],
                bucket_by: None,
                properties,
                is_external,
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;

        let request = r#gen::CreateSchema {
            catalog_name: catalog_name.clone(),
            name: schema_name.clone(),
            comment,
            properties: if props.is_empty() {
                None
            } else {
                Some(Box::new(r#gen::SecurablePropertiesMap(
                    props.into_iter().collect(),
                )))
            },
            storage_root: None,
        };

        let result = client.create_schema(request).await;

        match result {
            Ok(response) => {
                let schema_info = response.inner;
                Ok(self.schema_info_to_database_status(
                    schema_info,
                    &catalog_name,
                    Some(&schema_name),
                ))
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::CONFLICT) && if_not_exists => {
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::full_name(&[&catalog_name, &schema_name]);

        let result = client.get_schema(full_name).await;

        match result {
            Ok(response) => {
                let schema_info = response.inner;
                Ok(self.schema_info_to_database_status(
                    schema_info,
                    &catalog_name,
                    Some(&schema_name),
                ))
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(CatalogError::NotFound(
                    CatalogObject::Schema,
                    quote_names_if_needed(&[&catalog_name, &schema_name]),
                ))
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

        let catalog_name = match prefix {
            None => self.options.default_catalog.to_string(),
            Some(Namespace { head, tail }) if tail.is_empty() => head.to_string(),
            Some(x) => {
                return Err(CatalogError::InvalidArgument(format!(
                    "invalid prefix: {}",
                    quote_namespace_if_needed(x)
                )));
            }
        };
        let result = client.list_schemas(catalog_name.clone(), None, None).await;

        match result {
            Ok(response) => {
                let list_response = response.inner;
                Ok(list_response
                    .schemas
                    .unwrap_or_default()
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::full_name(&[&catalog_name, &schema_name]);

        let result = client.delete_schema(full_name, Some(cascade)).await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) && if_exists => Ok(()),
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
        self.create_table_metadata_requirement(&options)?;
        let CreateTableOptions {
            columns,
            comment,
            constraints: _,
            location,
            format,
            partition_by,
            sort_by: _,
            bucket_by: _,
            mode,
            properties,
            is_external,
            is_write_precondition,
        } = options;

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        if !is_external && mode.ignore_if_exists() {
            match self.get_table(database, table).await {
                Ok(status) => return Ok(status),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }

        let data_source_format = parse_data_source_format(&format)?;

        let unity_columns: Vec<r#gen::ColumnInfo> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let unity_type = data_type_to_unity_type(&col.data_type)?;
                let (type_precision, type_scale) = match &col.data_type {
                    DataType::Decimal32(precision, scale)
                    | DataType::Decimal64(precision, scale)
                    | DataType::Decimal128(precision, scale)
                    | DataType::Decimal256(precision, scale) => {
                        (Some(*precision as i32), Some(*scale as i32))
                    }
                    _ => (None, None),
                };
                let type_interval_type = match &col.data_type {
                    // TODO: Don't know if this is correct
                    DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
                        Some("YearMonth".to_string())
                    }
                    DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
                    | DataType::Duration(arrow::datatypes::TimeUnit::Microsecond) => {
                        Some("DayTime".to_string())
                    }
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                        Some("MonthDayNano".to_string())
                    }
                    _ => None,
                };
                let partition_index = partition_by
                    .iter()
                    .position(|p| p.column.trim().to_lowercase() == col.name.trim().to_lowercase())
                    .map(|i| i as i32);
                Ok(r#gen::ColumnInfo {
                    comment: col.comment.clone(),
                    name: Some(col.name.clone()),
                    nullable: Some(col.nullable),
                    partition_index,
                    position: Some(idx as i32),
                    type_interval_type,
                    type_json: Some(
                        unity_struct_field_type_json(
                            &col.name,
                            &col.data_type,
                            col.nullable,
                            &HashMap::new(),
                        )?
                        .to_string(),
                    ),
                    type_name: Some(Box::new(unity_type.type_name)),
                    type_precision,
                    type_scale,
                    type_text: Some(unity_type.type_text),
                })
            })
            .collect::<CatalogResult<Vec<_>>>()?;

        let mut props = HashMap::new();
        if let Some(c) = &comment {
            props.insert("comment".to_string(), c.to_string());
        }
        for (k, v) in properties {
            props.insert(k, v);
        }

        let table_type = if is_external {
            r#gen::TableType::External
        } else {
            r#gen::TableType::Managed
        };
        let storage_location = if is_external {
            location.ok_or_else(|| {
                CatalogError::External(
                    "Storage location is required for external Unity Catalog tables".to_string(),
                )
            })?
        } else if is_write_precondition {
            // CTAS/data-write preconditions run before the writer, so they must use
            // the staging location already returned by plan_lakehouse_create.
            location.ok_or_else(|| {
                CatalogError::External(
                    "Unity Catalog managed table write precondition is missing a planned staging location"
                        .to_string(),
                )
            })?
        } else {
            // The SQL planner supplies generated default locations for managed tables. Unity
            // managed table creation must use a staging location allocated by the catalog.
            self.create_managed_staging_location(&client, &catalog_name, &schema_name, table)
                .await?
        };

        let request = r#gen::CreateTable {
            name: table.to_string(),
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_type: Box::new(table_type),
            data_source_format: Box::new(data_source_format),
            columns: unity_columns,
            storage_location,
            comment,
            properties: if props.is_empty() {
                None
            } else {
                Some(Box::new(r#gen::SecurablePropertiesMap(
                    props.into_iter().collect(),
                )))
            },
        };

        let result = client.create_table(request).await;

        match result {
            Ok(response) => {
                let table_info = response.inner;
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
            }
            Err(e)
                if e.status() == Some(reqwest::StatusCode::CONFLICT) && mode.ignore_if_exists() =>
            {
                self.get_table(database, table).await
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    Err(CatalogError::External(format!(
                        "Failed to create table: HTTP {status}: {e}"
                    )))
                } else {
                    Err(CatalogError::External(format!(
                        "Failed to create table: {e}"
                    )))
                }
            }
        }
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::full_name(&[&catalog_name, &schema_name, table]);

        let result = client.get_table(full_name, None, None).await;

        match result {
            Ok(response) => {
                let table_info = response.inner;
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                Err(CatalogError::NotFound(
                    CatalogObject::Table,
                    quote_names_if_needed(&[&catalog_name, &schema_name, table]),
                ))
            }
            Err(e) => Err(CatalogError::External(format!("Failed to get table: {e}"))),
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;

        let result = client
            .list_tables(catalog_name.clone(), schema_name.clone(), None, None)
            .await;

        match result {
            Ok(response) => {
                let list_response = response.inner;
                list_response
                    .tables
                    .unwrap_or_default()
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::full_name(&[&catalog_name, &schema_name, table]);

        match client.delete_table(full_name).await {
            Ok(_) => Ok(()),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) && if_exists => Ok(()),
            Err(e) => Err(CatalogError::External(format!("Failed to drop table: {e}"))),
        }
    }

    async fn alter_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: AlterTableOptions,
    ) -> CatalogResult<()> {
        // External Delta tables use their transaction log as metadata authority.
        // The Unity table API has no schema/property update endpoint. Managed Delta
        // updates arrive through commit_lakehouse_table instead.
        Ok(())
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        vec![
            LakehouseCapability::CatalogCommit,
            LakehouseCapability::DeltaRatifiedCommits,
        ]
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        if options.mode.is_replace() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support REPLACE option".to_string(),
            ));
        }

        if !options.sort_by.is_empty() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support SORT BY option".to_string(),
            ));
        }

        if options.bucket_by.is_some() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support BUCKET BY option".to_string(),
            ));
        }

        if !options.constraints.is_empty() {
            return Err(CatalogError::NotSupported(
                "Open source Unity Catalog does not support CONSTRAINT option".to_string(),
            ));
        }

        if options
            .partition_by
            .iter()
            .any(|f| f.transform.is_some() && f.transform != Some(PartitionTransform::Identity))
        {
            return Err(CatalogError::NotSupported(
                "partition transforms are not supported by Unity catalog".to_string(),
            ));
        }

        if options.format.eq_ignore_ascii_case("iceberg") {
            return Err(CatalogError::NotSupported(
                "Native Iceberg table DDL requires an Iceberg REST catalog; the Unity Catalog table API does not support it".to_string(),
            ));
        }
        if options.is_external
            && !options.is_write_precondition
            && options.format.eq_ignore_ascii_case("delta")
        {
            Ok(CreateTableMetadataRequirement::LakeSource {
                mode: LakeSourceCreateMetadataMode::PathManaged,
            })
        } else {
            Ok(CreateTableMetadataRequirement::None)
        }
    }

    async fn plan_lakehouse_create(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCreateRequest,
    ) -> CatalogResult<LakehouseCreatePlan> {
        let mut plan = sail_catalog::lakehouse::plan_lakehouse_create_from_requirement(
            self.get_name(),
            request.catalog_table,
            &request.options,
            self.create_table_metadata_requirement(&request.options)?,
            &self.lakehouse_capabilities(),
        );
        if request.options.format.eq_ignore_ascii_case("delta") && !request.options.is_external {
            plan.materialization = LakehouseCreateMaterialization::AfterCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
            };
            if request.options.is_write_precondition {
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
                let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
                let location = self
                    .create_managed_staging_location(&client, &catalog_name, &schema_name, table)
                    .await?;
                plan.table.status.location = Some(location.clone());
                plan.table.execution.table_identity.table_uri = Some(location);
            }
        }
        Ok(plan)
    }

    async fn commit_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        let LakehouseCommitRequest {
            context,
            format,
            requirements,
            updates,
            payload,
        } = request;
        if !format.eq_ignore_ascii_case("delta") {
            return Err(CatalogError::NotSupported(format!(
                "Unity Catalog commit for {format} tables"
            )));
        }
        if !requirements.is_empty() {
            return Err(CatalogError::NotSupported(
                "Unity Catalog Delta commits do not support generic commit requirements"
                    .to_string(),
            ));
        }
        let [update] = updates.as_slice() else {
            return Err(CatalogError::InvalidArgument(
                "Unity Catalog Delta commit expects exactly one update payload".to_string(),
            ));
        };
        let commit: r#gen::DeltaCommit = serde_json::from_value(update.clone()).map_err(|e| {
            CatalogError::InvalidArgument(format!("Invalid Delta commit payload: {e}"))
        })?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let delta_client = self
            .get_delta_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        // The Delta REST API commit carries no table metadata, so a commit that changes
        // the table metadata uses the legacy API.
        let api = if commit.metadata.is_some() {
            DeltaCommitApi::Preview
        } else {
            self.get_delta_commit_api(&delta_client, &catalog_name)
                .await?
        };
        let (status, error) = match api {
            DeltaCommitApi::V1 => {
                let request = delta_update_table_request(commit)?;
                match delta_client
                    .update_table(catalog_name, schema_name, table.to_string(), request)
                    .await
                {
                    Ok(_) => return Ok(LakehouseCommitOutcome::Committed { context, payload }),
                    Err(e) => (e.status(), e.to_string()),
                }
            }
            DeltaCommitApi::Preview => {
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
                match client.commit(commit).await {
                    Ok(_) => return Ok(LakehouseCommitOutcome::Committed { context, payload }),
                    Err(e) => (e.status(), e.to_string()),
                }
            }
        };
        match status {
            // The commit is accepted by the server if the status code indicates success,
            // even if the response body cannot be deserialized.
            // The OSS Unity Catalog server currently returns a plain "200 OK" body
            // for legacy Delta commits even though the OpenAPI spec declares a JSON response.
            // Such responses are considered unknown errors since JSON deserialization fails.
            Some(s) if s.is_success() => Ok(LakehouseCommitOutcome::Committed { context, payload }),
            Some(reqwest::StatusCode::CONFLICT) => Err(CatalogError::Conflict(format!(
                "Unity Catalog Delta commit conflict: {error}"
            ))),
            Some(reqwest::StatusCode::BAD_REQUEST) => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog Delta commit invalid argument: {error}"
            ))),
            Some(reqwest::StatusCode::UNAUTHORIZED) => Err(CatalogError::Unauthorized(format!(
                "Unity Catalog Delta commit unauthorized: {error}"
            ))),
            Some(reqwest::StatusCode::FORBIDDEN) => Err(CatalogError::Forbidden(format!(
                "Unity Catalog Delta commit forbidden: {error}"
            ))),
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => Err(CatalogError::RateLimited(
                format!("Unity Catalog Delta commit rate limited: {error}"),
            )),
            Some(reqwest::StatusCode::NOT_IMPLEMENTED) => Err(CatalogError::NotSupported(format!(
                "Unity Catalog Delta commit endpoint: {error}"
            ))),
            _ => Err(CatalogError::External(format!(
                "Failed to commit Delta table to Unity Catalog: {error}"
            ))),
        }
    }

    async fn get_delta_ratified_commits(
        &self,
        database: &Namespace,
        table: &str,
        request: DeltaRatifiedCommitRequest,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        let status = self.get_table(database, table).await?;
        let (format, location, properties) = match &status.kind {
            TableKind::Table {
                format,
                location,
                properties,
                ..
            } => (format, location, properties),
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "Unity Catalog commit discovery expects a table: {table}"
                )));
            }
        };
        if !format.eq_ignore_ascii_case("delta") {
            return Err(CatalogError::NotSupported(format!(
                "Unity Catalog commit discovery for {format} tables"
            )));
        }
        let table_uri = if let Some(location) = location {
            if location.trim_end_matches('/') != request.table_uri.trim_end_matches('/') {
                return Err(CatalogError::InvalidArgument(format!(
                    "Unity Catalog Delta commit discovery table URI mismatch: catalog location `{location}`, requested `{}`",
                    request.table_uri
                )));
            }
            location.clone()
        } else {
            request.table_uri
        };
        let table_id = unity_table_id_value(
            properties
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .ok_or_else(|| {
            CatalogError::InvalidArgument(format!(
                "Unity Catalog Delta table `{table}` is missing table id property"
            ))
        })?;

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let delta_client = self
            .get_delta_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        let (status, error) = match self
            .get_delta_commit_api(&delta_client, &catalog_name)
            .await?
        {
            DeltaCommitApi::V1 => {
                match delta_client
                    .load_table(catalog_name.clone(), schema_name.clone(), table.to_string())
                    .await
                {
                    Ok(response) => {
                        return delta_ratified_commits_from_load_table(
                            response.inner,
                            table,
                            table_id,
                            request.start_version,
                            request.end_version,
                        );
                    }
                    Err(e) => (e.status(), e.to_string()),
                }
            }
            DeltaCommitApi::Preview => {
                let request = r#gen::DeltaGetCommits {
                    end_version: request.end_version,
                    start_version: request.start_version,
                    table_id: table_id.to_string(),
                    table_uri,
                };
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
                match client.get_commits(request).await {
                    Ok(response) => {
                        let response = response.inner;
                        return Ok(DeltaRatifiedCommitResponse {
                            latest_table_version: response.latest_table_version,
                            commits: response
                                .commits
                                .into_iter()
                                .map(|commit| DeltaRatifiedCommit {
                                    version: commit.version,
                                    timestamp: commit.timestamp,
                                    file_name: commit.file_name,
                                    file_size: commit.file_size,
                                    file_modification_timestamp: commit.file_modification_timestamp,
                                })
                                .collect(),
                        });
                    }
                    Err(e) => (e.status(), e.to_string()),
                }
            }
        };
        match status {
            Some(reqwest::StatusCode::BAD_REQUEST) => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog Delta commit discovery invalid argument: {error}"
            ))),
            Some(reqwest::StatusCode::NOT_FOUND) => Err(CatalogError::NotFound(
                CatalogObject::Table,
                quote_names_if_needed(&[&catalog_name, &schema_name, table]),
            )),
            Some(reqwest::StatusCode::NOT_IMPLEMENTED) => Err(CatalogError::NotSupported(format!(
                "Unity Catalog Delta commit discovery endpoint: {error}"
            ))),
            _ => Err(CatalogError::External(format!(
                "Failed to get Delta commits from Unity Catalog: {error}"
            ))),
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

/// Converts the table loaded via the Delta REST API to the ratified commits
/// in the requested version range, in ascending version order.
fn delta_ratified_commits_from_load_table(
    response: delta_gen::DeltaLoadTableResponse,
    table: &str,
    table_id: &str,
    start_version: i64,
    end_version: Option<i64>,
) -> CatalogResult<DeltaRatifiedCommitResponse> {
    let actual_table_id = response.metadata.table_uuid.as_str();
    if !actual_table_id.eq_ignore_ascii_case(table_id) {
        return Err(CatalogError::InvalidArgument(format!(
            "Unity Catalog Delta commit discovery table id mismatch for `{table}`: expected `{table_id}`, got `{actual_table_id}`"
        )));
    }
    let commits = response.commits.unwrap_or_default();
    // A response without the latest table version is at the version of its latest commit.
    let latest_table_version = response
        .latest_table_version
        .or_else(|| commits.iter().map(|commit| commit.version).max())
        .unwrap_or(-1);
    // The server returns all the unbackfilled ratified commits without version filtering.
    let mut commits = commits
        .into_iter()
        .filter(|commit| {
            commit.version >= start_version && end_version.is_none_or(|end| commit.version <= end)
        })
        .map(|commit| DeltaRatifiedCommit {
            version: commit.version,
            timestamp: commit.timestamp,
            file_name: commit.file_name,
            file_size: commit.file_size,
            file_modification_timestamp: commit.file_modification_timestamp,
        })
        .collect::<Vec<_>>();
    commits.sort_by_key(|commit| commit.version);
    Ok(DeltaRatifiedCommitResponse {
        latest_table_version,
        commits,
    })
}

/// Converts the legacy Delta commit payload to an update table request of the Delta REST API.
fn delta_update_table_request(
    commit: r#gen::DeltaCommit,
) -> CatalogResult<delta_gen::DeltaUpdateTableRequest> {
    let r#gen::DeltaCommit {
        commit_info,
        latest_backfilled_version,
        metadata: _,
        table_id,
        table_uri: _,
        uniform,
    } = commit;
    let mut updates = vec![];
    match (commit_info, uniform) {
        (Some(info), uniform) => {
            let uniform = uniform.map(|x| delta_uniform_metadata(*x)).transpose()?;
            updates.push(delta_gen::DeltaTableUpdate::AddCommit {
                commit: Box::new(delta_gen::DeltaCommit {
                    version: info.version,
                    timestamp: info.timestamp,
                    file_name: info.file_name,
                    file_size: info.file_size,
                    file_modification_timestamp: info.file_modification_timestamp,
                }),
                uniform: uniform.map(Box::new),
            });
        }
        (None, Some(_)) => {
            return Err(CatalogError::InvalidArgument(
                "Unity Catalog Delta commit with UniForm metadata expects commit info".to_string(),
            ));
        }
        (None, None) => {}
    }
    if let Some(latest_published_version) = latest_backfilled_version {
        updates.push(delta_gen::DeltaTableUpdate::SetLatestBackfilledVersion {
            latest_published_version,
        });
    }
    if updates.is_empty() {
        return Err(CatalogError::InvalidArgument(
            "Unity Catalog Delta commit expects commit info or the latest backfilled version"
                .to_string(),
        ));
    }
    Ok(delta_gen::DeltaUpdateTableRequest {
        requirements: vec![delta_gen::DeltaTableRequirement::AssertTableUuid { uuid: table_id }],
        updates,
    })
}

fn delta_uniform_metadata(
    uniform: r#gen::DeltaUniform,
) -> CatalogResult<delta_gen::DeltaUniformMetadata> {
    let iceberg = *uniform.iceberg;
    let converted_delta_timestamp =
        chrono::DateTime::parse_from_rfc3339(&iceberg.converted_delta_timestamp)
            .map_err(|e| {
                CatalogError::InvalidArgument(format!(
                    "Invalid UniForm converted Delta timestamp `{}`: {e}",
                    iceberg.converted_delta_timestamp
                ))
            })?
            .timestamp_millis();
    Ok(delta_gen::DeltaUniformMetadata {
        iceberg: delta_gen::delta_uniform_metadata::Iceberg {
            metadata_location: Some(iceberg.metadata_location),
            converted_delta_version: Some(iceberg.converted_delta_version),
            converted_delta_timestamp: Some(converted_delta_timestamp),
            base_converted_delta_version: iceberg.base_converted_delta_version,
        },
    })
}

fn parse_data_source_format(format: &str) -> CatalogResult<r#gen::DataSourceFormat> {
    match format.trim().to_ascii_uppercase().as_str() {
        "DELTA" => Ok(r#gen::DataSourceFormat::Delta),
        "CSV" => Ok(r#gen::DataSourceFormat::Csv),
        "JSON" => Ok(r#gen::DataSourceFormat::Json),
        "AVRO" => Ok(r#gen::DataSourceFormat::Avro),
        "PARQUET" => Ok(r#gen::DataSourceFormat::Parquet),
        "ORC" => Ok(r#gen::DataSourceFormat::Orc),
        "TEXT" => Ok(r#gen::DataSourceFormat::Text),
        "ICEBERG" => Ok(r#gen::DataSourceFormat::Iceberg),
        other => Err(CatalogError::InvalidArgument(format!(
            "Invalid data source format: {other}"
        ))),
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use sail_catalog::credentials::{CatalogCredentials, EmptyCatalogCredentials};
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, LakehouseAuthority, LakehouseExecutionContext,
        LakehouseFormat, LakehouseOperation, ScanAuthority,
    };
    use secrecy::ExposeSecret;
    use wiremock::matchers::{method, path, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::UnityCatalogConfig;

    fn options_from_config(
        default_catalog: impl Into<String>,
        config: UnityCatalogConfig,
    ) -> UnityCatalogOptions {
        let credentials = config
            .get_credential_provider()
            .map(|credentials| Arc::new(credentials) as Arc<dyn CatalogCredentials>)
            .unwrap_or_else(|| Arc::new(EmptyCatalogCredentials));
        UnityCatalogOptions {
            default_catalog: default_catalog.into(),
            uri: config.uri,
            credentials,
        }
    }

    fn empty_options(default_catalog: impl Into<String>) -> UnityCatalogOptions {
        UnityCatalogOptions {
            default_catalog: default_catalog.into(),
            uri: DEFAULT_URI.to_string(),
            credentials: Arc::new(EmptyCatalogCredentials),
        }
    }

    #[test]
    fn test_config_with_token() {
        let mut options = HashMap::new();
        options.insert("databricks_token".to_string(), "test_token".to_string());
        options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );

        let config = UnityCatalogConfig::new(None, &None, Some(options)).unwrap();
        assert!(config.bearer_token.is_some());
        assert_eq!(config.bearer_token.unwrap().expose_secret(), "test_token");
        assert_eq!(config.uri, "https://test.databricks.com");
    }

    #[test]
    fn test_config_with_client_credentials() {
        let mut options = HashMap::new();
        options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );
        options.insert(
            "databricks_client_id".to_string(),
            "test_client_id".to_string(),
        );
        options.insert(
            "databricks_client_secret".to_string(),
            "test_secret".to_string(),
        );
        options.insert(
            "databricks_authority_id".to_string(),
            "test_tenant".to_string(),
        );

        let config = UnityCatalogConfig::new(None, &None, Some(options)).unwrap();
        assert_eq!(config.uri, "https://test.databricks.com");
        assert_eq!(config.client_id, Some("test_client_id".to_string()));
        assert!(config.client_secret.is_some());
        assert_eq!(config.authority_id, Some("test_tenant".to_string()));
    }

    #[test]
    fn test_config_boolean_options() {
        let test_cases = vec![
            ("true", true),
            ("false", false),
            ("1", true),
            ("0", false),
            ("yes", true),
            ("no", false),
        ];

        for (value, expected) in test_cases {
            let mut options = HashMap::new();
            options.insert("unity_allow_http_url".to_string(), value.to_string());
            options.insert("unity_use_azure_cli".to_string(), value.to_string());

            let config = UnityCatalogConfig::new(None, &None, Some(options)).unwrap();
            assert_eq!(config.allow_http_url, expected, "Failed for value: {value}");
            assert_eq!(config.use_azure_cli, expected, "Failed for value: {value}");
        }
    }

    #[test]
    fn test_config_key_variations() {
        let test_cases = vec![
            ("databricks_host", "uri"),
            ("unity_workspace_url", "uri"),
            ("databricks_workspace_url", "uri"),
            ("databricks_token", "token"),
            ("token", "token"),
            ("unity_client_id", "client_id"),
            ("databricks_client_id", "client_id"),
            ("client_id", "client_id"),
        ];

        for (key, field) in test_cases {
            let mut options = HashMap::new();
            let test_value = format!("test_value_for_{key}");
            options.insert(key.to_string(), test_value.clone());

            let result = UnityCatalogConfig::new(None, &None, Some(options));
            assert!(result.is_ok(), "Failed to parse key: {key}");

            let config = result.unwrap();
            match field {
                "uri" => assert_eq!(config.uri, test_value),
                "token" => assert_eq!(config.bearer_token.unwrap().expose_secret(), &test_value),
                "client_id" => assert_eq!(config.client_id, Some(test_value)),
                _ => {}
            }
        }
    }

    #[test]
    fn test_provider_with_options() {
        let mut options = HashMap::new();
        options.insert("databricks_token".to_string(), "test_token".to_string());
        options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );

        let config = UnityCatalogConfig::new(None, &None, Some(options)).unwrap();
        assert!(config.bearer_token.is_some());

        let provider = UnityCatalogProvider::new(
            "test".to_string(),
            options_from_config("test_catalog", config),
        )
        .unwrap();

        assert_eq!(provider.name, "test");
        assert_eq!(provider.options.default_catalog, "test_catalog");
        assert_eq!(provider.options.uri, "https://test.databricks.com");
    }

    fn mock_provider(server: &MockServer, default_catalog: &str) -> UnityCatalogProvider {
        let options = UnityCatalogOptions {
            uri: server.uri(),
            ..empty_options(default_catalog)
        };
        UnityCatalogProvider::new("test".to_string(), options).unwrap()
    }

    /// Returns the method and the percent-decoded path and query of each received request.
    async fn received_targets(server: &MockServer) -> Vec<String> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| {
                let url = &request.url;
                let target = match url.query() {
                    Some(query) => format!("{}?{query}", url.path()),
                    None => url.path().to_string(),
                };
                let target = percent_encoding::percent_decode_str(&target)
                    .decode_utf8()
                    .unwrap();
                format!("{} {target}", request.method)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_schema_requests_do_not_quote_names() {
        let server = MockServer::start().await;
        let schema = serde_json::json!({"catalog_name": "cat-x", "name": "a12b34cd-5678"});
        Mock::given(method("POST"))
            .and(path("/schemas"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&schema))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"schemas": [schema]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/schemas/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&schema))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex("^/schemas/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let catalog = Namespace::try_from(vec!["cat-x"]).unwrap();
        let database = Namespace::try_from(vec!["cat-x", "a12b34cd-5678"]).unwrap();
        provider
            .create_database(
                &database,
                CreateDatabaseOptions {
                    comment: None,
                    location: None,
                    if_not_exists: false,
                    properties: vec![],
                },
            )
            .await
            .unwrap();
        provider.get_database(&database).await.unwrap();
        provider.list_databases(Some(&catalog)).await.unwrap();
        provider
            .drop_database(
                &database,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            received_targets(&server).await,
            vec![
                "POST /schemas",
                "GET /schemas/cat-x.a12b34cd-5678",
                "GET /schemas?catalog_name=cat-x",
                "DELETE /schemas/cat-x.a12b34cd-5678?force=false",
            ]
        );
        let requests = server.received_requests().await.unwrap();
        let body: serde_json::Value = requests[0].body_json().unwrap();
        assert_eq!(body["catalog_name"], "cat-x");
        assert_eq!(body["name"], "a12b34cd-5678");
    }

    #[tokio::test]
    async fn test_table_requests_do_not_quote_names() {
        let server = MockServer::start().await;
        // OneLake catalog names contain a dot, which must not be quoted either.
        let table = serde_json::json!({
            "catalog_name": "lakehouse.Lakehouse",
            "schema_name": "a12b34cd-5678",
            "name": "dash-table",
            "columns": []
        });
        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"tables": [table]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/tables/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&table))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex("^/tables/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "lakehouse.Lakehouse");
        let database = Namespace::try_from(vec!["a12b34cd-5678"]).unwrap();
        provider.get_table(&database, "dash-table").await.unwrap();
        provider.list_tables(&database).await.unwrap();
        provider
            .drop_table(
                &database,
                "dash-table",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /tables/lakehouse.Lakehouse.a12b34cd-5678.dash-table",
                "GET /tables?catalog_name=lakehouse.Lakehouse&schema_name=a12b34cd-5678",
                "DELETE /tables/lakehouse.Lakehouse.a12b34cd-5678.dash-table",
            ]
        );
    }

    #[tokio::test]
    async fn test_not_found_errors_quote_names_for_display() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "not found"
            })))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["a12b34cd-5678"]).unwrap();
        let schema_error = provider.get_database(&database).await.unwrap_err();
        let table_error = provider.get_table(&database, "t").await.unwrap_err();

        assert_eq!(
            schema_error.to_string(),
            "Schema not found: main.`a12b34cd-5678`"
        );
        assert_eq!(
            table_error.to_string(),
            "Table not found: main.`a12b34cd-5678`.t"
        );
        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /schemas/main.a12b34cd-5678",
                "GET /tables/main.a12b34cd-5678.t",
            ]
        );
    }

    #[tokio::test]
    async fn test_error_responses_include_body_without_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/delta/preview/commits"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Credential was not sent or was of an unsupported type"
            })))
            .mount(&server)
            .await;

        let mut options = HashMap::new();
        options.insert("databricks_token".to_string(), "secret-token".to_string());
        options.insert("databricks_host".to_string(), server.uri());
        let config = UnityCatalogConfig::new(None, &None, Some(options)).unwrap();
        let provider =
            UnityCatalogProvider::new("test".to_string(), options_from_config("main", config))
                .unwrap();
        let client = provider.get_client().await.unwrap();
        let error = client
            .get_commits(r#gen::DeltaGetCommits {
                end_version: None,
                start_version: 1,
                table_id: "table-id".to_string(),
                table_uri: "s3://bucket/table".to_string(),
            })
            .await
            .unwrap_err();

        assert_eq!(error.status(), Some(reqwest::StatusCode::UNAUTHORIZED));
        let message = error.to_string();
        assert!(
            message.contains("Credential was not sent or was of an unsupported type"),
            "unexpected message: {message}"
        );
        assert!(
            !message.contains("secret-token"),
            "unexpected message: {message}"
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests[0]
                .headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer secret-token")
        );
    }

    #[derive(Debug)]
    struct NoopSigner;

    impl r#gen::RequestSigner for NoopSigner {
        fn sign(
            &self,
            _: &mut reqwest::Request,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_error_response_body_is_bounded_and_hidden_for_signed_requests() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/delta/preview/commits"))
            .respond_with(ResponseTemplate::new(403).set_body_string("x".repeat(100_000)))
            .mount(&server)
            .await;
        let request = || r#gen::DeltaGetCommits {
            end_version: None,
            start_version: 1,
            table_id: "table-id".to_string(),
            table_uri: "s3://bucket/table".to_string(),
        };

        let mut client = mock_provider(&server, "main").get_client().await.unwrap();
        let error = client.get_commits(request()).await.unwrap_err();
        assert_eq!(error.status(), Some(reqwest::StatusCode::FORBIDDEN));
        assert_eq!(
            error.to_string(),
            format!(
                "response error: status code 403 Forbidden: {}...",
                "x".repeat(4096)
            )
        );

        client.signer = Some(Arc::new(NoopSigner));
        let error = client.get_commits(request()).await.unwrap_err();
        assert_eq!(error.status(), Some(reqwest::StatusCode::FORBIDDEN));
        assert_eq!(
            error.to_string(),
            "response error: status code 403 Forbidden"
        );
    }

    #[test]
    #[expect(clippy::panic)]
    fn test_table_info_accepts_null_columns_and_lowercase_column_type_name() {
        let response: r#gen::ListTablesResponse = serde_json::from_value(serde_json::json!({
            "tables": [{
                "catalog_name": "catalog",
                "schema_name": "schema",
                "name": "empty_table",
                "columns": null
            }, {
                "catalog_name": "catalog",
                "schema_name": "schema",
                "name": "typed_table",
                "columns": [{
                    "name": "value",
                    "type_name": "string",
                    "type_text": "string",
                    "nullable": true
                }]
            }]
        }))
        .unwrap();

        let provider =
            UnityCatalogProvider::new("test".to_string(), empty_options("catalog")).unwrap();

        let mut tables = response.tables.unwrap_or_default().into_iter();
        let empty_table = provider
            .table_info_to_table_status(tables.next().unwrap(), "catalog", "schema")
            .unwrap();
        let typed_table = provider
            .table_info_to_table_status(tables.next().unwrap(), "catalog", "schema")
            .unwrap();

        let TableKind::Table {
            columns, comment, ..
        } = empty_table.kind
        else {
            panic!("expected table status");
        };
        assert_eq!(comment, None);
        assert!(columns.is_empty());

        let TableKind::Table { columns, .. } = typed_table.kind else {
            panic!("expected table status");
        };
        assert_eq!(columns[0].data_type, DataType::Utf8);
    }

    #[test]
    #[expect(clippy::panic)]
    fn test_table_info_ignores_onelake_all_zero_partition_indexes() {
        let response: r#gen::TableInfo = serde_json::from_value(serde_json::json!({
            "catalog_name": "catalog",
            "schema_name": "schema",
            "name": "typed_table",
            "columns": [{
                "name": "id",
                "type_name": "int",
                "type_text": "int",
                "nullable": true,
                "partition_index": 0
            }, {
                "name": "value",
                "type_name": "string",
                "type_text": "string",
                "nullable": true,
                "partition_index": 0
            }]
        }))
        .unwrap();

        let provider =
            UnityCatalogProvider::new("test".to_string(), empty_options("catalog")).unwrap();

        let status = provider
            .table_info_to_table_status(response, "catalog", "schema")
            .unwrap();

        let TableKind::Table {
            columns,
            partition_by,
            ..
        } = status.kind
        else {
            panic!("expected table status");
        };
        assert!(partition_by.is_empty());
        assert!(columns.iter().all(|column| !column.is_partition));
    }

    #[test]
    #[expect(clippy::panic)]
    fn test_table_info_preserves_real_partition_index() {
        let response: r#gen::TableInfo = serde_json::from_value(serde_json::json!({
            "catalog_name": "catalog",
            "schema_name": "schema",
            "name": "typed_table",
            "columns": [{
                "name": "id",
                "type_name": "int",
                "type_text": "int",
                "nullable": true
            }, {
                "name": "value",
                "type_name": "string",
                "type_text": "string",
                "nullable": true,
                "partition_index": 0
            }]
        }))
        .unwrap();

        let provider =
            UnityCatalogProvider::new("test".to_string(), empty_options("catalog")).unwrap();

        let status = provider
            .table_info_to_table_status(response, "catalog", "schema")
            .unwrap();

        let TableKind::Table {
            columns,
            partition_by,
            ..
        } = status.kind
        else {
            panic!("expected table status");
        };
        assert_eq!(partition_by.len(), 1);
        assert_eq!(partition_by[0].column, "value");
        assert!(!columns[0].is_partition);
        assert!(columns[1].is_partition);
    }

    const DELTA_TABLE_ID: &str = "8a1b9c3e-0f5d-4a6b-9c7d-2e1f0a3b4c5d";
    const DELTA_TABLE_URI: &str = "s3://bucket/tables/8a1b9c3e";
    const DELTA_TABLE_PATH: &str = "/delta/v1/catalogs/main/schemas/default/tables/events";

    fn delta_execution_context() -> LakehouseExecutionContext {
        LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("test".to_string()),
            vec![
                "main".to_string(),
                "default".to_string(),
                "events".to_string(),
            ],
            CatalogTableIdentity {
                table_id: Some(DELTA_TABLE_ID.to_string()),
                table_uri: Some(DELTA_TABLE_URI.to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        )
    }

    fn delta_ratified_commit_request(
        start_version: i64,
        end_version: Option<i64>,
    ) -> DeltaRatifiedCommitRequest {
        DeltaRatifiedCommitRequest {
            context: delta_execution_context(),
            table_uri: DELTA_TABLE_URI.to_string(),
            start_version,
            end_version,
        }
    }

    fn delta_commit_request(update: serde_json::Value) -> LakehouseCommitRequest {
        LakehouseCommitRequest {
            context: delta_execution_context(),
            format: "delta".to_string(),
            requirements: vec![],
            updates: vec![update],
            payload: None,
        }
    }

    fn legacy_delta_commit(version: i64) -> serde_json::Value {
        serde_json::json!({
            "table_id": DELTA_TABLE_ID,
            "table_uri": DELTA_TABLE_URI,
            "commit_info": {
                "version": version,
                "timestamp": 1700000000000i64 + version,
                "file_name": format!("{version:020}.uuid.json"),
                "file_size": 100 + version,
                "file_modification_timestamp": 1700000001000i64 + version,
            }
        })
    }

    fn delta_commit_json(version: i64) -> serde_json::Value {
        serde_json::json!({
            "version": version,
            "timestamp": 1700000000000i64 + version,
            "file-name": format!("{version:020}.uuid.json"),
            "file-size": 100 + version,
            "file-modification-timestamp": 1700000001000i64 + version,
        })
    }

    fn delta_load_table_json(
        table_uuid: &str,
        commits: Option<Vec<i64>>,
        latest_table_version: Option<i64>,
    ) -> serde_json::Value {
        let mut response = serde_json::json!({
            "metadata": {
                "etag": "etag-1",
                "table-type": "MANAGED",
                "table-uuid": table_uuid,
                "location": DELTA_TABLE_URI,
                "created-time": 1700000000000i64,
                "updated-time": 1700000000000i64,
                "columns": {
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                        {
                            "name": "tags",
                            "type": {
                                "type": "array",
                                "element-type": "string",
                                "contains-null": true
                            },
                            "nullable": true,
                            "metadata": {"comment": "tags"}
                        }
                    ]
                },
                "properties": {"delta.feature.catalogManaged": "supported"}
            }
        });
        if let Some(commits) = commits {
            response["commits"] = commits.into_iter().map(delta_commit_json).collect();
        }
        if let Some(latest_table_version) = latest_table_version {
            response["latest-table-version"] = serde_json::json!(latest_table_version);
        }
        response
    }

    async fn mount_delta_config(server: &MockServer, response: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path("/delta/v1/config"))
            .and(query_param("catalog", "main"))
            .and(query_param("protocol-versions", "1.0"))
            .respond_with(response)
            .mount(server)
            .await;
    }

    async fn mount_delta_config_ok(server: &MockServer) {
        let config = serde_json::json!({
            "endpoints": [
                "GET /v1/catalogs/main/schemas/{schema}/tables/{table}",
                "POST /v1/catalogs/main/schemas/{schema}/tables/{table}"
            ],
            "protocol-version": "1.0"
        });
        mount_delta_config(server, ResponseTemplate::new(200).set_body_json(config)).await;
    }

    async fn mount_legacy_delta_table(server: &MockServer) {
        let table = serde_json::json!({
            "catalog_name": "main",
            "schema_name": "default",
            "name": "events",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "storage_location": DELTA_TABLE_URI,
            "table_id": DELTA_TABLE_ID,
            "columns": []
        });
        Mock::given(method("GET"))
            .and(path_regex("^/tables/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(table))
            .mount(server)
            .await;
    }

    async fn received_json_bodies(server: &MockServer, path: &str) -> Vec<serde_json::Value> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "POST" && request.url.path() == path)
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn test_delta_ratified_commits_use_delta_api() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        // The server returns the commits in descending version order.
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    Some(vec![5, 4, 3, 2]),
                    Some(5),
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let response = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(3, None))
            .await
            .unwrap();
        assert_eq!(response.latest_table_version, 5);
        assert_eq!(
            response
                .commits
                .iter()
                .map(|commit| commit.version)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(
            response.commits[0],
            DeltaRatifiedCommit {
                version: 3,
                timestamp: 1700000000003,
                file_name: "00000000000000000003.uuid.json".to_string(),
                file_size: 103,
                file_modification_timestamp: 1700000001003,
            }
        );

        let response = provider
            .get_delta_ratified_commits(
                &database,
                "events",
                delta_ratified_commit_request(3, Some(4)),
            )
            .await
            .unwrap();
        assert_eq!(response.latest_table_version, 5);
        assert_eq!(
            response
                .commits
                .iter()
                .map(|commit| commit.version)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        // The API is negotiated only once and the legacy endpoint is not used.
        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /tables/main.default.events".to_string(),
                "GET /delta/v1/config?catalog=main&protocol-versions=1.0".to_string(),
                format!("GET {DELTA_TABLE_PATH}"),
                "GET /tables/main.default.events".to_string(),
                format!("GET {DELTA_TABLE_PATH}"),
            ]
        );
    }

    #[tokio::test]
    async fn test_delta_ratified_commits_without_latest_table_version() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    None,
                    None,
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let response = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap();
        assert_eq!(response.latest_table_version, -1);
        assert!(response.commits.is_empty());
    }

    #[tokio::test]
    async fn test_delta_ratified_commits_table_id_mismatch() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    "00000000-0000-0000-0000-000000000000",
                    Some(vec![1]),
                    Some(1),
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let error = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::InvalidArgument(message) if message.contains("table id mismatch")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_delta_ratified_commits_table_not_found() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let error = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap_err();
        assert!(
            matches!(error, CatalogError::NotFound(CatalogObject::Table, _)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_delta_commits_fall_back_to_legacy_api() {
        for status in [404, 501, 400] {
            let server = MockServer::start().await;
            mount_delta_config(&server, ResponseTemplate::new(status)).await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("GET"))
                .and(path("/delta/preview/commits"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "commits": [{
                        "version": 2,
                        "timestamp": 1700000000002i64,
                        "file_name": "00000000000000000002.uuid.json",
                        "file_size": 102,
                        "file_modification_timestamp": 1700000001002i64
                    }],
                    "latest_table_version": 2
                })))
                .mount(&server)
                .await;
            // The legacy endpoint returns a plain text body on success.
            Mock::given(method("POST"))
                .and(path("/delta/preview/commits"))
                .respond_with(ResponseTemplate::new(200).set_body_string("200 OK"))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let response = provider
                .get_delta_ratified_commits(
                    &database,
                    "events",
                    delta_ratified_commit_request(1, None),
                )
                .await
                .unwrap();
            assert_eq!(response.latest_table_version, 2);
            assert_eq!(response.commits.len(), 1);
            assert_eq!(response.commits[0].version, 2);

            let mut commit = legacy_delta_commit(3);
            commit["metadata"] = serde_json::json!({"description": "events"});
            let outcome = provider
                .commit_lakehouse_table(&database, "events", delta_commit_request(commit.clone()))
                .await
                .unwrap();
            assert!(matches!(outcome, LakehouseCommitOutcome::Committed { .. }));

            assert_eq!(
                received_targets(&server).await,
                vec![
                    "GET /tables/main.default.events",
                    "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                    "GET /delta/preview/commits",
                    "POST /delta/preview/commits",
                ],
                "status {status}"
            );
            assert_eq!(
                received_json_bodies(&server, "/delta/preview/commits").await,
                vec![commit]
            );
        }
    }

    #[tokio::test]
    async fn test_delta_api_configuration_error_is_not_cached() {
        let server = MockServer::start().await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path("/delta/v1/config"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/delta/v1/config"))
            .respond_with(ResponseTemplate::new(401))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let error = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::External(message) if message.contains("Delta API configuration")),
            "{error:?}"
        );
        let error = provider
            .commit_lakehouse_table(
                &database,
                "events",
                delta_commit_request(legacy_delta_commit(1)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::Unauthorized(_)), "{error:?}");

        // The legacy endpoint is not used when the negotiation fails,
        // and the negotiation is retried for the next request.
        mount_delta_config_ok(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    Some(vec![]),
                    Some(0),
                )),
            )
            .mount(&server)
            .await;
        let response = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap();
        assert_eq!(response.latest_table_version, 0);
        assert!(
            !received_targets(&server)
                .await
                .iter()
                .any(|target| target.contains("/delta/preview/commits"))
        );
    }

    #[tokio::test]
    async fn test_delta_commit_uses_delta_api() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        Mock::given(method("POST"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    Some(vec![5]),
                    Some(5),
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let outcome = provider
            .commit_lakehouse_table(
                &database,
                "events",
                delta_commit_request(legacy_delta_commit(5)),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, LakehouseCommitOutcome::Committed { .. }));

        let mut commit = legacy_delta_commit(6);
        commit["latest_backfilled_version"] = serde_json::json!(5);
        commit["uniform"] = serde_json::json!({
            "iceberg": {
                "metadata_location": "s3://bucket/tables/8a1b9c3e/metadata/v6.json",
                "converted_delta_version": 6,
                "converted_delta_timestamp": "2025-01-04T03:13:11.423Z"
            }
        });
        provider
            .commit_lakehouse_table(&database, "events", delta_commit_request(commit))
            .await
            .unwrap();

        let requirements = serde_json::json!([
            {"type": "assert-table-uuid", "uuid": DELTA_TABLE_ID}
        ]);
        assert_eq!(
            received_json_bodies(&server, DELTA_TABLE_PATH).await,
            vec![
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(5)}
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {
                            "action": "add-commit",
                            "commit": delta_commit_json(6),
                            "uniform": {
                                "iceberg": {
                                    "metadata-location":
                                        "s3://bucket/tables/8a1b9c3e/metadata/v6.json",
                                    "converted-delta-version": 6,
                                    "converted-delta-timestamp": 1735960391423i64
                                }
                            }
                        },
                        {"action": "set-latest-backfilled-version", "latest-published-version": 5}
                    ]
                }),
            ]
        );
        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /delta/v1/config?catalog=main&protocol-versions=1.0".to_string(),
                format!("POST {DELTA_TABLE_PATH}"),
                format!("POST {DELTA_TABLE_PATH}"),
            ]
        );
    }

    #[tokio::test]
    async fn test_delta_commit_with_metadata_uses_legacy_api() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        Mock::given(method("POST"))
            .and(path("/delta/preview/commits"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let mut commit = legacy_delta_commit(5);
        commit["metadata"] = serde_json::json!({"description": "events"});
        let outcome = provider
            .commit_lakehouse_table(&database, "events", delta_commit_request(commit.clone()))
            .await
            .unwrap();
        assert!(matches!(outcome, LakehouseCommitOutcome::Committed { .. }));
        assert_eq!(
            received_json_bodies(&server, "/delta/preview/commits").await,
            vec![commit]
        );
        assert_eq!(
            received_targets(&server).await,
            vec!["POST /delta/preview/commits"]
        );
    }

    #[tokio::test]
    async fn test_delta_api_is_used_for_any_successful_configuration_response() {
        for config in [
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"endpoints": null})),
            ResponseTemplate::new(200).set_body_string("OK"),
            ResponseTemplate::new(204),
        ] {
            let server = MockServer::start().await;
            mount_delta_config(&server, config).await;
            mount_legacy_delta_table(&server).await;
            // The response has only the fields used for commit discovery, with values of
            // other fields that are not in the specification.
            Mock::given(method("GET"))
                .and(path(DELTA_TABLE_PATH))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "metadata": {
                        "table-uuid": DELTA_TABLE_ID.to_uppercase(),
                        "table-type": "MANAGED_SHALLOW_CLONE",
                        "columns": null
                    },
                    "commits": [
                        {"version": 4, "file-name": "00000000000000000004.uuid.json"},
                        {"version": 3, "file-name": "00000000000000000003.uuid.json"}
                    ]
                })))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let response = provider
                .get_delta_ratified_commits(
                    &database,
                    "events",
                    delta_ratified_commit_request(1, None),
                )
                .await
                .unwrap();
            // The latest table version defaults to the version of the latest commit.
            assert_eq!(response.latest_table_version, 4);
            assert_eq!(
                response
                    .commits
                    .iter()
                    .map(|commit| (commit.version, commit.file_name.as_str()))
                    .collect::<Vec<_>>(),
                vec![
                    (3, "00000000000000000003.uuid.json"),
                    (4, "00000000000000000004.uuid.json")
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_delta_ratified_commits_invalid_response_reports_reason() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"commits": []})),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let error = provider
            .get_delta_ratified_commits(&database, "events", delta_ratified_commit_request(1, None))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::External(message) if message.contains("missing field `metadata`")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_delta_commit_errors_from_delta_api() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        let error = |status: u16, r#type: &str| {
            ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "error": {"message": format!("{type} message", type = r#type), "type": r#type, "code": status}
            }))
        };
        for (version, response) in [
            (5, error(409, "CommitVersionConflictException")),
            (6, error(400, "InvalidParameterValueException")),
            (7, error(429, "ResourceExhaustedException")),
            (8, ResponseTemplate::new(403)),
            (9, error(500, "InternalServerError")),
        ] {
            Mock::given(method("POST"))
                .and(path(DELTA_TABLE_PATH))
                .and(wiremock::matchers::body_partial_json(serde_json::json!({
                    "updates": [{"commit": {"version": version}}]
                })))
                .respond_with(response)
                .mount(&server)
                .await;
        }

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let commit = async |version: i64| {
            provider
                .commit_lakehouse_table(
                    &database,
                    "events",
                    delta_commit_request(legacy_delta_commit(version)),
                )
                .await
                .unwrap_err()
        };
        let error = commit(5).await;
        assert!(matches!(error, CatalogError::Conflict(_)), "{error:?}");
        assert!(
            error
                .to_string()
                .contains("CommitVersionConflictException message")
        );
        let error = commit(6).await;
        assert!(
            matches!(error, CatalogError::InvalidArgument(_)),
            "{error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("InvalidParameterValueException message")
        );
        let error = commit(7).await;
        assert!(matches!(error, CatalogError::RateLimited(_)), "{error:?}");
        assert!(
            error
                .to_string()
                .contains("ResourceExhaustedException message")
        );
        let error = commit(8).await;
        assert!(matches!(error, CatalogError::Forbidden(_)), "{error:?}");
        let error = commit(9).await;
        assert!(matches!(error, CatalogError::External(_)), "{error:?}");
        assert!(error.to_string().contains("InternalServerError message"));
    }
}
