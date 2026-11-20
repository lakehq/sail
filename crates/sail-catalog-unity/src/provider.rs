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
    LakehouseResolvedTable, ResolveLakehouseTableRequest, TableStorageCredentialsRequest,
    resolve_lakehouse_table_status,
};
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableMetadataRequirement,
    CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions,
    LakeSourceCreateMetadataMode, Namespace, PartitionTransform, StagedTableCreate,
};
use sail_catalog::utils::{get_property, quote_names_if_needed, quote_namespace_if_needed};
use sail_common::spec::{SAIL_SPARK_UDT_METADATA_KEY, SPARK_METADATA_JSON_KEY};
use sail_common::utils::http::SAIL_USER_AGENT;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY, unity_table_id_value,
};
use sail_common_datafusion::catalog::{
    CommitAuthority, DatabaseStatus, LakehouseOperation, StorageAccess, StorageCredential,
    TableColumnStatus, TableKind, TableStatus, TableStorageCredentials, identity_partition_fields,
};
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::data_type::{
    data_type_to_unity_type, unity_struct_field_type_json, unity_type_to_data_type,
};
use crate::r#gen::ApiClient;
use crate::{delta_gen, r#gen};

pub(crate) const DEFAULT_URI: &str = "http://localhost:8080/api/2.1/unity-catalog";

/// The path of the Unity Catalog API in a workspace.
pub(crate) const WORKSPACE_API_PATH: &str = "/api/2.1/unity-catalog";

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
    /// The IDs of the staging tables that are not registered as tables yet,
    /// by the storage location of the staging table.
    staging_table_ids: Mutex<HashMap<String, String>>,
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
            staging_table_ids: Mutex::new(HashMap::new()),
        })
    }

    fn staging_table_id_cache(&self) -> CatalogResult<MutexGuard<'_, HashMap<String, String>>> {
        self.staging_table_ids
            .lock()
            .map_err(|e| CatalogError::Internal(format!("Unity Catalog staging table IDs: {e}")))
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
            // The server has the endpoint but not the catalog.
            Err(e)
                if e.status() == Some(reqwest::StatusCode::NOT_FOUND)
                    && delta_error_type(&e).as_deref() == Some("NoSuchCatalogException") =>
            {
                return Err(CatalogError::NotFound(
                    CatalogObject::Catalog,
                    quote_names_if_needed(&[catalog_name]),
                ));
            }
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

    /// Creates a staging table for a managed Delta table with the Delta REST API.
    ///
    /// Returns the location allocated for the table, the create-table properties extended
    /// with the properties that the server requires for version 0 of the table, and the
    /// protocol that the server requires for it as a Delta `protocol` action in JSON.
    /// Returns nothing if the table exists.
    async fn create_delta_staging_table(
        &self,
        client: &delta_gen::ApiClient,
        catalog_name: &str,
        schema_name: &str,
        table: &str,
        properties: Vec<(String, String)>,
    ) -> CatalogResult<Option<(String, Vec<(String, String)>, String)>> {
        let request = delta_gen::DeltaCreateStagingTableRequest {
            name: table.to_string(),
        };
        let response = match client
            .create_staging_table(catalog_name.to_string(), schema_name.to_string(), request)
            .await
        {
            Ok(response) => response.inner,
            Err(e) if e.status() == Some(reqwest::StatusCode::CONFLICT) => return Ok(None),
            Err(e) => {
                return Err(CatalogError::External(format!(
                    "Failed to create staging table: {e}"
                )));
            }
        };
        let delta_gen::DeltaStagingTableResponse {
            table_id,
            location,
            required_protocol,
            required_properties,
        } = response;
        let mut properties = properties
            .into_iter()
            .filter(|(key, _)| {
                !key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_KEY)
                    && !key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_LEGACY_KEY)
            })
            .collect::<Vec<_>>();
        let mut required_properties = required_properties
            .unwrap_or_default()
            .into_iter()
            .filter(|(key, _)| !key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_KEY))
            .collect::<Vec<_>>();
        required_properties.sort();
        for (key, required) in required_properties {
            let existing = properties
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                .map(|(_, v)| v.as_str());
            match (existing, required) {
                (Some(_), None) => {}
                (Some(existing), Some(required)) if existing == required => {}
                (Some(existing), Some(required)) => {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Unity Catalog requires the table property {key}={required} \
                         for managed Delta tables, but it is set to {existing}"
                    )));
                }
                (None, Some(required)) => properties.push((key, required)),
                (None, None) => {
                    return Err(CatalogError::NotSupported(format!(
                        "Unity Catalog requires the table property {key} \
                         for managed Delta tables"
                    )));
                }
            }
        }
        self.staging_table_id_cache()?.insert(
            storage_location_key(&location).to_string(),
            table_id.clone(),
        );
        properties.push((DELTA_UNITY_TABLE_ID_KEY.to_string(), table_id));
        let required_protocol = required_protocol.map_or_else(
            || serde_json::json!({ "minReaderVersion": 1, "minWriterVersion": 2 }),
            |protocol| {
                serde_json::json!({
                    "minReaderVersion": protocol.min_reader_version,
                    "minWriterVersion": protocol.min_writer_version,
                    "readerFeatures": protocol.reader_features.unwrap_or_default(),
                    "writerFeatures": protocol.writer_features.unwrap_or_default(),
                })
            },
        );
        Ok(Some((location, properties, required_protocol.to_string())))
    }

    /// Registers a staged managed Delta table with the Delta REST API,
    /// after version 0 of the table is written at the staging location.
    async fn create_staged_delta_table(
        &self,
        database: &Namespace,
        table: &str,
        location: Option<String>,
        comment: Option<String>,
        create_commit: &str,
    ) -> CatalogResult<TableStatus> {
        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let location = location.ok_or_else(|| {
            CatalogError::External(
                "Unity Catalog managed table is missing a planned staging location".to_string(),
            )
        })?;
        let staging_location = storage_location_key(&location).to_string();
        let request = delta_create_table_request(table, location, comment, create_commit)?;
        let table_id = request.properties.get(DELTA_UNITY_TABLE_ID_KEY).cloned();
        let client = self
            .get_delta_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        let error = match client
            .create_table(catalog_name.clone(), schema_name.clone(), request)
            .await
        {
            Ok(_) => {
                self.staging_table_id_cache()?.remove(&staging_location);
                return self.get_table(database, table).await;
            }
            // The table is created if the status code indicates success,
            // even if the response body cannot be deserialized.
            Err(e) if e.status().is_some_and(|status| status.is_success()) => {
                self.staging_table_id_cache()?.remove(&staging_location);
                return self.get_table(database, table).await;
            }
            Err(e) => e,
        };
        let status = error.status();
        // The outcome is unknown when the request fails without a client error.
        // The table is created if it is loaded with the ID of the staging table,
        // since only the creator of the staging table can finalize it.
        if status.is_none_or(|status| status.is_server_error())
            && let Some(table_id) = &table_id
            && let Ok(response) = client
                .load_table(catalog_name.clone(), schema_name.clone(), table.to_string())
                .await
            && response
                .inner
                .metadata
                .table_uuid
                .eq_ignore_ascii_case(table_id)
        {
            self.staging_table_id_cache()?.remove(&staging_location);
            return self.get_table(database, table).await;
        }
        match status {
            Some(reqwest::StatusCode::CONFLICT) => Err(CatalogError::AlreadyExists(
                CatalogObject::Table,
                quote_names_if_needed(&[&catalog_name, &schema_name, table]),
            )),
            Some(status) => Err(CatalogError::External(format!(
                "Failed to create table: HTTP {status}: {error}"
            ))),
            None => Err(CatalogError::External(format!(
                "Failed to create table: {error}"
            ))),
        }
    }

    /// Returns full name of an object for the REST API (`catalog.schema.table`).
    ///
    /// Names are joined without SQL quoting. Unity Catalog does not parse backticks in
    /// full names, so `` main.`a-b` `` would refer to a schema named `` `a-b` `` instead of `a-b`.
    /// Returns the existing table that a `REPLACE` targets, or `None` when the table
    /// is created instead.
    async fn get_table_to_replace(
        &self,
        database: &Namespace,
        table: &str,
        options: &CreateTableOptions,
    ) -> CatalogResult<Option<TableStatus>> {
        if !options.mode.is_replace() {
            return Ok(None);
        }
        let status = match self.get_table(database, table).await {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _)) if !options.mode.replace_requires_existing() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        ensure_table_access(&status)?;
        let TableKind::Table {
            format, location, ..
        } = &status.kind
        else {
            return Err(CatalogError::NotSupported(
                "Unity Catalog supports REPLACE for Delta tables only".to_string(),
            ));
        };
        if !format.eq_ignore_ascii_case("delta") || !options.format.eq_ignore_ascii_case("delta") {
            return Err(CatalogError::NotSupported(
                "Unity Catalog supports REPLACE for Delta tables only".to_string(),
            ));
        }
        if options.is_external
            && let (Some(requested), Some(existing)) = (&options.location, location)
            && !is_same_location(requested, existing)
        {
            return Err(CatalogError::NotSupported(format!(
                "Unity Catalog does not support REPLACE with a different table location: \
                 the table is at {existing}"
            )));
        }
        Ok(Some(status))
    }

    /// Plans the replacement of an existing Delta table. The table keeps its identity
    /// and its location, and the replacement is a commit to its Delta log.
    fn plan_delta_table_replace(
        &self,
        status: &TableStatus,
        request: LakehouseCreateRequest,
    ) -> LakehouseCreatePlan {
        let LakehouseCreateRequest {
            catalog_table,
            options,
        } = request;
        let mut table = resolve_lakehouse_table_status(
            self.get_name(),
            catalog_table,
            status,
            LakehouseOperation::Write,
            &self.lakehouse_capabilities(),
        );
        let catalog_managed = table.execution.commit == CommitAuthority::DeltaRatifiedCommit;
        table.status.display_properties = options.properties;
        if catalog_managed && let Some(table_id) = &table.execution.table_identity.table_id {
            table
                .status
                .display_properties
                .push((DELTA_UNITY_TABLE_ID_KEY.to_string(), table_id.clone()));
        }
        let materialization = if options.is_write_precondition {
            // The writer commits the replacement together with the data.
            LakehouseCreateMaterialization::None
        } else if catalog_managed {
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
                context: Some(Box::new(table.execution.clone())),
            }
        } else {
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::PathManaged,
                context: None,
            }
        };
        LakehouseCreatePlan {
            table,
            materialization,
            staged: None,
        }
    }

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
        require_columns: bool,
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
                let name = name.unwrap_or_default();
                let data_type = unity_type_to_data_type(
                    type_name.map(|type_name| *type_name),
                    type_json,
                    type_text,
                    type_precision,
                    type_scale,
                    type_interval_type,
                )?;
                Ok(TableColumnStatus {
                    metadata: Default::default(),
                    name,
                    data_type,
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
            // All the columns are visited for the partition columns.
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<CatalogResult<Vec<_>>>();

        let format = data_source_format
            .map(|f| f.to_string().to_lowercase())
            .unwrap_or_else(|| "delta".to_string());

        let columns = match columns {
            Ok(columns) => columns,
            // The schema of a Delta table is in its transaction log, and the table is
            // resolved with the schema of the log if the catalog columns are missing.
            // So the columns that cannot be converted do not fail the table.
            // They do not fail the table list either, which does not need the columns.
            Err(_) if format == "delta" || !require_columns => vec![],
            Err(e) => return Err(e),
        };

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
            Err(e) if if_not_exists && is_already_exists_error(&e, "SCHEMA_ALREADY_EXISTS") => {
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
        let mut output = vec![];
        let mut page_token = None;
        loop {
            let response = client
                .list_schemas(catalog_name.clone(), None, page_token)
                .await
                .map_err(|e| CatalogError::External(format!("Failed to list schemas: {e}")))?
                .inner;
            output.extend(
                response
                    .schemas
                    .unwrap_or_default()
                    .into_iter()
                    .map(|schema_info| {
                        self.schema_info_to_database_status(schema_info, &catalog_name, None)
                    }),
            );
            // A page can be empty while there are more pages.
            page_token = response.next_page_token.filter(|token| !token.is_empty());
            if page_token.is_none() {
                return Ok(output);
            }
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
        // The replacement of an existing Delta table is a commit to its Delta log. The
        // commit of a managed table updates the catalog, and the catalog has no metadata
        // to update for an external table.
        if let Some(status) = self.get_table_to_replace(database, table, &options).await? {
            return Ok(status);
        }
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
            staged,
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

        // Version 0 of a managed Delta table staged with the Delta REST API is written
        // before the table is registered with the same API.
        if !is_external
            && let Some(create_commit) = staged
                .as_ref()
                .and_then(|staged| staged.created_metadata.as_deref())
        {
            return match self
                .create_staged_delta_table(database, table, location, comment, create_commit)
                .await
            {
                Err(CatalogError::AlreadyExists(_, _)) if mode.ignore_if_exists() => {
                    self.get_table(database, table).await
                }
                result => result,
            };
        }

        let data_source_format = parse_data_source_format(&format)?;

        // The servers with the Delta REST API have the variant type, while the variant
        // type is a struct of its storage fields for the other servers.
        let variant_type = columns
            .iter()
            .any(|col| contains_variant_type(&col.data_type))
            && self
                .get_delta_commit_api(&self.get_delta_client().await?, &catalog_name)
                .await?
                == DeltaCommitApi::V1;

        let unity_columns: Vec<r#gen::ColumnInfo> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let unity_type = data_type_to_unity_type(&col.data_type, variant_type)?;
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
                            variant_type,
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
            data_source_format: Some(Box::new(data_source_format)),
            columns: unity_columns,
            storage_location: Some(storage_location),
            comment,
            properties: if props.is_empty() {
                None
            } else {
                Some(Box::new(r#gen::SecurablePropertiesMap(
                    props.into_iter().collect(),
                )))
            },
            view_definition: None,
            view_dependencies: None,
        };

        let result = client.create_table(request).await;

        match result {
            Ok(response) => {
                let table_info = response.inner;
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name, true)
            }
            Err(e)
                if mode.ignore_if_exists()
                    && is_already_exists_error(&e, "TABLE_ALREADY_EXISTS") =>
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
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name, true)
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

        let mut output = vec![];
        let mut page_token = None;
        loop {
            let response = client
                .list_tables(catalog_name.clone(), schema_name.clone(), None, page_token)
                .await
                .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?
                .inner;
            for table_info in response.tables.unwrap_or_default() {
                output.push(self.table_info_to_table_status(
                    table_info,
                    &catalog_name,
                    &schema_name,
                    false,
                )?);
            }
            // A page can be empty while there are more pages.
            page_token = response.next_page_token.filter(|token| !token.is_empty());
            if page_token.is_none() {
                return Ok(output);
            }
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

    async fn resolve_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: ResolveLakehouseTableRequest,
    ) -> CatalogResult<LakehouseResolvedTable> {
        let status = self.get_table(database, table).await?;
        ensure_table_access(&status)?;
        Ok(resolve_lakehouse_table_status(
            self.get_name(),
            request.catalog_table,
            &status,
            request.operation,
            &self.lakehouse_capabilities(),
        ))
    }

    async fn get_table_storage_credentials(
        &self,
        database: &Namespace,
        table: &str,
        request: TableStorageCredentialsRequest,
    ) -> CatalogResult<Option<TableStorageCredentials>> {
        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let client = self
            .get_delta_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        if self.get_delta_commit_api(&client, &catalog_name).await? != DeltaCommitApi::V1 {
            return Ok(None);
        }
        let staging_table_id = self
            .staging_table_id_cache()?
            .get(storage_location_key(&request.location))
            .cloned();
        // The response body and the error are not reported in full, since a response
        // that is not understood can contain credentials.
        let (status, message) = if let Some(table_id) = staging_table_id {
            match client.get_staging_table_credentials(table_id).await {
                Ok(response) => {
                    return Ok(select_storage_credentials(
                        response.inner,
                        &request.location,
                    ));
                }
                Err(e) => (e.status(), delta_error_message(&e)),
            }
        } else {
            let operation = match request.access {
                StorageAccess::Read => delta_gen::DeltaCredentialOperation::Read,
                StorageAccess::ReadWrite => delta_gen::DeltaCredentialOperation::ReadWrite,
            };
            match client
                .get_table_credentials(
                    catalog_name.clone(),
                    schema_name.clone(),
                    table.to_string(),
                    operation,
                )
                .await
            {
                Ok(response) => {
                    return Ok(select_storage_credentials(
                        response.inner,
                        &request.location,
                    ));
                }
                Err(e) => (e.status(), delta_error_message(&e)),
            }
        };
        let name = quote_names_if_needed(&[&catalog_name, &schema_name, table]);
        let message = message.map(|x| format!(": {x}")).unwrap_or_default();
        match status {
            // The server does not issue credentials for the table,
            // e.g. when the table is not managed by the catalog.
            Some(
                reqwest::StatusCode::BAD_REQUEST
                | reqwest::StatusCode::NOT_FOUND
                | reqwest::StatusCode::NOT_IMPLEMENTED,
            ) => Ok(None),
            Some(reqwest::StatusCode::UNAUTHORIZED) => Err(CatalogError::Unauthorized(format!(
                "Unity Catalog storage credentials for table {name} unauthorized{message}"
            ))),
            Some(reqwest::StatusCode::FORBIDDEN) => Err(CatalogError::Forbidden(format!(
                "Unity Catalog storage credentials for table {name} forbidden{message}"
            ))),
            Some(status) => Err(CatalogError::External(format!(
                "Failed to get storage credentials for table {name} from Unity Catalog: \
                 HTTP {status}{message}"
            ))),
            None => Err(CatalogError::External(format!(
                "Failed to get storage credentials for table {name} from Unity Catalog"
            ))),
        }
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
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
        let requirement = self.create_table_metadata_requirement(&request.options)?;
        if let Some(status) = self
            .get_table_to_replace(database, table, &request.options)
            .await?
        {
            return Ok(self.plan_delta_table_replace(&status, request));
        }
        let mut plan = sail_catalog::lakehouse::plan_lakehouse_create_from_requirement(
            self.get_name(),
            request.catalog_table,
            &request.options,
            requirement,
            &self.lakehouse_capabilities(),
        );
        if request.options.format.eq_ignore_ascii_case("delta") && !request.options.is_external {
            let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
            // A table staged with the Delta REST API is not staged again when the table
            // is planned as a write precondition.
            let staged = plan.staged.is_some();
            let delta_client = self
                .get_delta_client()
                .await
                .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
            if staged
                || self
                    .get_delta_commit_api(&delta_client, &catalog_name)
                    .await?
                    == DeltaCommitApi::V1
            {
                if !staged {
                    let ignore_if_exists = request.options.mode.ignore_if_exists();
                    let Some((location, properties, required_protocol)) = self
                        .create_delta_staging_table(
                            &delta_client,
                            &catalog_name,
                            &schema_name,
                            table,
                            request.options.properties,
                        )
                        .await?
                    else {
                        if !ignore_if_exists {
                            return Err(CatalogError::AlreadyExists(
                                CatalogObject::Table,
                                quote_names_if_needed(&[&catalog_name, &schema_name, table]),
                            ));
                        }
                        // The table is created concurrently, so the existing table is used.
                        let status = self.get_table(database, table).await?;
                        if let TableKind::Table {
                            location: Some(location),
                            ..
                        } = status.kind
                        {
                            plan.table.status.location = Some(location.clone());
                            plan.table.execution.table_identity.table_uri = Some(location);
                        }
                        plan.materialization = LakehouseCreateMaterialization::None;
                        return Ok(plan);
                    };
                    plan.table.status.location = Some(location.clone());
                    plan.table.status.display_properties = properties;
                    plan.table.execution.table_identity.table_uri = Some(location);
                    plan.staged = Some(Box::new(StagedTableCreate {
                        required_protocol: Some(required_protocol),
                        created_metadata: None,
                    }));
                }
                // Version 0 of the table is written before the table is registered.
                plan.materialization = LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                    mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
                    context: Some(Box::new(plan.table.execution.clone())),
                };
                return Ok(plan);
            }
            plan.materialization = LakehouseCreateMaterialization::AfterCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
            };
            if request.options.is_write_precondition {
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
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
        let invalid =
            |e| CatalogError::InvalidArgument(format!("Invalid Delta commit payload: {e}"));
        let commit: r#gen::DeltaCommit = serde_json::from_value(update.clone()).map_err(invalid)?;
        let changes: DeltaLogChanges = update
            .get("delta_log")
            .map(|changes| serde_json::from_value(changes.clone()))
            .transpose()
            .map_err(invalid)?
            .unwrap_or_default();

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let delta_client = self
            .get_delta_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        let (status, error, unpublished_commit_limit, request_failed) = match self
            .get_delta_commit_api(&delta_client, &catalog_name)
            .await?
        {
            DeltaCommitApi::V1 => {
                let request = delta_update_table_request(commit, changes)?;
                match delta_client
                    .update_table(catalog_name, schema_name, table.to_string(), request)
                    .await
                {
                    Ok(_) => return Ok(LakehouseCommitOutcome::Committed { context, payload }),
                    Err(e) => (
                        e.status(),
                        e.to_string(),
                        delta_error_type(&e).as_deref() == Some("ResourceExhaustedException"),
                        matches!(e, delta_gen::ApiError::Request(_)),
                    ),
                }
            }
            DeltaCommitApi::Preview => {
                if let Some(error) = update.get("metadata_error").and_then(|x| x.as_str()) {
                    return Err(CatalogError::NotSupported(format!(
                        "Unity Catalog Delta commit metadata: {error}"
                    )));
                }
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
                match client.commit(commit).await {
                    Ok(_) => return Ok(LakehouseCommitOutcome::Committed { context, payload }),
                    Err(e) => (
                        e.status(),
                        e.to_string(),
                        legacy_error_code(&e).as_deref() == Some("RESOURCE_EXHAUSTED"),
                        matches!(e, r#gen::ApiError::Request(_)),
                    ),
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
            // The server refuses new commits until the ratified commits of the table are
            // published, which is not the same as a request that the server throttles.
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS) if unpublished_commit_limit => {
                Err(CatalogError::ResourceExhausted(format!(
                    "Unity Catalog Delta commit refused until ratified commits are published: {error}"
                )))
            }
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => Err(CatalogError::RateLimited(
                format!("Unity Catalog Delta commit rate limited: {error}"),
            )),
            Some(reqwest::StatusCode::NOT_IMPLEMENTED) => Err(CatalogError::NotSupported(format!(
                "Unity Catalog Delta commit endpoint: {error}"
            ))),
            // The server may have ratified the commit when it reports a server error, and
            // when the request fails without a response (a timeout or a network error).
            Some(s) if s.is_server_error() => Err(CatalogError::CommitStateUnknown(
                format!("Failed to commit Delta table to Unity Catalog: {error}").into(),
            )),
            None if request_failed => Err(CatalogError::CommitStateUnknown(
                format!("Failed to commit Delta table to Unity Catalog: {error}").into(),
            )),
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
        ensure_table_access(&status)?;
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
                            request.discover,
                        );
                    }
                    // The table is then not known to be catalog-managed.
                    Err(_) if request.discover => {
                        return Ok(DeltaRatifiedCommitResponse::default());
                    }
                    Err(e) => (e.status(), e.to_string()),
                }
            }
            // The legacy API serves only catalog-managed tables.
            DeltaCommitApi::Preview if request.discover => {
                return Ok(DeltaRatifiedCommitResponse::default());
            }
            DeltaCommitApi::Preview => {
                let legacy_request = r#gen::DeltaGetCommits {
                    end_version: request.end_version,
                    start_version: request.start_version,
                    table_id: table_id.to_string(),
                    table_uri,
                };
                let client = self
                    .get_client()
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
                match client.get_commits(legacy_request).await {
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
                            requires_published_log: false,
                            catalog_managed: false,
                        });
                    }
                    Err(e) => {
                        // A server can have the Delta REST API without its configuration
                        // endpoint, and then refuse the legacy API for the table.
                        // So the table is loaded via the Delta REST API once, which is
                        // used for the catalog from then on if the table is loaded.
                        if matches!(
                            e.status(),
                            Some(
                                reqwest::StatusCode::UNAUTHORIZED
                                    | reqwest::StatusCode::FORBIDDEN
                                    | reqwest::StatusCode::NOT_FOUND
                            )
                        ) && let Ok(response) = delta_client
                            .load_table(
                                catalog_name.clone(),
                                schema_name.clone(),
                                table.to_string(),
                            )
                            .await
                        {
                            self.delta_commit_api_cache()?
                                .insert(catalog_name.clone(), DeltaCommitApi::V1);
                            return delta_ratified_commits_from_load_table(
                                response.inner,
                                table,
                                table_id,
                                request.start_version,
                                request.end_version,
                                false,
                            );
                        }
                        (e.status(), e.to_string())
                    }
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

/// Whether the two locations refer to the same directory. A local path is compared
/// with and without the `file` scheme.
fn is_same_location(left: &str, right: &str) -> bool {
    let normalize = |location: &str| {
        let location = location.trim_end_matches('/');
        let location = location
            .strip_prefix("file://")
            .or_else(|| location.strip_prefix("file:"))
            .unwrap_or(location);
        location.to_string()
    };
    normalize(left) == normalize(right)
}

/// Converts the table loaded via the Delta REST API to the ratified commits
/// in the requested version range, in ascending version order.
/// The loaded table is catalog-managed if and only if its properties have the
/// `delta.feature.catalogManaged` property with the `supported` value, so the response
/// to a discovery request for another table has no commits.
fn delta_ratified_commits_from_load_table(
    response: delta_gen::DeltaLoadTableResponse,
    table: &str,
    table_id: &str,
    start_version: i64,
    end_version: Option<i64>,
    discover: bool,
) -> CatalogResult<DeltaRatifiedCommitResponse> {
    let catalog_managed = response
        .metadata
        .properties
        .iter()
        .flatten()
        .any(|(key, value)| {
            key.eq_ignore_ascii_case("delta.feature.catalogManaged")
                && value
                    .as_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case("supported"))
        });
    if discover && !catalog_managed {
        return Ok(DeltaRatifiedCommitResponse::default());
    }
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
        // A table is created by the Delta API with version 0 published.
        requires_published_log: true,
        catalog_managed,
    })
}

/// The metadata action and the protocol action of a Delta commit that differ from the table
/// that the commit is based on, and the domain metadata actions of the commit, in the form
/// of the Delta log.
#[derive(Default, serde::Deserialize)]
struct DeltaLogChanges {
    #[serde(rename = "metaData", default)]
    metadata: Option<serde_json::Value>,
    /// The metadata of the table that the commit is based on.
    #[serde(rename = "baseMetaData", default)]
    base_metadata: Option<serde_json::Value>,
    #[serde(default)]
    protocol: Option<serde_json::Value>,
    #[serde(rename = "domainMetadata", default)]
    domain_metadata: Vec<serde_json::Value>,
}

/// Converts the legacy Delta commit payload to an update table request of the Delta REST API.
/// The changes of the commit to the metadata and the protocol of the table are sent with
/// the commit, so that the table in the catalog stays consistent with the Delta log.
fn delta_update_table_request(
    commit: r#gen::DeltaCommit,
    changes: DeltaLogChanges,
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
    let DeltaLogChanges {
        metadata,
        base_metadata,
        protocol,
        domain_metadata,
    } = changes;
    match (metadata, base_metadata) {
        (Some(metadata), Some(base)) => delta_metadata_updates(&base, &metadata, &mut updates)?,
        (Some(_), None) => {
            return Err(CatalogError::InvalidArgument(
                "Unity Catalog Delta commit that changes the table metadata expects the current table metadata"
                    .to_string(),
            ));
        }
        (None, _) => {}
    }
    if let Some(protocol) = protocol {
        let protocol = delta_protocol(&protocol).map_err(|message| {
            CatalogError::InvalidArgument(format!("Invalid Delta commit payload: {message}"))
        })?;
        updates.push(delta_gen::DeltaTableUpdate::SetProtocol {
            protocol: Box::new(protocol),
        });
    }
    delta_domain_metadata_updates(&domain_metadata, &mut updates)?;
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

/// Adds the updates of the Delta REST API for the differences between two metadata actions
/// of the Delta log.
fn delta_metadata_updates(
    base: &serde_json::Value,
    metadata: &serde_json::Value,
    updates: &mut Vec<delta_gen::DeltaTableUpdate>,
) -> CatalogResult<()> {
    let schema = |metadata: &serde_json::Value| {
        metadata["schemaString"]
            .as_str()
            .and_then(|schema| serde_json::from_str::<serde_json::Value>(schema).ok())
            .ok_or_else(|| {
                CatalogError::InvalidArgument(
                    "Invalid Delta commit payload: missing schema".to_string(),
                )
            })
    };
    let new_schema = schema(metadata)?;
    if schema(base)? != new_schema {
        updates.push(delta_gen::DeltaTableUpdate::SetColumns {
            columns: Box::new(delta_schema_to_wire_type(&new_schema)),
        });
    }
    let partition_columns = |metadata: &serde_json::Value| {
        delta_strings(&metadata["partitionColumns"]).unwrap_or_default()
    };
    let new_partition_columns = partition_columns(metadata);
    if partition_columns(base) != new_partition_columns {
        updates.push(delta_gen::DeltaTableUpdate::SetPartitionColumns {
            partition_columns: new_partition_columns,
        });
    }
    let comment = metadata["description"].as_str();
    if base["description"].as_str() != comment {
        updates.push(delta_gen::DeltaTableUpdate::SetTableComment {
            comment: comment.unwrap_or_default().to_string(),
        });
    }
    // The properties that the catalog derives are not table properties.
    let properties = |metadata: &serde_json::Value| {
        metadata["configuration"]
            .as_object()
            .map(|configuration| {
                configuration
                    .iter()
                    .filter(|(key, _)| !is_delta_derived_property(key))
                    .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default()
    };
    let base_properties = properties(base);
    let mut new_properties = properties(metadata);
    let mut removals = base_properties
        .keys()
        .filter(|key| !new_properties.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    removals.sort();
    new_properties.retain(|key, value| base_properties.get(key) != Some(value));
    if !new_properties.is_empty() {
        updates.push(delta_gen::DeltaTableUpdate::SetProperties {
            updates: new_properties,
        });
    }
    if !removals.is_empty() {
        updates.push(delta_gen::DeltaTableUpdate::RemoveProperties { removals });
    }
    Ok(())
}

/// Adds the updates of the Delta REST API for the domain metadata actions of a Delta commit.
/// The catalog tracks the row tracking domain only.
fn delta_domain_metadata_updates(
    domain_metadata: &[serde_json::Value],
    updates: &mut Vec<delta_gen::DeltaTableUpdate>,
) -> CatalogResult<()> {
    let invalid = |message: String| {
        CatalogError::InvalidArgument(format!("Invalid Delta commit payload: {message}"))
    };
    let mut row_tracking = None;
    let mut removals = vec![];
    for action in domain_metadata {
        let domain = action["domain"]
            .as_str()
            .ok_or_else(|| invalid("missing metadata domain".to_string()))?;
        if action["removed"].as_bool().unwrap_or_default() {
            removals.push(domain.to_string());
        } else if domain == "delta.rowTracking" {
            let configuration = action["configuration"]
                .as_str()
                .ok_or_else(|| invalid(format!("missing configuration of domain {domain}")))?;
            row_tracking = Some(
                serde_json::from_str::<delta_gen::DeltaRowTrackingDomainMetadata>(configuration)
                    .map_err(|e| invalid(format!("configuration of domain {domain}: {e}")))?,
            );
        } else {
            return Err(CatalogError::NotSupported(format!(
                "Unity Catalog Delta commit that sets the metadata domain {domain}"
            )));
        }
    }
    if let Some(row_tracking) = row_tracking {
        updates.push(delta_gen::DeltaTableUpdate::SetDomainMetadata {
            updates: Box::new(delta_gen::DeltaDomainMetadataUpdates {
                delta_clustering: None,
                delta_row_tracking: Some(Box::new(row_tracking)),
            }),
        });
    }
    if !removals.is_empty() {
        updates.push(delta_gen::DeltaTableUpdate::RemoveDomainMetadata { domains: removals });
    }
    Ok(())
}

/// Whether the catalog derives the table property from the protocol, the domain metadata,
/// or the commits of the Delta table, so that it is never set or removed by a commit.
fn is_delta_derived_property(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.starts_with("delta.feature.")
        || matches!(
            key.as_str(),
            "delta.minreaderversion"
                | "delta.minwriterversion"
                | "delta.clusteringcolumns"
                | "delta.rowtracking.rowidhighwatermark"
                | "delta.lastupdateversion"
                | "delta.lastcommittimestamp"
        )
}

fn delta_strings(value: &serde_json::Value) -> Option<Vec<String>> {
    value.as_array().map(|values| {
        values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect()
    })
}

/// Converts a protocol action of the Delta log to its form in the Delta REST API.
fn delta_protocol(protocol: &serde_json::Value) -> Result<delta_gen::DeltaProtocol, String> {
    let version = |key: &str| {
        protocol[key]
            .as_i64()
            .and_then(|version| i32::try_from(version).ok())
            .ok_or_else(|| format!("missing protocol {key}"))
    };
    Ok(delta_gen::DeltaProtocol {
        min_reader_version: version("minReaderVersion")?,
        min_writer_version: version("minWriterVersion")?,
        reader_features: delta_strings(&protocol["readerFeatures"]),
        writer_features: delta_strings(&protocol["writerFeatures"]),
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

/// Builds the create table request of the Delta REST API for a staged managed Delta table
/// from version 0 of the table.
fn delta_create_table_request(
    table: &str,
    location: String,
    comment: Option<String>,
    create_commit: &str,
) -> CatalogResult<delta_gen::DeltaCreateTableRequest> {
    let invalid = |message: &str| {
        CatalogError::Internal(format!(
            "Invalid Delta table version 0 for Unity Catalog: {message}"
        ))
    };
    let commit: serde_json::Value =
        serde_json::from_str(create_commit).map_err(|e| invalid(&e.to_string()))?;
    let metadata = &commit["metaData"];
    let schema: serde_json::Value = metadata["schemaString"]
        .as_str()
        .and_then(|schema| serde_json::from_str(schema).ok())
        .ok_or_else(|| invalid("missing schema"))?;
    let properties = metadata["configuration"]
        .as_object()
        .map(|configuration| {
            configuration
                .iter()
                .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_string())))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    Ok(delta_gen::DeltaCreateTableRequest {
        columns: Box::new(delta_schema_to_wire_type(&schema)),
        comment: metadata["description"]
            .as_str()
            .map(str::to_string)
            .or(comment),
        domain_metadata: None,
        last_commit_timestamp_ms: commit["inCommitTimestamp"]
            .as_i64()
            .ok_or_else(|| invalid("missing in-commit timestamp"))?,
        location,
        name: table.to_string(),
        partition_columns: delta_strings(&metadata["partitionColumns"])
            .filter(|columns| !columns.is_empty()),
        properties,
        protocol: Box::new(delta_protocol(&commit["protocol"]).map_err(|e| invalid(&e))?),
        table_type: Box::new(delta_gen::DeltaTableType::Managed),
        uniform: None,
    })
}

/// Converts a data type in the schema of the Delta log to its form in the Delta REST API,
/// where the properties of array types and map types are named differently.
fn delta_schema_to_wire_type(data_type: &serde_json::Value) -> serde_json::Value {
    let Some(object) = data_type.as_object() else {
        return data_type.clone();
    };
    let convert = |key: &str| delta_schema_to_wire_type(&data_type[key]);
    match object.get("type").and_then(serde_json::Value::as_str) {
        Some("struct") => {
            let fields = object
                .get("fields")
                .and_then(serde_json::Value::as_array)
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| {
                            let mut field = field.clone();
                            if let Some(field) = field.as_object_mut() {
                                if let Some(data_type) = field.get_mut("type") {
                                    *data_type = delta_schema_to_wire_type(data_type);
                                }
                                let metadata = field
                                    .entry("metadata")
                                    .or_insert_with(|| serde_json::json!({}));
                                delta_field_metadata_to_wire(metadata);
                            }
                            field
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            serde_json::json!({ "type": "struct", "fields": fields })
        }
        Some("array") => serde_json::json!({
            "type": "array",
            "element-type": convert("elementType"),
            "contains-null": data_type["containsNull"],
        }),
        Some("map") => serde_json::json!({
            "type": "map",
            "key-type": convert("keyType"),
            "value-type": convert("valueType"),
            "value-contains-null": data_type["valueContainsNull"],
        }),
        _ => data_type.clone(),
    }
}

/// Converts the metadata of a field in the schema of the Delta log to the metadata of
/// the field in the catalog, where the entries of the Spark field metadata are entries
/// of the field metadata rather than the JSON object under a single key.
/// This is how the metadata is sent to the servers without the Delta REST API as well.
fn delta_field_metadata_to_wire(metadata: &mut serde_json::Value) {
    let Some(object) = metadata.as_object_mut() else {
        return;
    };
    let spark = match object.get(SPARK_METADATA_JSON_KEY) {
        Some(serde_json::Value::Object(spark)) => spark.clone(),
        Some(serde_json::Value::String(spark)) => {
            match serde_json::from_str::<serde_json::Value>(spark) {
                Ok(serde_json::Value::Object(spark)) => spark,
                _ => return,
            }
        }
        Some(_) => return,
        None => serde_json::Map::new(),
    };
    object.remove(SPARK_METADATA_JSON_KEY);
    object.remove(SAIL_SPARK_UDT_METADATA_KEY);
    for (key, value) in spark {
        object.insert(key, value);
    }
}

/// Whether the error of creating an object says that the object exists.
/// Some servers report it as a bad request with an error code for the kind of object,
/// such as `SCHEMA_ALREADY_EXISTS`, instead of a conflict.
/// Returns the storage location in the form used to compare locations.
fn storage_location_key(location: &str) -> &str {
    location.trim_end_matches('/')
}

/// Selects the credential with the longest prefix that covers the storage location.
/// Returns nothing if no credential that the client can use covers the location.
fn select_storage_credentials(
    response: delta_gen::DeltaCredentialsResponse,
    location: &str,
) -> Option<TableStorageCredentials> {
    let location = storage_location_key(location);
    response
        .storage_credentials
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            let prefix = storage_location_key(item.prefix.as_deref()?);
            let rest = location.strip_prefix(prefix)?;
            if !(rest.is_empty() || rest.starts_with('/')) {
                return None;
            }
            let length = prefix.len();
            let mut config = item.config?;
            let mut take = |key: &str| config.remove(key).flatten().filter(|x| !x.is_empty());
            let credential = if let Some(access_key_id) = take("s3.access-key-id")
                && let Some(secret_access_key) = take("s3.secret-access-key")
            {
                StorageCredential::S3 {
                    access_key_id,
                    secret_access_key,
                    session_token: take("s3.session-token"),
                }
            } else if let Some(token) = take("azure.sas-token") {
                StorageCredential::AzureSas { token }
            } else {
                StorageCredential::GcsOauth {
                    token: take("gcs.oauth-token")?,
                }
            };
            Some((
                length,
                TableStorageCredentials {
                    credential,
                    expires_at_ms: item.expiration_time_ms,
                },
            ))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, credentials)| credentials)
}

/// Returns the error message that the server reports in a Delta REST API error response.
fn delta_error_message<E>(error: &delta_gen::ApiError<E>) -> Option<String> {
    let body = match error {
        delta_gen::ApiError::Response(response) => response.body.as_str(),
        delta_gen::ApiError::Unknown(response) => response.body.as_str(),
        _ => return None,
    };
    let body: serde_json::Value = serde_json::from_str(body).ok()?;
    let message = body.get("error")?.get("message")?.as_str()?;
    Some(message.to_string())
}

fn is_already_exists_error<E>(error: &r#gen::ApiError<E>, error_code: &str) -> bool {
    let body = match error {
        r#gen::ApiError::Response(response) => response.body.as_str(),
        r#gen::ApiError::Unknown(response) => response.body.as_str(),
        _ => "",
    };
    match error.status() {
        Some(reqwest::StatusCode::CONFLICT) => true,
        Some(reqwest::StatusCode::BAD_REQUEST) => serde_json::from_str::<serde_json::Value>(body)
            .is_ok_and(|body| body["error_code"] == error_code),
        _ => false,
    }
}

/// Returns the error code in the error response of the legacy API.
fn legacy_error_code<E>(error: &r#gen::ApiError<E>) -> Option<String> {
    let body = match error {
        r#gen::ApiError::Response(response) => response.body.as_str(),
        r#gen::ApiError::Unknown(response) => response.body.as_str(),
        _ => return None,
    };
    let body = serde_json::from_str::<serde_json::Value>(body).ok()?;
    body["error_code"].as_str().map(str::to_string)
}

/// Returns the error type in the error response of the Delta REST API.
fn delta_error_type<E>(error: &delta_gen::ApiError<E>) -> Option<String> {
    let body = match error {
        delta_gen::ApiError::Response(response) => response.body.as_str(),
        delta_gen::ApiError::Unknown(response) => response.body.as_str(),
        _ => return None,
    };
    let body = serde_json::from_str::<serde_json::Value>(body).ok()?;
    body["error"]["type"].as_str().map(str::to_string)
}

/// Whether the data type is the variant type or has the variant type nested in it.
fn contains_variant_type(data_type: &DataType) -> bool {
    if is_marked_variant_storage_type(data_type) {
        return true;
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_variant_type(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_variant_type(field.data_type())),
        _ => false,
    }
}

/// Fails for the Unity Catalog objects that are not accessible as tables.
///
/// Views are not tables, and Databricks does not let external engines access
/// the data of materialized views, streaming tables, and foreign tables.
/// Such objects are still returned as catalog metadata.
fn ensure_table_access(status: &TableStatus) -> CatalogResult<()> {
    let TableKind::Table { properties, .. } = &status.kind else {
        return Ok(());
    };
    let table_type = properties
        .iter()
        .find(|(key, _)| key == "table_type")
        .map(|(_, value)| value.as_str());
    match table_type {
        Some(
            table_type @ ("VIEW" | "MATERIALIZED_VIEW" | "METRIC_VIEW" | "STREAMING_TABLE"
            | "FOREIGN"),
        ) => {
            let mut names = status
                .database
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            names.push(&status.name);
            Err(CatalogError::NotSupported(format!(
                "Unity Catalog object {} of type {table_type} cannot be read or written as a table",
                quote_names_if_needed(&names),
            )))
        }
        _ => Ok(()),
    }
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
    use wiremock::matchers::{method, path, path_regex, query_param, query_param_is_missing};
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
        assert_eq!(
            config.uri,
            "https://test.databricks.com/api/2.1/unity-catalog"
        );
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
        assert_eq!(
            config.uri,
            "https://test.databricks.com/api/2.1/unity-catalog"
        );
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
        assert_eq!(
            provider.options.uri,
            "https://test.databricks.com/api/2.1/unity-catalog"
        );
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
        let config = UnityCatalogConfig::new(Some(server.uri()), &None, Some(options)).unwrap();
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
            .table_info_to_table_status(tables.next().unwrap(), "catalog", "schema", true)
            .unwrap();
        let typed_table = provider
            .table_info_to_table_status(tables.next().unwrap(), "catalog", "schema", true)
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
            .table_info_to_table_status(response, "catalog", "schema", true)
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
            .table_info_to_table_status(response, "catalog", "schema", true)
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
            discover: false,
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

    async fn discover_delta_catalog_managed(
        server: &MockServer,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        let provider = mock_provider(server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let request = DeltaRatifiedCommitRequest {
            discover: true,
            ..delta_ratified_commit_request(1, None)
        };
        provider
            .get_delta_ratified_commits(&database, "events", request)
            .await
    }

    #[tokio::test]
    async fn test_delta_api_states_whether_table_is_catalog_managed() {
        // The legacy table properties do not have the catalog-managed feature.
        let delta_api_targets = vec![
            "GET /tables/main.default.events".to_string(),
            "GET /delta/v1/config?catalog=main&protocol-versions=1.0".to_string(),
            format!("GET {DELTA_TABLE_PATH}"),
        ];

        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    Some(vec![2, 1]),
                    Some(2),
                )),
            )
            .mount(&server)
            .await;
        let response = discover_delta_catalog_managed(&server).await.unwrap();
        assert!(response.catalog_managed);
        assert_eq!(response.latest_table_version, 2);
        let versions = response.commits.iter().map(|commit| commit.version);
        assert_eq!(versions.collect::<Vec<_>>(), vec![1, 2]);
        // The table is loaded once for the answer and the commits.
        assert_eq!(received_targets(&server).await, delta_api_targets);

        // A table without the property is not catalog-managed, whatever else the response has.
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_legacy_delta_table(&server).await;
        let mut table = delta_load_table_json("another-table-id", Some(vec![1]), Some(1));
        table["metadata"]["properties"] = serde_json::json!({"delta.enableChangeDataFeed": "true"});
        Mock::given(method("GET"))
            .and(path(DELTA_TABLE_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(table))
            .mount(&server)
            .await;
        let response = discover_delta_catalog_managed(&server).await.unwrap();
        assert!(!response.catalog_managed);
        assert_eq!(response.commits, vec![]);
        assert_eq!(received_targets(&server).await, delta_api_targets);

        // A table that the Delta REST API does not load is not known to be catalog-managed.
        for status in [400, 401, 404, 500] {
            let server = MockServer::start().await;
            mount_delta_config_ok(&server).await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("GET"))
                .and(path(DELTA_TABLE_PATH))
                .respond_with(ResponseTemplate::new(status).set_body_string("failed"))
                .mount(&server)
                .await;
            let response = discover_delta_catalog_managed(&server).await.unwrap();
            assert!(!response.catalog_managed, "status {status}");
            assert_eq!(received_targets(&server).await, delta_api_targets);
        }
    }

    #[tokio::test]
    async fn test_legacy_api_does_not_state_whether_table_is_catalog_managed() {
        let server = MockServer::start().await;
        mount_delta_config(&server, ResponseTemplate::new(404)).await;
        mount_legacy_delta_table(&server).await;
        let response = discover_delta_catalog_managed(&server).await.unwrap();
        assert!(!response.catalog_managed);
        // Neither the legacy commits of the table nor the table of the Delta REST API
        // are requested.
        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /tables/main.default.events",
                "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
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

            // A commit that changes the table metadata sends the legacy metadata only.
            let mut commit = legacy_delta_commit(3);
            commit["metadata"] = serde_json::json!({"description": "events"});
            let mut request = commit.clone();
            request["delta_log"] = serde_json::json!({
                "metaData": delta_log_metadata(&["id"], &[], Some("events"), &[]),
                "baseMetaData": delta_log_metadata(&["id"], &[], None, &[]),
                "protocol": delta_log_protocol(&["catalogManaged"]),
            });
            let outcome = provider
                .commit_lakehouse_table(&database, "events", delta_commit_request(request))
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

    /// Returns a metadata action in the form of the Delta log.
    fn delta_log_metadata(
        columns: &[&str],
        partition_columns: &[&str],
        description: Option<&str>,
        configuration: &[(&str, &str)],
    ) -> serde_json::Value {
        let mut fields = columns
            .iter()
            .map(|column| {
                serde_json::json!({
                    "name": column,
                    "type": "long",
                    "nullable": true,
                    "metadata": {}
                })
            })
            .collect::<Vec<_>>();
        fields.push(serde_json::json!({
            "name": "tags",
            "type": {"type": "array", "elementType": "string", "containsNull": true},
            "nullable": true,
            "metadata": {"comment": "tags"}
        }));
        let schema = serde_json::json!({"type": "struct", "fields": fields});
        let mut metadata = serde_json::json!({
            "id": "metadata-id",
            "name": null,
            "description": description,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": partition_columns,
            "createdTime": 1700000000000i64,
            "configuration": {"io.unitycatalog.tableId": DELTA_TABLE_ID}
        });
        for (key, value) in configuration {
            metadata["configuration"][*key] = serde_json::json!(value);
        }
        metadata
    }

    fn delta_log_protocol(features: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": features,
            "writerFeatures": features,
        })
    }

    /// Returns the columns of [`delta_log_metadata`] in the form of the Delta REST API.
    fn delta_columns_json(columns: &[&str]) -> serde_json::Value {
        let mut fields = columns
            .iter()
            .map(|column| {
                serde_json::json!({
                    "name": column,
                    "type": "long",
                    "nullable": true,
                    "metadata": {}
                })
            })
            .collect::<Vec<_>>();
        fields.push(serde_json::json!({
            "name": "tags",
            "type": {"type": "array", "element-type": "string", "contains-null": true},
            "nullable": true,
            "metadata": {"comment": "tags"}
        }));
        serde_json::json!({"type": "struct", "fields": fields})
    }

    /// A column written from a DataFrame has its Spark field metadata under a single key
    /// in the schema of the Delta log, and the catalog has the entries of that metadata.
    #[test]
    fn test_delta_schema_to_wire_type_field_metadata() {
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [
                {
                    "name": "extra",
                    "type": "string",
                    "nullable": true,
                    "metadata": {"SPARK::metadata::json": {}}
                },
                {
                    "name": "items",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [{
                                "name": "x",
                                "type": "integer",
                                "nullable": true,
                                "metadata": {
                                    "SPARK::metadata::json": "{\"comment\":\"x\",\"k\":1}",
                                    "delta.columnMapping.id": 2
                                }
                            }]
                        },
                        "containsNull": true
                    },
                    "nullable": true,
                    "metadata": {"delta.columnMapping.id": 1, "comment": "items"}
                },
                {"name": "plain", "type": "long", "nullable": false}
            ]
        });
        assert_eq!(
            delta_schema_to_wire_type(&schema),
            serde_json::json!({
                "type": "struct",
                "fields": [
                    {"name": "extra", "type": "string", "nullable": true, "metadata": {}},
                    {
                        "name": "items",
                        "type": {
                            "type": "array",
                            "element-type": {
                                "type": "struct",
                                "fields": [{
                                    "name": "x",
                                    "type": "integer",
                                    "nullable": true,
                                    "metadata": {
                                        "comment": "x",
                                        "k": 1,
                                        "delta.columnMapping.id": 2
                                    }
                                }]
                            },
                            "contains-null": true
                        },
                        "nullable": true,
                        "metadata": {"delta.columnMapping.id": 1, "comment": "items"}
                    },
                    {"name": "plain", "type": "long", "nullable": false, "metadata": {}}
                ]
            })
        );
    }

    /// Commits to a server with the Delta REST API and returns the bodies of the update
    /// table requests.
    async fn delta_api_commit_bodies(commits: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
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
        let count = commits.len();
        for commit in commits {
            let outcome = provider
                .commit_lakehouse_table(&database, "events", delta_commit_request(commit))
                .await
                .unwrap();
            assert!(matches!(outcome, LakehouseCommitOutcome::Committed { .. }));
        }
        let mut targets =
            vec!["GET /delta/v1/config?catalog=main&protocol-versions=1.0".to_string()];
        targets.extend(std::iter::repeat_n(
            format!("POST {DELTA_TABLE_PATH}"),
            count,
        ));
        assert_eq!(received_targets(&server).await, targets);
        received_json_bodies(&server, DELTA_TABLE_PATH).await
    }

    #[tokio::test]
    async fn test_delta_commit_with_schema_change_uses_delta_api() {
        let base = delta_log_metadata(&["id"], &[], None, &[]);
        // ALTER TABLE ADD COLUMNS.
        let mut add_column = legacy_delta_commit(5);
        add_column["metadata"] = serde_json::json!({"schema": {"columns": []}});
        add_column["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["id", "value"], &[], None, &[]),
            "baseMetaData": base,
        });
        // A write that evolves the schema.
        let mut append = legacy_delta_commit(6);
        append["latest_backfilled_version"] = serde_json::json!(5);
        append["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["id", "value", "extra"], &[], None, &[]),
            "baseMetaData": delta_log_metadata(&["id", "value"], &[], None, &[]),
        });
        // A write that replaces the schema, the partition columns, and the protocol.
        let mut replace = legacy_delta_commit(7);
        replace["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["day"], &["day"], None, &[]),
            "baseMetaData": delta_log_metadata(&["id", "value", "extra"], &[], None, &[]),
            "protocol": delta_log_protocol(&["catalogManaged", "deletionVectors"]),
        });
        // The metadata action of a commit that is the same as the metadata of the table.
        let mut unchanged = legacy_delta_commit(8);
        unchanged["metadata"] = serde_json::json!({"schema": {"columns": []}});

        let requirements = serde_json::json!([
            {"type": "assert-table-uuid", "uuid": DELTA_TABLE_ID}
        ]);
        assert_eq!(
            delta_api_commit_bodies(vec![add_column, append, replace, unchanged]).await,
            vec![
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(5)},
                        {"action": "set-columns", "columns": delta_columns_json(&["id", "value"])}
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(6)},
                        {"action": "set-latest-backfilled-version", "latest-published-version": 5},
                        {
                            "action": "set-columns",
                            "columns": delta_columns_json(&["id", "value", "extra"])
                        }
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(7)},
                        {"action": "set-columns", "columns": delta_columns_json(&["day"])},
                        {"action": "set-partition-columns", "partition-columns": ["day"]},
                        {
                            "action": "set-protocol",
                            "protocol": {
                                "min-reader-version": 3,
                                "min-writer-version": 7,
                                "reader-features": ["catalogManaged", "deletionVectors"],
                                "writer-features": ["catalogManaged", "deletionVectors"]
                            }
                        }
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(8)}
                    ]
                }),
            ]
        );
    }

    #[tokio::test]
    async fn test_delta_commit_with_property_and_comment_changes_uses_delta_api() {
        let base = delta_log_metadata(
            &["id"],
            &["id"],
            Some("events"),
            &[
                ("owner", "a"),
                ("team", "x"),
                ("stale", "1"),
                ("delta.feature.catalogManaged", "supported"),
            ],
        );
        // ALTER TABLE SET TBLPROPERTIES and ALTER TABLE UNSET TBLPROPERTIES. The properties
        // that the catalog derives from the protocol are not sent.
        let mut properties = legacy_delta_commit(5);
        properties["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(
                &["id"],
                &["id"],
                Some("events"),
                &[
                    ("owner", "b"),
                    ("team", "x"),
                    ("added", "2"),
                    ("delta.minWriterVersion", "7"),
                    ("delta.lastUpdateVersion", "4"),
                ],
            ),
            "baseMetaData": base,
        });
        let mut comment = legacy_delta_commit(6);
        comment["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["id"], &["id"], Some("all events"), &[]),
            "baseMetaData": delta_log_metadata(&["id"], &["id"], Some("events"), &[]),
        });
        let mut no_comment = legacy_delta_commit(7);
        no_comment["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["id"], &["id"], None, &[]),
            "baseMetaData": delta_log_metadata(&["id"], &["id"], Some("all events"), &[]),
        });

        let requirements = serde_json::json!([
            {"type": "assert-table-uuid", "uuid": DELTA_TABLE_ID}
        ]);
        assert_eq!(
            delta_api_commit_bodies(vec![properties, comment, no_comment]).await,
            vec![
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(5)},
                        {"action": "set-properties", "updates": {"owner": "b", "added": "2"}},
                        {"action": "remove-properties", "removals": ["stale"]}
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(6)},
                        {"action": "set-table-comment", "comment": "all events"}
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(7)},
                        {"action": "set-table-comment", "comment": ""}
                    ]
                }),
            ]
        );
    }

    /// A commit whose metadata has no form in the legacy commit API, such as the metadata
    /// of a table with a variant column, succeeds with the Delta REST API.
    #[tokio::test]
    async fn test_delta_commit_without_legacy_metadata() {
        let variant = |columns: &[&str]| {
            let mut metadata = delta_log_metadata(columns, &[], None, &[]);
            let schema = serde_json::json!({
                "type": "struct",
                "fields": columns
                    .iter()
                    .map(|column| {
                        serde_json::json!({
                            "name": column,
                            "type": "variant",
                            "nullable": true,
                            "metadata": {}
                        })
                    })
                    .collect::<Vec<_>>()
            });
            metadata["schemaString"] = serde_json::json!(schema.to_string());
            (metadata, schema)
        };
        let (base, _) = variant(&["a"]);
        let (metadata, columns) = variant(&["a", "b"]);
        let mut commit = legacy_delta_commit(5);
        commit["metadata_error"] = serde_json::json!("variant columns");
        commit["delta_log"] = serde_json::json!({"metaData": metadata, "baseMetaData": base});

        assert_eq!(
            delta_api_commit_bodies(vec![commit.clone()]).await,
            vec![serde_json::json!({
                "requirements": [{"type": "assert-table-uuid", "uuid": DELTA_TABLE_ID}],
                "updates": [
                    {"action": "add-commit", "commit": delta_commit_json(5)},
                    {"action": "set-columns", "columns": columns}
                ]
            })]
        );

        let server = MockServer::start().await;
        mount_delta_config(&server, ResponseTemplate::new(404)).await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let error = provider
            .commit_lakehouse_table(&database, "events", delta_commit_request(commit))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::NotSupported(message) if message.contains("variant columns")),
            "{error}"
        );
        assert_eq!(
            received_targets(&server).await,
            vec!["GET /delta/v1/config?catalog=main&protocol-versions=1.0"]
        );
    }

    #[tokio::test]
    async fn test_delta_commit_with_domain_metadata_uses_delta_api() {
        // An append to a table with row tracking raises the row ID high-water mark.
        let mut append = legacy_delta_commit(5);
        append["delta_log"] = serde_json::json!({
            "domainMetadata": [{
                "domain": "delta.rowTracking",
                "configuration": r#"{"rowIdHighWaterMark":41}"#,
                "removed": false,
            }],
        });
        let mut removal = legacy_delta_commit(6);
        removal["delta_log"] = serde_json::json!({
            "domainMetadata": [{
                "domain": "delta.rowTracking",
                "configuration": r#"{"rowIdHighWaterMark":41}"#,
                "removed": true,
            }],
        });
        let requirements = serde_json::json!([
            {"type": "assert-table-uuid", "uuid": DELTA_TABLE_ID}
        ]);
        assert_eq!(
            delta_api_commit_bodies(vec![append, removal]).await,
            vec![
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(5)},
                        {
                            "action": "set-domain-metadata",
                            "updates": {"delta.rowTracking": {"rowIdHighWaterMark": 41}}
                        }
                    ]
                }),
                serde_json::json!({
                    "requirements": requirements,
                    "updates": [
                        {"action": "add-commit", "commit": delta_commit_json(6)},
                        {"action": "remove-domain-metadata", "domains": ["delta.rowTracking"]}
                    ]
                }),
            ]
        );
    }

    #[tokio::test]
    async fn test_delta_commit_with_metadata_change_expects_current_metadata() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let mut commit = legacy_delta_commit(5);
        commit["delta_log"] = serde_json::json!({
            "metaData": delta_log_metadata(&["id"], &[], None, &[]),
        });
        let error = provider
            .commit_lakehouse_table(&database, "events", delta_commit_request(commit))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::InvalidArgument(message) if message.contains("expects the current table metadata")),
            "{error}"
        );
        assert_eq!(
            received_targets(&server).await,
            vec!["GET /delta/v1/config?catalog=main&protocol-versions=1.0"]
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
            (10, error(429, "TooManyRequestsException")),
            (11, error(500, "CommitStateUnknownException")),
            (12, ResponseTemplate::new(503)),
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
        assert!(
            matches!(error, CatalogError::ResourceExhausted(_)),
            "{error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("ResourceExhaustedException message")
        );
        let error = commit(8).await;
        assert!(matches!(error, CatalogError::Forbidden(_)), "{error:?}");
        // The commit may be ratified when the server fails or does not answer.
        for (version, message) in [
            (9, "InternalServerError message"),
            (11, "CommitStateUnknownException message"),
            (12, "503"),
        ] {
            let error = commit(version).await;
            assert!(
                matches!(error, CatalogError::CommitStateUnknown(_)),
                "{error:?}"
            );
            assert!(error.to_string().contains(message), "{error}");
        }
        let error = commit(10).await;
        assert!(matches!(error, CatalogError::RateLimited(_)), "{error:?}");

        // A request without a response, such as a network error.
        let options = UnityCatalogOptions {
            uri: "http://127.0.0.1:1".to_string(),
            ..empty_options("main")
        };
        let provider = UnityCatalogProvider::new("test".to_string(), options).unwrap();
        provider
            .delta_commit_api_cache()
            .unwrap()
            .insert("main".to_string(), DeltaCommitApi::V1);
        let error = provider
            .commit_lakehouse_table(
                &database,
                "events",
                delta_commit_request(legacy_delta_commit(13)),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, CatalogError::CommitStateUnknown(_)),
            "{error:?}"
        );
    }

    const DELTA_STAGING_PATH: &str = "/delta/v1/catalogs/main/schemas/default/staging-tables";
    const DELTA_TABLES_PATH: &str = "/delta/v1/catalogs/main/schemas/default/tables";

    fn managed_delta_create_options(
        properties: Vec<(&str, &str)>,
        is_write_precondition: bool,
    ) -> CreateTableOptions {
        CreateTableOptions {
            columns: vec![],
            comment: None,
            constraints: vec![],
            location: Some("file:///warehouse/default/events".to_string()),
            format: "delta".to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode: Default::default(),
            properties: properties
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            is_external: false,
            is_write_precondition,
            staged: None,
        }
    }

    fn lakehouse_create_request(options: CreateTableOptions) -> LakehouseCreateRequest {
        LakehouseCreateRequest {
            catalog_table: vec![
                "test".to_string(),
                "default".to_string(),
                "events".to_string(),
            ],
            options,
        }
    }

    /// The staging table response with fields that the client does not use.
    fn delta_staging_table_json() -> serde_json::Value {
        serde_json::json!({
            "table-id": DELTA_TABLE_ID,
            "table-type": "MANAGED",
            "location": DELTA_TABLE_URI,
            "storage-credentials": [{
                "prefix": DELTA_TABLE_URI,
                "operation": "READ_WRITE",
                "config": {"s3.access-key-id": "key"}
            }],
            "required-protocol": {
                "min-reader-version": 3,
                "min-writer-version": 7,
                "reader-features": ["catalogManaged", "deletionVectors", "v2Checkpoint"],
                "writer-features": [
                    "catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint"
                ]
            },
            "suggested-protocol": {"writer-features": ["rowTracking"]},
            "required-properties": {
                "delta.checkpointPolicy": "v2",
                "delta.enableDeletionVectors": "true",
                "io.unitycatalog.tableId": DELTA_TABLE_ID
            },
            "suggested-properties": {"delta.columnMapping.maxColumnId": null}
        })
    }

    async fn mount_delta_staging_table(server: &MockServer, response: serde_json::Value) {
        Mock::given(method("POST"))
            .and(path(DELTA_STAGING_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }

    /// Version 0 of the table as reported after it is written to the Delta log.
    fn delta_create_commit() -> String {
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {"comment": "key"}},
                {
                    "name": "tags",
                    "type": {
                        "type": "map",
                        "keyType": "string",
                        "valueType": {
                            "type": "array",
                            "elementType": {
                                "type": "struct",
                                "fields": [{"name": "x", "type": "decimal(10,2)", "nullable": true}]
                            },
                            "containsNull": false
                        },
                        "valueContainsNull": true
                    },
                    "nullable": true,
                    "metadata": {}
                }
            ]
        });
        serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["catalogManaged", "deletionVectors"],
                "writerFeatures": ["catalogManaged", "deletionVectors", "inCommitTimestamp"]
            },
            "metaData": {
                "id": "metadata-id",
                "description": "events table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["id"],
                "createdTime": 1700000000000i64,
                "configuration": {
                    "delta.enableDeletionVectors": "true",
                    "io.unitycatalog.tableId": DELTA_TABLE_ID
                }
            },
            "inCommitTimestamp": 1700000000123i64
        })
        .to_string()
    }

    fn staged_delta_create_options() -> CreateTableOptions {
        let mut options = managed_delta_create_options(vec![("owner", "sail")], true);
        options.location = Some(DELTA_TABLE_URI.to_string());
        options.staged = Some(Box::new(StagedTableCreate {
            required_protocol: Some("{}".to_string()),
            created_metadata: Some(delta_create_commit()),
        }));
        options
    }

    #[tokio::test]
    async fn test_managed_delta_create_uses_delta_api() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_delta_staging_table(&server, delta_staging_table_json()).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("POST"))
            .and(path(DELTA_TABLES_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    None,
                    Some(0),
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let options = managed_delta_create_options(
            vec![
                ("owner", "sail"),
                ("delta.enabledeletionvectors", "true"),
                ("io.unitycatalog.tableId", "user-table-id"),
            ],
            false,
        );
        let plan = provider
            .plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
            .await
            .unwrap();
        assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
        assert_eq!(
            plan.table.execution.table_identity.table_uri.as_deref(),
            Some(DELTA_TABLE_URI)
        );
        assert!(matches!(
            plan.materialization,
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
                context: Some(_),
            }
        ));
        let required_protocol = serde_json::json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged", "deletionVectors", "v2Checkpoint"],
            "writerFeatures": [
                "catalogManaged", "deletionVectors", "inCommitTimestamp", "v2Checkpoint"
            ]
        });
        assert_eq!(
            plan.table.status.display_properties,
            vec![
                ("owner".to_string(), "sail".to_string()),
                (
                    "delta.enabledeletionvectors".to_string(),
                    "true".to_string()
                ),
                ("delta.checkpointPolicy".to_string(), "v2".to_string()),
                (
                    "io.unitycatalog.tableId".to_string(),
                    DELTA_TABLE_ID.to_string()
                ),
            ]
        );
        assert_eq!(
            plan.staged,
            Some(Box::new(StagedTableCreate {
                required_protocol: Some(required_protocol.to_string()),
                created_metadata: None,
            }))
        );

        // The staging table is not created again when the staged table is planned
        // as a write precondition.
        let mut options = managed_delta_create_options(vec![], true);
        options.location = plan.table.status.location.clone();
        options.properties = plan.table.status.display_properties.clone();
        options.staged = plan.staged.clone();
        let precondition = provider
            .plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
            .await
            .unwrap();
        assert_eq!(precondition.table.status, plan.table.status);
        assert_eq!(precondition.materialization, plan.materialization);
        assert_eq!(precondition.staged, plan.staged);

        provider
            .create_table(&database, "events", staged_delta_create_options())
            .await
            .unwrap();

        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /delta/v1/config?catalog=main&protocol-versions=1.0".to_string(),
                format!("POST {DELTA_STAGING_PATH}"),
                format!("POST {DELTA_TABLES_PATH}"),
                "GET /tables/main.default.events".to_string(),
            ]
        );
        assert_eq!(
            received_json_bodies(&server, DELTA_STAGING_PATH).await,
            vec![serde_json::json!({"name": "events"})]
        );
        assert_eq!(
            received_json_bodies(&server, DELTA_TABLES_PATH).await,
            vec![serde_json::json!({
                "name": "events",
                "location": DELTA_TABLE_URI,
                "table-type": "MANAGED",
                "comment": "events table",
                "columns": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "id",
                            "type": "long",
                            "nullable": false,
                            "metadata": {"comment": "key"}
                        },
                        {
                            "name": "tags",
                            "type": {
                                "type": "map",
                                "key-type": "string",
                                "value-type": {
                                    "type": "array",
                                    "element-type": {
                                        "type": "struct",
                                        "fields": [{
                                            "name": "x",
                                            "type": "decimal(10,2)",
                                            "nullable": true,
                                            "metadata": {}
                                        }]
                                    },
                                    "contains-null": false
                                },
                                "value-contains-null": true
                            },
                            "nullable": true,
                            "metadata": {}
                        }
                    ]
                },
                "partition-columns": ["id"],
                "protocol": {
                    "min-reader-version": 3,
                    "min-writer-version": 7,
                    "reader-features": ["catalogManaged", "deletionVectors"],
                    "writer-features": ["catalogManaged", "deletionVectors", "inCommitTimestamp"]
                },
                "properties": {
                    "delta.enableDeletionVectors": "true",
                    "io.unitycatalog.tableId": DELTA_TABLE_ID
                },
                "last-commit-timestamp-ms": 1700000000123i64
            })]
        );
    }

    #[tokio::test]
    async fn test_managed_delta_create_uses_legacy_api() {
        let server = MockServer::start().await;
        mount_delta_config(&server, ResponseTemplate::new(404)).await;
        Mock::given(method("POST"))
            .and(path("/staging-tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "staging_location": DELTA_TABLE_URI
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "catalog_name": "main",
                "schema_name": "default",
                "name": "events",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "storage_location": DELTA_TABLE_URI,
                "table_id": DELTA_TABLE_ID,
                "columns": []
            })))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let after_catalog = LakehouseCreateMaterialization::AfterCatalogLakeSource {
            mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
        };

        let options = managed_delta_create_options(vec![("owner", "sail")], false);
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(options.clone()),
            )
            .await
            .unwrap();
        assert_eq!(plan.materialization, after_catalog);
        assert_eq!(plan.table.status.location, options.location);
        assert_eq!(plan.table.status.display_properties, options.properties);
        provider
            .create_table(&database, "events", options)
            .await
            .unwrap();

        let options = managed_delta_create_options(vec![("owner", "sail")], true);
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(options.clone()),
            )
            .await
            .unwrap();
        assert_eq!(plan.materialization, after_catalog);
        assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
        assert_eq!(plan.table.status.display_properties, options.properties);

        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                "POST /staging-tables",
                "POST /tables",
                "POST /staging-tables",
            ]
        );
        let bodies = received_json_bodies(&server, "/tables").await;
        assert_eq!(bodies[0]["table_type"], "MANAGED");
        assert_eq!(bodies[0]["storage_location"], DELTA_TABLE_URI);
        assert_eq!(
            bodies[0]["properties"],
            serde_json::json!({"owner": "sail"})
        );
    }

    async fn mount_existing_table(server: &MockServer, table_type: &str, format: &str) {
        Mock::given(method("GET"))
            .and(path_regex("^/tables/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "catalog_name": "main",
                "schema_name": "default",
                "name": "events",
                "table_type": table_type,
                "data_source_format": format,
                "storage_location": DELTA_TABLE_URI,
                "table_id": DELTA_TABLE_ID,
                "columns": []
            })))
            .mount(server)
            .await;
    }

    fn replace_options(
        mode: sail_catalog::provider::CreateTableMode,
        is_write_precondition: bool,
    ) -> CreateTableOptions {
        CreateTableOptions {
            mode,
            ..managed_delta_create_options(vec![("owner", "sail")], is_write_precondition)
        }
    }

    #[tokio::test]
    async fn test_replace_of_managed_delta_table_is_a_commit_to_the_existing_table() {
        use sail_catalog::provider::CreateTableMode;

        let server = MockServer::start().await;
        mount_existing_table(&server, "MANAGED", "DELTA").await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();

        for mode in [CreateTableMode::CreateOrReplace, CreateTableMode::Replace] {
            // The statement without a query replaces the metadata with a catalog commit.
            let plan = provider
                .plan_lakehouse_create(
                    &database,
                    "events",
                    lakehouse_create_request(replace_options(mode, false)),
                )
                .await
                .unwrap();
            let context = &plan.table.execution;
            assert_eq!(
                plan.materialization,
                LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                    mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
                    context: Some(Box::new(context.clone())),
                }
            );
            assert_eq!(context.commit, CommitAuthority::DeltaRatifiedCommit);
            assert_eq!(context.operation, LakehouseOperation::Write);
            assert_eq!(
                context.table_identity.table_id.as_deref(),
                Some(DELTA_TABLE_ID)
            );
            assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
            assert_eq!(
                plan.table.status.display_properties,
                vec![
                    ("owner".to_string(), "sail".to_string()),
                    (
                        DELTA_UNITY_TABLE_ID_KEY.to_string(),
                        DELTA_TABLE_ID.to_string()
                    ),
                ]
            );
            assert_eq!(plan.staged, None);

            // The writer of the statement with a query commits the replacement.
            let plan = provider
                .plan_lakehouse_create(
                    &database,
                    "events",
                    lakehouse_create_request(replace_options(mode, true)),
                )
                .await
                .unwrap();
            assert_eq!(plan.materialization, LakehouseCreateMaterialization::None);
            assert_eq!(
                plan.table.execution.commit,
                CommitAuthority::DeltaRatifiedCommit
            );
            assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));

            let status = provider
                .create_table(&database, "events", replace_options(mode, true))
                .await
                .unwrap();
            assert_eq!(status.name, "events");
        }

        // The table is neither staged, created, nor dropped.
        let targets = received_targets(&server).await;
        assert_eq!(targets.len(), 6);
        assert!(
            targets
                .iter()
                .all(|target| target == "GET /tables/main.default.events"),
            "{targets:?}"
        );
    }

    #[tokio::test]
    async fn test_replace_of_external_delta_table_is_a_commit_to_its_location() {
        use sail_catalog::provider::CreateTableMode;

        let server = MockServer::start().await;
        mount_existing_table(&server, "EXTERNAL", "DELTA").await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();

        let external = |location: &str, is_write_precondition| CreateTableOptions {
            location: Some(location.to_string()),
            is_external: true,
            ..replace_options(CreateTableMode::CreateOrReplace, is_write_precondition)
        };
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(external(&format!("{DELTA_TABLE_URI}/"), false)),
            )
            .await
            .unwrap();
        assert_eq!(
            plan.materialization,
            LakehouseCreateMaterialization::BeforeCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::PathManaged,
                context: None,
            }
        );
        assert_eq!(plan.table.execution.commit, CommitAuthority::Filesystem);
        assert_eq!(
            plan.table.status.display_properties,
            vec![("owner".to_string(), "sail".to_string())]
        );
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(external(DELTA_TABLE_URI, true)),
            )
            .await
            .unwrap();
        assert_eq!(plan.materialization, LakehouseCreateMaterialization::None);

        let result = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(external("s3://bucket/tables/other", false)),
            )
            .await;
        assert!(
            matches!(&result, Err(CatalogError::NotSupported(message)) if message.contains("different table location")),
            "{result:?}"
        );
        assert!(
            received_targets(&server)
                .await
                .iter()
                .all(|target| target == "GET /tables/main.default.events")
        );
    }

    #[tokio::test]
    async fn test_replace_of_other_tables_and_missing_tables() {
        use sail_catalog::provider::CreateTableMode;

        let database = Namespace::try_from(vec!["default"]).unwrap();

        let server = MockServer::start().await;
        mount_existing_table(&server, "EXTERNAL", "PARQUET").await;
        let provider = mock_provider(&server, "main");
        for is_write_precondition in [false, true] {
            let options = replace_options(CreateTableMode::CreateOrReplace, is_write_precondition);
            let result = provider
                .plan_lakehouse_create(
                    &database,
                    "events",
                    lakehouse_create_request(options.clone()),
                )
                .await;
            assert!(
                matches!(&result, Err(CatalogError::NotSupported(message)) if message.contains("Delta tables only")),
                "{result:?}"
            );
            let result = provider.create_table(&database, "events", options).await;
            assert!(
                matches!(&result, Err(CatalogError::NotSupported(_))),
                "{result:?}"
            );
        }

        // A missing table is an error for `REPLACE` and is created for `CREATE OR REPLACE`.
        let server = MockServer::start().await;
        mount_delta_config(&server, ResponseTemplate::new(404)).await;
        Mock::given(method("GET"))
            .and(path_regex("^/tables/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/staging-tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "staging_location": DELTA_TABLE_URI
            })))
            .mount(&server)
            .await;
        let provider = mock_provider(&server, "main");
        let result = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(replace_options(CreateTableMode::Replace, false)),
            )
            .await;
        assert!(
            matches!(&result, Err(CatalogError::NotFound(_, _))),
            "{result:?}"
        );
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(replace_options(CreateTableMode::CreateOrReplace, true)),
            )
            .await
            .unwrap();
        assert_eq!(
            plan.materialization,
            LakehouseCreateMaterialization::AfterCatalogLakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
            }
        );
        assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
    }

    #[tokio::test]
    async fn test_managed_delta_create_rejects_unsupported_required_properties() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        let mut response = delta_staging_table_json();
        response["required-properties"]["delta.futureProperty"] = serde_json::Value::Null;
        mount_delta_staging_table(&server, response).await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let plan = |properties| {
            let options = managed_delta_create_options(properties, false);
            provider.plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
        };

        // The property without a required value must be set by the client.
        let error = plan(vec![]).await.unwrap_err();
        assert!(matches!(error, CatalogError::NotSupported(_)), "{error:?}");
        assert!(error.to_string().contains("delta.futureProperty"));

        let error = plan(vec![
            ("delta.futureProperty", "value"),
            ("delta.enableDeletionVectors", "false"),
        ])
        .await
        .unwrap_err();
        assert!(
            matches!(error, CatalogError::InvalidArgument(_)),
            "{error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("delta.enableDeletionVectors=true")
        );

        let plan = plan(vec![("delta.futureProperty", "value")]).await.unwrap();
        assert!(
            plan.table
                .status
                .display_properties
                .contains(&("delta.futureProperty".to_string(), "value".to_string()))
        );
    }

    #[tokio::test]
    async fn test_managed_delta_create_staging_table_errors() {
        for (status, body) in [
            (409, "TableAlreadyExists message"),
            (403, "Forbidden message"),
        ] {
            let server = MockServer::start().await;
            mount_delta_config_ok(&server).await;
            Mock::given(method("POST"))
                .and(path(DELTA_STAGING_PATH))
                .respond_with(ResponseTemplate::new(status).set_body_string(body))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let options = managed_delta_create_options(vec![], false);
            let error = provider
                .plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
                .await
                .unwrap_err();
            if status == 409 {
                // The existing table is planned if the table is created only if it
                // does not exist.
                mount_legacy_delta_table(&server).await;
                let mut options = managed_delta_create_options(vec![], true);
                options.mode = sail_catalog::provider::CreateTableMode::CreateIfNotExists;
                let plan = provider
                    .plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
                    .await
                    .unwrap();
                assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
                assert_eq!(plan.materialization, LakehouseCreateMaterialization::None);
                assert_eq!(plan.staged, None);
                assert!(
                    matches!(error, CatalogError::AlreadyExists(_, _)),
                    "{error:?}"
                );
            } else {
                assert!(matches!(error, CatalogError::External(_)), "{error:?}");
                assert!(error.to_string().contains(body), "{error}");
            }
        }
    }

    #[tokio::test]
    async fn test_managed_delta_create_table_errors_and_recovery() {
        // (create table status, table ID of the loaded table, whether the table is created)
        let cases = [
            (500, Some(DELTA_TABLE_ID.to_ascii_uppercase()), true),
            (500, Some("another-table-id".to_string()), false),
            (500, None, false),
            (400, Some(DELTA_TABLE_ID.to_string()), false),
            (409, Some(DELTA_TABLE_ID.to_string()), false),
        ];
        for (status, loaded_table_id, created) in cases {
            let server = MockServer::start().await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("POST"))
                .and(path(DELTA_TABLES_PATH))
                .respond_with(ResponseTemplate::new(status).set_body_string("server message"))
                .mount(&server)
                .await;
            let load_table =
                match &loaded_table_id {
                    Some(table_id) => ResponseTemplate::new(200)
                        .set_body_json(delta_load_table_json(table_id, None, Some(0))),
                    None => ResponseTemplate::new(404),
                };
            Mock::given(method("GET"))
                .and(path(DELTA_TABLE_PATH))
                .respond_with(load_table)
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let result = provider
                .create_table(&database, "events", staged_delta_create_options())
                .await;
            let case = format!("{status} {loaded_table_id:?}");
            let mut targets = vec![format!("POST {DELTA_TABLES_PATH}")];
            if status == 500 {
                targets.push(format!("GET {DELTA_TABLE_PATH}"));
            }
            match result {
                Ok(_) => {
                    assert!(created, "{case}");
                    targets.push("GET /tables/main.default.events".to_string());
                }
                Err(error) => {
                    assert!(!created, "{case}");
                    if status == 409 {
                        assert!(
                            matches!(error, CatalogError::AlreadyExists(_, _)),
                            "{case}: {error:?}"
                        );
                    } else {
                        assert!(
                            matches!(error, CatalogError::External(_)),
                            "{case}: {error:?}"
                        );
                        assert!(error.to_string().contains("server message"), "{case}");
                    }
                }
            }
            assert_eq!(received_targets(&server).await, targets, "{case}");
        }
    }

    #[tokio::test]
    async fn test_create_schema_if_not_exists_with_existing_schema() {
        let already_exists = serde_json::json!({
            "error_code": "SCHEMA_ALREADY_EXISTS",
            "details": [{"reason": "SCHEMA_ALREADY_EXISTS"}],
            "message": "Schema already exists: default"
        });
        let invalid = serde_json::json!({
            "error_code": "INVALID_ARGUMENT",
            "message": "\"SCHEMA_ALREADY_EXISTS\" is not the error code"
        });
        for (response, exists) in [
            (ResponseTemplate::new(409), true),
            (
                ResponseTemplate::new(400).set_body_json(already_exists),
                true,
            ),
            (ResponseTemplate::new(400).set_body_json(invalid), false),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/schemas"))
                .respond_with(response)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path_regex("^/schemas/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "catalog_name": "main",
                    "name": "default"
                })))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let create = |if_not_exists| {
                provider.create_database(
                    &database,
                    CreateDatabaseOptions {
                        comment: None,
                        location: None,
                        if_not_exists,
                        properties: vec![],
                    },
                )
            };
            let result = create(true).await;
            assert_eq!(result.is_ok(), exists, "{result:?}");
            assert!(create(false).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_delta_api_configuration_for_missing_catalog() {
        let server = MockServer::start().await;
        mount_delta_config(
            &server,
            ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": {
                    "message": "Catalog not found: main",
                    "type": "NoSuchCatalogException",
                    "code": 404
                }
            })),
        )
        .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        // The legacy API is not selected, and the failure is not remembered.
        for _ in 0..2 {
            let options = managed_delta_create_options(vec![], false);
            let error = provider
                .plan_lakehouse_create(&database, "events", lakehouse_create_request(options))
                .await
                .unwrap_err();
            assert!(
                matches!(error, CatalogError::NotFound(CatalogObject::Catalog, _)),
                "{error:?}"
            );
        }
        assert_eq!(
            received_targets(&server).await,
            vec!["GET /delta/v1/config?catalog=main&protocol-versions=1.0"; 2]
        );
    }

    #[tokio::test]
    async fn test_list_requests_follow_page_tokens() {
        let server = MockServer::start().await;
        let schema = |name: &str| serde_json::json!({"catalog_name": "main", "name": name});
        let table = |name: &str| {
            serde_json::json!({
                "catalog_name": "main",
                "schema_name": "default",
                "name": name,
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "columns": []
            })
        };
        // A page can be empty while there are more pages, and the last page can have
        // an empty token.
        for (endpoint, key, pages) in [
            (
                "/schemas",
                "schemas",
                vec![
                    (None, serde_json::json!([schema("a")]), Some("p1")),
                    (Some("p1"), serde_json::json!([]), Some("p2")),
                    (
                        Some("p2"),
                        serde_json::json!([schema("b"), schema("c")]),
                        Some(""),
                    ),
                ],
            ),
            (
                "/tables",
                "tables",
                vec![
                    (None, serde_json::json!([table("a")]), Some("p1")),
                    (Some("p1"), serde_json::Value::Null, Some("p2")),
                    (
                        Some("p2"),
                        serde_json::json!([table("b"), table("c")]),
                        None,
                    ),
                ],
            ),
        ] {
            for (page_token, items, next_page_token) in pages {
                let mut response = serde_json::json!({ key: items });
                if let Some(next_page_token) = next_page_token {
                    response["next_page_token"] = serde_json::json!(next_page_token);
                }
                let mock = Mock::given(method("GET")).and(path(endpoint));
                let mock = match page_token {
                    Some(page_token) => mock.and(query_param("page_token", page_token)),
                    None => mock.and(query_param_is_missing("page_token")),
                };
                mock.respond_with(ResponseTemplate::new(200).set_body_json(response))
                    .mount(&server)
                    .await;
            }
        }

        let provider = mock_provider(&server, "main");
        let schemas = provider.list_databases(None).await.unwrap();
        let names = schemas
            .iter()
            .map(|x| x.database.last().unwrap().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "c"]);

        let database = Namespace::try_from(vec!["default"]).unwrap();
        let tables = provider.list_tables(&database).await.unwrap();
        let names = tables.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "c"]);

        assert_eq!(
            received_targets(&server).await,
            vec![
                "GET /schemas?catalog_name=main",
                "GET /schemas?catalog_name=main&page_token=p1",
                "GET /schemas?catalog_name=main&page_token=p2",
                "GET /tables?catalog_name=main&schema_name=default",
                "GET /tables?catalog_name=main&schema_name=default&page_token=p1",
                "GET /tables?catalog_name=main&schema_name=default&page_token=p2",
            ]
        );
    }

    fn unknown_type_table_json(name: &str, format: &str) -> serde_json::Value {
        serde_json::json!({
            "catalog_name": "main",
            "schema_name": "default",
            "name": name,
            "table_type": "MANAGED",
            "data_source_format": format,
            "storage_location": DELTA_TABLE_URI,
            "table_id": DELTA_TABLE_ID,
            "columns": [{
                "name": "id",
                "type_name": "LONG",
                "type_text": "bigint",
                "type_json": "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}",
                "position": 0,
                "nullable": true
            }, {
                "name": "area",
                "type_name": "GEOGRAPHY",
                "type_text": "geography(4326)",
                "type_json": "{\"name\":\"area\",\"type\":\"geography(4326)\",\"nullable\":true,\"metadata\":{}}",
                "position": 1,
                "nullable": true
            }, {
                "name": "day",
                "type_name": "DATE",
                "type_text": "date",
                "position": 2,
                "partition_index": 0,
                "nullable": true
            }]
        })
    }

    #[tokio::test]
    #[expect(clippy::panic)]
    async fn test_unknown_column_types_do_not_fail_delta_tables_and_table_lists() {
        let server = MockServer::start().await;
        for (name, format) in [("deltatable", "DELTA"), ("parquettable", "PARQUET")] {
            Mock::given(method("GET"))
                .and(path_regex(format!("^/tables/.*{name}$")))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(unknown_type_table_json(name, format)),
                )
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "tables": [
                    unknown_type_table_json("deltatable", "DELTA"),
                    unknown_type_table_json("parquettable", "PARQUET"),
                    {
                        "catalog_name": "main",
                        "schema_name": "default",
                        "name": "futuretable",
                        "table_type": "FUTURE_TABLE_TYPE",
                        "data_source_format": "FUTURE_FORMAT",
                        "columns": [{"name": "id", "type_name": "INT", "type_text": "int"}]
                    }
                ]
            })))
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();

        // The Delta table is resolved with the schema of its transaction log.
        let status = provider.get_table(&database, "deltatable").await.unwrap();
        let TableKind::Table {
            columns,
            format,
            partition_by,
            ..
        } = status.kind
        else {
            panic!("expected table status");
        };
        assert_eq!(format, "delta");
        assert!(columns.is_empty());
        assert_eq!(
            partition_by,
            identity_partition_fields(&["day".to_string()])
        );

        // The columns of other tables are required.
        let error = provider
            .get_table(&database, "parquettable")
            .await
            .unwrap_err();
        assert!(
            matches!(&error, CatalogError::NotSupported(message) if message.contains("GEOGRAPHY")),
            "unexpected error: {error:?}"
        );

        let tables = provider.list_tables(&database).await.unwrap();
        let tables = tables
            .into_iter()
            .map(|status| {
                let TableKind::Table {
                    columns,
                    format,
                    properties,
                    ..
                } = status.kind
                else {
                    panic!("expected table status");
                };
                let table_type = get_property(&properties.into_iter().collect(), "table_type");
                (status.name, format, table_type, columns.len())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tables,
            vec![
                (
                    "deltatable".to_string(),
                    "delta".to_string(),
                    Some("MANAGED".to_string()),
                    0
                ),
                (
                    "parquettable".to_string(),
                    "parquet".to_string(),
                    Some("MANAGED".to_string()),
                    0
                ),
                (
                    "futuretable".to_string(),
                    "future_format".to_string(),
                    Some("FUTURE_TABLE_TYPE".to_string()),
                    1
                ),
            ]
        );
    }

    #[test]
    fn test_unknown_enum_values_are_kept() {
        let value = serde_json::json!({
            "table_type": "FUTURE_TABLE_TYPE",
            "data_source_format": "FUTURE_FORMAT",
            "columns": [{"type_name": "GEOMETRY"}, {"type_name": "variant"}]
        });
        let table: r#gen::TableInfo = serde_json::from_value(value.clone()).unwrap();
        assert!(matches!(
            table.table_type.as_deref(),
            Some(r#gen::TableType::Unknown(x)) if x == "FUTURE_TABLE_TYPE"
        ));
        assert!(matches!(
            table.data_source_format.as_deref(),
            Some(r#gen::DataSourceFormat::Unknown(x)) if x == "FUTURE_FORMAT"
        ));
        let columns = table.columns.as_deref().unwrap();
        assert!(matches!(
            columns[0].type_name.as_deref(),
            Some(r#gen::ColumnTypeName::Unknown(x)) if x == "GEOMETRY"
        ));
        assert!(matches!(
            columns[1].type_name.as_deref(),
            Some(r#gen::ColumnTypeName::Variant)
        ));
        let output = serde_json::to_value(&table).unwrap();
        assert_eq!(output["table_type"], value["table_type"]);
        assert_eq!(output["data_source_format"], value["data_source_format"]);
        assert_eq!(output["columns"][0]["type_name"], "GEOMETRY");
        assert_eq!(output["columns"][1]["type_name"], "VARIANT");

        let known: r#gen::TableType = serde_json::from_value(serde_json::json!("VIEW")).unwrap();
        assert!(matches!(known, r#gen::TableType::View));
    }

    #[tokio::test]
    #[expect(clippy::panic)]
    async fn test_views_and_other_objects_are_not_accessed_as_tables() {
        for table_type in [
            "VIEW",
            "MATERIALIZED_VIEW",
            "STREAMING_TABLE",
            "METRIC_VIEW",
            "FOREIGN",
        ] {
            let server = MockServer::start().await;
            // Such an object can have no data source format and no storage location.
            let object = serde_json::json!({
                "catalog_name": "main",
                "schema_name": "default",
                "name": "events",
                "table_type": table_type,
                "table_id": DELTA_TABLE_ID,
                "columns": [{"name": "id", "type_name": "LONG", "type_text": "bigint"}]
            });
            Mock::given(method("GET"))
                .and(path_regex("^/tables/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(object.clone()))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/tables"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"tables": [object]})),
                )
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();

            // The object is returned for the commands that only need its metadata
            // (e.g. checking whether it exists or describing it), and it is listed.
            let status = provider.get_table(&database, "events").await.unwrap();
            let tables = provider.list_tables(&database).await.unwrap();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0].name, "events");
            assert_eq!(status.name, "events");
            let TableKind::Table {
                columns,
                properties,
                ..
            } = status.kind
            else {
                panic!("expected table status");
            };
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name, "id");
            assert_eq!(
                get_property(&properties.into_iter().collect(), "table_type").as_deref(),
                Some(table_type)
            );

            // The object is not read or written as a table, and it does not get
            // commits from the catalog.
            let expected = format!(
                "Unity Catalog object main.default.events of type {table_type} cannot be read or written as a table"
            );
            for operation in [LakehouseOperation::Read, LakehouseOperation::Write] {
                let error = provider
                    .resolve_lakehouse_table(
                        &database,
                        "events",
                        ResolveLakehouseTableRequest {
                            catalog_table: vec![
                                "main".to_string(),
                                "default".to_string(),
                                "events".to_string(),
                            ],
                            operation,
                            requested_format: None,
                            options: vec![],
                        },
                    )
                    .await
                    .map(|_| ())
                    .unwrap_err();
                assert!(
                    matches!(&error, CatalogError::NotSupported(message) if *message == expected),
                    "unexpected error: {error:?}"
                );
            }
            let error = provider
                .get_delta_ratified_commits(
                    &database,
                    "events",
                    delta_ratified_commit_request(0, None),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(&error, CatalogError::NotSupported(message) if *message == expected),
                "unexpected error: {error:?}"
            );

            // No Delta API request is made for the object.
            let mut targets = received_targets(&server).await;
            targets.dedup();
            assert_eq!(
                targets,
                vec![
                    "GET /tables/main.default.events",
                    "GET /tables?catalog_name=main&schema_name=default",
                    "GET /tables/main.default.events",
                ],
                "table type {table_type}"
            );
        }
    }

    #[tokio::test]
    async fn test_tables_are_accessed_with_catalog_commits() {
        let server = MockServer::start().await;
        mount_legacy_delta_table(&server).await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let resolved = provider
            .resolve_lakehouse_table(
                &database,
                "events",
                ResolveLakehouseTableRequest {
                    catalog_table: vec![
                        "main".to_string(),
                        "default".to_string(),
                        "events".to_string(),
                    ],
                    operation: LakehouseOperation::Read,
                    requested_format: None,
                    options: vec![],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            resolved.execution.commit,
            sail_common_datafusion::catalog::CommitAuthority::DeltaRatifiedCommit
        );
    }

    #[tokio::test]
    async fn test_delta_api_is_tried_when_legacy_api_refuses_the_table() {
        for status in [401, 403, 404] {
            let server = MockServer::start().await;
            mount_delta_config(&server, ResponseTemplate::new(404)).await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("GET"))
                .and(path("/delta/preview/commits"))
                .respond_with(ResponseTemplate::new(status).set_body_string("refused"))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path(DELTA_TABLE_PATH))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                        DELTA_TABLE_ID,
                        Some(vec![3, 2]),
                        Some(3),
                    )),
                )
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            // The Delta REST API is used for the catalog once it loads a table.
            for _ in 0..2 {
                let response = provider
                    .get_delta_ratified_commits(
                        &database,
                        "events",
                        delta_ratified_commit_request(2, None),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.latest_table_version, 3);
                let versions = response
                    .commits
                    .iter()
                    .map(|commit| commit.version)
                    .collect::<Vec<_>>();
                assert_eq!(versions, vec![2, 3]);
            }

            let table = format!("GET {DELTA_TABLE_PATH}");
            assert_eq!(
                received_targets(&server).await,
                vec![
                    "GET /tables/main.default.events",
                    "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                    "GET /delta/preview/commits",
                    table.as_str(),
                    "GET /tables/main.default.events",
                    table.as_str(),
                ],
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn test_legacy_api_error_is_reported_when_delta_api_fails_as_well() {
        for (status, load_table_status) in [(401, 401), (403, 404), (404, 404), (401, 500)] {
            let server = MockServer::start().await;
            mount_delta_config(&server, ResponseTemplate::new(404)).await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("GET"))
                .and(path("/delta/preview/commits"))
                .respond_with(ResponseTemplate::new(status).set_body_string("legacy refused"))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path(DELTA_TABLE_PATH))
                .respond_with(ResponseTemplate::new(load_table_status).set_body_string("failed"))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            // The legacy API remains the API of the catalog.
            for _ in 0..2 {
                let error = provider
                    .get_delta_ratified_commits(
                        &database,
                        "events",
                        delta_ratified_commit_request(2, None),
                    )
                    .await
                    .unwrap_err();
                match status {
                    404 => assert!(
                        matches!(error, CatalogError::NotFound(CatalogObject::Table, _)),
                        "unexpected error: {error:?}"
                    ),
                    _ => assert!(
                        matches!(
                            &error,
                            CatalogError::External(message) if message.contains("legacy refused")
                        ),
                        "unexpected error: {error:?}"
                    ),
                }
            }

            let table = format!("GET {DELTA_TABLE_PATH}");
            assert_eq!(
                received_targets(&server).await,
                vec![
                    "GET /tables/main.default.events",
                    "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                    "GET /delta/preview/commits",
                    table.as_str(),
                    "GET /tables/main.default.events",
                    "GET /delta/preview/commits",
                    table.as_str(),
                ],
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn test_delta_api_is_not_tried_for_other_legacy_api_errors() {
        for status in [400, 429, 500, 501] {
            let server = MockServer::start().await;
            mount_delta_config(&server, ResponseTemplate::new(404)).await;
            mount_legacy_delta_table(&server).await;
            Mock::given(method("GET"))
                .and(path("/delta/preview/commits"))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            provider
                .get_delta_ratified_commits(
                    &database,
                    "events",
                    delta_ratified_commit_request(2, None),
                )
                .await
                .unwrap_err();
            assert_eq!(
                received_targets(&server).await,
                vec![
                    "GET /tables/main.default.events",
                    "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                    "GET /delta/preview/commits",
                ],
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn test_create_table_variant_column_depends_on_delta_api() {
        // (Delta REST API configuration status, type name, type text)
        for (status, type_name, type_json) in [
            (200, "VARIANT", Some(serde_json::json!("variant"))),
            (404, "STRUCT", None),
        ] {
            let server = MockServer::start().await;
            mount_delta_config(&server, ResponseTemplate::new(status)).await;
            Mock::given(method("POST"))
                .and(path("/tables"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "catalog_name": "main",
                    "schema_name": "default",
                    "name": "events",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": DELTA_TABLE_URI,
                    "columns": []
                })))
                .mount(&server)
                .await;

            let provider = mock_provider(&server, "main");
            let database = Namespace::try_from(vec!["default"]).unwrap();
            let column = |name: &str, data_type: DataType| {
                sail_catalog::provider::CreateTableColumnOptions {
                    metadata: Default::default(),
                    name: name.to_string(),
                    data_type,
                    nullable: true,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                }
            };
            let mut options = managed_delta_create_options(vec![], false);
            options.is_external = true;
            options.columns = vec![column("id", DataType::Int64)];
            provider
                .create_table(&database, "events", options.clone())
                .await
                .unwrap();
            // The Delta REST API is only negotiated for the variant type.
            assert_eq!(received_targets(&server).await, vec!["POST /tables"]);

            let variant = crate::data_type::parse_simple_type_from_string("variant").unwrap();
            options.columns.push(column("v", variant));
            provider
                .create_table(&database, "events", options)
                .await
                .unwrap();
            assert_eq!(
                received_targets(&server).await,
                vec![
                    "POST /tables",
                    "GET /delta/v1/config?catalog=main&protocol-versions=1.0",
                    "POST /tables",
                ],
                "status {status}"
            );
            let bodies = received_json_bodies(&server, "/tables").await;
            let column = &bodies[1]["columns"][1];
            assert_eq!(column["type_name"], type_name, "status {status}");
            let field: serde_json::Value =
                serde_json::from_str(column["type_json"].as_str().unwrap()).unwrap();
            match type_json {
                Some(type_json) => {
                    assert_eq!(column["type_text"], "variant");
                    assert_eq!(field["type"], type_json);
                }
                None => assert_eq!(field["type"]["type"], "struct", "status {status}"),
            }
        }
    }

    const DELTA_CREDENTIALS_PATH: &str =
        "/delta/v1/catalogs/main/schemas/default/tables/events/credentials";

    fn storage_credentials_request(access: StorageAccess) -> TableStorageCredentialsRequest {
        TableStorageCredentialsRequest {
            location: format!("{DELTA_TABLE_URI}/"),
            access,
        }
    }

    async fn mount_table_credentials(
        server: &MockServer,
        operation: &str,
        response: ResponseTemplate,
    ) {
        Mock::given(method("GET"))
            .and(path(DELTA_CREDENTIALS_PATH))
            .and(query_param("operation", operation))
            .respond_with(response)
            .mount(server)
            .await;
    }

    async fn table_storage_credentials(
        operation: &str,
        access: StorageAccess,
        response: ResponseTemplate,
    ) -> CatalogResult<Option<TableStorageCredentials>> {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_table_credentials(&server, operation, response).await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        provider
            .get_table_storage_credentials(&database, "events", storage_credentials_request(access))
            .await
    }

    #[tokio::test]
    async fn test_table_storage_credentials_use_longest_matching_prefix() {
        let response = serde_json::json!({
            "storage-credentials": [
                {
                    "prefix": "s3://bucket",
                    "operation": "READ",
                    "expiration-time-ms": 1,
                    "config": {"s3.access-key-id": "bucket-key", "s3.secret-access-key": "x"}
                },
                {
                    "prefix": format!("{DELTA_TABLE_URI}/"),
                    "operation": "READ",
                    "expiration-time-ms": 1_800_000_000_000_i64,
                    "config": {
                        "s3.access-key-id": "table-key",
                        "s3.secret-access-key": "table-secret",
                        "s3.session-token": "table-token",
                        "azure.sas-token": null,
                        "gcs.oauth-token": null
                    }
                },
                {
                    // The prefix is not a parent directory of the table location.
                    "prefix": "s3://bucket/tables/8a1b",
                    "operation": "READ",
                    "expiration-time-ms": 2,
                    "config": {"s3.access-key-id": "other-key", "s3.secret-access-key": "x"}
                },
                {
                    "prefix": format!("{DELTA_TABLE_URI}/_delta_log"),
                    "operation": "READ",
                    "expiration-time-ms": 3,
                    "config": {"s3.access-key-id": "log-key", "s3.secret-access-key": "x"}
                }
            ]
        });
        let credentials = table_storage_credentials(
            "READ",
            StorageAccess::Read,
            ResponseTemplate::new(200).set_body_json(response),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            credentials,
            TableStorageCredentials {
                credential: StorageCredential::S3 {
                    access_key_id: "table-key".to_string(),
                    secret_access_key: "table-secret".to_string(),
                    session_token: Some("table-token".to_string()),
                },
                expires_at_ms: Some(1_800_000_000_000),
            }
        );
        let debug = format!("{credentials:?}");
        assert!(!debug.contains("table-"), "{debug}");
    }

    #[tokio::test]
    async fn test_table_storage_credentials_for_each_storage_and_access() {
        for (config, expected) in [
            (
                serde_json::json!({"s3.access-key-id": "key", "s3.secret-access-key": "secret"}),
                StorageCredential::S3 {
                    access_key_id: "key".to_string(),
                    secret_access_key: "secret".to_string(),
                    session_token: None,
                },
            ),
            (
                serde_json::json!({"azure.sas-token": "sv=1&sig=abc"}),
                StorageCredential::AzureSas {
                    token: "sv=1&sig=abc".to_string(),
                },
            ),
            (
                serde_json::json!({"gcs.oauth-token": "token"}),
                StorageCredential::GcsOauth {
                    token: "token".to_string(),
                },
            ),
        ] {
            let response = serde_json::json!({
                "storage-credentials": [{
                    "prefix": DELTA_TABLE_URI,
                    "operation": "READ_WRITE",
                    "config": config
                }]
            });
            let credentials = table_storage_credentials(
                "READ_WRITE",
                StorageAccess::ReadWrite,
                ResponseTemplate::new(200).set_body_json(response),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(credentials.credential, expected);
            assert_eq!(credentials.expires_at_ms, None);
        }
    }

    #[tokio::test]
    async fn test_table_storage_credentials_not_issued() {
        // The server issues no credential that the client can use for the location.
        for response in [
            serde_json::json!({"storage-credentials": []}),
            serde_json::json!({}),
            serde_json::json!({"storage-credentials": [{
                "prefix": DELTA_TABLE_URI, "operation": "READ", "config": {}
            }]}),
            serde_json::json!({"storage-credentials": [{
                "prefix": DELTA_TABLE_URI, "operation": "READ",
                "config": {"s3.access-key-id": "key", "s3.secret-access-key": null}
            }]}),
            serde_json::json!({"storage-credentials": [{
                "prefix": "s3://bucket/other", "operation": "READ",
                "config": {"gcs.oauth-token": "token"}
            }]}),
        ] {
            let credentials = table_storage_credentials(
                "READ",
                StorageAccess::Read,
                ResponseTemplate::new(200).set_body_json(response),
            )
            .await
            .unwrap();
            assert_eq!(credentials, None);
        }
        // The server does not issue credentials for the table.
        for status in [400, 404, 501] {
            let error = serde_json::json!({
                "error": {"message": "not managed", "type": "BadRequestException", "code": status}
            });
            let credentials = table_storage_credentials(
                "READ",
                StorageAccess::Read,
                ResponseTemplate::new(status).set_body_json(error),
            )
            .await
            .unwrap();
            assert_eq!(credentials, None);
        }
    }

    #[tokio::test]
    async fn test_table_storage_credentials_errors() {
        let error = |status: u16| {
            ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "error": {"message": "no EXTERNAL USE SCHEMA", "type": "X", "code": status}
            }))
        };
        let result =
            table_storage_credentials("READ_WRITE", StorageAccess::ReadWrite, error(403)).await;
        assert!(
            matches!(&result, Err(CatalogError::Forbidden(message))
                if message.contains("main.default.events")
                    && message.contains("no EXTERNAL USE SCHEMA")),
            "{result:?}"
        );
        let result = table_storage_credentials("READ", StorageAccess::Read, error(401)).await;
        assert!(
            matches!(result, Err(CatalogError::Unauthorized(_))),
            "{result:?}"
        );
        let result = table_storage_credentials("READ", StorageAccess::Read, error(500)).await;
        assert!(
            matches!(result, Err(CatalogError::External(_))),
            "{result:?}"
        );

        // A response that is not understood is not reported, since it can contain credentials.
        let response = serde_json::json!({
            "storage-credentials": [{
                "prefix": DELTA_TABLE_URI,
                "operation": "READ",
                "expiration-time-ms": "soon",
                "config": {"s3.access-key-id": "leaked-key", "s3.secret-access-key": 5}
            }]
        });
        let result = table_storage_credentials(
            "READ",
            StorageAccess::Read,
            ResponseTemplate::new(200).set_body_json(response),
        )
        .await;
        // A successful status with an invalid body is not a refusal of the server.
        let message = format!("{result:?}");
        assert!(result.is_err(), "{message}");
        assert!(
            !message.contains("leaked-key") && !message.contains("soon"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn test_table_storage_credentials_are_not_requested_from_legacy_servers() {
        let server = MockServer::start().await;
        mount_delta_config(&server, ResponseTemplate::new(404)).await;
        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let credentials = provider
            .get_table_storage_credentials(
                &database,
                "events",
                storage_credentials_request(StorageAccess::Read),
            )
            .await
            .unwrap();
        assert_eq!(credentials, None);
        let targets = received_targets(&server).await;
        assert!(
            !targets.iter().any(|target| target.contains("credentials")),
            "{targets:?}"
        );
    }

    #[tokio::test]
    async fn test_staging_table_storage_credentials() {
        let server = MockServer::start().await;
        mount_delta_config_ok(&server).await;
        mount_delta_staging_table(&server, delta_staging_table_json()).await;
        mount_legacy_delta_table(&server).await;
        Mock::given(method("GET"))
            // The table ID is percent-encoded in the request path.
            .and(path_regex("^/delta/v1/staging-tables/[^/]+/credentials$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "storage-credentials": [{
                    "prefix": DELTA_TABLE_URI,
                    "operation": "READ_WRITE",
                    "expiration-time-ms": 5,
                    "config": {"gcs.oauth-token": "staging-token"}
                }]
            })))
            .mount(&server)
            .await;
        mount_table_credentials(
            &server,
            "READ",
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "storage-credentials": [{
                    "prefix": DELTA_TABLE_URI,
                    "operation": "READ",
                    "expiration-time-ms": 6,
                    "config": {"gcs.oauth-token": "table-token"}
                }]
            })),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(DELTA_TABLES_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(delta_load_table_json(
                    DELTA_TABLE_ID,
                    None,
                    Some(0),
                )),
            )
            .mount(&server)
            .await;

        let provider = mock_provider(&server, "main");
        let database = Namespace::try_from(vec!["default"]).unwrap();
        let token = async |provider: &UnityCatalogProvider| {
            let credentials = provider
                .get_table_storage_credentials(
                    &database,
                    "events",
                    storage_credentials_request(StorageAccess::Read),
                )
                .await
                .unwrap()
                .unwrap();
            match credentials.credential {
                StorageCredential::GcsOauth { token } => token,
                other => format!("{other:?}"),
            }
        };

        // The table credentials are used before the table is staged.
        assert_eq!(token(&provider).await, "table-token");
        let plan = provider
            .plan_lakehouse_create(
                &database,
                "events",
                lakehouse_create_request(managed_delta_create_options(vec![], false)),
            )
            .await
            .unwrap();
        assert_eq!(plan.table.status.location.as_deref(), Some(DELTA_TABLE_URI));
        // The staging table credentials are used until the table is created.
        assert_eq!(token(&provider).await, "staging-token");
        let staging_target = format!("GET /delta/v1/staging-tables/{DELTA_TABLE_ID}/credentials");
        assert!(received_targets(&server).await.contains(&staging_target));
        provider
            .create_staged_delta_table(
                &database,
                "events",
                plan.table.status.location.clone(),
                None,
                &delta_create_commit(),
            )
            .await
            .unwrap();
        assert_eq!(token(&provider).await, "table-token");
    }
}
