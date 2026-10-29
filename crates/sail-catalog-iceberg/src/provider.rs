// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use percent_encoding::percent_decode_str;
use sail_catalog::credentials::CatalogCredentials;
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest,
    TableAccessSession,
};
use sail_catalog::provider::{
    AlterTableOptions, CatalogPartitionField, CatalogProvider, CreateDatabaseOptions,
    CreateTableColumnOptions, CreateTableOptions, CreateViewColumnOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace, PartitionTransform,
};
use sail_catalog::utils::{get_property, quote_name_if_needed, quote_namespace_if_needed};
use sail_common::utils::http::SAIL_USER_AGENT;
use sail_common_datafusion::catalog::managed::METADATA_LOCATION_KEY;
use sail_common_datafusion::catalog::{
    CapabilityFingerprint, CatalogTableBucketBy, CatalogTableConstraint, CatalogTableSort,
    DatabaseStatus, IcebergRestTableSessionRef, ScanAuthority, TableAccessSessionRef,
    TableColumnStatus, TableKind, TableStatus,
};
use sail_iceberg::utils::partition_transform::catalog_partition_field_from_iceberg;
use sail_iceberg::{
    FormatVersion, Literal, NestedField, StructType, arrow_type_to_iceberg, iceberg_type_to_arrow,
};
use tokio::sync::OnceCell;

use crate::r#gen::{ApiClient, ApiError};

pub const REST_CATALOG_PROP_URI: &str = "uri";

pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

pub const REST_CATALOG_PROP_PREFIX: &str = "prefix";

pub const REST_CATALOG_PROP_NAMESPACE_SEPARATOR: &str = "namespace-separator";

const REST_CATALOG_DEFAULT_NAMESPACE_SEPARATOR: &str = "\x1F";
const REST_ACCESS_DELEGATION_VENDED_CREDENTIALS: &str = "vended-credentials";
const REST_TABLE_SCAN_PLANNING_MODE_KEY: &str = "scan-planning-mode";
const REST_REMOTE_SIGNING_ENABLED_KEY: &str = "s3.remote-signing-enabled";

// TODO: Further properties and configurations may be needed from:
//  - https://iceberg.apache.org/docs/nightly/configuration/#catalog-properties
//  - https://iceberg.apache.org/docs/nightly/spark-configuration/
#[derive(Clone, Debug)]
pub struct CatalogConfig<'a> {
    properties: Cow<'a, HashMap<String, String>>,
}

impl CatalogConfig<'_> {
    fn uri(&self) -> Option<String> {
        self.properties
            .get(REST_CATALOG_PROP_URI)
            .cloned()
            .map(|value| value.trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
    }

    fn warehouse(&self) -> Option<String> {
        self.properties
            .get(REST_CATALOG_PROP_WAREHOUSE)
            .map(|value| value.trim_end_matches('/').to_string())
    }

    fn prefix(&self) -> Option<&str> {
        self.properties
            .get(REST_CATALOG_PROP_PREFIX)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    }

    fn namespace_separator(&self) -> CatalogResult<String> {
        let separator = self
            .properties
            .get(REST_CATALOG_PROP_NAMESPACE_SEPARATOR)
            .map(|value| value.trim());

        match separator {
            Some("") | None => Ok(REST_CATALOG_DEFAULT_NAMESPACE_SEPARATOR.to_string()),
            Some(separator) => percent_decode_str(separator)
                .decode_utf8()
                .map(|s| s.into_owned())
                .map_err(|e| {
                    CatalogError::InvalidArgument(format!(
                        "{REST_CATALOG_PROP_NAMESPACE_SEPARATOR} must be valid URL-encoded UTF-8: {e}"
                    ))
                }),
        }
    }

    /// Converts a `Namespace` into a string representation for the REST API URL.
    fn namespace_string(&self, database: &Namespace) -> CatalogResult<String> {
        let separator = self.namespace_separator()?;
        let mut result = database.head.to_string();
        for s in &database.tail {
            result.push_str(&separator);
            result.push_str(s.as_ref());
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct IcebergRestCatalogOptions {
    pub credentials: Arc<dyn CatalogCredentials>,
    pub properties: HashMap<String, String>,
}

/// Provider for Apache Iceberg REST Catalog.
pub struct IcebergRestCatalogProvider {
    name: String,
    options: IcebergRestCatalogOptions,
    http_client: reqwest::Client,
    resolved_catalog_config: OnceCell<CatalogConfig<'static>>,
}

impl IcebergRestCatalogProvider {
    pub fn new(name: String, options: IcebergRestCatalogOptions) -> Self {
        Self {
            name,
            options,
            http_client: reqwest::Client::new(),
            resolved_catalog_config: OnceCell::new(),
        }
    }

    async fn configuration(
        catalog_config: &CatalogConfig<'_>,
        credentials: &Arc<dyn CatalogCredentials>,
        http_client: reqwest::Client,
    ) -> CatalogResult<ApiClient> {
        let base_path = catalog_config.uri().ok_or_else(|| {
            CatalogError::InvalidArgument(format!(
                "Iceberg REST catalog property '{REST_CATALOG_PROP_URI}' is required"
            ))
        })?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_static(SAIL_USER_AGENT),
        );
        if let Some(credential) = credentials.retrieve().await? {
            let header = reqwest::header::HeaderValue::from_str(&format!("Bearer {credential}"))
                .map_err(|e| {
                    CatalogError::External(format!("Failed to create header value from token: {e}"))
                })?;
            headers.insert(reqwest::header::AUTHORIZATION, header);
        }
        Ok(ApiClient::new(base_path, http_client, headers))
    }

    async fn bootstrap_client(&self) -> CatalogResult<ApiClient> {
        let catalog_config = CatalogConfig {
            properties: Cow::Borrowed(&self.options.properties),
        };
        Self::configuration(
            &catalog_config,
            &self.options.credentials,
            self.http_client.clone(),
        )
        .await
    }

    async fn client(&self) -> CatalogResult<ApiClient> {
        let catalog_config = self.resolved_catalog_config().await?;
        Self::configuration(
            catalog_config,
            &self.options.credentials,
            self.http_client.clone(),
        )
        .await
    }

    /// Retry the bootstrap configuration request once on `401 Unauthorized`.
    /// This uses [`Self::bootstrap_client`] rather than [`Self::client`]
    /// because the resolved client itself depends on the bootstrap response.
    /// Each attempt reloads the credential while reusing the shared HTTP
    /// client and its connection pool.
    async fn with_bootstrap_auth_retry<T, E, F, Fut>(
        &self,
        call: F,
    ) -> CatalogResult<Result<T, ApiError<E>>>
    where
        F: Fn(ApiClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, ApiError<E>>>,
    {
        let client = self.bootstrap_client().await?;
        let result = call(client).await;
        if matches!(&result, Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED)) {
            let client = self.bootstrap_client().await?;
            return Ok(call(client).await);
        }
        Ok(result)
    }

    /// Run a single outbound REST request, retrying it once if the server
    /// answers `401 Unauthorized`. Each attempt builds an [`ApiClient`] from a
    /// freshly resolved credential, so a projected service account token that
    /// rotated mid-operation is picked up on the retry. The credential is
    /// re-read per request, so every request in a `drop_database` cascade sees
    /// the current token. The shared `reqwest::Client` and its connection pool
    /// are reused across attempts.
    async fn with_auth_retry<T, E, F, Fut>(&self, call: F) -> CatalogResult<Result<T, ApiError<E>>>
    where
        F: Fn(ApiClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, ApiError<E>>>,
    {
        let client = self.client().await?;
        let result = call(client).await;
        if matches!(&result, Err(e) if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED)) {
            let client = self.client().await?;
            return Ok(call(client).await);
        }
        Ok(result)
    }

    fn commit_state_is_unknown<E>(error: &ApiError<E>) -> bool {
        match error.status() {
            Some(status) => matches!(status.as_u16(), 500 | 502 | 503 | 504) || status.is_success(),
            // A malformed success response or a lost response may follow a successful commit.
            // Builder and connection failures happen before the request reaches the server.
            None => match error {
                ApiError::Request(error) => !error.is_builder() && !error.is_connect(),
                _ => false,
            },
        }
    }

    // Merge the local catalog config with the [`crate::r#gen::CatalogConfig`] fetched from the REST server.
    // This only happens once, then the result is cached.
    async fn resolved_catalog_config(&self) -> CatalogResult<&CatalogConfig<'static>> {
        self.resolved_catalog_config
            .get_or_try_init(|| async {
                let catalog_config = CatalogConfig {
                    properties: Cow::Borrowed(&self.options.properties),
                };
                let warehouse = catalog_config.warehouse();
                let config = self
                    .with_bootstrap_auth_retry(|client| {
                        let warehouse = warehouse.clone();
                        async move { client.get_config(warehouse).await }
                    })
                    .await?
                    .map(|response| response.inner)
                    .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;

                let mut properties = config
                    .defaults
                    .into_iter()
                    .filter(|(key, _)| key != REST_CATALOG_PROP_URI)
                    .collect::<HashMap<_, _>>();
                properties.extend(self.options.properties.clone());
                properties.extend(config.overrides);

                Ok::<_, CatalogError>(CatalogConfig {
                    properties: Cow::Owned(properties),
                })
            })
            .await
    }

    async fn load_table_result(
        &self,
        database: &Namespace,
        table: &str,
        access_delegation: Option<&str>,
    ) -> CatalogResult<crate::r#gen::LoadTableResult> {
        let catalog_config = self.resolved_catalog_config().await?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let table_name = table.to_string();
        let access_delegation = access_delegation.map(ToOwned::to_owned);
        self.with_auth_retry(|client| {
            let prefix = prefix.clone();
            let namespace = namespace.clone();
            let table_name = table_name.clone();
            let access_delegation = access_delegation.clone();
            async move {
                client
                    .load_table(prefix, namespace, table_name, access_delegation, None, None)
                    .await
            }
        })
        .await?
        .map(|response| response.inner)
        .map_err(|e| match e {
            e if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => CatalogError::NotFound(
                CatalogObject::Table,
                format!(
                    "{}.{}",
                    quote_namespace_if_needed(database),
                    quote_name_if_needed(table)
                ),
            ),
            _ => CatalogError::External(format!(
                "Failed to load table {}.{}: {e}",
                quote_namespace_if_needed(database),
                quote_name_if_needed(table)
            )),
        })
    }

    fn normalize_scan_planning_mode(value: &str) -> CatalogResult<String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "client" => Ok("client".to_string()),
            "server" => Ok("server".to_string()),
            other => Err(CatalogError::InvalidArgument(format!(
                "Invalid Iceberg REST {REST_TABLE_SCAN_PLANNING_MODE_KEY}: {other}"
            ))),
        }
    }

    fn effective_scan_planning_mode(
        table_config: Option<&HashMap<String, String>>,
        catalog_config: &CatalogConfig<'_>,
    ) -> CatalogResult<Option<String>> {
        table_config
            .and_then(|config| config.get(REST_TABLE_SCAN_PLANNING_MODE_KEY))
            .or_else(|| {
                catalog_config
                    .properties
                    .get(REST_TABLE_SCAN_PLANNING_MODE_KEY)
            })
            .map(|value| Self::normalize_scan_planning_mode(value))
            .transpose()
    }

    fn remote_signing_enabled(
        table_config: Option<&HashMap<String, String>>,
        catalog_config: &CatalogConfig<'_>,
    ) -> bool {
        table_config
            .and_then(|config| config.get(REST_REMOTE_SIGNING_ENABLED_KEY))
            .or_else(|| {
                catalog_config
                    .properties
                    .get(REST_REMOTE_SIGNING_ENABLED_KEY)
            })
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    fn hash_string_map<'a>(
        entries: impl IntoIterator<Item = (&'a String, &'a String)>,
        hasher: &mut impl Hasher,
    ) {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by_key(|(key, _)| *key);
        for (key, value) in entries {
            key.hash(hasher);
            value.hash(hasher);
        }
    }

    fn rest_table_session_fingerprint(
        catalog: &str,
        database: &Namespace,
        table: &str,
        scan_planning_mode: Option<&str>,
        remote_signing_enabled: bool,
        result: &crate::r#gen::LoadTableResult,
    ) -> String {
        let mut hasher = DefaultHasher::new();
        catalog.hash(&mut hasher);
        let namespace: Vec<String> = database.clone().into();
        namespace.hash(&mut hasher);
        table.hash(&mut hasher);
        result.metadata_location.hash(&mut hasher);
        result.metadata.table_uuid.hash(&mut hasher);
        scan_planning_mode.hash(&mut hasher);
        remote_signing_enabled.hash(&mut hasher);
        if let Some(config) = result.config.as_ref() {
            Self::hash_string_map(config.iter(), &mut hasher);
        }
        if let Some(credentials) = result.storage_credentials.as_ref() {
            let mut summaries = credentials
                .iter()
                .map(|credential| {
                    let mut credential_hasher = DefaultHasher::new();
                    credential.prefix.hash(&mut credential_hasher);
                    Self::hash_string_map(credential.config.iter(), &mut credential_hasher);
                    (credential.prefix.as_str(), credential_hasher.finish())
                })
                .collect::<Vec<_>>();
            summaries.sort_by_key(|(prefix, _)| *prefix);
            summaries.hash(&mut hasher);
        }
        format!("iceberg-rest:{:016x}", hasher.finish())
    }

    fn rest_table_session_ref(
        catalog: &str,
        database: &Namespace,
        table: &str,
        catalog_config: &CatalogConfig<'_>,
        result: &crate::r#gen::LoadTableResult,
    ) -> CatalogResult<IcebergRestTableSessionRef> {
        let config = result.config.as_ref();
        let credentials = result.storage_credentials.as_deref();
        let scan_planning_mode = Self::effective_scan_planning_mode(config, catalog_config)?;
        let remote_signing_enabled = Self::remote_signing_enabled(config, catalog_config);
        let fingerprint = Self::rest_table_session_fingerprint(
            catalog,
            database,
            table,
            scan_planning_mode.as_deref(),
            remote_signing_enabled,
            result,
        );
        Ok(IcebergRestTableSessionRef {
            fingerprint,
            scan_planning_mode,
            storage_credential_count: credentials
                .map(|credentials| credentials.len())
                .unwrap_or(0),
            remote_signing_enabled,
        })
    }

    fn validate_create_table_access_session_requirements(
        catalog: &str,
        database: &Namespace,
        table: &str,
        catalog_config: &CatalogConfig<'_>,
        result: &crate::r#gen::LoadTableResult,
    ) -> CatalogResult<()> {
        let scan_planning_mode =
            Self::effective_scan_planning_mode(result.config.as_ref(), catalog_config)?;
        if scan_planning_mode.as_deref() == Some("server") {
            return Err(CatalogError::UnsupportedCapability(
                "Iceberg REST access session requirements returned by create_table are not supported for create+write yet: server-side scan planning".to_string(),
            ));
        }

        let mut configured_storage_fallbacks = Vec::new();
        if Self::remote_signing_enabled(result.config.as_ref(), catalog_config) {
            configured_storage_fallbacks.push("remote signing");
        }
        if result
            .storage_credentials
            .as_ref()
            .is_some_and(|credentials| !credentials.is_empty())
        {
            configured_storage_fallbacks.push("vended credentials");
        }
        if !configured_storage_fallbacks.is_empty() {
            log::warn!(
                "Iceberg REST catalog {} create_table for {}.{} returned {}; using configured object-store credentials for create+write",
                catalog,
                quote_namespace_if_needed(database),
                quote_name_if_needed(table),
                configured_storage_fallbacks.join(", "),
            );
        }

        Ok(())
    }

    fn validate_create_table_write_schema(
        expected: &crate::r#gen::Schema,
        result: &crate::r#gen::LoadTableResult,
    ) -> CatalogResult<()> {
        let schema = find_by_id_or_last(
            result.metadata.schemas.as_ref(),
            result.metadata.current_schema_id,
            |schema| schema.schema_id,
        )
        .ok_or_else(|| CatalogError::External("Created Iceberg table has no schema".to_string()))?;
        let parse_schema =
            |schema: &crate::r#gen::Schema| -> CatalogResult<sail_iceberg::spec::Schema> {
                serde_json::to_value(schema)
                    .and_then(serde_json::from_value)
                    .map_err(|error| {
                        CatalogError::External(format!("Invalid Iceberg write schema: {error}"))
                    })
            };
        let actual = parse_schema(schema)?;
        let expected = parse_schema(expected)?;
        // CTAS writers are planned before the catalog creates the table.
        // Parse both REST schemas to normalize collection element names and type spellings.
        if actual.fields() != expected.fields() {
            return Err(CatalogError::UnsupportedCapability(
                "Iceberg REST catalog changed the schema or field IDs for create+write; create the table first and then insert into it".to_string(),
            ));
        }
        Ok(())
    }

    /// Converts an Iceberg REST API table load result into a catalog `TableStatus`.
    fn load_table_result_to_status(
        catalog: &str,
        database: &Namespace,
        table: &str,
        result: crate::r#gen::LoadTableResult,
    ) -> CatalogResult<TableStatus> {
        log::trace!(
            "Iceberg REST table load result: catalog={}, database={:?}, table={}, metadata.location={:?}, metadata-location={:?}",
            catalog,
            database,
            table,
            result.metadata.location,
            result.metadata_location,
        );
        // Table-specific config and storage credentials are access-session state, not
        // display table properties.
        // TODO: Preserve unused fields in `TableMetadata` when Sail exposes them.
        let crate::r#gen::TableMetadata {
            format_version,
            table_uuid,
            location,
            last_updated_ms,
            next_row_id,
            properties,
            schemas,
            current_schema_id,
            last_column_id,
            partition_specs,
            default_spec_id,
            last_partition_id,
            sort_orders,
            default_sort_order_id,
            encryption_keys: _,
            snapshots: _,
            refs: _,
            current_snapshot_id,
            last_sequence_number,
            snapshot_log: _,
            metadata_log: _,
            statistics,
            partition_statistics,
        } = *result.metadata;

        let current_schema =
            find_by_id_or_last(schemas.as_ref(), current_schema_id, |s| s.schema_id);
        let default_partition_spec =
            find_by_id_or_last(partition_specs.as_ref(), default_spec_id, |s| s.spec_id);

        let partition_field_ids: std::collections::HashSet<i32> = default_partition_spec
            .map(|spec| spec.fields.iter().map(|f| f.source_id).collect())
            .unwrap_or_default();

        let bucket_field_ids: std::collections::HashSet<i32> = default_partition_spec
            .map(|spec| {
                spec.fields
                    .iter()
                    .filter(|f| f.transform.0.trim().to_lowercase().starts_with("bucket"))
                    .map(|f| f.source_id)
                    .collect()
            })
            .unwrap_or_default();

        let partition_by = match (current_schema, default_partition_spec) {
            (Some(schema), Some(spec)) => spec
                .fields
                .iter()
                .map(|field| {
                    let source_column = schema
                        .fields
                        .iter()
                        .find(|f| f.id == field.source_id)
                        .ok_or_else(|| {
                            CatalogError::External(format!(
                                "Partition field source id {} not found in schema",
                                field.source_id
                            ))
                        })?
                        .name
                        .clone();
                    let transform = field.transform.0.parse().map_err(CatalogError::External)?;
                    catalog_partition_field_from_iceberg(source_column, transform)
                        .map_err(CatalogError::External)
                })
                .collect::<CatalogResult<Vec<_>>>()?,
            _ => Vec::new(),
        };

        let columns = if let Some(schema) = current_schema {
            let mut cols = Vec::new();
            for field in &schema.fields {
                let iceberg_type =
                    sail_iceberg::spec::types::Type::try_from(field.r#type.as_ref().clone())?;
                let data_type = iceberg_type_to_arrow(&iceberg_type).map_err(|e| {
                    CatalogError::External(format!(
                        "Failed to convert Iceberg type to Arrow type for field '{}': {e}",
                        field.name
                    ))
                })?;
                let field_id = field.id;
                cols.push(TableColumnStatus {
                    name: field.name.clone(),
                    data_type,
                    nullable: !field.required,
                    comment: field.doc.clone(),
                    default: None,
                    generated_always_as: None,
                    identity: None,
                    is_partition: partition_field_ids.contains(&field_id),
                    is_bucket: bucket_field_ids.contains(&field_id),
                    is_cluster: false,
                });
            }
            cols
        } else {
            Vec::new()
        };

        let default_sort_order =
            find_by_id_or_last(sort_orders.as_ref(), default_sort_order_id, |o| {
                Some(o.order_id)
            });

        let sort_by: Vec<CatalogTableSort> = default_sort_order
            .map(|order| {
                order
                    .fields
                    .iter()
                    .filter_map(|sort_field| {
                        let field_id = sort_field.source_id;
                        current_schema.and_then(|schema| {
                            schema
                                .fields
                                .iter()
                                .find(|f| f.id == field_id)
                                .map(|field| {
                                    let ascending = match *sort_field.direction {
                                        crate::r#gen::SortDirection::Asc => true,
                                        crate::r#gen::SortDirection::Desc => false,
                                    };
                                    CatalogTableSort {
                                        column: field.name.clone(),
                                        ascending,
                                    }
                                })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let constraints = current_schema
            .and_then(|schema| {
                schema.identifier_field_ids.as_ref().and_then(|ids| {
                    if ids.is_empty() {
                        None
                    } else {
                        let pk_columns: Vec<String> = ids
                            .iter()
                            .filter_map(|id| {
                                schema
                                    .fields
                                    .iter()
                                    .find(|f| f.id == *id)
                                    .map(|f| f.name.clone())
                            })
                            .collect();
                        if pk_columns.is_empty() {
                            None
                        } else {
                            Some(vec![CatalogTableConstraint::PrimaryKey {
                                name: None,
                                columns: pk_columns,
                            }])
                        }
                    }
                })
            })
            .unwrap_or_default();

        let mut properties: HashMap<String, String> = properties.unwrap_or_default();

        let comment = get_property(&properties, "comment");

        if let Some(metadata_location) = result.metadata_location {
            properties.insert(METADATA_LOCATION_KEY.to_string(), metadata_location);
        }
        properties.insert(
            "metadata.format-version".to_string(),
            format_version.to_string(),
        );
        properties.insert("metadata.table-uuid".to_string(), table_uuid);

        if let Some(v) = last_updated_ms {
            properties.insert("metadata.last-updated-ms".to_string(), v.to_string());
        }
        if let Some(v) = next_row_id {
            properties.insert("metadata.next-row-id".to_string(), v.to_string());
        }
        if let Some(v) = current_schema_id {
            properties.insert("metadata.current-schema-id".to_string(), v.to_string());
        }
        if let Some(v) = last_column_id {
            properties.insert("metadata.last-column-id".to_string(), v.to_string());
        }
        if let Some(v) = default_spec_id {
            properties.insert("metadata.default-spec-id".to_string(), v.to_string());
        }
        if let Some(v) = last_partition_id {
            properties.insert("metadata.last-partition-id".to_string(), v.to_string());
        }
        if let Some(v) = default_sort_order_id {
            properties.insert("metadata.default-sort-order-id".to_string(), v.to_string());
        }
        if let Some(v) = current_snapshot_id {
            properties.insert("metadata.current-snapshot-id".to_string(), v.to_string());
        }
        if let Some(v) = last_sequence_number {
            properties.insert("metadata.last-sequence-number".to_string(), v.to_string());
        }
        if let Some(v) = statistics {
            properties.insert(
                "metadata.statistics".to_string(),
                serde_json::to_string(&v).unwrap_or_default(),
            );
        }
        if let Some(v) = partition_statistics {
            properties.insert(
                "metadata.partition-statistics".to_string(),
                serde_json::to_string(&v).unwrap_or_default(),
            );
        }

        let properties: Vec<_> = properties.into_iter().collect();

        Ok(TableStatus {
            catalog: Some(catalog.to_string()),
            database: database.clone().into(),
            name: table.to_string(),
            kind: TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format: "iceberg".to_string(),
                partition_by,
                sort_by,
                bucket_by: None,
                properties,
                is_external: true,
            },
        })
    }

    async fn load_view_result(
        &self,
        database: &Namespace,
        view: &str,
    ) -> CatalogResult<crate::r#gen::LoadViewResult> {
        let catalog_config = self.resolved_catalog_config().await?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let view_name = view.to_string();
        self.with_auth_retry(|client| {
            let prefix = prefix.clone();
            let namespace = namespace.clone();
            let view_name = view_name.clone();
            async move { client.load_view(prefix, namespace, view_name).await }
        })
        .await?
        .map(|response| response.inner)
        .map_err(|e| match e.status().map(|status| status.as_u16()) {
            Some(405 | 501) => CatalogError::NotSupported("get view".to_string()),
            Some(404) => CatalogError::NotFound(
                CatalogObject::View,
                format!(
                    "{}.{}",
                    quote_namespace_if_needed(database),
                    quote_name_if_needed(view)
                ),
            ),
            Some(401) => CatalogError::Unauthorized(format!("Failed to load view: {e}")),
            Some(403) => CatalogError::Forbidden(format!("Failed to load view: {e}")),
            Some(429) => CatalogError::RateLimited(format!("Failed to load view: {e}")),
            _ => CatalogError::External(format!(
                "Failed to load view {}.{}: {e}",
                quote_namespace_if_needed(database),
                quote_name_if_needed(view)
            )),
        })
    }

    async fn replace_view(
        &self,
        database: &Namespace,
        view: &str,
        metadata: &crate::r#gen::ViewMetadata,
        request: crate::r#gen::CreateViewRequest,
    ) -> CatalogResult<TableStatus> {
        let request = replace_view_request(metadata, request)?;
        let catalog_config = self.resolved_catalog_config().await?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let view_name = view.to_string();
        let unknown = |error: String| {
            CatalogError::CommitStateUnknown(
                format!(
                    "Iceberg REST view replacement for {}.{} may have succeeded: {error}. Check the view state before retrying",
                    quote_namespace_if_needed(database),
                    quote_name_if_needed(view)
                )
                .into(),
            )
        };
        let response = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let view_name = view_name.clone();
                let request = request.clone();
                async move {
                    client
                        .replace_view(prefix, namespace, view_name, request)
                        .await
                }
            })
            .await?
            .map_err(|error| {
                if Self::commit_state_is_unknown(&error) {
                    return unknown(error.to_string());
                }
                let message = format!("Failed to replace view: {error}");
                match error.status().map(|status| status.as_u16()) {
                    Some(404) => CatalogError::NotFound(
                        CatalogObject::View,
                        format!(
                            "{}.{}",
                            quote_namespace_if_needed(database),
                            quote_name_if_needed(view)
                        ),
                    ),
                    Some(409) => CatalogError::Conflict(message),
                    Some(400) => CatalogError::CommitRejected(message),
                    Some(401) => CatalogError::Unauthorized(message),
                    Some(403) => CatalogError::Forbidden(message),
                    Some(405 | 501) => CatalogError::NotSupported(message),
                    Some(429) => CatalogError::RateLimited(message),
                    _ => CatalogError::External(message),
                }
            })?;
        // A successful commit can still return metadata that we cannot interpret.
        // Do not report that as a definite failure or retry the mutation.
        validate_view_metadata(&response.inner.metadata)
            .and_then(|()| {
                Self::load_view_result_to_status(&self.name, database, view, response.inner)
            })
            .map_err(|error| unknown(error.to_string()))
    }

    fn load_view_result_to_status(
        catalog: &str,
        database: &Namespace,
        view: &str,
        result: crate::r#gen::LoadViewResult,
    ) -> CatalogResult<TableStatus> {
        // TODO: Do we want to do anything with:
        //  - `result.config``
        //  - Unused fields in `ViewMetadata`?
        let crate::r#gen::ViewMetadata {
            view_uuid,
            format_version,
            location,
            current_version_id,
            versions,
            version_log: _,
            schemas,
            properties,
        } = *result.metadata;

        let current_version = versions.iter().find(|v| v.version_id == current_version_id);

        let current_schema = if let Some(version) = current_version {
            schemas
                .iter()
                .find(|s| s.schema_id == Some(version.schema_id))
        } else {
            schemas.last()
        };

        let columns = if let Some(schema) = current_schema {
            let mut cols = Vec::new();
            for field in &schema.fields {
                let iceberg_type =
                    sail_iceberg::spec::types::Type::try_from(field.r#type.as_ref().clone())?;
                let data_type = iceberg_type_to_arrow(&iceberg_type).map_err(|e| {
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
                    identity: None,
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
            .and_then(|v| {
                v.representations
                    .iter()
                    .find(|r| match r {
                        crate::r#gen::ViewRepresentation::SqlViewRepresentation(r) => {
                            r.dialect.trim().to_lowercase() == "spark"
                        }
                    })
                    .or_else(|| v.representations.last())
            })
            .map(|r| match r {
                crate::r#gen::ViewRepresentation::SqlViewRepresentation(r) => r.sql.clone(),
            })
            .unwrap_or_default();

        let mut properties: HashMap<String, String> = properties.unwrap_or_default();

        let comment = get_property(&properties, "comment");

        properties.insert(METADATA_LOCATION_KEY.to_string(), result.metadata_location);
        properties.insert("metadata.view-uuid".to_string(), view_uuid);
        properties.insert(
            "metadata.format-version".to_string(),
            format_version.to_string(),
        );
        properties.insert("metadata.location".to_string(), location);
        properties.insert(
            "metadata.current-version-id".to_string(),
            current_version_id.to_string(),
        );

        let properties: Vec<_> = properties.into_iter().collect();

        Ok(TableStatus {
            catalog: Some(catalog.to_string()),
            database: database.clone().into(),
            name: view.to_string(),
            kind: TableKind::View {
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

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        vec![
            LakehouseCapability::TableAccessSessions,
            LakehouseCapability::IcebergRestCommit,
        ]
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<sail_catalog::provider::CreateTableMetadataRequirement> {
        use sail_catalog::provider::{
            CreateTableMetadataRequirement, LakeSourceCreateMetadataMode,
        };
        if !options.format.eq_ignore_ascii_case("iceberg") {
            return Err(CatalogError::NotSupported(format!(
                "Iceberg REST catalog cannot create '{}' tables",
                options.format
            )));
        }
        if options.columns.is_empty()
            && options.location.is_some()
            && !options.is_write_precondition
        {
            Ok(CreateTableMetadataRequirement::LakeSource {
                mode: LakeSourceCreateMetadataMode::CatalogCoordinated,
            })
        } else {
            Ok(CreateTableMetadataRequirement::None)
        }
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let catalog_config = self.resolved_catalog_config().await?;

        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;

        let mut props: HashMap<String, String> = properties.into_iter().collect();
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }
        if let Some(l) = location {
            props.insert("location".to_string(), l);
        }

        let request = crate::r#gen::CreateNamespaceRequest {
            namespace: Box::new(database.clone().into()),
            properties: if props.is_empty() { None } else { Some(props) },
        };
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);

        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let request = request.clone();
                async move { client.create_namespace(prefix, request).await }
            })
            .await?
            .map(|response| response.inner);

        match result {
            Ok(result) => {
                let comment = result
                    .properties
                    .as_ref()
                    .and_then(|p| get_property(p, "comment"));
                let location = result
                    .properties
                    .as_ref()
                    .and_then(|p| get_property(p, "location"));
                let properties: Vec<_> =
                    result.properties.unwrap_or_default().into_iter().collect();

                Ok(DatabaseStatus {
                    catalog: self.name.clone(),
                    database: (*result.namespace).into(),
                    comment,
                    location,
                    properties,
                })
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::CONFLICT) && if_not_exists => {
                self.get_database(database).await
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to create namespace: {e}"
            ))),
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let catalog_config = self.resolved_catalog_config().await?;
        let namespace = catalog_config.namespace_string(database)?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);

        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                async move { client.load_namespace_metadata(prefix, namespace).await }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| match e {
                e if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => CatalogError::NotFound(
                    CatalogObject::Namespace,
                    quote_namespace_if_needed(database),
                ),
                _ => CatalogError::External(format!(
                    "Failed to load namespace {}: {e}",
                    quote_namespace_if_needed(database)
                )),
            })?;

        let comment = result
            .properties
            .as_ref()
            .and_then(|p| get_property(p, "comment"));
        let location = result
            .properties
            .as_ref()
            .and_then(|p| get_property(p, "location"));
        let properties: Vec<_> = result.properties.unwrap_or_default().into_iter().collect();

        Ok(DatabaseStatus {
            catalog: self.name.clone(),
            database: (*result.namespace).into(),
            comment,
            location,
            properties,
        })
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let catalog_config = self.resolved_catalog_config().await?;
        let parent = prefix
            .map(|namespace| catalog_config.namespace_string(namespace))
            .transpose()?;
        let request_prefix = catalog_config.prefix().map(ToOwned::to_owned);

        let result = self
            .with_auth_retry(|client| {
                let request_prefix = request_prefix.clone();
                let parent = parent.clone();
                async move {
                    client
                        .list_namespaces(request_prefix, None, None, parent)
                        .await
                }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| CatalogError::External(format!("Failed to list namespaces: {e}")))?;

        Ok(result
            .namespaces
            .unwrap_or_default()
            .into_iter()
            .map(|namespace| DatabaseStatus {
                catalog: self.get_name().to_string(),
                database: namespace.into(),
                comment: None,
                location: None,
                properties: Vec::new(),
            })
            .collect())
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let catalog_config = self.resolved_catalog_config().await?;

        let DropDatabaseOptions { if_exists, cascade } = options;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let ns_string = catalog_config.namespace_string(database)?;
        let drop_namespace = || async {
            match self
                .with_auth_retry(|client| {
                    let prefix = prefix.clone();
                    let ns_string = ns_string.clone();
                    async move { client.drop_namespace(prefix, ns_string).await }
                })
                .await?
            {
                Ok(_) => Ok(()),
                Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) && if_exists => Ok(()),
                Err(e) => Err(CatalogError::External(format!(
                    "Failed to drop namespace: {e}"
                ))),
            }
        };

        if cascade {
            // For CASCADE, first drop all tables and views in the namespace before dropping the namespace.
            // Each request re-reads the credential and retries once on a 401, so a token that rotates
            // partway through the cascade is recovered per request instead of leaving a partial drop.
            match self
                .with_auth_retry(|client| {
                    let prefix = prefix.clone();
                    let ns_string = ns_string.clone();
                    async move { client.list_tables(prefix, ns_string, None, None).await }
                })
                .await?
            {
                Ok(tables) => {
                    for identifier in tables.inner.identifiers.unwrap_or_default() {
                        match self
                            .with_auth_retry(|client| {
                                let prefix = prefix.clone();
                                let ns_string = ns_string.clone();
                                let name = identifier.name.clone();
                                async move {
                                    client.drop_table(prefix, ns_string, name, Some(true)).await
                                }
                            })
                            .await?
                        {
                            Ok(_) => {}
                            // The table was already removed (a concurrent drop), which is
                            // an acceptable outcome for a cascade, so keep going.
                            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {}
                            Err(e) => {
                                return Err(CatalogError::External(format!(
                                    "Failed to drop table '{}' while cascading namespace drop: {e}",
                                    identifier.name
                                )));
                            }
                        }
                    }
                }
                // The namespace itself is already gone. Skip the optional views endpoint and
                // fall through to drop_namespace, which applies canonical if_exists handling.
                Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                    return drop_namespace().await;
                }
                Err(e) => {
                    return Err(CatalogError::External(format!(
                        "Failed to list tables while cascading namespace drop: {e}"
                    )));
                }
            }
            match self
                .with_auth_retry(|client| {
                    let prefix = prefix.clone();
                    let ns_string = ns_string.clone();
                    async move { client.list_views(prefix, ns_string, None, None).await }
                })
                .await?
            {
                Ok(views) => {
                    for identifier in views.inner.identifiers.unwrap_or_default() {
                        match self
                            .with_auth_retry(|client| {
                                let prefix = prefix.clone();
                                let ns_string = ns_string.clone();
                                let name = identifier.name.clone();
                                async move { client.drop_view(prefix, ns_string, name).await }
                            })
                            .await?
                        {
                            Ok(_) => {}
                            // The view was already removed (a concurrent drop), which is
                            // an acceptable outcome for a cascade, so keep going.
                            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {}
                            Err(e) => {
                                return Err(CatalogError::External(format!(
                                    "Failed to drop view '{}' while cascading namespace drop: {e}",
                                    identifier.name
                                )));
                            }
                        }
                    }
                }
                // The namespace itself is already gone; fall through to drop_namespace,
                // which applies the canonical if_exists handling below.
                Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {}
                // The views endpoint is optional in the Iceberg REST spec, so a catalog
                // that does not implement it answers 405 or 501. There are then no views
                // to cascade, so tolerate it and continue to the namespace drop. The tables
                // endpoint is mandatory, so the list_tables arm above does not tolerate these
                // statuses and a 405 or 501 there is surfaced as a genuine failure.
                Err(e)
                    if matches!(
                        e.status(),
                        Some(reqwest::StatusCode::METHOD_NOT_ALLOWED)
                            | Some(reqwest::StatusCode::NOT_IMPLEMENTED)
                    ) => {}
                Err(e) => {
                    return Err(CatalogError::External(format!(
                        "Failed to list views while cascading namespace drop: {e}"
                    )));
                }
            }
        }

        drop_namespace().await
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
            constraints,
            location,
            format,
            partition_by,
            sort_by,
            bucket_by,
            mode,
            properties,
            is_external: _,
            is_write_precondition,
        } = options;

        if !format.eq_ignore_ascii_case("iceberg") {
            return Err(CatalogError::NotSupported(format!(
                "Iceberg REST catalog cannot create '{format}' tables"
            )));
        }

        let catalog_config = self.resolved_catalog_config().await?;

        if mode.ignore_if_exists()
            && let Ok(existing) = self.get_table(database, table).await
        {
            return Ok(existing);
        }

        if mode.is_replace() {
            return Err(CatalogError::NotSupported(
                "Replace table is not supported yet".to_string(),
            ));
        }

        if let Some(metadata_location) =
            sail_common_datafusion::catalog::managed::metadata_location_value(
                properties
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
        {
            let request = crate::r#gen::RegisterTableRequest {
                name: table.to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: Some(false),
            };
            let prefix = catalog_config.prefix().map(ToOwned::to_owned);
            let namespace = catalog_config.namespace_string(database)?;
            let result = self
                .with_auth_retry(|client| {
                    let prefix = prefix.clone();
                    let namespace = namespace.clone();
                    let request = request.clone();
                    async move { client.register_table(prefix, namespace, request).await }
                })
                .await?
                .map_err(|error| {
                    CatalogError::External(format!("Failed to register Iceberg table: {error}"))
                })?;
            return Self::load_table_result_to_status(&self.name, database, table, result.inner);
        }

        let format_version = requested_iceberg_format_version(&properties)?;
        let fields = columns_to_nested_fields(&columns, format_version)?;

        let struct_type = StructType::new(fields.clone());

        let (name_to_id, _id_to_name) =
            sail_iceberg::spec::SchemaBuilder::build_name_indexes(&struct_type);

        let identifier_field_ids = constraints
            .iter()
            .filter_map(|c| match c {
                CatalogTableConstraint::PrimaryKey { columns, .. } => Some(
                    columns
                        .iter()
                        .filter_map(|col_name| name_to_id.get(col_name).copied())
                        .collect::<Vec<_>>(),
                ),
                CatalogTableConstraint::Unique { .. } => None,
            })
            .flatten()
            .collect::<Vec<_>>();

        let schema = sail_iceberg::spec::Schema::builder()
            .with_fields(fields)
            .with_identifier_field_ids(identifier_field_ids.clone())
            .build()
            .map_err(|e| CatalogError::External(format!("Failed to build schema: {e}")))?;
        let partition_spec = build_partition_spec(&partition_by, bucket_by.as_ref(), &name_to_id)?;
        let write_order = build_sort_order(&sort_by, &name_to_id)?;

        let mut props = HashMap::new();
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }
        for (k, v) in properties {
            props.insert(k, v);
        }

        let request = crate::r#gen::CreateTableRequest {
            name: table.to_string(),
            location,
            schema: Box::new(crate::r#gen::Schema::try_from(schema)?),
            partition_spec,
            write_order,
            stage_create: Some(false),
            properties: if props.is_empty() { None } else { Some(props) },
        };

        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let request = request.clone();
                async move { client.create_table(prefix, namespace, None, request).await }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| CatalogError::External(format!("Failed to create table: {e}")))?;

        if is_write_precondition {
            Self::validate_create_table_write_schema(&request.schema, &result)?;
            Self::validate_create_table_access_session_requirements(
                &self.name,
                database,
                table,
                catalog_config,
                &result,
            )?;
        }
        Self::load_table_result_to_status(&self.name, database, table, result)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let result = self.load_table_result(database, table, None).await?;
        Self::load_table_result_to_status(&self.name, database, table, result)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let catalog_config = self.resolved_catalog_config().await?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;

        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                async move { client.list_tables(prefix, namespace, None, None).await }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?;

        Ok(result
            .identifiers
            .unwrap_or_default()
            .into_iter()
            .map(|identifier| TableStatus {
                catalog: Some(self.name.clone()),
                database: (*identifier.namespace).into(),
                name: identifier.name,
                kind: TableKind::Table {
                    columns: Vec::new(),
                    comment: None,
                    constraints: Vec::new(),
                    location: None,
                    format: "iceberg".to_string(),
                    partition_by: Vec::new(),
                    sort_by: Vec::new(),
                    bucket_by: None,
                    properties: Vec::new(),
                    is_external: true,
                },
            })
            .collect())
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let catalog_config = self.resolved_catalog_config().await?;
        let DropTableOptions { if_exists, purge } = options;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let table_name = table.to_string();
        match self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let table_name = table_name.clone();
                async move {
                    client
                        .drop_table(prefix, namespace, table_name, Some(purge))
                        .await
                }
            })
            .await?
        {
            Ok(_) => Ok(()),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) && if_exists => Ok(()),
            Err(e) => Err(CatalogError::External(format!("Failed to drop table: {e}"))),
        }
    }

    async fn rename_table(
        &self,
        database: &Namespace,
        table: &str,
        destination_database: &Namespace,
        destination_table: &str,
    ) -> CatalogResult<()> {
        let config = self.resolved_catalog_config().await?;
        let prefix = config.prefix().map(ToOwned::to_owned);
        let body = crate::r#gen::RenameTableRequest {
            source: Box::new(crate::r#gen::TableIdentifier {
                name: table.into(),
                namespace: Box::new(crate::r#gen::Namespace(database.clone().into())),
            }),
            destination: Box::new(crate::r#gen::TableIdentifier {
                name: destination_table.into(),
                namespace: Box::new(crate::r#gen::Namespace(destination_database.clone().into())),
            }),
        };
        self.with_auth_retry(|client| {
            let prefix = prefix.clone();
            let body = body.clone();
            async move { client.rename_table(prefix, body).await }
        })
        .await?
        .map(|_| ())
        .map_err(|e| CatalogError::External(format!("Failed to rename Iceberg table: {e}")))
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let result = self.load_table_result(database, table, None).await?;
        let metadata = serde_json::to_vec(result.metadata.as_ref())
            .map_err(|error| CatalogError::External(error.to_string()))?;
        let metadata = sail_iceberg::spec::TableMetadata::from_json(&metadata)
            .map_err(|error| CatalogError::External(error.to_string()))?;
        let (requirements, updates) =
            sail_iceberg::ddl::catalog_alter_updates(&metadata, &(&options).into())
                .map_err(|error| CatalogError::InvalidArgument(error.to_string()))?;
        if updates.is_empty() {
            return Ok(());
        }
        let status = Self::load_table_result_to_status(&self.name, database, table, result)?;
        let mut catalog_table = vec![self.name.clone()];
        catalog_table.extend(Vec::<String>::from(database.clone()));
        catalog_table.push(table.to_string());
        let context = sail_catalog::lakehouse::resolve_lakehouse_table_status(
            self.name.clone(),
            catalog_table,
            &status,
            sail_common_datafusion::catalog::LakehouseOperation::Alter,
            &self.lakehouse_capabilities(),
        )
        .execution;
        self.commit_lakehouse_table(
            database,
            table,
            LakehouseCommitRequest {
                context,
                format: "iceberg".to_string(),
                requirements: requirements
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<_, _>>()
                    .map_err(|error| CatalogError::External(error.to_string()))?,
                updates: updates
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<_, _>>()
                    .map_err(|error| CatalogError::External(error.to_string()))?,
                payload: None,
            },
        )
        .await
        .and_then(|outcome| match outcome {
            LakehouseCommitOutcome::Committed { .. } | LakehouseCommitOutcome::Noop { .. } => {
                Ok(())
            }
            LakehouseCommitOutcome::StateUnknown { message } => {
                Err(CatalogError::CommitStateUnknown(message.into()))
            }
            LakehouseCommitOutcome::RetryableConflict { message } => {
                Err(CatalogError::Conflict(message))
            }
            LakehouseCommitOutcome::Rejected { message } => {
                Err(CatalogError::CommitRejected(message))
            }
        })
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
        if !format.eq_ignore_ascii_case("iceberg") {
            return Err(CatalogError::NotSupported(format!(
                "Iceberg REST catalog cannot commit '{format}' tables",
            )));
        }

        let catalog_config = self.resolved_catalog_config().await?;
        let namespace = catalog_config.namespace_string(database)?;
        let requirements = requirements
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<crate::r#gen::TableRequirement>, _>>()
            .map_err(|e| {
                CatalogError::External(format!(
                    "Failed to parse Iceberg REST commit requirements: {e}"
                ))
            })?;
        let updates = updates
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<crate::r#gen::TableUpdate>, _>>()
            .map_err(|e| {
                CatalogError::External(format!("Failed to parse Iceberg REST commit updates: {e}"))
            })?;
        let request = crate::r#gen::CommitTableRequest {
            identifier: Some(Box::new(crate::r#gen::TableIdentifier {
                namespace: Box::new(database.clone().into()),
                name: table.to_string(),
            })),
            requirements,
            updates,
        };
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let table_name = table.to_string();
        let response = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let table_name = table_name.clone();
                let request = request.clone();
                async move {
                    client
                        .update_table(prefix, namespace, table_name, request)
                        .await
                }
            })
            .await?;
        if let Err(error) = &response
            && Self::commit_state_is_unknown(error)
        {
            return Ok(LakehouseCommitOutcome::StateUnknown {
                message: format!(
                    "Iceberg REST catalog commit for {}.{} may have succeeded: {error}. Check the table state before retrying",
                    quote_namespace_if_needed(database),
                    quote_name_if_needed(table)
                ),
            });
        }
        let response = response
            .map(|response| response.inner)
            .map_err(|e| match e {
                e if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => CatalogError::NotFound(
                    CatalogObject::Table,
                    format!(
                        "{}.{}",
                        quote_namespace_if_needed(database),
                        quote_name_if_needed(table)
                    ),
                ),
                e if e.status() == Some(reqwest::StatusCode::CONFLICT) => {
                    CatalogError::Conflict(format!(
                        "Iceberg REST catalog commit conflict for {}.{}: {e}",
                        quote_namespace_if_needed(database),
                        quote_name_if_needed(table)
                    ))
                }
                e if e.status() == Some(reqwest::StatusCode::UNAUTHORIZED) => {
                    CatalogError::Unauthorized(format!(
                        "Iceberg REST catalog commit unauthorized for {}.{}: {e}",
                        quote_namespace_if_needed(database),
                        quote_name_if_needed(table)
                    ))
                }
                e if e.status() == Some(reqwest::StatusCode::FORBIDDEN) => {
                    CatalogError::Forbidden(format!(
                        "Iceberg REST catalog commit forbidden for {}.{}: {e}",
                        quote_namespace_if_needed(database),
                        quote_name_if_needed(table)
                    ))
                }
                e if e.status() == Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => {
                    CatalogError::RateLimited(format!(
                        "Iceberg REST catalog commit rate limited for {}.{}: {e}",
                        quote_namespace_if_needed(database),
                        quote_name_if_needed(table)
                    ))
                }
                e if e.status().is_some() => CatalogError::External(format!(
                    "Failed to commit Iceberg table {}.{}: {e}",
                    quote_namespace_if_needed(database),
                    quote_name_if_needed(table)
                )),
                e => CatalogError::External(format!("Failed to commit table: {e}")),
            })?;
        let payload = match payload {
            Some(payload) => Some(payload),
            None => Some(serde_json::to_value(response).map_err(|e| {
                CatalogError::External(format!("Failed to serialize commit response: {e}"))
            })?),
        };
        Ok(LakehouseCommitOutcome::Committed { context, payload })
    }

    async fn begin_table_access(
        &self,
        database: &Namespace,
        table: &str,
        request: BeginTableAccessRequest,
    ) -> CatalogResult<TableAccessSession> {
        let BeginTableAccessRequest {
            mut context,
            purpose: _,
        } = request;
        let catalog_config = self.resolved_catalog_config().await?;
        let result = self
            .load_table_result(
                database,
                table,
                Some(REST_ACCESS_DELEGATION_VENDED_CREDENTIALS),
            )
            .await?;
        // TODO: Convert preserved REST table-session credentials into operation-scoped
        // FileIO/object-store access instead of only fingerprinting the session.
        let rest_session =
            Self::rest_table_session_ref(&self.name, database, table, catalog_config, &result)?;
        if rest_session.scan_planning_mode.as_deref() == Some("server") {
            context.scan = ScanAuthority::IcebergRestServerSide;
        } else {
            context.scan = ScanAuthority::ClientLakeSource;
        }
        let reference = TableAccessSessionRef {
            fingerprint: rest_session.fingerprint.clone(),
        };
        context.access_session = Some(reference.clone());
        context.rest_session = Some(rest_session.clone());
        context.capability_fingerprint = CapabilityFingerprint(format!(
            "{}:{}",
            context.capability_fingerprint.0, rest_session.fingerprint
        ));
        let credential_scope = (rest_session.storage_credential_count > 0).then(|| {
            format!(
                "iceberg-rest:{}.{}",
                quote_namespace_if_needed(database),
                table
            )
        });
        Ok(TableAccessSession {
            reference,
            context: context.clone(),
            expires_at_ms: None,
            credential_scope,
            capability_fingerprint: context.capability_fingerprint,
        })
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let catalog_config = self.resolved_catalog_config().await?;

        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties,
        } = options;

        let existing = if if_not_exists || replace {
            match self.load_view_result(database, view).await {
                Ok(existing) if if_not_exists => {
                    return Self::load_view_result_to_status(&self.name, database, view, existing);
                }
                Ok(existing) => Some(existing),
                Err(CatalogError::NotFound(CatalogObject::View, _)) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };

        let mut fields = Vec::new();
        for col in columns.iter() {
            let CreateViewColumnOptions {
                name,
                data_type,
                nullable,
                comment,
            } = col;
            let field_type = arrow_type_to_iceberg(data_type).map_err(|e| {
                CatalogError::External(format!(
                    "Failed to convert Arrow type to Iceberg type for column '{name}': {e}"
                ))
            })?;
            // Use a placeholder field id of 0; after all fields are collected,
            // `SchemaEvolver::assign_schema_field_ids` assigns unique IDs
            // including for nested struct/list/map children.
            let mut field = NestedField::new(0, name.clone(), field_type, !nullable);
            if let Some(comment) = comment {
                field = field.with_doc(comment);
            }
            fields.push(Arc::new(field));
        }

        let schema = sail_iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| CatalogError::External(format!("Failed to build schema: {e}")))?;
        let schema = sail_iceberg::SchemaEvolver::assign_schema_field_ids(&schema)
            .map_err(|e| CatalogError::External(format!("Failed to assign field ids: {e}")))?;
        let schema = crate::r#gen::Schema::try_from(schema)?;

        let sql_representation = crate::r#gen::SqlViewRepresentation {
            r#type: "sql".to_string(),
            sql: definition,
            dialect: "spark".to_string(),
        };

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let view_version = crate::r#gen::ViewVersion {
            version_id: 1,
            timestamp_ms,
            schema_id: -1,
            summary: HashMap::new(),
            representations: vec![crate::r#gen::ViewRepresentation::SqlViewRepresentation(
                Box::new(sql_representation),
            )],
            default_catalog: None,
            default_namespace: Box::new(database.clone().into()),
        };

        let mut props = HashMap::new();
        let mut path = None;
        let mut location = None;
        for (k, v) in properties {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                if k.eq_ignore_ascii_case("path") {
                    path = Some(trimmed.to_string());
                } else if k.eq_ignore_ascii_case("location") {
                    location = Some(trimmed.to_string());
                }
            }
            props.insert(k, v);
        }
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }

        if location.is_none()
            && let Some(path) = path
        {
            props.insert("location".to_string(), path.clone());
            location = Some(path);
        }
        let request = crate::r#gen::CreateViewRequest {
            name: view.to_string(),
            location,
            schema: Box::new(schema),
            view_version: Box::new(view_version),
            properties: props,
        };

        if let Some(existing) = existing {
            return self
                .replace_view(database, view, &existing.metadata, request)
                .await;
        }

        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let request = request.clone();
                async move { client.create_view(prefix, namespace, request).await }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| CatalogError::External(format!("Failed to create view: {e}")))?;

        Self::load_view_result_to_status(&self.name, database, view, result)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let result = self.load_view_result(database, view).await?;
        Self::load_view_result_to_status(&self.name, database, view, result)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let catalog_config = self.resolved_catalog_config().await?;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;

        let result = self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                async move { client.list_views(prefix, namespace, None, None).await }
            })
            .await?
            .map(|response| response.inner)
            .map_err(|e| match e {
                e if matches!(e.status(), Some(reqwest::StatusCode::NOT_FOUND)) => {
                    CatalogError::NotFound(
                        CatalogObject::Namespace,
                        quote_namespace_if_needed(database),
                    )
                }
                e if matches!(
                    e.status(),
                    Some(reqwest::StatusCode::METHOD_NOT_ALLOWED)
                        | Some(reqwest::StatusCode::NOT_IMPLEMENTED)
                ) =>
                {
                    CatalogError::NotSupported("list views".to_string())
                }
                _ => CatalogError::External(format!("Failed to list views: {e}")),
            })?;
        let catalog = &self.name;
        Ok(result
            .identifiers
            .unwrap_or_default()
            .into_iter()
            .map(|identifier| TableStatus {
                catalog: Some(catalog.clone()),
                database: (*identifier.namespace).into(),
                name: identifier.name,
                kind: TableKind::View {
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
        let catalog_config = self.resolved_catalog_config().await?;
        let DropViewOptions { if_exists } = options;
        let prefix = catalog_config.prefix().map(ToOwned::to_owned);
        let namespace = catalog_config.namespace_string(database)?;
        let view_name = view.to_string();
        match self
            .with_auth_retry(|client| {
                let prefix = prefix.clone();
                let namespace = namespace.clone();
                let view_name = view_name.clone();
                async move { client.drop_view(prefix, namespace, view_name).await }
            })
            .await?
        {
            Ok(_) => Ok(()),
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) && if_exists => Ok(()),
            Err(e) => Err(CatalogError::External(format!("Failed to drop view: {e}"))),
        }
    }
}

/// Finds an item by ID, falling back to the last item if not found or no ID is provided.
fn find_by_id_or_last<T, F>(items: Option<&Vec<T>>, id: Option<i32>, get_id: F) -> Option<&T>
where
    F: Fn(&T) -> Option<i32>,
{
    items.and_then(|items| {
        if let Some(id) = id {
            items
                .iter()
                .find(|item| get_id(item) == Some(id))
                .or_else(|| items.last())
        } else {
            items.last()
        }
    })
}

fn validate_view_metadata(metadata: &crate::r#gen::ViewMetadata) -> CatalogResult<()> {
    let version = metadata
        .versions
        .iter()
        .find(|version| version.version_id == metadata.current_version_id)
        .ok_or_else(|| CatalogError::External("View current version is missing".to_string()))?;
    if !metadata
        .schemas
        .iter()
        .any(|schema| schema.schema_id == Some(version.schema_id))
    {
        return Err(CatalogError::External(
            "View current schema is missing".to_string(),
        ));
    }
    if version.representations.is_empty() {
        return Err(CatalogError::External(
            "View SQL representation is missing".to_string(),
        ));
    }
    Ok(())
}

// Normalize only valid decimal type spellings, not arbitrary strings or a round-trip
// of the whole schema, which could discard metadata not understood by Sail's types.
fn normalize_view_type(data_type: &mut crate::r#gen::Type) {
    use crate::r#gen::Type;

    match data_type {
        Type::PrimitiveType(primitive) => {
            let spelling = primitive.0.to_ascii_lowercase();
            let Some((precision, scale)) = spelling
                .strip_prefix("decimal(")
                .and_then(|s| s.strip_suffix(')'))
                .and_then(|s| s.split_once(','))
            else {
                return;
            };
            let parse_parameter = |s: &str| -> Option<u32> {
                let s = s.trim_ascii();
                if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                s.parse().ok()
            };
            if let (Some(precision), Some(scale)) =
                (parse_parameter(precision), parse_parameter(scale))
                && (1..=38).contains(&precision)
                && scale <= precision
            {
                primitive.0 =
                    sail_iceberg::spec::types::PrimitiveType::Decimal { precision, scale }
                        .to_string();
            }
        }
        Type::StructType(struct_type) => {
            for field in &mut struct_type.fields {
                normalize_view_type(&mut field.r#type);
            }
        }
        Type::ListType(list) => normalize_view_type(&mut list.element),
        Type::MapType(map) => {
            normalize_view_type(&mut map.key);
            normalize_view_type(&mut map.value);
        }
    }
}

/// Iceberg schema identity includes field IDs, types, nullability, docs and identifiers,
/// but not the schema ID. Identifier IDs are a set; absent and empty are equivalent.
fn view_schema_key(schema: &crate::r#gen::Schema) -> CatalogResult<serde_json::Value> {
    let mut schema = schema.clone();
    schema.schema_id = None;
    let mut identifiers = schema.identifier_field_ids.take().unwrap_or_default();
    identifiers.sort_unstable();
    identifiers.dedup();
    schema.identifier_field_ids = Some(identifiers);
    for field in &mut schema.fields {
        normalize_view_type(&mut field.r#type);
    }
    serde_json::to_value(schema).map_err(|error| CatalogError::External(error.to_string()))
}

fn replace_view_request(
    metadata: &crate::r#gen::ViewMetadata,
    request: crate::r#gen::CreateViewRequest,
) -> CatalogResult<crate::r#gen::CommitViewRequest> {
    use crate::r#gen::{ViewRepresentation, ViewRequirement, ViewUpdate};

    validate_view_metadata(metadata)?;
    let crate::r#gen::CreateViewRequest {
        mut schema,
        mut view_version,
        location,
        properties,
        name: _,
    } = request;
    let current = metadata
        .versions
        .iter()
        .find(|version| version.version_id == metadata.current_version_id)
        .ok_or_else(|| CatalogError::External("View current version is missing".to_string()))?;
    let drop_dialect_allowed = properties
        .get("replace.drop-dialect.allowed")
        .or_else(|| {
            metadata
                .properties
                .as_ref()?
                .get("replace.drop-dialect.allowed")
        })
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    if !drop_dialect_allowed
        && current
            .representations
            .iter()
            .any(|representation| match representation {
                ViewRepresentation::SqlViewRepresentation(sql) => {
                    !sql.dialect.eq_ignore_ascii_case("spark")
                }
            })
    {
        return Err(CatalogError::InvalidArgument(
            "Cannot replace view due to loss of view dialects (replace.drop-dialect.allowed=false)"
                .to_string(),
        ));
    }

    let mut updates = Vec::new();
    let schema_key = view_schema_key(&schema)?;
    let mut schema_id = None;
    for existing in &metadata.schemas {
        if view_schema_key(existing)? == schema_key {
            schema_id = existing.schema_id;
            break;
        }
    }
    let new_schema = schema_id.is_none();
    let schema_id = match schema_id {
        Some(id) => id,
        None => metadata
            .schemas
            .iter()
            .filter_map(|schema| schema.schema_id)
            .max()
            .unwrap_or(-1)
            .checked_add(1)
            .ok_or_else(|| CatalogError::InvalidArgument("View schema ID overflow".to_string()))?,
    };
    if new_schema {
        schema.schema_id = Some(schema_id);
        updates.push(ViewUpdate::AddSchema {
            schema,
            last_column_id: None,
        });
    }

    // Match ViewMetadata.Builder's equivalence, including summaries but excluding
    // timestamps and IDs. Reusing a retained version must not use LAST_ADDED.
    let representations = serde_json::to_value(&view_version.representations)
        .map_err(|error| CatalogError::External(error.to_string()))?;
    let mut version_id = None;
    for existing in &metadata.versions {
        if existing.schema_id == schema_id
            && existing.summary == view_version.summary
            && existing.default_catalog == view_version.default_catalog
            && existing.default_namespace.0 == view_version.default_namespace.0
            && serde_json::to_value(&existing.representations)
                .map_err(|error| CatalogError::External(error.to_string()))?
                == representations
        {
            version_id = Some(existing.version_id);
            break;
        }
    }
    let version_id = match version_id {
        Some(id) => id,
        None => {
            view_version.version_id = metadata
                .versions
                .iter()
                .map(|version| version.version_id)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    CatalogError::InvalidArgument("View version ID overflow".to_string())
                })?;
            view_version.schema_id = if new_schema { -1 } else { schema_id };
            updates.push(ViewUpdate::AddViewVersion { view_version });
            -1
        }
    };
    updates.push(ViewUpdate::SetCurrentViewVersion {
        view_version_id: version_id,
    });

    // This reserved Spark property stores pre-alias query names, which are not
    // available here. Spark's Iceberg resolver projects by ordinal when it is absent.
    // Do not retain stale query names or replace them with potentially different aliases.
    const QUERY_COLUMN_NAMES: &str = "spark.query-column-names";
    if metadata
        .properties
        .as_ref()
        .is_some_and(|props| props.contains_key(QUERY_COLUMN_NAMES))
        && !properties.contains_key(QUERY_COLUMN_NAMES)
    {
        updates.push(ViewUpdate::RemoveProperties {
            removals: vec![QUERY_COLUMN_NAMES.to_string()],
        });
    }
    if !properties.is_empty() {
        updates.push(ViewUpdate::SetProperties {
            updates: properties,
        });
    }
    if let Some(location) = location {
        updates.push(ViewUpdate::SetLocation { location });
    }
    Ok(crate::r#gen::CommitViewRequest {
        identifier: None,
        requirements: Some(vec![ViewRequirement::AssertViewUuid {
            uuid: metadata.view_uuid.clone(),
        }]),
        updates,
    })
}

fn requested_iceberg_format_version(
    properties: &[(String, String)],
) -> CatalogResult<FormatVersion> {
    let Some((_, value)) = properties.iter().find(|(key, _)| {
        matches!(
            key.trim().to_ascii_lowercase().as_str(),
            "format-version"
                | "format_version"
                | "formatversion"
                | "metadata.format-version"
                | "metadata.format_version"
                | "metadata.formatversion"
        )
    }) else {
        return Ok(FormatVersion::default());
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "v1" => Ok(FormatVersion::V1),
        "2" | "v2" => Ok(FormatVersion::V2),
        "3" | "v3" => Ok(FormatVersion::V3),
        value => Err(CatalogError::InvalidArgument(format!(
            "unsupported Iceberg format-version: {value}"
        ))),
    }
}

/// Converts table column options to Iceberg nested fields.
///
/// Ensures all fields (top-level and nested) receive sequential, unique field
/// IDs starting at 1 via `SchemaEvolver::assign_schema_field_ids`. This is
/// required because Iceberg uses field IDs for schema indexing and column
/// identity, and nested children produced by `arrow_type_to_iceberg` would
/// otherwise default to id 0 since the source `CreateTableColumnOptions` carry
/// no Iceberg field-id metadata.
fn columns_to_nested_fields(
    columns: &[CreateTableColumnOptions],
    format_version: FormatVersion,
) -> CatalogResult<Vec<Arc<NestedField>>> {
    let mut fields = Vec::new();
    for col in columns.iter() {
        let CreateTableColumnOptions {
            name,
            data_type,
            nullable,
            comment,
            default,
            generated_always_as: _,
            identity: _,
        } = col;

        let field_type = arrow_type_to_iceberg(data_type).map_err(|e| {
            CatalogError::External(format!(
                "Failed to convert Arrow type to Iceberg type for column '{name}': {e}"
            ))
        })?;

        // `default` is not supported until Iceberg V3.
        let default_literal = if let Some(default) = default {
            if format_version >= FormatVersion::V3 {
                Some(Literal::try_from_str(default, &field_type)
                    .map_err(|e| {
                        CatalogError::InvalidArgument(format!(
                            "Failed to convert default value to Iceberg literal for column '{name}': {e}"
                        ))
                    })?
                    .ok_or_else(|| {
                        CatalogError::InvalidArgument(format!(
                            "column '{name}' has NULL/null default; null defaults are not supported here"
                        ))
                    })?)
            } else {
                None
            }
        } else {
            None
        };

        // Use a placeholder field id of 0; after all fields are collected,
        // `SchemaEvolver::assign_schema_field_ids` assigns unique IDs including
        // for nested struct/list/map children.
        let mut field = NestedField::new(0, name.clone(), field_type, !nullable);
        if let Some(comment) = comment {
            field = field.with_doc(comment);
        }

        if format_version >= FormatVersion::V3
            && let Some(default_literal) = default_literal
        {
            field = field
                .with_initial_default(default_literal.clone())
                .with_write_default(default_literal);
        }

        fields.push(Arc::new(field));
    }

    let temp_schema = sail_iceberg::spec::Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| CatalogError::External(format!("Failed to build schema: {e}")))?;
    let assigned = sail_iceberg::SchemaEvolver::assign_schema_field_ids(&temp_schema)
        .map_err(|e| CatalogError::External(format!("Failed to assign field ids: {e}")))?;
    Ok(assigned.fields().to_vec())
}

/// Builds an Iceberg partition spec from partition fields and their field ID mappings.
fn build_partition_spec(
    partition_by: &[CatalogPartitionField],
    bucket_by: Option<&CatalogTableBucketBy>,
    name_to_id: &HashMap<String, i32>,
) -> CatalogResult<Option<Box<crate::r#gen::PartitionSpec>>> {
    if partition_by.is_empty() && bucket_by.is_none() {
        return Ok(None);
    }
    let mut partition_spec_builder = sail_iceberg::PartitionSpec::builder();
    for field in partition_by {
        if let Some(&source_id) = name_to_id.get(&field.column) {
            let (transform, name) = match &field.transform {
                None | Some(PartitionTransform::Identity) => {
                    (sail_iceberg::Transform::Identity, field.column.clone())
                }
                Some(PartitionTransform::Year) => (
                    sail_iceberg::Transform::Year,
                    format!("{}_year", field.column),
                ),
                Some(PartitionTransform::Month) => (
                    sail_iceberg::Transform::Month,
                    format!("{}_month", field.column),
                ),
                Some(PartitionTransform::Day) => (
                    sail_iceberg::Transform::Day,
                    format!("{}_day", field.column),
                ),
                Some(PartitionTransform::Hour) => (
                    sail_iceberg::Transform::Hour,
                    format!("{}_hour", field.column),
                ),
                Some(PartitionTransform::Bucket(n)) => (
                    sail_iceberg::Transform::Bucket(*n),
                    format!("{}_bucket", field.column),
                ),
                Some(PartitionTransform::Truncate(w)) => (
                    sail_iceberg::Transform::Truncate(*w),
                    format!("{}_trunc", field.column),
                ),
            };
            partition_spec_builder = partition_spec_builder.add_field(source_id, &name, transform);
        }
    }
    if let Some(bucket_by) = bucket_by {
        let num_buckets = u32::try_from(bucket_by.num_buckets).map_err(|e| {
            CatalogError::InvalidArgument(format!("Invalid number of buckets: {e}"))
        })?;
        if num_buckets == 0 {
            return Err(CatalogError::InvalidArgument(
                "number of buckets must be a positive integer".to_string(),
            ));
        }
        for column in &bucket_by.columns {
            if let Some(&source_id) = name_to_id.get(column) {
                partition_spec_builder = partition_spec_builder.add_field(
                    source_id,
                    format!("{column}_bucket"),
                    sail_iceberg::Transform::Bucket(num_buckets),
                );
            }
        }
    }
    let spec = partition_spec_builder.build();
    Ok(Some(Box::new(crate::r#gen::PartitionSpec {
        spec_id: Some(spec.spec_id()),
        fields: spec
            .fields()
            .iter()
            .map(|f| crate::r#gen::PartitionField {
                field_id: Some(f.field_id),
                source_id: f.source_id,
                name: f.name.to_string(),
                transform: Box::new(crate::r#gen::Transform(f.transform.to_string())),
            })
            .collect(),
    })))
}

fn build_sort_order(
    sort_by: &[CatalogTableSort],
    name_to_id: &HashMap<String, i32>,
) -> CatalogResult<Option<Box<crate::r#gen::SortOrder>>> {
    if sort_by.is_empty() {
        return Ok(None);
    }

    let mut sort_fields = Vec::new();
    for sort in sort_by {
        let (column, transform) = parse_sort_column(&sort.column)?;
        if let Some(&source_id) = name_to_id.get(&column) {
            sort_fields.push(sail_iceberg::spec::sort::SortField {
                source_id,
                source_ids: vec![],
                transform,
                direction: if sort.ascending {
                    sail_iceberg::spec::sort::SortDirection::Ascending
                } else {
                    sail_iceberg::spec::sort::SortDirection::Descending
                },
                null_order: sail_iceberg::spec::sort::NullOrder::Last, // TODO: Use specified null order when supported by `resolve_catalog_table_sort` in @crates/sail-plan/src/resolver/command/catalog/table.rs
            });
        }
    }

    if sort_fields.is_empty() {
        return Ok(None);
    }

    let order = sail_iceberg::spec::sort::SortOrder {
        order_id: 1,
        fields: sort_fields,
    };

    Ok(Some(Box::new(crate::r#gen::SortOrder {
        order_id: i32::try_from(order.order_id).map_err(|e| {
            CatalogError::External(format!("Failed to convert sort order ID to i32: {e}"))
        })?,
        fields: order
            .fields
            .iter()
            .map(|f| crate::r#gen::SortField {
                source_id: f.source_id,
                transform: Box::new(crate::r#gen::Transform(f.transform.to_string())),
                direction: if f.direction == sail_iceberg::spec::sort::SortDirection::Ascending {
                    Box::new(crate::r#gen::SortDirection::Asc)
                } else {
                    Box::new(crate::r#gen::SortDirection::Desc)
                },
                null_order: if f.null_order == sail_iceberg::spec::sort::NullOrder::First {
                    Box::new(crate::r#gen::NullOrder::NullsFirst)
                } else {
                    Box::new(crate::r#gen::NullOrder::NullsLast)
                },
            })
            .collect(),
    })))
}

/// Parses a catalog sort column into the Iceberg source column and transform.
///
/// The input is either a plain column name or a transform expression such as
/// `years(ts)`, `bucket(16, id)`, or `truncate(4, category)`.
fn parse_sort_column(column: &str) -> CatalogResult<(String, sail_iceberg::Transform)> {
    let column = column.trim();
    let Some((function, arguments)) = parse_transform_function(column) else {
        return Ok((column.to_string(), sail_iceberg::Transform::Identity));
    };

    let arguments = arguments.split(',').map(str::trim).collect::<Vec<_>>();

    match function.to_ascii_lowercase().as_str() {
        "year" | "years" => {
            parse_unary_sort_transform(function, arguments, sail_iceberg::Transform::Year)
        }
        "month" | "months" => {
            parse_unary_sort_transform(function, arguments, sail_iceberg::Transform::Month)
        }
        "day" | "days" => {
            parse_unary_sort_transform(function, arguments, sail_iceberg::Transform::Day)
        }
        "hour" | "hours" => {
            parse_unary_sort_transform(function, arguments, sail_iceberg::Transform::Hour)
        }
        "bucket" => {
            let [num_buckets_str, column] = arguments.as_slice() else {
                return Err(CatalogError::InvalidArgument(
                    "bucket sort transform expects bucket count and column".to_string(),
                ));
            };
            let num_buckets = num_buckets_str.parse::<u32>().map_err(|_| {
                CatalogError::InvalidArgument(format!(
                    "bucket count for sort transform must be a valid unsigned integer: {num_buckets_str}"
                ))
            })?;
            if num_buckets == 0 {
                return Err(CatalogError::InvalidArgument(
                    "bucket count for sort transform must be a positive integer".to_string(),
                ));
            }
            Ok((
                column.to_string(),
                sail_iceberg::Transform::Bucket(num_buckets),
            ))
        }
        "truncate" => {
            let [first, second] = arguments.as_slice() else {
                return Err(CatalogError::InvalidArgument(
                    "truncate sort transform expects width and column".to_string(),
                ));
            };
            let parse_positive_u32 =
                |s: &str| -> Option<u32> { s.parse::<u32>().ok().filter(|&w| w > 0) };
            if let Some(width) = parse_positive_u32(first) {
                Ok((second.to_string(), sail_iceberg::Transform::Truncate(width)))
            } else if let Some(width) = parse_positive_u32(second) {
                Ok((first.to_string(), sail_iceberg::Transform::Truncate(width)))
            } else {
                Err(CatalogError::InvalidArgument(format!(
                    "truncate sort transform requires one argument to be a positive integer width, got: {first}, {second}"
                )))
            }
        }
        _ => Err(CatalogError::InvalidArgument(format!(
            "Unsupported sort transform function: {function}"
        ))),
    }
}

fn parse_transform_function(column: &str) -> Option<(&str, &str)> {
    let column = column.strip_suffix(')')?;
    let (function, arguments) = column.split_once('(')?;
    Some((function.trim(), arguments))
}

fn parse_unary_sort_transform(
    function: &str,
    arguments: Vec<&str>,
    transform: sail_iceberg::Transform,
) -> CatalogResult<(String, sail_iceberg::Transform)> {
    let [column] = arguments.as_slice() else {
        return Err(CatalogError::InvalidArgument(format!(
            "{function} sort transform expects a single column"
        )));
    };
    Ok((column.to_string(), transform))
}

#[expect(clippy::unwrap_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, Field};
    use sail_catalog::credentials::{EmptyCatalogCredentials, FileCatalogCredentials};
    use sail_catalog::lakehouse::TableAccessPurpose;
    use sail_common::spec;
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, CommitAuthority, LakehouseAuthority,
        LakehouseExecutionContext, LakehouseFormat, LakehouseOperation, MetadataPointerAuthority,
        TableLifecycle,
    };
    use tempfile::TempDir;
    use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::*;

    struct TestContext {
        name: String,
        server: MockServer,
        catalog: IcebergRestCatalogProvider,
    }

    impl TestContext {
        async fn new(name: Option<&str>) -> Self {
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

            let name_str = name.unwrap_or("");
            let props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), server.uri())]);
            let catalog =
                IcebergRestCatalogProvider::new(name_str.to_string(), test_options(props));

            Self {
                name: name_str.to_string(),
                server,
                catalog,
            }
        }

        fn path(&self, suffix: &str) -> String {
            format!("/v1{suffix}")
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

        async fn commit_table(&self) -> CatalogResult<LakehouseCommitOutcome> {
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let context = LakehouseExecutionContext::catalog_table_context(
                CatalogProviderId(self.name.clone()),
                vec![self.name.clone(), "db1".to_string(), "table1".to_string()],
                CatalogTableIdentity {
                    table_id: None,
                    table_uri: Some("s3://bucket/table".to_string()),
                },
                LakehouseOperation::Write,
                LakehouseFormat::Iceberg,
                LakehouseAuthority::CatalogAuthoritative {
                    lifecycle: TableLifecycle::Managed,
                    pointer: MetadataPointerAuthority::IcebergRest,
                    commit: CommitAuthority::IcebergRestCommit,
                },
                ScanAuthority::ClientLakeSource,
            );
            self.catalog
                .commit_lakehouse_table(
                    &namespace,
                    "table1",
                    LakehouseCommitRequest {
                        context,
                        format: "iceberg".to_string(),
                        requirements: vec![],
                        updates: vec![],
                        payload: None,
                    },
                )
                .await
        }
    }

    fn test_options(properties: HashMap<String, String>) -> IcebergRestCatalogOptions {
        IcebergRestCatalogOptions {
            credentials: Arc::new(EmptyCatalogCredentials),
            properties,
        }
    }

    fn simple_create_table_options() -> CreateTableOptions {
        CreateTableOptions {
            columns: vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            }],
            comment: None,
            constraints: vec![],
            location: None,
            format: "iceberg".to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode: spec::CreateTableMode::Create,
            properties: vec![],
            is_external: false,
            is_write_precondition: true,
        }
    }

    fn create_table_response_with_access_session_hints() -> serde_json::Value {
        serde_json::json!({
            "metadata-location": "s3://bucket/table/metadata/v1.metadata.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": "12345678-1234-1234-1234-123456789012",
                "location": "s3://bucket/table",
                "current-schema-id": 0,
                "schemas": [
                    {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            {
                                "id": 1,
                                "name": "id",
                                "required": true,
                                "type": "long"
                            }
                        ]
                    }
                ]
            },
            "config": {
                "s3.remote-signing-enabled": "true"
            },
            "storage-credentials": [
                {
                    "prefix": "s3://bucket/table",
                    "config": {
                        "s3.access-key-id": "AKIA-SECRET",
                        "s3.secret-access-key": "storage-secret"
                    }
                }
            ]
        })
    }

    fn create_table_response_with_server_side_scan_planning() -> serde_json::Value {
        let mut result = create_table_response_with_access_session_hints();
        result["config"]["scan-planning-mode"] = serde_json::json!("server");
        result
    }

    async fn load_merged_test_config(
        defaults: HashMap<String, String>,
        mut client_props: HashMap<String, String>,
        overrides: HashMap<String, String>,
        expected_config_warehouse: Option<&str>,
    ) -> CatalogConfig<'static> {
        let server = MockServer::start().await;
        let response = ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "defaults": defaults,
            "overrides": overrides
        }));

        if let Some(warehouse) = expected_config_warehouse {
            Mock::given(method("GET"))
                .and(path("/v1/config"))
                .and(query_param("warehouse", warehouse))
                .respond_with(response)
                .expect(1)
                .mount(&server)
                .await;
        } else {
            Mock::given(method("GET"))
                .and(path("/v1/config"))
                .and(query_param_is_missing("warehouse"))
                .respond_with(response)
                .expect(1)
                .mount(&server)
                .await;
        }

        client_props
            .entry(REST_CATALOG_PROP_URI.to_string())
            .or_insert_with(|| server.uri());

        let catalog =
            IcebergRestCatalogProvider::new("test".to_string(), test_options(client_props));

        let config = catalog.resolved_catalog_config().await.unwrap();
        config.clone()
    }

    #[tokio::test]
    async fn test_catalog_config_merge_precedence_for_properties() {
        let key = "rest-page-size".to_string();

        let config = load_merged_test_config(
            HashMap::from([(key.clone(), "defaults".to_string())]),
            HashMap::new(),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(
            config.properties.get(&key).map(String::as_str),
            Some("defaults")
        );

        let config = load_merged_test_config(
            HashMap::from([(key.clone(), "defaults".to_string())]),
            HashMap::from([(key.clone(), "client".to_string())]),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(
            config.properties.get(&key).map(String::as_str),
            Some("client")
        );

        let config = load_merged_test_config(
            HashMap::from([(key.clone(), "defaults".to_string())]),
            HashMap::from([(key.clone(), "client".to_string())]),
            HashMap::from([(key.clone(), "overrides".to_string())]),
            None,
        )
        .await;
        assert_eq!(
            config.properties.get(&key).map(String::as_str),
            Some("overrides")
        );
    }

    #[tokio::test]
    async fn test_uri_merge_matrix() {
        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_string(),
                "http://default.example".to_string(),
            )]),
            HashMap::new(),
            HashMap::new(),
            None,
        )
        .await;
        assert_ne!(config.uri().as_deref(), Some("http://default.example"));

        let config = load_merged_test_config(
            HashMap::new(),
            HashMap::new(),
            HashMap::from([(
                REST_CATALOG_PROP_URI.to_string(),
                "http://server.example".to_string(),
            )]),
            None,
        )
        .await;
        assert_eq!(config.uri().as_deref(), Some("http://server.example"));
    }

    #[tokio::test]
    async fn test_warehouse_merge_matrix() {
        let config =
            load_merged_test_config(HashMap::new(), HashMap::new(), HashMap::new(), None).await;
        assert_eq!(config.warehouse(), None);

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://default/warehouse".to_string(),
            )]),
            HashMap::new(),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(
            config.warehouse().as_deref(),
            Some("s3://default/warehouse")
        );

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://default/warehouse".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://client/warehouse".to_string(),
            )]),
            HashMap::new(),
            Some("s3://client/warehouse"),
        )
        .await;
        assert_eq!(config.warehouse().as_deref(), Some("s3://client/warehouse"));

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://default/warehouse".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://client/warehouse".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                "s3://server/warehouse".to_string(),
            )]),
            Some("s3://client/warehouse"),
        )
        .await;
        assert_eq!(config.warehouse().as_deref(), Some("s3://server/warehouse"));
    }

    #[tokio::test]
    async fn test_prefix_merge_matrix() {
        let config =
            load_merged_test_config(HashMap::new(), HashMap::new(), HashMap::new(), None).await;
        assert_eq!(config.prefix(), None);

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "default_prefix".to_string(),
            )]),
            HashMap::new(),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(config.prefix(), Some("default_prefix"));

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "default_prefix".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "client_prefix".to_string(),
            )]),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(config.prefix(), Some("client_prefix"));

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "default_prefix".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "client_prefix".to_string(),
            )]),
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "server_prefix".to_string(),
            )]),
            None,
        )
        .await;
        assert_eq!(config.prefix(), Some("server_prefix"));

        let config = load_merged_test_config(
            HashMap::new(),
            HashMap::from([(
                REST_CATALOG_PROP_PREFIX.to_string(),
                "client_prefix".to_string(),
            )]),
            HashMap::from([(REST_CATALOG_PROP_PREFIX.to_string(), " ".to_string())]),
            None,
        )
        .await;
        assert_eq!(config.prefix(), None);
    }

    #[tokio::test]
    async fn test_namespace_separator_merge_matrix() {
        let namespace =
            Namespace::try_from(vec!["accounting".to_string(), "tax".to_string()]).unwrap();

        let config =
            load_merged_test_config(HashMap::new(), HashMap::new(), HashMap::new(), None).await;
        assert_eq!(
            config.namespace_string(&namespace).unwrap(),
            "accounting\x1Ftax"
        );

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "/".to_string(),
            )]),
            HashMap::new(),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(
            config.namespace_string(&namespace).unwrap(),
            "accounting/tax"
        );

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "/".to_string(),
            )]),
            HashMap::from([]),
            HashMap::new(),
            None,
        )
        .await;
        assert_eq!(
            config.namespace_string(&namespace).unwrap(),
            "accounting/tax"
        );

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "/".to_string(),
            )]),
            HashMap::from([]),
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "%7C".to_string(),
            )]),
            None,
        )
        .await;
        assert_eq!(
            config.namespace_string(&namespace).unwrap(),
            "accounting|tax"
        );

        let config = load_merged_test_config(
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "/".to_string(),
            )]),
            HashMap::from([]),
            HashMap::from([(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                " ".to_string(),
            )]),
            None,
        )
        .await;
        assert_eq!(
            config.namespace_string(&namespace).unwrap(),
            "accounting\x1Ftax"
        );
    }

    async fn test_list_databases_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;

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
    async fn test_list_databases() {
        test_list_databases_impl(None).await;
        test_list_databases_impl(Some("test")).await;
    }

    async fn test_list_databases_parent_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
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
    async fn test_list_databases_parent() {
        test_list_databases_parent_impl(None).await;
        test_list_databases_parent_impl(Some("test")).await;
    }

    async fn test_list_tables_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;

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

    async fn test_list_views_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;

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

    #[tokio::test]
    async fn test_list_views_unsupported_endpoint() {
        let ctx = TestContext::new(None).await;

        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/views")))
            .respond_with(ResponseTemplate::new(405))
            .mount(&ctx.server)
            .await;

        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();
        let error = ctx.catalog.list_views(&namespace).await.unwrap_err();

        assert!(matches!(error, CatalogError::NotSupported(_)));
    }

    async fn test_drop_database_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;

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

    async fn test_drop_database_cascade_propagates_table_drop_failure_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    }
                ]
            }),
        )
        .await;

        // The per-object table drop hits a real server error, so the cascade must abort.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1/tables/table1").as_str()))
            .respond_with(ResponseTemplate::new(500))
            .mount(&ctx.server)
            .await;

        // The namespace drop must never be attempted once a table drop fails.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_propagates_table_drop_failure() {
        test_drop_database_cascade_propagates_table_drop_failure_impl(None).await;
        test_drop_database_cascade_propagates_table_drop_failure_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_tolerates_missing_table_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "table1"
                    }
                ]
            }),
        )
        .await;

        // A concurrent removal leaves the table already gone; the cascade tolerates that.
        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1/tables/table1"),
            "NoSuchTableException",
            "The given table does not exist",
        )
        .await;

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/views"),
            serde_json::json!({ "identifiers": [] }),
        )
        .await;

        // The cascade proceeds and drops the namespace itself.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_tolerates_missing_table() {
        test_drop_database_cascade_tolerates_missing_table_impl(None).await;
        test_drop_database_cascade_tolerates_missing_table_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_propagates_list_tables_failure_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        // Listing the tables fails outright, which the cascade must surface.
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/tables").as_str()))
            .respond_with(ResponseTemplate::new(500))
            .mount(&ctx.server)
            .await;

        // The namespace drop must never be attempted once the listing fails.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_propagates_list_tables_failure() {
        test_drop_database_cascade_propagates_list_tables_failure_impl(None).await;
        test_drop_database_cascade_propagates_list_tables_failure_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_tolerates_missing_views_endpoint_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({ "identifiers": [] }),
        )
        .await;

        // A catalog without a views endpoint answers 405, which must not abort the cascade.
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/views").as_str()))
            .respond_with(ResponseTemplate::new(405))
            .mount(&ctx.server)
            .await;

        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_tolerates_missing_views_endpoint() {
        test_drop_database_cascade_tolerates_missing_views_endpoint_impl(None).await;
        test_drop_database_cascade_tolerates_missing_views_endpoint_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_tolerates_unimplemented_views_endpoint_impl(
        name: Option<&str>,
    ) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({ "identifiers": [] }),
        )
        .await;

        // A catalog without a views endpoint may answer 501 instead of 405, which
        // must also be tolerated so the cascade still drops the namespace.
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/views").as_str()))
            .respond_with(ResponseTemplate::new(501))
            .mount(&ctx.server)
            .await;

        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_tolerates_unimplemented_views_endpoint() {
        test_drop_database_cascade_tolerates_unimplemented_views_endpoint_impl(None).await;
        test_drop_database_cascade_tolerates_unimplemented_views_endpoint_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_propagates_view_drop_failure_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({ "identifiers": [] }),
        )
        .await;
        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/views"),
            serde_json::json!({
                "identifiers": [
                    {
                        "namespace": ["ns1"],
                        "name": "view1"
                    }
                ]
            }),
        )
        .await;

        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1/views/view1").as_str()))
            .respond_with(ResponseTemplate::new(500))
            .mount(&ctx.server)
            .await;

        // The namespace drop must never be attempted once a view drop fails.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_propagates_view_drop_failure() {
        test_drop_database_cascade_propagates_view_drop_failure_impl(None).await;
        test_drop_database_cascade_propagates_view_drop_failure_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_propagates_list_views_failure_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/ns1/tables"),
            serde_json::json!({ "identifiers": [] }),
        )
        .await;

        // A genuine views listing failure (not a missing endpoint) must abort the cascade.
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/views").as_str()))
            .respond_with(ResponseTemplate::new(500))
            .mount(&ctx.server)
            .await;

        // The namespace drop must never be attempted once the listing fails.
        Mock::given(method("DELETE"))
            .and(path(ctx.path("/namespaces/ns1").as_str()))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&ctx.server)
            .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_propagates_list_views_failure() {
        test_drop_database_cascade_propagates_list_views_failure_impl(None).await;
        test_drop_database_cascade_propagates_list_views_failure_impl(Some("test")).await;
    }

    async fn test_drop_database_cascade_tolerates_missing_namespace_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["ns1".to_string()]).unwrap();

        // The mandatory tables endpoint reports that the namespace is gone. No
        // optional views request should run after that definitive result.
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/tables").as_str()))
            .respond_with(ResponseTemplate::new(404))
            .mount(&ctx.server)
            .await;
        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/ns1/views").as_str()))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&ctx.server)
            .await;

        // With if_exists set, the trailing namespace 404 is the success path.
        ctx.mock_delete_404(
            &ctx.path("/namespaces/ns1"),
            "NoSuchNamespaceException",
            "The given namespace does not exist",
        )
        .await;

        let result = ctx
            .catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: true,
                    cascade: true,
                },
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_drop_database_cascade_tolerates_missing_namespace() {
        test_drop_database_cascade_tolerates_missing_namespace_impl(None).await;
        test_drop_database_cascade_tolerates_missing_namespace_impl(Some("test")).await;
    }

    async fn test_drop_table_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
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

    async fn test_drop_view_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
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

    async fn test_get_table_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/db1/tables/table1"),
            serde_json::json!({
                "metadata-location": "s3://bucket/table/metadata/v1.metadata.json",
                "metadata": {
                    "format-version": 2,
                    "table-uuid": "12345678-1234-1234-1234-123456789012",
                    "location": "s3://bucket/table",
                    "current-schema-id": 0,
                    "schemas": [
                        {
                            "type": "struct",
                            "schema-id": 0,
                            "fields": [
                                {
                                    "id": 1,
                                    "name": "id",
                                    "required": true,
                                    "type": "long"
                                },
                                {
                                    "id": 2,
                                    "name": "data",
                                    "required": false,
                                    "type": "string",
                                    "doc": "data column"
                                },
                                {
                                    "id": 3,
                                    "name": "category",
                                    "required": true,
                                    "type": "string"
                                }
                            ],
                            "identifier-field-ids": [1]
                        }
                    ],
                    "default-spec-id": 0,
                    "partition-specs": [
                        {
                            "spec-id": 0,
                            "fields": [
                                {
                                    "source-id": 3,
                                    "field-id": 1000,
                                    "name": "category",
                                    "transform": "identity"
                                }
                            ]
                        }
                    ],
                    "default-sort-order-id": 1,
                    "sort-orders": [
                        {
                            "order-id": 1,
                            "fields": [
                                {
                                    "source-id": 1,
                                    "transform": "identity",
                                    "direction": "asc",
                                    "null-order": "nulls-first"
                                }
                            ]
                        }
                    ],
                    "properties": {
                        "comment": "test table",
                        "owner": "test_user"
                    }
                }
            }),
        )
        .await;

        let result = ctx.catalog.get_table(&namespace, "table1").await.unwrap();

        assert_eq!(result.name, "table1");
        assert_eq!(result.catalog, Some(ctx.name));
        assert_eq!(result.database, vec!["db1".to_string()]);
        match result.kind {
            TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
                sort_by,
                properties,
                ..
            } => {
                assert_eq!(columns.len(), 3);

                assert_eq!(columns[0].name, "id");
                assert!(!columns[0].nullable);
                assert!(!columns[0].is_partition);
                assert_eq!(columns[0].comment, None);

                assert_eq!(columns[1].name, "data");
                assert!(columns[1].nullable);
                assert_eq!(columns[1].comment, Some("data column".to_string()));

                assert_eq!(columns[2].name, "category");
                assert!(!columns[2].nullable);
                assert!(columns[2].is_partition);

                assert_eq!(comment, Some("test table".to_string()));
                assert_eq!(location, Some("s3://bucket/table".to_string()));
                assert_eq!(format, "iceberg");

                assert_eq!(
                    partition_by,
                    vec![CatalogPartitionField {
                        column: "category".to_string(),
                        transform: None,
                    }]
                );

                assert_eq!(sort_by.len(), 1);
                assert_eq!(sort_by[0].column, "id");
                assert!(sort_by[0].ascending);

                assert_eq!(constraints.len(), 1);
                match &constraints[0] {
                    CatalogTableConstraint::PrimaryKey { name, columns } => {
                        assert_eq!(name, &None);
                        assert_eq!(columns, &vec!["id".to_string()]);
                    }
                    _ => panic!("Expected PrimaryKey constraint"),
                }

                assert!(
                    properties
                        .iter()
                        .any(|(k, v)| k == "comment" && v == "test table")
                );
                assert!(
                    properties
                        .iter()
                        .any(|(k, v)| k == "owner" && v == "test_user")
                );
            }
            _ => panic!("Expected Table kind"),
        }
    }

    #[tokio::test]
    async fn test_get_table() {
        test_get_table_impl(None).await;
        test_get_table_impl(Some("test")).await;
    }

    #[tokio::test]
    async fn create_table_allows_rest_access_session_hints() {
        let ctx = TestContext::new(Some("test")).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_post_json(
            &ctx.path("/namespaces/db1/tables"),
            create_table_response_with_access_session_hints(),
        )
        .await;

        let status = ctx
            .catalog
            .create_table(&namespace, "table1", simple_create_table_options())
            .await
            .unwrap();

        assert_eq!(status.name, "table1");
    }

    #[tokio::test]
    async fn create_table_validates_nested_field_ids_before_writing() {
        for is_write_precondition in [false, true] {
            let ctx = TestContext::new(Some("test")).await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let mut response = create_table_response_with_access_session_hints();
            response["metadata"]["schemas"][0]["fields"][0]["type"] = serde_json::json!({
                "type": "list",
                "element-id": 99,
                "element": "string",
                "element-required": true
            });
            ctx.mock_post_json(&ctx.path("/namespaces/db1/tables"), response)
                .await;
            let mut options = simple_create_table_options();
            options.is_write_precondition = is_write_precondition;
            options.columns[0].data_type =
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, false)));

            let result = ctx
                .catalog
                .create_table(&namespace, "table1", options)
                .await;
            if is_write_precondition {
                let error = result.unwrap_err();
                assert!(matches!(error, CatalogError::UnsupportedCapability(_)));
                assert!(
                    error
                        .to_string()
                        .contains("changed the schema or field IDs")
                );
            } else {
                assert!(result.is_ok());
            }
        }
    }

    #[tokio::test]
    async fn create_table_rejects_server_side_scan_planning() {
        let ctx = TestContext::new(Some("test")).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_post_json(
            &ctx.path("/namespaces/db1/tables"),
            create_table_response_with_server_side_scan_planning(),
        )
        .await;

        let err = ctx
            .catalog
            .create_table(&namespace, "table1", simple_create_table_options())
            .await
            .unwrap_err();

        assert!(matches!(err, CatalogError::UnsupportedCapability(_)));
        assert!(err.to_string().contains("server-side scan planning"));
    }

    #[tokio::test]
    async fn metadata_only_create_table_allows_server_side_scan_planning() {
        let ctx = TestContext::new(Some("test")).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_post_json(
            &ctx.path("/namespaces/db1/tables"),
            create_table_response_with_server_side_scan_planning(),
        )
        .await;

        let mut options = simple_create_table_options();
        options.is_write_precondition = false;
        let status = ctx
            .catalog
            .create_table(&namespace, "table1", options)
            .await
            .unwrap();

        assert_eq!(status.name, "table1");
    }

    #[tokio::test]
    async fn begin_table_access_preserves_rest_session_hints_without_secret_values() {
        let ctx = TestContext::new(Some("test")).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/db1/tables/table1")))
            .and(header(
                "X-Iceberg-Access-Delegation",
                REST_ACCESS_DELEGATION_VENDED_CREDENTIALS,
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "metadata-location": "s3://bucket/table/metadata/v1.metadata.json",
                "metadata": {
                    "format-version": 2,
                    "table-uuid": "12345678-1234-1234-1234-123456789012",
                    "location": "s3://bucket/table",
                    "current-schema-id": 0,
                    "schemas": [
                        {
                            "type": "struct",
                            "schema-id": 0,
                            "fields": [
                                {
                                    "id": 1,
                                    "name": "id",
                                    "required": true,
                                    "type": "long"
                                }
                            ]
                        }
                    ]
                },
                "config": {
                    "scan-planning-mode": "server",
                    "token": "session-token-secret",
                    "s3.remote-signing-enabled": "true"
                },
                "storage-credentials": [
                    {
                        "prefix": "s3://credential-bucket/private-prefix",
                        "config": {
                            "s3.access-key-id": "AKIA-SECRET",
                            "s3.secret-access-key": "storage-secret"
                        }
                    }
                ]
            })))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("test".to_string()),
            vec!["test".to_string(), "db1".to_string(), "table1".to_string()],
            CatalogTableIdentity {
                table_id: None,
                table_uri: Some("s3://bucket/table".to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Iceberg,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::External,
                pointer: MetadataPointerAuthority::IcebergRest,
                commit: CommitAuthority::IcebergRestCommit,
            },
            ScanAuthority::ClientLakeSource,
        );

        let session = ctx
            .catalog
            .begin_table_access(
                &namespace,
                "table1",
                BeginTableAccessRequest {
                    context,
                    purpose: TableAccessPurpose::DataRead,
                },
            )
            .await
            .unwrap();

        assert_eq!(session.context.scan, ScanAuthority::IcebergRestServerSide);
        let rest_session = session.context.rest_session.as_ref().unwrap();
        assert_eq!(rest_session.scan_planning_mode.as_deref(), Some("server"));
        assert_eq!(rest_session.storage_credential_count, 1);
        assert!(rest_session.remote_signing_enabled);
        assert_eq!(
            session.reference.fingerprint,
            session.context.access_session.as_ref().unwrap().fingerprint
        );
        assert_eq!(session.reference.fingerprint, rest_session.fingerprint);

        let serialized = serde_json::to_string(&session.context).unwrap();
        assert!(!serialized.contains("s3://bucket/table/metadata/v1.metadata.json"));
        assert!(!serialized.contains("s3://credential-bucket/private-prefix"));
        assert!(!serialized.contains("s3.remote-signing-enabled"));
        assert!(!serialized.contains("s3.access-key-id"));
        assert!(!serialized.contains("s3.secret-access-key"));
        assert!(!serialized.contains("token"));
        assert!(!serialized.contains("session-token-secret"));
        assert!(!serialized.contains("AKIA-SECRET"));
        assert!(!serialized.contains("storage-secret"));
    }

    fn view_result() -> crate::r#gen::LoadViewResult {
        serde_json::from_value(serde_json::json!({
            "metadata-location": "s3://bucket/view/metadata/v1.json",
            "metadata": {
                "view-uuid": "87654321-4321-4321-4321-210987654321",
                "format-version": 1,
                "location": "s3://bucket/view",
                "current-version-id": 7,
                "versions": [{
                    "version-id": 7, "timestamp-ms": 1234567890000_i64, "schema-id": 4,
                    "summary": {}, "default-namespace": ["db1"],
                    "representations": [{"type": "sql", "sql": "SELECT 1 AS x", "dialect": "spark"}]
                }],
                "schemas": [{
                    "type": "struct", "schema-id": 4, "identifier-field-ids": [],
                    "fields": [{"id": 1, "name": "x", "type": "long", "required": true}]
                }],
                "properties": {"comment": "keep comment", "owner": "keep owner"},
                "version-log": [{"version-id": 7, "timestamp-ms": 1234567890000_i64}]
            }
        }))
        .unwrap()
    }

    fn view_options(definition: &str) -> CreateViewOptions {
        CreateViewOptions {
            columns: vec![CreateViewColumnOptions {
                name: "x".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
            }],
            definition: definition.to_string(),
            replace: true,
            if_not_exists: false,
            comment: None,
            properties: vec![],
        }
    }

    async fn view_mutations(ctx: &TestContext) -> Vec<Request> {
        ctx.server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.method != "GET")
            .collect()
    }

    fn view_commit(request: &Request) -> crate::r#gen::CommitViewRequest {
        assert_eq!(request.method, "POST");
        let commit: crate::r#gen::CommitViewRequest =
            serde_json::from_slice(&request.body).unwrap();
        assert!(commit.identifier.is_none());
        assert!(
            matches!(commit.requirements.as_deref(), Some([crate::r#gen::ViewRequirement::AssertViewUuid { uuid }])
            if uuid == "87654321-4321-4321-4321-210987654321")
        );
        commit
    }

    #[tokio::test]
    async fn test_replace_view_named_commit_preserves_identity_and_uses_response() {
        use crate::r#gen::ViewUpdate;
        for prefix in [None, Some("warehouse")] {
            let mut ctx = TestContext::new(Some("lake")).await;
            if let Some(prefix) = prefix {
                ctx.catalog
                    .options
                    .properties
                    .insert(REST_CATALOG_PROP_PREFIX.to_string(), prefix.to_string());
            }
            ctx.catalog.options.properties.insert(
                REST_CATALOG_PROP_NAMESPACE_SEPARATOR.to_string(),
                "-".to_string(),
            );
            let namespace =
                Namespace::try_from(vec!["db1".to_string(), "nested".to_string()]).unwrap();
            let url = format!(
                "/v1{}/namespaces/db1%2Dnested/views/view1",
                prefix.map(|s| format!("/{s}")).unwrap_or_default()
            );
            let original = view_result();
            let mut committed = original.clone();
            committed.metadata_location = "s3://bucket/view/metadata/v2.json".to_string();
            let mut version = committed.metadata.versions[0].clone();
            version.version_id = 8;
            version.default_namespace =
                Box::new(vec!["db1".to_string(), "nested".to_string()].into());
            version.representations =
                vec![crate::r#gen::ViewRepresentation::SqlViewRepresentation(
                    Box::new(crate::r#gen::SqlViewRepresentation {
                        r#type: "sql".to_string(),
                        sql: "SELECT 2 AS x".to_string(),
                        dialect: "spark".to_string(),
                    }),
                )];
            committed.metadata.versions.push(version);
            committed.metadata.current_version_id = 8;
            ctx.mock_get_json(&url, serde_json::to_value(original).unwrap())
                .await;
            Mock::given(method("POST"))
                .and(path(&url))
                .respond_with(ResponseTemplate::new(200).set_body_json(&committed))
                .expect(1)
                .mount(&ctx.server)
                .await;
            let status = ctx
                .catalog
                .create_view(&namespace, "view1", view_options("SELECT 2 AS x"))
                .await
                .unwrap();
            let TableKind::View {
                definition,
                comment,
                properties,
                ..
            } = status.kind
            else {
                panic!("expected view")
            };
            assert_eq!(definition, "SELECT 2 AS x");
            assert_eq!(comment.as_deref(), Some("keep comment"));
            assert!(properties.contains(&(
                METADATA_LOCATION_KEY.to_string(),
                committed.metadata_location
            )));
            assert!(properties.contains(&("owner".to_string(), "keep owner".to_string())));
            let mutations = view_mutations(&ctx).await;
            assert_eq!(mutations.len(), 1);
            assert_eq!(mutations[0].url.path(), url);
            let commit = view_commit(&mutations[0]);
            let [
                ViewUpdate::AddViewVersion { view_version },
                ViewUpdate::SetCurrentViewVersion { view_version_id },
            ] = commit.updates.as_slice()
            else {
                panic!("unexpected updates: {:?}", commit.updates)
            };
            assert_eq!(view_version.version_id, 8);
            assert_eq!(view_version.schema_id, 4);
            assert_eq!(view_version.default_namespace.0, ["db1", "nested"]);
            assert!(view_version.default_catalog.is_none());
            assert_eq!(*view_version_id, -1);
        }
    }

    #[tokio::test]
    async fn test_create_or_replace_view_missing_and_if_not_exists_precedence() {
        for exists in [false, true] {
            for if_not_exists in [false, true] {
                let ctx = TestContext::new(None).await;
                let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
                let result = view_result();
                Mock::given(method("GET"))
                    .and(path(ctx.path("/namespaces/db1/views/view1")))
                    .respond_with(if exists {
                        ResponseTemplate::new(200).set_body_json(&result)
                    } else {
                        ResponseTemplate::new(404)
                    })
                    .expect(1)
                    .mount(&ctx.server)
                    .await;
                let endpoint = if exists {
                    "/namespaces/db1/views/view1"
                } else {
                    "/namespaces/db1/views"
                };
                Mock::given(method("POST"))
                    .and(path(ctx.path(endpoint)))
                    .respond_with(ResponseTemplate::new(200).set_body_json(&result))
                    .expect(if exists && if_not_exists { 0 } else { 1 })
                    .mount(&ctx.server)
                    .await;
                let mut options = view_options("SELECT 2 AS x");
                options.if_not_exists = if_not_exists;
                ctx.catalog
                    .create_view(&namespace, "view1", options)
                    .await
                    .unwrap();
                let mutations = view_mutations(&ctx).await;
                assert_eq!(mutations.len(), usize::from(!(exists && if_not_exists)));
                if !exists {
                    let create: crate::r#gen::CreateViewRequest =
                        serde_json::from_slice(&mutations[0].body).unwrap();
                    assert_eq!(create.name, "view1");
                    assert_eq!(create.view_version.version_id, 1);
                    assert_eq!(create.view_version.schema_id, -1);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_replace_view_load_errors_never_create() {
        for status in [400, 401, 403, 405, 429, 500, 501] {
            let ctx = TestContext::new(None).await;
            Mock::given(method("GET"))
                .and(path(ctx.path("/namespaces/db1/views/view1")))
                .respond_with(ResponseTemplate::new(status))
                .expect(if status == 401 { 2 } else { 1 })
                .mount(&ctx.server)
                .await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let error = ctx
                .catalog
                .create_view(&namespace, "view1", view_options("SELECT 2 AS x"))
                .await
                .unwrap_err();
            match status {
                401 => assert!(matches!(error, CatalogError::Unauthorized(_))),
                403 => assert!(matches!(error, CatalogError::Forbidden(_))),
                405 | 501 => assert!(matches!(error, CatalogError::NotSupported(_))),
                429 => assert!(matches!(error, CatalogError::RateLimited(_))),
                _ => assert!(matches!(error, CatalogError::External(_))),
            }
            assert!(view_mutations(&ctx).await.is_empty());
        }
    }

    #[tokio::test]
    async fn test_replace_view_reuses_current_and_historical_versions() {
        use crate::r#gen::ViewUpdate;
        for historical in [false, true] {
            let ctx = TestContext::new(None).await;
            let mut result = view_result();
            if historical {
                let mut current = result.metadata.versions[0].clone();
                current.version_id = 12;
                current
                    .summary
                    .insert("engine".to_string(), "different".to_string());
                result.metadata.versions.push(current);
                result.metadata.current_version_id = 12;
            }
            ctx.mock_get_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(&result).unwrap(),
            )
            .await;
            ctx.mock_post_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(result).unwrap(),
            )
            .await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            ctx.catalog
                .create_view(&namespace, "view1", view_options("SELECT 1 AS x"))
                .await
                .unwrap();
            let mutations = view_mutations(&ctx).await;
            assert_eq!(mutations.len(), 1);
            let commit = view_commit(&mutations[0]);
            assert!(matches!(
                commit.updates.as_slice(),
                [ViewUpdate::SetCurrentViewVersion { view_version_id: 7 }]
            ));
        }
    }

    fn decimal_view_schemas(primitive: &str) -> Vec<crate::r#gen::Schema> {
        [
            serde_json::json!(primitive),
            serde_json::json!({"type": "struct", "fields": [
                {"id": 2, "name": "amount", "required": true, "type": primitive}
            ]}),
            serde_json::json!({"type": "list", "element-id": 2, "element-required": false,
                "element": primitive}),
            serde_json::json!({"type": "map", "key-id": 2, "key": primitive,
                "value-id": 3, "value-required": false, "value": primitive}),
            serde_json::json!({"type": "struct", "fields": [
                {"id": 2, "name": "items", "required": true, "type": {
                    "type": "list", "element-id": 3, "element-required": false, "element": {
                        "type": "map", "key-id": 4, "key": "string", "value-id": 5,
                        "value-required": false, "value": primitive
                    }
                }}
            ]}),
        ]
        .into_iter()
        .map(|data_type| {
            let mut schema = view_result().metadata.schemas[0].clone();
            schema.fields[0].r#type = Box::new(serde_json::from_value(data_type).unwrap());
            schema
        })
        .collect()
    }

    #[test]
    fn test_view_schema_key_normalizes_decimal_types_only() {
        for spelling in ["decimal(10, 2)", "decimal( 10 , 2 )", "DECIMAL(10,2)"] {
            for (canonical, mut equivalent) in decimal_view_schemas("decimal(10,2)")
                .into_iter()
                .zip(decimal_view_schemas(spelling))
            {
                equivalent.schema_id = Some(19);
                equivalent.identifier_field_ids = None;
                assert_eq!(
                    view_schema_key(&canonical).unwrap(),
                    view_schema_key(&equivalent).unwrap()
                );
            }
        }
        // Do not let a permissive primitive parser turn malformed types into valid ones.
        for spelling in [
            "decimal(11,2)",
            "decimal(10,3)",
            "decimal(1 0,2)",
            "decimal(10,2",
            "decimal(decimal(10,2))",
            "decimal(10,2))",
            "decimal(10,2,3)",
            "decimal(+10,2)",
            "decimal(0,0)",
            "decimal(39,2)",
            "decimal(10,11)",
            "decimal(4294967296,2)",
            "future-type(10, 2)",
        ] {
            for (canonical, different) in decimal_view_schemas("decimal(10,2)")
                .into_iter()
                .zip(decimal_view_schemas(spelling))
            {
                assert_ne!(
                    view_schema_key(&canonical).unwrap(),
                    view_schema_key(&different).unwrap()
                );
                let mut unchanged = different.clone();
                unchanged.schema_id = None;
                assert_eq!(
                    view_schema_key(&different).unwrap(),
                    serde_json::to_value(unchanged).unwrap()
                );
            }
        }
        for spelling in [
            "decimal(0, 0)",
            "decimal(39, 2)",
            "decimal(10, 11)",
            "geometry(bad, extra)",
        ] {
            let schema = decimal_view_schemas(spelling).remove(0);
            assert_eq!(
                view_schema_key(&schema).unwrap()["fields"][0]["type"],
                spelling
            );
        }
    }

    #[test]
    fn test_view_schema_key_preserves_field_metadata() {
        let schema: crate::r#gen::Schema = serde_json::from_value(serde_json::json!({
            "type": "struct", "schema-id": 4, "identifier-field-ids": [2, 1],
            "fields": [{"id": 1, "name": "decimal(10, 2)", "required": true,
                "type": "decimal(10, 2)", "doc": "decimal(10, 2)",
                "initial-default": "1.00", "write-default": "2.00"}]
        }))
        .unwrap();
        let key = view_schema_key(&schema).unwrap();
        let mut expected = serde_json::to_value(&schema).unwrap();
        expected.as_object_mut().unwrap().remove("schema-id");
        expected["identifier-field-ids"] = serde_json::json!([1, 2]);
        expected["fields"][0]["type"] = serde_json::json!("decimal(10,2)");
        assert_eq!(key, expected);
        for (attribute, value) in [
            ("id", serde_json::json!(2)),
            ("name", serde_json::json!("decimal(10,2)")),
            ("required", serde_json::json!(false)),
            ("doc", serde_json::json!("decimal(10,2)")),
            ("initial-default", serde_json::json!("3.00")),
            ("write-default", serde_json::json!("3.00")),
        ] {
            let mut changed = serde_json::to_value(&schema).unwrap();
            changed["fields"][0][attribute] = value;
            let changed = serde_json::from_value(changed).unwrap();
            assert_ne!(key, view_schema_key(&changed).unwrap(), "{attribute}");
        }
        let mut identifiers = schema.clone();
        identifiers.identifier_field_ids = Some(vec![1, 2, 1]);
        assert_eq!(key, view_schema_key(&identifiers).unwrap());
        identifiers.identifier_field_ids = Some(vec![1]);
        assert_ne!(key, view_schema_key(&identifiers).unwrap());

        for (index, attributes) in [
            (
                2,
                vec![
                    ("element-id", serde_json::json!(9)),
                    ("element-required", serde_json::json!(true)),
                ],
            ),
            (
                3,
                vec![
                    ("key-id", serde_json::json!(9)),
                    ("value-id", serde_json::json!(9)),
                    ("value-required", serde_json::json!(true)),
                    ("key", serde_json::json!("decimal(11,2)")),
                ],
            ),
        ] {
            let schema = decimal_view_schemas("decimal(10, 2)").remove(index);
            for (attribute, value) in attributes {
                let mut changed = serde_json::to_value(&schema).unwrap();
                changed["fields"][0]["type"][attribute] = value;
                let changed = serde_json::from_value(changed).unwrap();
                assert_ne!(
                    view_schema_key(&schema).unwrap(),
                    view_schema_key(&changed).unwrap(),
                    "{attribute}"
                );
            }
        }
    }

    #[test]
    fn test_replace_view_decimal_schema_updates() {
        use crate::r#gen::ViewUpdate;
        for (spelling, new_schema) in [
            ("decimal(10,2)", false),
            ("decimal(11,2)", true),
            ("decimal(10,3)", true),
        ] {
            for (existing, requested) in decimal_view_schemas("decimal(10, 2)")
                .into_iter()
                .zip(decimal_view_schemas(spelling))
            {
                let mut result = view_result();
                result.metadata.schemas = vec![existing];
                let mut version = result.metadata.versions[0].clone();
                version.representations = serde_json::from_value(serde_json::json!([
                    {"type": "sql", "dialect": "spark", "sql": "SELECT 2 AS x"}
                ]))
                .unwrap();
                let request = crate::r#gen::CreateViewRequest {
                    name: "view1".to_string(),
                    schema: Box::new(requested),
                    view_version: Box::new(version),
                    location: None,
                    properties: HashMap::new(),
                };
                let commit = replace_view_request(&result.metadata, request).unwrap();
                let updates = if new_schema {
                    let ViewUpdate::AddSchema { schema, .. } = &commit.updates[0] else {
                        panic!("changed decimal must add a schema")
                    };
                    assert_eq!(schema.schema_id, Some(5));
                    &commit.updates[1..]
                } else {
                    commit.updates.as_slice()
                };
                let [
                    ViewUpdate::AddViewVersion { view_version },
                    ViewUpdate::SetCurrentViewVersion { view_version_id },
                ] = updates
                else {
                    panic!("unexpected updates: {:?}", commit.updates)
                };
                assert_eq!(view_version.schema_id, if new_schema { -1 } else { 4 });
                assert_eq!(*view_version_id, -1);
            }
        }
    }

    #[tokio::test]
    async fn test_replace_view_nested_schema_and_property_deltas() {
        use crate::r#gen::ViewUpdate;
        let ctx = TestContext::new(None).await;
        let mut result = view_result();
        result
            .metadata
            .properties
            .as_mut()
            .unwrap()
            .insert("spark.query-column-names".to_string(), "old".to_string());
        let mut old_schema = result.metadata.schemas[0].clone();
        old_schema.schema_id = Some(19);
        old_schema.fields[0].name = "older".to_string();
        result.metadata.schemas.push(old_schema);
        let mut old_version = result.metadata.versions[0].clone();
        old_version.version_id = 25;
        old_version.schema_id = 19;
        result.metadata.versions.push(old_version);
        ctx.mock_get_json(
            &ctx.path("/namespaces/db1/views/view1"),
            serde_json::to_value(&result).unwrap(),
        )
        .await;
        ctx.mock_post_json(
            &ctx.path("/namespaces/db1/views/view1"),
            serde_json::to_value(result).unwrap(),
        )
        .await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let mut options = view_options("SELECT named_struct('items', array(2)) AS x");
        options.columns[0].data_type = DataType::Struct(
            vec![Arc::new(Field::new(
                "items",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
                true,
            ))]
            .into(),
        );
        options.comment = Some("new comment".to_string());
        options.properties = vec![
            ("path".to_string(), "  s3://bucket/new  ".to_string()),
            ("owner".to_string(), "new owner".to_string()),
        ];
        ctx.catalog
            .create_view(&namespace, "view1", options)
            .await
            .unwrap();
        let mutations = view_mutations(&ctx).await;
        assert_eq!(mutations.len(), 1);
        let commit = view_commit(&mutations[0]);
        let [
            ViewUpdate::AddSchema {
                schema,
                last_column_id,
            },
            ViewUpdate::AddViewVersion { view_version },
            ViewUpdate::SetCurrentViewVersion { view_version_id },
            ViewUpdate::RemoveProperties { removals },
            ViewUpdate::SetProperties { updates },
            ViewUpdate::SetLocation { location },
        ] = commit.updates.as_slice()
        else {
            panic!("unexpected updates: {:?}", commit.updates)
        };
        assert_eq!(schema.schema_id, Some(20));
        assert!(last_column_id.is_none());
        assert_eq!(
            serde_json::to_value(&schema.fields).unwrap(),
            serde_json::json!([
                {"id": 1, "name": "x", "required": true, "type": {"type": "struct", "fields": [
                    {"id": 2, "name": "items", "required": false, "type": {"type": "list", "element-id": 3, "element": "int", "element-required": false}}
                ]}}
            ])
        );
        assert_eq!(view_version.version_id, 26);
        assert_eq!(view_version.schema_id, -1);
        assert_eq!(*view_version_id, -1);
        assert_eq!(removals, &["spark.query-column-names"]);
        assert_eq!(
            updates,
            &HashMap::from([
                ("path".to_string(), "  s3://bucket/new  ".to_string()),
                ("location".to_string(), "s3://bucket/new".to_string()),
                ("owner".to_string(), "new owner".to_string()),
                ("comment".to_string(), "new comment".to_string()),
            ])
        );
        assert_eq!(location, "s3://bucket/new");
    }

    #[tokio::test]
    async fn test_replace_view_dialect_loss_requires_effective_permission() {
        for (stored, supplied, allowed) in [
            (None, None, false),
            (Some("true"), None, true),
            (None, Some("true"), true),
            (Some("true"), Some("false"), false),
        ] {
            let ctx = TestContext::new(None).await;
            let mut result = view_result();
            result.metadata.versions[0].representations.push(
                crate::r#gen::ViewRepresentation::SqlViewRepresentation(Box::new(
                    crate::r#gen::SqlViewRepresentation {
                        r#type: "sql".to_string(),
                        sql: "SELECT 1 AS x".to_string(),
                        dialect: "trino".to_string(),
                    },
                )),
            );
            if let Some(stored) = stored {
                result.metadata.properties.as_mut().unwrap().insert(
                    "replace.drop-dialect.allowed".to_string(),
                    stored.to_string(),
                );
            }
            ctx.mock_get_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(&result).unwrap(),
            )
            .await;
            ctx.mock_post_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(result).unwrap(),
            )
            .await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let mut options = view_options("SELECT 2 AS x");
            if let Some(supplied) = supplied {
                options.properties.push((
                    "replace.drop-dialect.allowed".to_string(),
                    supplied.to_string(),
                ));
            }
            let result = ctx.catalog.create_view(&namespace, "view1", options).await;
            if allowed {
                result.unwrap();
            } else {
                assert!(matches!(result, Err(CatalogError::InvalidArgument(_))));
            }
            let mutations = view_mutations(&ctx).await;
            assert_eq!(mutations.len(), usize::from(allowed));
            if allowed {
                let commit = view_commit(&mutations[0]);
                let crate::r#gen::ViewUpdate::AddViewVersion { view_version } = &commit.updates[0]
                else {
                    panic!("expected version")
                };
                assert_eq!(view_version.representations.len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn test_replace_view_id_overflow_rejected_before_commit() {
        for schema_overflow in [false, true] {
            let ctx = TestContext::new(None).await;
            let mut result = view_result();
            if schema_overflow {
                result.metadata.schemas[0].schema_id = Some(i32::MAX);
                result.metadata.versions[0].schema_id = i32::MAX;
            } else {
                result.metadata.versions[0].version_id = i32::MAX;
                result.metadata.current_version_id = i32::MAX;
            }
            ctx.mock_get_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(result).unwrap(),
            )
            .await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let mut options = view_options("SELECT 'new' AS x");
            options.columns[0].data_type = DataType::Utf8;
            let error = ctx
                .catalog
                .create_view(&namespace, "view1", options)
                .await
                .unwrap_err();
            assert!(matches!(error, CatalogError::InvalidArgument(_)));
            assert!(view_mutations(&ctx).await.is_empty());
        }
    }

    #[tokio::test]
    async fn test_replace_view_commit_failures_do_not_retry_or_create() {
        for status in [400, 401, 403, 404, 405, 409, 429, 500, 501, 502, 503, 504] {
            let ctx = TestContext::new(None).await;
            ctx.mock_get_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(view_result()).unwrap(),
            )
            .await;
            Mock::given(method("POST")).and(path(ctx.path("/namespaces/db1/views/view1")))
                .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({
                    "error": {"code": status, "type": if status == 400 { "ViewUuidMismatch" } else { "TestError" }, "message": "rejected"}
                })))
                .expect(if status == 401 { 2 } else { 1 }).mount(&ctx.server).await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let error = ctx
                .catalog
                .create_view(&namespace, "view1", view_options("SELECT 2 AS x"))
                .await
                .unwrap_err();
            match status {
                400 => assert!(matches!(error, CatalogError::CommitRejected(_))),
                401 => assert!(matches!(error, CatalogError::Unauthorized(_))),
                403 => assert!(matches!(error, CatalogError::Forbidden(_))),
                404 => assert!(matches!(
                    error,
                    CatalogError::NotFound(CatalogObject::View, _)
                )),
                405 | 501 => assert!(matches!(error, CatalogError::NotSupported(_))),
                409 => assert!(matches!(error, CatalogError::Conflict(_))),
                429 => assert!(matches!(error, CatalogError::RateLimited(_))),
                _ => assert!(matches!(error, CatalogError::CommitStateUnknown(_))),
            }
            let mutations = view_mutations(&ctx).await;
            assert_eq!(mutations.len(), if status == 401 { 2 } else { 1 });
            for request in mutations {
                view_commit(&request);
            }
        }
    }

    #[tokio::test]
    async fn test_replace_view_malformed_and_lost_success_are_unknown() {
        for failure in [
            "json",
            "conversion",
            "missing-version",
            "missing-schema",
            "timeout",
        ] {
            let mut ctx = TestContext::new(None).await;
            let original = view_result();
            ctx.mock_get_json(
                &ctx.path("/namespaces/db1/views/view1"),
                serde_json::to_value(&original).unwrap(),
            )
            .await;
            let mut response = serde_json::to_value(original).unwrap();
            match failure {
                "conversion" => {
                    response["metadata"]["schemas"][0]["fields"][0]["type"] =
                        serde_json::json!("not-an-iceberg-type")
                }
                "missing-version" => response["metadata"]["versions"] = serde_json::json!([]),
                "missing-schema" => response["metadata"]["schemas"] = serde_json::json!([]),
                _ => {}
            }
            let template = if failure == "json" {
                ResponseTemplate::new(200).set_body_string("{")
            } else if failure == "timeout" {
                ctx.catalog.http_client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_millis(100))
                    .build()
                    .unwrap();
                ResponseTemplate::new(200)
                    .set_body_json(response)
                    .set_delay(std::time::Duration::from_millis(500))
            } else {
                ResponseTemplate::new(200).set_body_json(response)
            };
            Mock::given(method("POST"))
                .and(path(ctx.path("/namespaces/db1/views/view1")))
                .respond_with(template)
                .expect(1)
                .mount(&ctx.server)
                .await;
            let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
            let error = ctx
                .catalog
                .create_view(&namespace, "view1", view_options("SELECT 2 AS x"))
                .await
                .unwrap_err();
            assert!(
                matches!(error, CatalogError::CommitStateUnknown(_)),
                "{failure}: {error:?}"
            );
            let mutations = view_mutations(&ctx).await;
            assert_eq!(mutations.len(), 1);
            view_commit(&mutations[0]);
        }
    }

    async fn test_get_view_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/db1/views/view1"),
            serde_json::json!({
                "metadata-location": "s3://bucket/view/metadata/v1.metadata.json",
                "metadata": {
                    "view-uuid": "87654321-4321-4321-4321-210987654321",
                    "format-version": 1,
                    "location": "s3://bucket/view",
                    "current-version-id": 1,
                    "versions": [
                        {
                            "version-id": 1,
                            "timestamp-ms": 1234567890000_i64,
                            "schema-id": 0,
                            "summary": {
                                "operation": "create"
                            },
                            "representations": [
                                {
                                    "type": "sql",
                                    "sql": "SELECT id, data FROM table1 WHERE id > 100",
                                    "dialect": "spark"
                                }
                            ],
                            "default-namespace": ["db1"]
                        }
                    ],
                    "schemas": [
                        {
                            "type": "struct",
                            "schema-id": 0,
                            "fields": [
                                {
                                    "id": 1,
                                    "name": "id",
                                    "required": true,
                                    "type": "long"
                                },
                                {
                                    "id": 2,
                                    "name": "data",
                                    "required": false,
                                    "type": "string",
                                    "doc": "filtered data"
                                }
                            ]
                        }
                    ],
                    "properties": {
                        "comment": "test view",
                        "created_by": "test_user"
                    },
                    "version-log": [
                        {
                            "version-id": 1,
                            "timestamp-ms": 1234567890000_i64
                        }
                    ]
                }
            }),
        )
        .await;

        let result = ctx.catalog.get_view(&namespace, "view1").await.unwrap();

        assert_eq!(result.name, "view1");
        assert_eq!(result.catalog, Some(ctx.name));
        assert_eq!(result.database, vec!["db1".to_string()]);
        match result.kind {
            TableKind::View {
                definition,
                columns,
                comment,
                properties,
            } => {
                assert_eq!(definition, "SELECT id, data FROM table1 WHERE id > 100");

                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert!(!columns[0].nullable);

                assert_eq!(columns[1].name, "data");
                assert!(columns[1].nullable);
                assert_eq!(columns[1].comment, Some("filtered data".to_string()));

                assert_eq!(comment, Some("test view".to_string()));
                assert!(
                    properties
                        .iter()
                        .any(|(k, v)| k == "comment" && v == "test view")
                );
                assert!(
                    properties
                        .iter()
                        .any(|(k, v)| k == "created_by" && v == "test_user")
                );
            }
            _ => panic!("Expected View kind"),
        }
    }

    #[tokio::test]
    async fn test_get_view() {
        test_get_view_impl(None).await;
        test_get_view_impl(Some("test")).await;
    }

    #[tokio::test]
    async fn test_get_view_unsupported_endpoint() {
        let ctx = TestContext::new(None).await;

        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/db1/views/view1")))
            .respond_with(ResponseTemplate::new(501))
            .mount(&ctx.server)
            .await;

        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let error = ctx.catalog.get_view(&namespace, "view1").await.unwrap_err();

        assert!(matches!(error, CatalogError::NotSupported(_)));
    }

    async fn test_create_database_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;

        Mock::given(method("POST"))
            .and(path(ctx.path("/namespaces").as_str()))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "namespace": ["db1"],
                "properties": {
                    "comment": "test database",
                    "location": "s3://bucket/db1",
                    "custom_prop": "custom_value"
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "namespace": ["db1"],
                "properties": {
                    "comment": "test database",
                    "location": "s3://bucket/db1",
                    "custom_prop": "custom_value"
                }
            })))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let result = ctx
            .catalog
            .create_database(
                &namespace,
                CreateDatabaseOptions {
                    if_not_exists: false,
                    comment: Some("test database".to_string()),
                    location: Some("s3://bucket/db1".to_string()),
                    properties: vec![("custom_prop".to_string(), "custom_value".to_string())],
                },
            )
            .await;

        assert!(result.is_ok());
        let db = result.unwrap();
        assert_eq!(db.database, vec!["db1".to_string()]);
        assert_eq!(db.comment, Some("test database".to_string()));
        assert_eq!(db.location, Some("s3://bucket/db1".to_string()));
        assert!(
            db.properties
                .iter()
                .any(|(k, v)| k == "custom_prop" && v == "custom_value")
        );

        Mock::given(method("POST"))
            .and(path(ctx.path("/namespaces").as_str()))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "namespace": ["db1"],
            })))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {
                    "message": "Failed to create namespace: error in response: status code 409 Conflict",
                    "type": "NamespaceAlreadyExistsException",
                    "code": 409
                }
            })))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let result = ctx
            .catalog
            .create_database(
                &namespace,
                CreateDatabaseOptions {
                    if_not_exists: false,
                    comment: None,
                    location: None,
                    properties: vec![],
                },
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("status code 409 Conflict"));

        Mock::given(method("POST"))
            .and(path(ctx.path("/namespaces").as_str()))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "namespace": ["db1"],
                "properties": {
                    "comment": "should be ignored",
                    "location": "should be ignored"
                }
            })))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {
                    "message": "error in response: status code 409 Conflict",
                    "type": "NamespaceAlreadyExistsException",
                    "code": 409
                }
            })))
            .expect(1)
            .mount(&ctx.server)
            .await;

        Mock::given(method("GET"))
            .and(path(ctx.path("/namespaces/db1").as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
               "namespace": ["db1"],
               "properties": {
                   "comment": "test database",
                   "location": "s3://bucket/db1",
                   "custom_prop": "custom_value"
               }
            })))
            .expect(1)
            .mount(&ctx.server)
            .await;

        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();
        let result = ctx
            .catalog
            .create_database(
                &namespace,
                CreateDatabaseOptions {
                    if_not_exists: true,
                    comment: Some("should be ignored".to_string()),
                    location: Some("should be ignored".to_string()),
                    properties: vec![],
                },
            )
            .await;

        assert!(result.is_ok());
        let db = result.unwrap();
        assert_eq!(db.database, vec!["db1".to_string()]);
        assert_eq!(db.comment, Some("test database".to_string()));
        assert_eq!(db.location, Some("s3://bucket/db1".to_string()));
        assert!(
            db.properties
                .iter()
                .any(|(k, v)| k == "custom_prop" && v == "custom_value")
        );
    }

    #[tokio::test]
    async fn test_create_database() {
        test_create_database_impl(None).await;
        test_create_database_impl(Some("test")).await;
    }

    async fn test_get_database_impl(name: Option<&str>) {
        let ctx = TestContext::new(name).await;
        let namespace = Namespace::try_from(vec!["db1".to_string()]).unwrap();

        ctx.mock_get_json(
            &ctx.path("/namespaces/db1"),
            serde_json::json!({
                "namespace": ["db1"],
                "properties": {
                    "comment": "test database",
                    "location": "s3://bucket/db1",
                    "owner": "alice",
                    "custom_prop": "custom_value"
                }
            }),
        )
        .await;

        let result = ctx.catalog.get_database(&namespace).await.unwrap();

        assert_eq!(result.database, vec!["db1".to_string()]);
        assert_eq!(result.comment, Some("test database".to_string()));
        assert_eq!(result.location, Some("s3://bucket/db1".to_string()));
        assert!(
            result
                .properties
                .iter()
                .any(|(k, v)| k == "comment" && v == "test database")
        );
        assert!(
            result
                .properties
                .iter()
                .any(|(k, v)| k == "location" && v == "s3://bucket/db1")
        );
        assert!(
            result
                .properties
                .iter()
                .any(|(k, v)| k == "owner" && v == "alice")
        );
        assert!(
            result
                .properties
                .iter()
                .any(|(k, v)| k == "custom_prop" && v == "custom_value")
        );

        ctx.mock_get_json(
            &ctx.path("/namespaces/db2"),
            serde_json::json!({
                "namespace": ["db2"],
                "properties": {}
            }),
        )
        .await;

        let namespace = Namespace::try_from(vec!["db2".to_string()]).unwrap();
        let result = ctx.catalog.get_database(&namespace).await.unwrap();

        assert_eq!(result.database, vec!["db2".to_string()]);
        assert_eq!(result.comment, None);
        assert_eq!(result.location, None);
        assert_eq!(result.properties.len(), 0);

        ctx.mock_get_json(
            &ctx.path("/namespaces/db3"),
            serde_json::json!({
                "namespace": ["db3"],
                "properties": {
                    "COMMENT": "case insensitive",
                    "LOCATION": "s3://bucket/db3"
                }
            }),
        )
        .await;

        let namespace = Namespace::try_from(vec!["db3".to_string()]).unwrap();
        let result = ctx.catalog.get_database(&namespace).await.unwrap();

        assert_eq!(result.database, vec!["db3".to_string()]);
        assert_eq!(result.comment, Some("case insensitive".to_string()));
        assert_eq!(result.location, Some("s3://bucket/db3".to_string()));
    }

    #[tokio::test]
    async fn test_get_database() {
        test_get_database_impl(None).await;
        test_get_database_impl(Some("test")).await;
    }

    #[tokio::test]
    async fn commit_reports_unknown_state_without_retrying() {
        for status in [200, 500, 502, 503, 504] {
            for json_body in [false, true] {
                let ctx = TestContext::new(Some("test")).await;
                let response = ResponseTemplate::new(status);
                let response = if json_body {
                    response.set_body_json(serde_json::json!({
                        "error": {"code": status, "type": "ServerError", "message": "uncertain"}
                    }))
                } else {
                    response.set_body_string("invalid response body")
                };
                Mock::given(method("POST"))
                    .and(path(ctx.path("/namespaces/db1/tables/table1")))
                    .respond_with(response)
                    .expect(1)
                    .mount(&ctx.server)
                    .await;

                assert!(
                    matches!(
                        ctx.commit_table().await.unwrap(),
                        LakehouseCommitOutcome::StateUnknown { .. }
                    ),
                    "status {status}, JSON body {json_body}"
                );
            }
        }
    }

    #[tokio::test]
    async fn commit_preserves_definite_failures() {
        for status in [400, 401, 403, 404, 409, 422, 429, 501] {
            let ctx = TestContext::new(Some("test")).await;
            Mock::given(method("POST"))
                .and(path(ctx.path("/namespaces/db1/tables/table1")))
                .respond_with(ResponseTemplate::new(status).set_body_string("rejected"))
                .expect(if status == 401 { 2 } else { 1 })
                .mount(&ctx.server)
                .await;

            let error = ctx.commit_table().await.unwrap_err();
            assert!(matches!(
                (status, error),
                (401, CatalogError::Unauthorized(_))
                    | (403, CatalogError::Forbidden(_))
                    | (404, CatalogError::NotFound(CatalogObject::Table, _))
                    | (409, CatalogError::Conflict(_))
                    | (429, CatalogError::RateLimited(_))
                    | (400 | 422 | 501, CatalogError::External(_))
            ));
        }
    }

    #[tokio::test]
    async fn commit_response_timeout_is_unknown() {
        let mut ctx = TestContext::new(Some("test")).await;
        ctx.catalog.resolved_catalog_config().await.unwrap();
        ctx.catalog.http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        Mock::given(method("POST"))
            .and(path(ctx.path("/namespaces/db1/tables/table1")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(create_table_response_with_access_session_hints())
                    .set_delay(std::time::Duration::from_secs(1)),
            )
            .expect(1)
            .mount(&ctx.server)
            .await;

        assert!(matches!(
            ctx.commit_table().await.unwrap(),
            LakehouseCommitOutcome::StateUnknown { .. }
        ));
    }

    #[tokio::test]
    async fn commit_connection_failure_is_definite() {
        let mut ctx = TestContext::new(Some("test")).await;
        ctx.catalog.resolved_catalog_config().await.unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        ctx.catalog.http_client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(format!("http://{address}")).unwrap())
            .build()
            .unwrap();

        assert!(matches!(
            ctx.commit_table().await,
            Err(CatalogError::External(_))
        ));
    }

    #[tokio::test]
    async fn commit_valid_response_is_committed() {
        let ctx = TestContext::new(Some("test")).await;
        ctx.mock_post_json(
            &ctx.path("/namespaces/db1/tables/table1"),
            create_table_response_with_access_session_hints(),
        )
        .await;
        assert!(matches!(
            ctx.commit_table().await.unwrap(),
            LakehouseCommitOutcome::Committed {
                payload: Some(_),
                ..
            }
        ));
    }

    fn error_with_status(status: reqwest::StatusCode) -> ApiError<()> {
        ApiError::Unknown(Box::new(crate::r#gen::Response {
            inner: (),
            status,
            headers: reqwest::header::HeaderMap::new(),
        }))
    }

    #[tokio::test]
    async fn bootstrap_config_recovers_when_token_rotates_before_response() {
        let dir = TempDir::new().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "token-a").unwrap();

        let server = MockServer::start().await;
        let rotate_path = token_path.clone();
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(header("authorization", "Bearer token-a"))
            .respond_with(move |_req: &Request| {
                std::fs::write(&rotate_path, "token-b").unwrap();
                ResponseTemplate::new(401).set_body_json(serde_json::json!({
                    "error": {
                        "message": "token expired",
                        "type": "NotAuthorizedException",
                        "code": 401
                    }
                }))
            })
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(header("authorization", "Bearer token-b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "overrides": { "warehouse": "s3://iceberg-catalog" },
                "defaults": {}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let properties = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), server.uri())]);
        let catalog = IcebergRestCatalogProvider::new(
            String::new(),
            IcebergRestCatalogOptions {
                credentials: Arc::new(FileCatalogCredentials::new(&token_path)),
                properties,
            },
        );

        let config = catalog.resolved_catalog_config().await.unwrap();
        assert_eq!(config.warehouse().as_deref(), Some("s3://iceberg-catalog"));
        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), "token-b");
    }

    #[tokio::test]
    async fn with_auth_retry_retries_once_on_unauthorized() {
        let ctx = TestContext::new(None).await;
        let calls = AtomicUsize::new(0);
        let outcome: Result<(), ApiError<()>> = ctx
            .catalog
            .with_auth_retry(|_client| {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(error_with_status(reqwest::StatusCode::UNAUTHORIZED))
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();
        assert!(outcome.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn with_auth_retry_does_not_retry_more_than_once() {
        let ctx = TestContext::new(None).await;
        let calls = AtomicUsize::new(0);
        let outcome: Result<(), ApiError<()>> = ctx
            .catalog
            .with_auth_retry(|_client| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(error_with_status(reqwest::StatusCode::UNAUTHORIZED)) }
            })
            .await
            .unwrap();
        assert!(outcome.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn with_auth_retry_does_not_retry_non_unauthorized() {
        let ctx = TestContext::new(None).await;
        let calls = AtomicUsize::new(0);
        let outcome: Result<(), ApiError<()>> = ctx
            .catalog
            .with_auth_retry(|_client| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    Err(error_with_status(
                        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            })
            .await
            .unwrap();
        assert!(outcome.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drop_database_cascade_recovers_when_token_rotates_midway() {
        let dir = TempDir::new().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "token-a").unwrap();

        let server = MockServer::start().await;

        // Bootstrap config. Reachable with the original token.
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "overrides": { "warehouse": "s3://iceberg-catalog" },
                "defaults": {}
            })))
            .mount(&server)
            .await;

        // The namespace still holds one table when the cascade begins. Listing
        // is authorized with the original token.
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/dbc/tables"))
            .and(header("authorization", "Bearer token-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [ { "namespace": ["dbc"], "name": "t1" } ]
            })))
            .mount(&server)
            .await;

        // The first drop of the table arrives with the old token. The server
        // rejects it with 401 and, at that moment, the projected token file
        // rotates to a new value (as kubelet would swap it).
        let rotate_path = token_path.clone();
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/dbc/tables/t1"))
            .and(header("authorization", "Bearer token-a"))
            .respond_with(move |_req: &Request| {
                std::fs::write(&rotate_path, "token-b").unwrap();
                ResponseTemplate::new(401).set_body_json(serde_json::json!({
                    "error": {
                        "message": "token expired",
                        "type": "NotAuthorizedException",
                        "code": 401
                    }
                }))
            })
            .expect(1)
            .mount(&server)
            .await;

        // The retry re-reads the rotated token and is authorized.
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/dbc/tables/t1"))
            .and(header("authorization", "Bearer token-b"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        // The remaining cascade requests all use the rotated token.
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/dbc/views"))
            .and(header("authorization", "Bearer token-b"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/dbc"))
            .and(header("authorization", "Bearer token-b"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let props = HashMap::from([(REST_CATALOG_PROP_URI.to_string(), server.uri())]);
        let options = IcebergRestCatalogOptions {
            credentials: Arc::new(FileCatalogCredentials::new(&token_path)),
            properties: props,
        };
        let catalog = IcebergRestCatalogProvider::new(String::new(), options);

        let namespace = Namespace::try_from(vec!["dbc".to_string()]).unwrap();
        let result = catalog
            .drop_database(
                &namespace,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await;

        assert!(result.is_ok(), "cascade drop should succeed: {result:?}");
        // The rotated token is the one the file ends up holding, and every
        // mounted request expectation (including the retried table drop) is
        // verified when the server is dropped.
        assert_eq!(
            std::fs::read_to_string(&token_path).unwrap(),
            "token-b".to_string()
        );
    }
}
