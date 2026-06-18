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

use std::collections::HashMap;
use std::str::FromStr;

use arrow::datatypes::DataType;
use reqwest::header::HeaderValue;
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaRatifiedCommit, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest,
    LakehouseCreateMaterialization, LakehouseCreatePlan, LakehouseCreateRequest,
};
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    TableFormatCreateMetadataMode,
};
use sail_catalog::utils::{get_property, quote_namespace_if_needed};
use sail_common_datafusion::catalog::delta::{
    unity_table_id_value, DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY,
};
use sail_common_datafusion::catalog::{
    identity_partition_fields, DatabaseStatus, TableColumnStatus, TableKind, TableStatus,
};
use secrecy::SecretString;
use tokio::sync::OnceCell;

use crate::config::UnityCatalogConfig;
use crate::credential::CredentialProvider;
use crate::data_type::{
    data_type_to_unity_type, unity_struct_field_type_json, unity_type_to_data_type,
};
use crate::unity::{types, Client};

pub(crate) const DEFAULT_URI: &str = "http://localhost:8080/api/2.1/unity-catalog";

/// Provider for Unity Catalog
pub struct UnityCatalogProvider {
    name: String,
    catalog_config: UnityCatalogConfig,
    client: OnceCell<Client>,
}

impl UnityCatalogProvider {
    pub fn new(
        name: String,
        default_catalog: &Option<String>,
        uri: &Option<String>,
        token: &Option<SecretString>,
    ) -> CatalogResult<Self> {
        Self::with_options(name, default_catalog.clone(), uri.clone(), token, None)
    }

    pub fn with_options(
        name: String,
        default_catalog: Option<String>,
        uri: Option<String>,
        token: &Option<SecretString>,
        options: Option<HashMap<String, String>>,
    ) -> CatalogResult<Self> {
        let catalog_config = UnityCatalogConfig::new(default_catalog, uri, token, options)?;
        Ok(Self {
            name,
            catalog_config,
            client: OnceCell::new(),
        })
    }

    async fn get_client(&self) -> CatalogResult<&Client> {
        let config = &self.catalog_config;
        let client = self
            .client
            .get_or_try_init(|| async move {
                let mut client_builder = reqwest::Client::builder();

                if let Some(credential_provider) = &config.credential_provider {
                    let header = match credential_provider {
                        CredentialProvider::BearerToken(token) => {
                            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
                                CatalogError::External(format!(
                                    "Failed to create header value from token: {e}"
                                ))
                            })
                        }
                        CredentialProvider::TokenCredential(cache, cred) => {
                            let oauth_client = reqwest::Client::builder().build().map_err(|e| {
                                CatalogError::External(format!("Failed to build OAuth client: {e}"))
                            })?;
                            let token = cache
                                .get_or_insert_with(|| cred.fetch_token(&oauth_client))
                                .await?;
                            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
                                CatalogError::External(format!(
                                    "Failed to create header value from token: {e}"
                                ))
                            })
                        }
                    }?;
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(reqwest::header::AUTHORIZATION, header);
                    client_builder = client_builder.default_headers(headers);
                }

                if config.allow_http_url {
                    client_builder = client_builder.https_only(false);
                }

                let reqwest_client = client_builder.build().map_err(|e| {
                    CatalogError::External(format!("Failed to build HTTP client: {e}"))
                })?;

                Ok::<_, CatalogError>(Client::new_with_client(&config.uri, reqwest_client))
            })
            .await?;
        Ok(client)
    }

    fn get_catalog_and_schema_name(
        &self,
        namespace: &Namespace,
    ) -> CatalogResult<(String, String)> {
        match namespace.tail.as_slice() {
            [] => Ok((
                self.catalog_config.default_catalog.to_string(),
                namespace.head.to_string(),
            )),
            [x] => Ok((namespace.head.to_string(), x.to_string())),
            _ => Err(CatalogError::InvalidArgument(format!(
                "Unity Catalog does not support multi-level schema name: {}",
                quote_namespace_if_needed(namespace)
            ))),
        }
    }

    // Unity Catalog does not quote names in full names even if they contain special characters,
    // so we only concatenate the names with dot ('.').

    fn get_full_schema_name(catalog_name: &str, schema_name: &str) -> String {
        format!("{catalog_name}.{schema_name}")
    }

    fn get_full_table_name(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
        format!("{catalog_name}.{schema_name}.{table_name}")
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
            catalog: self.name.clone(),
            database,
            comment: schema_info.comment,
            location: get_property(&properties, "location"),
            properties: properties.into_iter().collect(),
        }
    }

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
            ..
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
                    type_interval_type,
                    type_json,
                    type_name,
                    type_precision,
                    type_scale,
                    type_text,
                } = col;
                if partition_index.is_some() {
                    if let Some(col_name) = &name {
                        partition_by.push(col_name.to_string());
                    }
                }
                Ok(TableColumnStatus {
                    name: name.unwrap_or_default(),
                    data_type: unity_type_to_data_type(
                        type_name,
                        type_json,
                        type_text,
                        type_precision,
                        type_scale,
                        type_interval_type,
                    )?,
                    nullable,
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

        let mut properties: HashMap<String, String> = properties.map(|p| p.0).unwrap_or_default();

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
            .is_none_or(|table_type| matches!(table_type, types::TableType::External));
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::get_full_schema_name(&catalog_name, &schema_name);

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
                Err(CatalogError::NotFound(CatalogObject::Schema, full_name))
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
            None => self.catalog_config.default_catalog.to_string(),
            Some(Namespace { head, tail }) if tail.is_empty() => head.to_string(),
            Some(x) => {
                return Err(CatalogError::InvalidArgument(format!(
                    "invalid prefix: {}",
                    quote_namespace_if_needed(x)
                )))
            }
        };
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::get_full_schema_name(&catalog_name, &schema_name);

        let result = client
            .delete_schema()
            .full_name(full_name)
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
            is_external,
            is_write_precondition: _,
        } = options;

        if mode.is_replace() {
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

        if partition_by.iter().any(|f| f.transform.is_some()) {
            return Err(CatalogError::NotSupported(
                "partition transforms are not supported by Unity catalog".to_string(),
            ));
        }

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

        let data_source_format = types::DataSourceFormat::from_str(&format.trim().to_uppercase())
            .map_err(|e| {
            CatalogError::InvalidArgument(format!("Invalid data source format: {e}"))
        })?;

        let unity_columns: Vec<types::ColumnInfo> = columns
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
                Ok(types::ColumnInfo {
                    comment: col.comment.clone(),
                    name: Some(col.name.clone()),
                    nullable: col.nullable,
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
                    type_name: Some(unity_type.type_name),
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
            types::TableType::External
        } else {
            types::TableType::Managed
        };
        let storage_location = if is_external {
            location.ok_or_else(|| {
                CatalogError::External(
                    "Storage location is required for external Unity Catalog tables".to_string(),
                )
            })?
        } else {
            // The SQL planner supplies generated default locations for managed tables. Unity
            // managed table creation must use a staging location allocated by the catalog.
            let request = types::CreateStagingTable::builder()
                .name(table)
                .catalog_name(&catalog_name)
                .schema_name(&schema_name);
            let response = client
                .create_staging_table()
                .body(request)
                .send()
                .await
                .map_err(|e| {
                    CatalogError::External(format!("Failed to create staging table: {e}"))
                })?;
            response.into_inner().staging_location.ok_or_else(|| {
                CatalogError::External(
                    "Unity Catalog staging table response is missing a staging location"
                        .to_string(),
                )
            })?
        };

        let request = types::CreateTable::builder()
            .name(table)
            .catalog_name(&catalog_name)
            .schema_name(&schema_name)
            .table_type(table_type)
            .data_source_format(data_source_format)
            .columns(unity_columns)
            .storage_location(storage_location)
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
                if response.status().as_u16() == 409 && mode.ignore_if_exists() =>
            {
                self.get_table(database, table).await
            }
            Err(progenitor_client::Error::UnexpectedResponse(response)) => {
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
                Err(CatalogError::External(format!(
                    "Failed to create table: HTTP {status}: {body}"
                )))
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::get_full_table_name(&catalog_name, &schema_name, table);

        let result = client.get_table().full_name(&full_name).send().await;

        match result {
            Ok(response) => {
                let table_info = response.into_inner();
                self.table_info_to_table_status(table_info, &catalog_name, &schema_name)
            }
            Err(progenitor_client::Error::UnexpectedResponse(response))
                if response.status().as_u16() == 404 =>
            {
                Err(CatalogError::NotFound(CatalogObject::Table, full_name))
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

        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::get_full_table_name(&catalog_name, &schema_name, table);

        match client.delete_table().full_name(full_name).send().await {
            Ok(_) => Ok(()),
            // The OSS Unity Catalog server currently returns a plain "200 OK" body
            // for DELETE even though the OpenAPI spec declares a JSON response.
            Err(progenitor_client::Error::InvalidResponsePayload(bytes, _))
                if bytes.as_ref() == b"200 OK" =>
            {
                Ok(())
            }
            Err(e)
                if e.status()
                    .is_some_and(|status| status.as_u16() == 404 && if_exists) =>
            {
                Ok(())
            }
            Err(e) => Err(CatalogError::External(format!("Failed to drop table: {e}"))),
        }
    }

    async fn alter_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: AlterTableOptions,
    ) -> CatalogResult<()> {
        // The Unity catalog does not currently propagate ALTER TABLE property changes to
        // the Unity REST API. However, returning `NotSupported` here would abort a Delta
        // storage-side commit that has already succeeded. Treat this as a no-op so the
        // on-disk Delta table remains the source of truth until the REST integration
        // is wired up.
        Ok(())
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        vec![
            LakehouseCapability::CatalogCommit,
            LakehouseCapability::DeltaRatifiedCommits,
        ]
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
            sail_catalog::provider::CreateTableMetadataRequirement::None,
            &self.lakehouse_capabilities(),
        );
        let _ = (database, table);
        if request.options.format.eq_ignore_ascii_case("delta") && !request.options.is_external {
            plan.materialization = LakehouseCreateMaterialization::AfterCatalogTableFormat {
                mode: TableFormatCreateMetadataMode::CatalogCoordinated,
            };
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
        let commit: types::DeltaCommit = serde_json::from_value(update.clone()).map_err(|e| {
            CatalogError::InvalidArgument(format!("Invalid Delta commit payload: {e}"))
        })?;

        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;

        let _ = (database, table);
        match client.commit().body(commit).send().await {
            Ok(_) => Ok(LakehouseCommitOutcome::Committed { context, payload }),
            // The OSS Unity Catalog server currently returns a plain "200 OK" body
            // for Delta commits even though the OpenAPI spec declares a JSON response.
            Err(progenitor_client::Error::InvalidResponsePayload(bytes, _))
                if bytes.as_ref() == b"200 OK" =>
            {
                Ok(LakehouseCommitOutcome::Committed { context, payload })
            }
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 409) => Err(
                CatalogError::Conflict(format!("Unity Catalog Delta commit conflict: {e}")),
            ),
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 400) => {
                Err(CatalogError::InvalidArgument(format!(
                    "Unity Catalog Delta commit invalid argument: {e}"
                )))
            }
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 401) => Err(
                CatalogError::Unauthorized(format!("Unity Catalog Delta commit unauthorized: {e}")),
            ),
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 403) => Err(
                CatalogError::Forbidden(format!("Unity Catalog Delta commit forbidden: {e}")),
            ),
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 429) => Err(
                CatalogError::RateLimited(format!("Unity Catalog Delta commit rate limited: {e}")),
            ),
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 501) => Err(
                CatalogError::NotSupported("Unity Catalog Delta commit endpoint".to_string()),
            ),
            Err(e) => Err(CatalogError::External(format!(
                "Failed to commit Delta table to Unity Catalog: {e}"
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

        let request = types::DeltaGetCommits {
            end_version: request.end_version,
            start_version: request.start_version,
            table_id: table_id.to_string(),
            table_uri,
        };
        let client = self
            .get_client()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load client: {e}")))?;
        let (catalog_name, schema_name) = self.get_catalog_and_schema_name(database)?;
        let full_name = Self::get_full_table_name(&catalog_name, &schema_name, table);

        match client.get_commits().body(request).send().await {
            Ok(response) => {
                let response = response.into_inner();
                Ok(DeltaRatifiedCommitResponse {
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
                })
            }
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 400) => {
                Err(CatalogError::InvalidArgument(format!(
                    "Unity Catalog Delta commit discovery invalid argument: {e}"
                )))
            }
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 404) => {
                Err(CatalogError::NotFound(CatalogObject::Table, full_name))
            }
            Err(e) if e.status().is_some_and(|status| status.as_u16() == 501) => {
                Err(CatalogError::NotSupported(
                    "Unity Catalog Delta commit discovery endpoint".to_string(),
                ))
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to get Delta commits from Unity Catalog: {e}"
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

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn test_config_with_token() {
        let mut options = HashMap::new();
        options.insert("databricks_token".to_string(), "test_token".to_string());
        options.insert(
            "databricks_host".to_string(),
            "https://test.databricks.com".to_string(),
        );

        let config = UnityCatalogConfig::new(None, None, &None, Some(options)).unwrap();
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

        let config = UnityCatalogConfig::new(None, None, &None, Some(options)).unwrap();
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

            let config = UnityCatalogConfig::new(None, None, &None, Some(options)).unwrap();
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

            let result = UnityCatalogConfig::new(None, None, &None, Some(options));
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

        let provider = UnityCatalogProvider::with_options(
            "test".to_string(),
            Some("test_catalog".to_string()),
            None,
            &None,
            Some(options),
        )
        .unwrap();

        assert_eq!(provider.name, "test");
        assert_eq!(provider.catalog_config.default_catalog, "test_catalog");
        assert_eq!(provider.catalog_config.uri, "https://test.databricks.com");
        assert!(provider.catalog_config.bearer_token.is_some());
    }
}
