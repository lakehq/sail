use std::sync::Arc;

use reqwest::Client;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};
use serde::Deserialize;
use tokio::sync::OnceCell;

const ONELAKE_TABLE_API_BASE: &str = "https://onelake.table.fabric.microsoft.com/delta";

/// Fetches Azure access token using Azure CLI
async fn get_azure_cli_token() -> CatalogResult<String> {
    // On Windows, az is a batch file (.cmd) and must be run through cmd.exe
    // See: https://doc.rust-lang.org/nightly/std/process/struct.Command.html
    let (program, extra_args): (&str, &[&str]) = if cfg!(target_os = "windows") {
        ("cmd", &["/C", "az"])
    } else {
        ("az", &[])
    };

    let output = tokio::process::Command::new(program)
        .args(extra_args)
        .args([
            "account",
            "get-access-token",
            "--resource",
            "https://storage.azure.com/",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ])
        .output()
        .await
        .map_err(|e| CatalogError::External(format!("Failed to run az cli: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CatalogError::External(format!(
            "Azure CLI failed: {stderr}"
        )));
    }

    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(CatalogError::External(
            "Azure CLI returned empty token".to_string(),
        ));
    }

    Ok(token)
}

/// Converts OneLake https:// URL to abfss:// URL for Delta Lake access
/// From: https://onelake.dfs.fabric.microsoft.com/workspace/item.ItemType/Tables/schema/table
/// To:   abfss://workspace@onelake.dfs.fabric.microsoft.com/item.ItemType/Tables/schema/table
fn convert_to_abfss_url(https_url: &str) -> Option<String> {
    let url = https_url.strip_prefix("https://onelake.dfs.fabric.microsoft.com/")?;
    let (workspace, rest) = url.split_once('/')?;
    Some(format!(
        "abfss://{}@onelake.dfs.fabric.microsoft.com/{}",
        workspace, rest
    ))
}

/// Response from list schemas API
#[derive(Debug, Deserialize)]
struct ListSchemasResponse {
    schemas: Vec<SchemaInfo>,
}

#[derive(Debug, Deserialize)]
struct SchemaInfo {
    name: Option<String>,
    #[allow(dead_code)]
    catalog_name: Option<String>,
    comment: Option<String>,
}

/// Response from list tables API
#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    tables: Vec<TableInfo>,
}

#[derive(Debug, Deserialize)]
struct TableInfo {
    name: Option<String>,
    #[allow(dead_code)]
    catalog_name: Option<String>,
    schema_name: Option<String>,
    #[allow(dead_code)]
    table_type: Option<String>,
    data_source_format: Option<String>,
    storage_location: Option<String>,
    columns: Option<Vec<ColumnInfo>>,
    comment: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ColumnInfo {
    name: Option<String>,
    type_name: Option<String>,
    type_text: Option<String>,
    nullable: Option<bool>,
    comment: Option<String>,
}

/// Configuration for OneLake catalog
#[derive(Debug, Clone)]
pub struct OneLakeCatalogConfig {
    pub workspace: String,
    pub item_name: String,
    pub item_type: String,
    pub bearer_token: Option<String>,
}

/// Provider for Microsoft Fabric OneLake catalog.
///
/// This catalog uses the OneLake Table APIs to list schemas and tables,
/// and returns Delta table locations that can be read/written via abfss://.
pub struct OneLakeCatalogProvider {
    name: String,
    config: OneLakeCatalogConfig,
    client: OnceCell<Arc<Client>>,
}

impl OneLakeCatalogProvider {
    pub fn new(
        name: String,
        workspace: String,
        item_name: String,
        item_type: String,
        bearer_token: Option<String>,
    ) -> Self {
        Self {
            name,
            config: OneLakeCatalogConfig {
                workspace,
                item_name,
                item_type,
                bearer_token,
            },
            client: OnceCell::new(),
        }
    }

    fn base_url(&self) -> String {
        format!(
            "{}/{}/{}.{}",
            ONELAKE_TABLE_API_BASE,
            self.config.workspace,
            self.config.item_name,
            self.config.item_type
        )
    }

    fn catalog_name(&self) -> String {
        format!("{}.{}", self.config.item_name, self.config.item_type)
    }

    async fn get_client(&self) -> CatalogResult<&Arc<Client>> {
        self.client
            .get_or_try_init(|| async {
                // Priority: 1) config token, 2) env var, 3) Azure CLI
                let token = if let Some(token) = &self.config.bearer_token {
                    token.clone()
                } else if let Ok(token) = std::env::var("AZURE_STORAGE_TOKEN") {
                    token
                } else if let Ok(token) = std::env::var("AZURE_ACCESS_TOKEN") {
                    token
                } else {
                    // Fall back to Azure CLI
                    get_azure_cli_token().await?
                };

                let mut headers = reqwest::header::HeaderMap::new();
                let header_value = reqwest::header::HeaderValue::from_str(&format!(
                    "Bearer {token}"
                ))
                .map_err(|e| CatalogError::External(format!("Invalid bearer token: {e}")))?;
                headers.insert(reqwest::header::AUTHORIZATION, header_value);

                let client = Client::builder()
                    .default_headers(headers)
                    .build()
                    .map_err(|e| {
                        CatalogError::External(format!("Failed to build HTTP client: {e}"))
                    })?;

                Ok(Arc::new(client))
            })
            .await
    }

    fn schema_info_to_database_status(&self, info: SchemaInfo) -> DatabaseStatus {
        let schema_name = info.name.unwrap_or_else(|| "dbo".to_string());
        DatabaseStatus {
            catalog: self.name.clone(),
            database: vec![schema_name],
            comment: info.comment,
            location: None,
            properties: vec![],
        }
    }

    fn table_info_to_table_status(&self, info: TableInfo) -> CatalogResult<TableStatus> {
        let name = info.name.unwrap_or_default();
        let schema_name = info.schema_name.unwrap_or_else(|| "dbo".to_string());
        let format = info
            .data_source_format
            .map(|f| f.to_lowercase())
            .unwrap_or_else(|| "delta".to_string());

        // Convert https:// URL to abfss:// for Delta Lake access
        let location = info
            .storage_location
            .and_then(|url| convert_to_abfss_url(&url).or(Some(url)));

        let columns = info
            .columns
            .unwrap_or_default()
            .into_iter()
            .map(|col| {
                // Prefer type_name over type_text (OneLake API often returns type_text as null)
                let type_str = col
                    .type_name
                    .or(col.type_text)
                    .unwrap_or_else(|| "string".to_string());
                let data_type = parse_type_text(&type_str);
                TableColumnStatus {
                    name: col.name.unwrap_or_default(),
                    data_type,
                    nullable: col.nullable.unwrap_or(true),
                    comment: col.comment,
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }
            })
            .collect();

        Ok(TableStatus {
            name,
            kind: TableKind::Table {
                catalog: self.name.clone(),
                database: vec![schema_name],
                columns,
                comment: info.comment,
                constraints: vec![],
                location,
                format,
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                options: vec![],
                properties: vec![],
            },
        })
    }
}

/// Simple type text parser - converts Unity Catalog type strings to Arrow DataType
// TODO: Add support for complex types (array, map, struct) if OneLake returns them in JSON format.
fn parse_type_text(type_text: &str) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;

    let lower = type_text.to_lowercase();

    // Handle decimal(precision, scale) format
    if lower.starts_with("decimal") {
        if let Some(params) = lower
            .strip_prefix("decimal(")
            .and_then(|s| s.strip_suffix(')'))
        {
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                if let (Ok(precision), Ok(scale)) =
                    (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>())
                {
                    return DataType::Decimal128(precision, scale);
                }
            }
        }
        // Default decimal if parsing fails
        return DataType::Decimal128(38, 18);
    }

    match lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "byte" | "tinyint" => DataType::Int8,
        "short" | "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" | "real" => DataType::Float32,
        "double" => DataType::Float64,
        "string" | "varchar" | "text" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" | "timestamp_ntz" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        _ => DataType::Utf8, // Default to string for unknown types
    }
}

#[async_trait::async_trait]
impl CatalogProvider for OneLakeCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support creating databases".to_string(),
        ))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let schema_name = database.head_to_string();
        let client = self.get_client().await?;

        // OneLake API requires full qualified schema name: catalog.schema
        let full_schema_name = format!("{}.{}", self.catalog_name(), schema_name);
        let url = format!(
            "{}/api/2.1/unity-catalog/schemas/{}",
            self.base_url(),
            full_schema_name
        );

        let response = client
            .head(&url)
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to check schema: {e}")))?;

        if response.status().is_success() {
            Ok(DatabaseStatus {
                catalog: self.name.clone(),
                database: vec![schema_name],
                comment: None,
                location: None,
                properties: vec![],
            })
        } else if response.status().as_u16() == 404 {
            Err(CatalogError::NotFound("schema", schema_name))
        } else {
            Err(CatalogError::External(format!(
                "Failed to get schema: HTTP {}",
                response.status()
            )))
        }
    }

    async fn list_databases(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let client = self.get_client().await?;

        // TODO: Use Url methods to construct URLs in case catalog/schema/table names contain special characters.
        let url = format!(
            "{}/api/2.1/unity-catalog/schemas?catalog_name={}",
            self.base_url(),
            self.catalog_name()
        );

        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list schemas: {e}")))?;

        if !response.status().is_success() {
            return Err(CatalogError::External(format!(
                "Failed to list schemas: HTTP {}",
                response.status()
            )));
        }

        let list_response: ListSchemasResponse = response.json().await.map_err(|e| {
            CatalogError::External(format!("Failed to parse schemas response: {e}"))
        })?;

        Ok(list_response
            .schemas
            .into_iter()
            .map(|s| self.schema_info_to_database_status(s))
            .collect())
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support dropping databases".to_string(),
        ))
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support creating tables via API".to_string(),
        ))
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let schema_name = database.head_to_string();
        let client = self.get_client().await?;

        let full_table_name = format!("{}.{}.{}", self.catalog_name(), schema_name, table);
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.base_url(),
            full_table_name
        );

        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to get table: {e}")))?;

        if response.status().as_u16() == 404 {
            return Err(CatalogError::NotFound("table", table.to_string()));
        }

        if !response.status().is_success() {
            return Err(CatalogError::External(format!(
                "Failed to get table: HTTP {}",
                response.status()
            )));
        }

        let table_info: TableInfo = response
            .json()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to parse table response: {e}")))?;

        self.table_info_to_table_status(table_info)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let schema_name = database.head_to_string();
        let client = self.get_client().await?;

        let url = format!(
            "{}/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}",
            self.base_url(),
            self.catalog_name(),
            schema_name
        );

        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?;

        if !response.status().is_success() {
            return Err(CatalogError::External(format!(
                "Failed to list tables: HTTP {}",
                response.status()
            )));
        }

        let list_response: ListTablesResponse = response
            .json()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to parse tables response: {e}")))?;

        list_response
            .tables
            .into_iter()
            .map(|t| self.table_info_to_table_status(t))
            .collect()
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support dropping tables via API".to_string(),
        ))
    }

    async fn create_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support views".to_string(),
        ))
    }

    async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support views".to_string(),
        ))
    }

    async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support views".to_string(),
        ))
    }

    async fn drop_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: DropViewOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "OneLake catalog does not support views".to_string(),
        ))
    }
}
