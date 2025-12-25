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
use std::sync::Arc;

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions,
    DropViewOptions, Namespace,
};
use sail_catalog::utils::get_property;
use sail_common_datafusion::catalog::{
    CatalogTableConstraint, CatalogTableSort, DatabaseStatus, TableColumnStatus, TableKind,
    TableStatus,
};
use sail_iceberg::{arrow_type_to_iceberg, iceberg_type_to_arrow, NestedField, StructType};
use tokio::sync::OnceCell;

use crate::apis::configuration::Configuration;
use crate::apis::{self, Api, ApiClient};

pub const REST_CATALOG_PROP_URI: &str = "uri";

pub const REST_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

pub const REST_CATALOG_PROP_PREFIX: &str = "prefix";

// TODO: Further properties and configurations may be needed from:
//  - https://iceberg.apache.org/docs/nightly/configuration/#catalog-properties
//  - https://iceberg.apache.org/docs/nightly/spark-configuration/
#[derive(Clone, Debug)]
pub struct RestCatalogConfig {
    uri: String,
    warehouse: Option<String>,
    props: HashMap<String, String>,
}

/// Provider for Apache Iceberg REST Catalog.
pub struct IcebergRestCatalogProvider {
    name: String,
    catalog_config: RestCatalogConfig,
    merged_catalog_config: OnceCell<RestCatalogConfig>,
    client: OnceCell<ApiClient>,
}

impl IcebergRestCatalogProvider {
    pub fn new(name: String, props: HashMap<String, String>) -> Self {
        let catalog_config = RestCatalogConfig {
            uri: props
                .get(REST_CATALOG_PROP_URI)
                .cloned()
                .unwrap_or(Configuration::new().base_path),
            warehouse: props.get(REST_CATALOG_PROP_WAREHOUSE).cloned(),
            props: props
                .into_iter()
                .filter(|(k, _)| k != REST_CATALOG_PROP_URI && k != REST_CATALOG_PROP_WAREHOUSE)
                .collect(),
        };

        Self {
            name,
            catalog_config,
            merged_catalog_config: OnceCell::new(),
            client: OnceCell::new(),
        }
    }

    fn init_client(&self, catalog_config: &RestCatalogConfig) -> CatalogResult<ApiClient> {
        let mut client_config = Configuration::new();
        client_config.user_agent = Some("Sail".to_string());
        client_config.base_path = catalog_config.uri.to_string();
        for (key, value) in &catalog_config.props {
            // TODO: `basic_auth` and `api_key` are not used anything in the API yet.
            //  We may need to support them in the future.
            match key.as_str() {
                "oauth-access-token" => {
                    client_config.oauth_access_token = Some(value.to_string());
                }
                "bearer-access-token" => {
                    client_config.bearer_access_token = Some(value.to_string());
                }
                _ => {}
            }
        }
        Ok(ApiClient::new(Arc::new(client_config)))
    }

    async fn load_catalog_config(
        &self,
        client: &ApiClient,
        warehouse: Option<&str>,
    ) -> CatalogResult<crate::models::CatalogConfig> {
        let config = client
            .configuration_api_api()
            .get_config(warehouse)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to load config: {e}")))?;
        Ok(config)
    }

    // Merge the `RestCatalogConfig` with the a [`CatalogConfig`] (fetched from the REST server).
    // Then initialize the `ApiClient` with the merged config.
    // This only happens once, then the result is cached.
    async fn load_client_and_merged_config(
        &self,
    ) -> CatalogResult<(&ApiClient, &RestCatalogConfig)> {
        let merged_catalog_config = self
            .merged_catalog_config
            .get_or_try_init(|| async {
                let temp_client = self.init_client(&self.catalog_config)?;
                let mut config = self
                    .load_catalog_config(&temp_client, self.catalog_config.warehouse.as_deref())
                    .await?;

                let uri = if let Some(uri) = config.overrides.remove(REST_CATALOG_PROP_URI) {
                    uri
                } else {
                    self.catalog_config.uri.clone()
                };

                let mut props = config.defaults;
                props.extend(self.catalog_config.props.clone());
                props.extend(config.overrides);

                Ok::<_, CatalogError>(RestCatalogConfig {
                    uri,
                    warehouse: self.catalog_config.warehouse.clone(),
                    props,
                })
            })
            .await?;

        let client = self
            .client
            .get_or_try_init(|| async { self.init_client(merged_catalog_config) })
            .await?;

        Ok((client, merged_catalog_config))
    }

    fn load_table_result_to_status(
        &self,
        table_name: &str,
        database: &Namespace,
        result: crate::models::LoadTableResult,
    ) -> CatalogResult<TableStatus> {
        // TODO: Do we want to do anything with:
        //  - `result.config``
        //  - `result.storage_credentials`
        //  - Unused fields in `TableMetadata`?
        let crate::models::TableMetadata {
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

        let current_schema = if let Some(schemas) = &schemas {
            if let Some(schema_id) = current_schema_id {
                schemas
                    .iter()
                    .find(|s| s.schema_id == Some(schema_id))
                    .or_else(|| schemas.last())
            } else {
                schemas.last()
            }
        } else {
            None
        };

        let default_partition_spec = partition_specs.as_ref().and_then(|specs| {
            if let Some(spec_id) = default_spec_id {
                specs
                    .iter()
                    .find(|s| s.spec_id == Some(spec_id))
                    .or_else(|| specs.last())
            } else {
                specs.last()
            }
        });

        let partition_field_ids: std::collections::HashSet<i32> = default_partition_spec
            .map(|spec| spec.fields.iter().map(|f| f.source_id).collect())
            .unwrap_or_default();

        let bucket_field_ids: std::collections::HashSet<i32> = default_partition_spec
            .map(|spec| {
                spec.fields
                    .iter()
                    .filter(|f| f.transform.trim().to_lowercase().starts_with("bucket"))
                    .map(|f| f.source_id)
                    .collect()
            })
            .unwrap_or_default();

        let partition_by: Vec<String> = default_partition_spec
            .map(|spec| spec.fields.iter().map(|f| f.name.clone()).collect())
            .unwrap_or_default();

        let columns = if let Some(schema) = current_schema {
            let mut cols = Vec::new();
            for field in &schema.fields {
                let data_type = iceberg_type_to_arrow(&field.field_type).map_err(|e| {
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
                    is_partition: partition_field_ids.contains(&field_id),
                    is_bucket: bucket_field_ids.contains(&field_id),
                    is_cluster: false,
                });
            }
            cols
        } else {
            Vec::new()
        };

        let default_sort_order = sort_orders.as_ref().and_then(|orders| {
            if let Some(order_id) = default_sort_order_id {
                orders
                    .iter()
                    .find(|o| o.order_id == order_id)
                    .or_else(|| orders.last())
            } else {
                orders.last()
            }
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
                                    let ascending = match sort_field.direction {
                                        crate::models::SortDirection::Asc => true,
                                        crate::models::SortDirection::Desc => false,
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

        if let Some(metadata_location) = result.metadata_location {
            properties.insert("metadata-location".to_string(), metadata_location);
        }
        properties.insert(
            "metadata.format-version".to_string(),
            format_version.to_string(),
        );
        properties.insert("metadata.table-uuid".to_string(), table_uuid);
        if let Some(last_updated_ms) = last_updated_ms {
            properties.insert(
                "metadata.last-updated-ms".to_string(),
                last_updated_ms.to_string(),
            );
        }
        if let Some(next_row_id) = next_row_id {
            properties.insert("metadata.next-row-id".to_string(), next_row_id.to_string());
        }
        if let Some(current_schema_id) = current_schema_id {
            properties.insert(
                "metadata.current-schema-id".to_string(),
                current_schema_id.to_string(),
            );
        }
        if let Some(last_column_id) = last_column_id {
            properties.insert(
                "metadata.last-column-id".to_string(),
                last_column_id.to_string(),
            );
        }
        if let Some(default_spec_id) = default_spec_id {
            properties.insert(
                "metadata.default-spec-id".to_string(),
                default_spec_id.to_string(),
            );
        }
        if let Some(last_partition_id) = last_partition_id {
            properties.insert(
                "metadata.last-partition-id".to_string(),
                last_partition_id.to_string(),
            );
        }
        if let Some(default_sort_order_id) = default_sort_order_id {
            properties.insert(
                "metadata.default-sort-order-id".to_string(),
                default_sort_order_id.to_string(),
            );
        }
        if let Some(current_snapshot_id) = current_snapshot_id {
            properties.insert(
                "metadata.current-snapshot-id".to_string(),
                current_snapshot_id.to_string(),
            );
        }
        if let Some(last_sequence_number) = last_sequence_number {
            properties.insert(
                "metadata.last-sequence-number".to_string(),
                last_sequence_number.to_string(),
            );
        }
        if let Some(statistics) = statistics {
            properties.insert(
                "metadata.statistics".to_string(),
                serde_json::to_string(&statistics).unwrap_or_default(),
            );
        }
        if let Some(partition_statistics) = partition_statistics {
            properties.insert(
                "metadata.partition-statistics".to_string(),
                serde_json::to_string(&partition_statistics).unwrap_or_default(),
            );
        }

        let properties: Vec<_> = properties.into_iter().collect();

        Ok(TableStatus {
            name: table_name.to_string(),
            kind: TableKind::Table {
                catalog: self.name.clone(),
                database: database.clone().into(),
                columns,
                comment,
                constraints,
                location,
                format: "iceberg".to_string(),
                partition_by,
                sort_by,
                bucket_by: None,
                options,
                properties,
            },
        })
    }

    fn load_view_result_to_status(
        &self,
        view_name: &str,
        database: &Namespace,
        result: crate::models::LoadViewResult,
    ) -> CatalogResult<TableStatus> {
        // TODO: Do we want to do anything with:
        //  - `result.config``
        //  - Unused fields in `ViewMetadata`?
        let crate::models::ViewMetadata {
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
            .and_then(|v| {
                v.representations
                    .iter()
                    .find(|r| r.dialect.trim().to_lowercase() == "spark")
                    .or_else(|| v.representations.last())
            })
            .map(|r| r.sql.clone())
            .unwrap_or_default();

        let mut properties: HashMap<String, String> = properties.unwrap_or_default();

        let comment = get_property(&properties, "comment");

        properties.insert("metadata-location".to_string(), result.metadata_location);
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
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

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

        let request = crate::models::CreateNamespaceRequest {
            namespace: database.clone().into(),
            properties: if props.is_empty() { None } else { Some(props) },
        };

        let result = client
            .catalog_api_api()
            .create_namespace(
                request,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await;

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
                    database: result.namespace,
                    comment,
                    location,
                    properties,
                })
            }
            Err(apis::Error::ResponseError(apis::ResponseContent { status, .. }))
                if status == 409 && if_not_exists =>
            {
                self.get_database(database).await
            }
            Err(e) => Err(CatalogError::External(format!(
                "Failed to create namespace: {e}"
            ))),
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let namespace_str = database.to_string();

        let result = client
            .catalog_api_api()
            .load_namespace_metadata(
                &namespace_str,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| match e {
                apis::Error::ResponseError(apis::ResponseContent { status, .. }) => {
                    if status == 404 {
                        CatalogError::NotFound("namespace", database.to_string())
                    } else {
                        CatalogError::External(format!("Failed to load namespace {database}: {e}"))
                    }
                }
                _ => CatalogError::External(format!("Failed to load namespace {database}: {e}")),
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
            database: result.namespace,
            comment,
            location,
            properties,
        })
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let parent = prefix.map(|namespace| namespace.to_string());

        let result = client
            .catalog_api_api()
            .list_namespaces(
                None,
                None,
                parent.as_deref(),
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list namespaces: {e}")))?;

        Ok(result
            .namespaces
            .unwrap_or_default()
            .into_iter()
            .map(|namespace| DatabaseStatus {
                catalog: self.get_name().to_string(),
                database: namespace,
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
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

        let DropDatabaseOptions {
            if_exists,
            cascade: _,
        } = options;

        match client
            .catalog_api_api()
            .drop_namespace(
                &database.to_string(),
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
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

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

        let CreateTableOptions {
            columns,
            comment,
            constraints,
            location,
            format: _, // TODO: What to do with format?
            partition_by,
            sort_by,
            bucket_by,
            if_not_exists,
            replace,
            options,
            properties,
        } = options;

        if if_not_exists {
            if let Ok(existing) = self.get_table(database, table).await {
                return Ok(existing);
            }
        }

        if replace {
            return Err(CatalogError::NotSupported(
                "Replace table is not supported yet".to_string(),
            ));
        }

        if bucket_by.is_some() {
            return Err(CatalogError::NotSupported(
                "Bucketed table is not supported yet".to_string(),
            ));
        }

        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let CreateTableColumnOptions {
                name,
                data_type,
                nullable,
                comment,
                default: _,
                generated_always_as: _, // TODO: Support generated_always_as
            } = col;
            let field_id = idx as i32 + 1; // FIXME: Is this wrong?
            let field_type = arrow_type_to_iceberg(data_type).map_err(|e| {
                CatalogError::External(format!(
                    "Failed to convert Arrow type to Iceberg type for column '{name}': {e}"
                ))
            })?;

            // TODO: `default` is not supported until Iceberg V3
            // let default_literal = if let Some(default) = default {
            //     let json_default: serde_json::Value =
            //         serde_json::from_str(default).map_err(|e| {
            //             CatalogError::External(format!(
            //                 "Failed to parse default value as JSON for column '{name}': {e}"
            //             ))
            //         })?;
            //     Literal::try_from_json(json_default.clone(), &field_type).map_err(|e| {
            //         CatalogError::External(format!(
            //             "Failed to convert default value to Iceberg literal for column '{name}': {e}"
            //         ))
            //     })?
            // } else {
            //     None
            // };
            let mut field = NestedField::new(field_id, name.clone(), field_type, !nullable);
            if let Some(comment) = comment {
                field = field.with_doc(comment);
            }
            fields.push(Arc::new(field));
        }

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
        let schema = crate::models::Schema {
            r#type: crate::models::schema::Type::Struct,
            fields: schema.fields().to_vec(),
            schema_id: Some(schema.schema_id()),
            identifier_field_ids: Some(schema.identifier_field_ids().collect()),
        };

        let partition_spec = build_partition_spec(&partition_by, &name_to_id);
        let write_order = build_sort_order(&sort_by, &name_to_id)?;

        let mut props = HashMap::new();
        // TODO: Is this correct for options?
        for (k, v) in options {
            props.insert(format!("options.{k}"), v);
        }
        if let Some(c) = comment {
            props.insert("comment".to_string(), c);
        }
        for (k, v) in properties {
            props.insert(k, v);
        }

        let request = crate::models::CreateTableRequest {
            name: table.to_string(),
            location,
            schema: Box::new(schema),
            partition_spec,
            write_order,
            stage_create: None,
            properties: if props.is_empty() { None } else { Some(props) },
        };

        let result = client
            .catalog_api_api()
            .create_table(
                &database.to_string(),
                request,
                None,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create table: {e}")))?;

        self.load_table_result_to_status(table, database, result)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let result = client
            .catalog_api_api()
            .load_table(
                &database.to_string(),
                table,
                None,
                None,
                None,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to load table {database}.{table}: {e}"))
            })?;
        self.load_table_result_to_status(table, database, result)
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

        let result = client
            .catalog_api_api()
            .list_tables(
                &database.to_string(),
                None,
                None,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list tables: {e}")))?;

        Ok(result
            .identifiers
            .unwrap_or_default()
            .into_iter()
            .map(|identifier| TableStatus {
                name: identifier.name,
                kind: TableKind::Table {
                    catalog: self.name.clone(),
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

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let DropTableOptions { if_exists, purge } = options;
        match client
            .catalog_api_api()
            .drop_table(
                &database.to_string(),
                table,
                Some(purge),
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
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

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties,
        } = options;

        if if_not_exists || replace {
            if let Ok(existing) = self.get_view(database, view).await {
                if if_not_exists {
                    return Ok(existing);
                }
                if replace {
                    return Err(CatalogError::NotSupported(
                        "Replace view is not supported yet".to_string(),
                    ));
                }
            }
        }

        let mut fields = Vec::new();
        for (idx, col) in columns.iter().enumerate() {
            let CreateViewColumnOptions {
                name,
                data_type,
                nullable,
                comment,
            } = col;
            let field_id = idx as i32 + 1; // FIXME: Is this wrong?
            let field_type = arrow_type_to_iceberg(data_type).map_err(|e| {
                CatalogError::External(format!(
                    "Failed to convert Arrow type to Iceberg type for column '{name}': {e}"
                ))
            })?;
            let mut field = NestedField::new(field_id, name.clone(), field_type, !nullable);
            if let Some(comment) = comment {
                field = field.with_doc(comment);
            }
            fields.push(Arc::new(field));
        }

        let schema = sail_iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| CatalogError::External(format!("Failed to build schema: {e}")))?;
        let schema = crate::models::Schema {
            r#type: crate::models::schema::Type::Struct,
            fields: schema.fields().to_vec(),
            schema_id: Some(schema.schema_id()),
            identifier_field_ids: Some(schema.identifier_field_ids().collect()),
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

        // TODO: Is this correct?
        let view_version = crate::models::ViewVersion {
            version_id: 1,
            timestamp_ms,
            schema_id: schema
                .schema_id
                .ok_or_else(|| CatalogError::External("Schema ID is missing".to_string()))?,
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
            location: None, // TODO: Is this correct?
            schema: Box::new(schema),
            view_version: Box::new(view_version),
            properties: props,
        };

        let result = client
            .catalog_api_api()
            .create_view(
                &database.to_string(),
                request,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create view: {e}")))?;

        self.load_view_result_to_status(view, database, result)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let result = client
            .catalog_api_api()
            .load_view(
                &database.to_string(),
                view,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to load view {database}.{view}: {e}"))
            })?;
        self.load_view_result_to_status(view, database, result)
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let (client, catalog_config) = self.load_client_and_merged_config().await?;

        let result = client
            .catalog_api_api()
            .list_views(
                &database.to_string(),
                None,
                None,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list views: {e}")))?;
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
        let (client, catalog_config) = self.load_client_and_merged_config().await?;
        let DropViewOptions { if_exists } = options;
        match client
            .catalog_api_api()
            .drop_view(
                &database.to_string(),
                view,
                catalog_config
                    .props
                    .get(REST_CATALOG_PROP_PREFIX)
                    .map(|s| s.as_str()),
            )
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

fn build_partition_spec(
    partition_by: &[String],
    name_to_id: &HashMap<String, i32>,
) -> Option<Box<crate::models::PartitionSpec>> {
    if partition_by.is_empty() {
        return None;
    }
    let mut partition_spec_builder = sail_iceberg::PartitionSpec::builder();
    for partition_by_col in partition_by {
        if let Some(&source_id) = name_to_id.get(partition_by_col) {
            partition_spec_builder = partition_spec_builder.add_field(
                source_id,
                partition_by_col,
                sail_iceberg::Transform::Identity, // FIXME: This is wrong, col needs to be parsed.
            );
        }
    }
    let spec = partition_spec_builder.build();
    Some(Box::new(crate::models::PartitionSpec {
        spec_id: Some(spec.spec_id()),
        fields: spec
            .fields()
            .iter()
            .map(|f| crate::models::PartitionField {
                field_id: Some(f.field_id),
                source_id: f.source_id,
                name: f.name.to_string(),
                transform: f.transform.to_string(),
            })
            .collect(),
    }))
}

fn build_sort_order(
    sort_by: &[CatalogTableSort],
    name_to_id: &HashMap<String, i32>,
) -> CatalogResult<Option<Box<crate::models::SortOrder>>> {
    if sort_by.is_empty() {
        return Ok(None);
    }

    let mut sort_fields = Vec::new();
    for sort in sort_by {
        if let Some(&source_id) = name_to_id.get(&sort.column) {
            sort_fields.push(sail_iceberg::spec::sort::SortField {
                source_id,
                transform: sail_iceberg::Transform::Identity, // FIXME: This is wrong, col needs to be parsed.
                direction: if sort.ascending {
                    sail_iceberg::spec::sort::SortDirection::Ascending
                } else {
                    sail_iceberg::spec::sort::SortDirection::Descending
                },
                null_order: sail_iceberg::spec::sort::NullOrder::Last, // TODO: Should this be configurable?
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

    Ok(Some(Box::new(crate::models::SortOrder {
        order_id: i32::try_from(order.order_id).map_err(|e| {
            CatalogError::External(format!("Failed to convert sort order ID to i32: {e}"))
        })?,
        fields: order
            .fields
            .iter()
            .map(|f| crate::models::SortField {
                source_id: f.source_id,
                transform: f.transform.to_string(),
                direction: if f.direction == sail_iceberg::spec::sort::SortDirection::Ascending {
                    crate::models::SortDirection::Asc
                } else {
                    crate::models::SortDirection::Desc
                },
                null_order: if f.null_order == sail_iceberg::spec::sort::NullOrder::First {
                    crate::models::NullOrder::NullsFirst
                } else {
                    crate::models::NullOrder::NullsLast
                },
            })
            .collect(),
    })))
}

#[allow(clippy::unwrap_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            let catalog = IcebergRestCatalogProvider::new(name_str.to_string(), props);

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
        match result.kind {
            TableKind::Table {
                catalog,
                database,
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
                assert_eq!(catalog, ctx.name);
                assert_eq!(database, vec!["db1".to_string()]);
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

                assert_eq!(partition_by, vec!["category".to_string()]);

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

                assert!(properties
                    .iter()
                    .any(|(k, v)| k == "comment" && v == "test table"));
                assert!(properties
                    .iter()
                    .any(|(k, v)| k == "owner" && v == "test_user"));
            }
            _ => panic!("Expected Table kind"),
        }
    }

    #[tokio::test]
    async fn test_get_table() {
        test_get_table_impl(None).await;
        test_get_table_impl(Some("test")).await;
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
        match result.kind {
            TableKind::View {
                catalog,
                database,
                definition,
                columns,
                comment,
                properties,
            } => {
                assert_eq!(catalog, ctx.name);
                assert_eq!(database, vec!["db1".to_string()]);
                assert_eq!(definition, "SELECT id, data FROM table1 WHERE id > 100");

                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert!(!columns[0].nullable);

                assert_eq!(columns[1].name, "data");
                assert!(columns[1].nullable);
                assert_eq!(columns[1].comment, Some("filtered data".to_string()));

                assert_eq!(comment, Some("test view".to_string()));
                assert!(properties
                    .iter()
                    .any(|(k, v)| k == "comment" && v == "test view"));
                assert!(properties
                    .iter()
                    .any(|(k, v)| k == "created_by" && v == "test_user"));
            }
            _ => panic!("Expected View kind"),
        }
    }

    #[tokio::test]
    async fn test_get_view() {
        test_get_view_impl(None).await;
        test_get_view_impl(Some("test")).await;
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
        assert!(db
            .properties
            .iter()
            .any(|(k, v)| k == "custom_prop" && v == "custom_value"));

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
        assert!(db
            .properties
            .iter()
            .any(|(k, v)| k == "custom_prop" && v == "custom_value"));
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
        assert!(result
            .properties
            .iter()
            .any(|(k, v)| k == "comment" && v == "test database"));
        assert!(result
            .properties
            .iter()
            .any(|(k, v)| k == "location" && v == "s3://bucket/db1"));
        assert!(result
            .properties
            .iter()
            .any(|(k, v)| k == "owner" && v == "alice"));
        assert!(result
            .properties
            .iter()
            .any(|(k, v)| k == "custom_prop" && v == "custom_value"));

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
}
