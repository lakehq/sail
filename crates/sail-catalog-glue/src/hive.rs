/// Hive-style table creation logic for AWS Glue Data Catalog.
use std::collections::{HashMap, HashSet};

use aws_sdk_glue::types::{SerDeInfo, StorageDescriptor, TableInput};
use aws_sdk_glue::Client;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateTableColumnOptions, CreateTableOptions, Namespace, PartitionTransform,
};
use sail_common_datafusion::catalog::TableStatus;

use crate::data_type::arrow_to_glue_type;
use crate::format::GlueStorageFormat;
use crate::GlueCatalogProvider;

/// Validated options for Hive table creation.
pub(crate) struct ValidatedHiveOptions {
    pub columns: Vec<CreateTableColumnOptions>,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub format: String,
    pub partition_by: Vec<String>,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

/// Creates a Hive-style table in AWS Glue using the TableInput API.
pub(crate) async fn create_hive_table(
    provider: &GlueCatalogProvider,
    client: &Client,
    database: &Namespace,
    table: &str,
    options: CreateTableOptions,
) -> CatalogResult<TableStatus> {
    let db_name = database.to_string();

    let ValidatedHiveOptions {
        columns,
        comment,
        location,
        format,
        partition_by,
        if_not_exists,
        properties,
    } = validate_hive_options(options)?;

    let format_info = GlueStorageFormat::from_format(&format)?;

    let (regular_columns, partition_columns) = build_glue_columns(columns, &partition_by)?;

    let storage_descriptor =
        build_storage_descriptor(regular_columns, &format_info, location.as_deref());

    let table_input = build_table_input(
        table,
        storage_descriptor,
        partition_columns,
        comment.as_deref(),
        properties,
    )?;

    let result = client
        .create_table()
        .database_name(&db_name)
        .table_input(table_input)
        .send()
        .await;

    match result {
        Ok(_) => provider.get_table(database, table).await,
        Err(sdk_err) => {
            let service_err = sdk_err.into_service_error();
            if service_err.is_already_exists_exception() {
                if if_not_exists {
                    provider.get_table(database, table).await
                } else {
                    Err(CatalogError::AlreadyExists("table", table.to_string()))
                }
            } else {
                Err(CatalogError::External(format!(
                    "Failed to create table: {service_err}"
                )))
            }
        }
    }
}

/// Validates CreateTableOptions for Hive-style tables.
fn validate_hive_options(options: CreateTableOptions) -> CatalogResult<ValidatedHiveOptions> {
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
            "AWS Glue catalog does not support REPLACE".to_string(),
        ));
    }
    if !constraints.is_empty() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support CONSTRAINT".to_string(),
        ));
    }
    if !sort_by.is_empty() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support SORT BY".to_string(),
        ));
    }
    if !table_options.is_empty() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support OPTIONS".to_string(),
        ));
    }
    if bucket_by.is_some() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support BUCKET BY".to_string(),
        ));
    }

    // Hive-style tables only support identity partitions
    if partition_by
        .iter()
        .any(|f| f.transform.is_some() && f.transform != Some(PartitionTransform::Identity))
    {
        return Err(CatalogError::NotSupported(
            "Partition transforms are only supported for Iceberg tables (use format: 'iceberg')"
                .to_string(),
        ));
    }

    // Extract just the column names for partitioning
    let partition_columns: Vec<String> = partition_by.iter().map(|f| f.column.clone()).collect();

    Ok(ValidatedHiveOptions {
        columns,
        comment,
        location,
        format,
        partition_by: partition_columns,
        if_not_exists,
        properties,
    })
}

/// Builds Glue columns from CreateTableColumnOptions, separating regular and partition columns.
fn build_glue_columns(
    columns: Vec<CreateTableColumnOptions>,
    partition_by: &[String],
) -> CatalogResult<(
    Vec<aws_sdk_glue::types::Column>,
    Vec<aws_sdk_glue::types::Column>,
)> {
    let partition_set: HashSet<_> = partition_by.iter().map(|s| s.to_lowercase()).collect();

    let mut regular_columns = Vec::new();
    let mut partition_columns = Vec::new();

    for col in columns {
        let glue_type = arrow_to_glue_type(&col.data_type)?;
        let glue_col = aws_sdk_glue::types::Column::builder()
            .name(&col.name)
            .r#type(glue_type)
            .set_comment(col.comment.clone())
            .build()
            .map_err(|e| CatalogError::External(format!("Failed to build column: {e}")))?;

        if partition_set.contains(&col.name.to_lowercase()) {
            partition_columns.push(glue_col);
        } else {
            regular_columns.push(glue_col);
        }
    }

    Ok((regular_columns, partition_columns))
}

/// Builds a Glue StorageDescriptor from column definitions and format info.
fn build_storage_descriptor(
    columns: Vec<aws_sdk_glue::types::Column>,
    format_info: &GlueStorageFormat,
    location: Option<&str>,
) -> StorageDescriptor {
    let serde_info = SerDeInfo::builder()
        .serialization_library(format_info.serde_library)
        .build();

    let mut builder = StorageDescriptor::builder()
        .set_columns(Some(columns))
        .input_format(format_info.input_format)
        .output_format(format_info.output_format)
        .serde_info(serde_info);

    if let Some(loc) = location {
        builder = builder.location(loc);
    }

    builder.build()
}

/// Builds a Glue TableInput from storage descriptor and metadata.
fn build_table_input(
    table_name: &str,
    storage_descriptor: StorageDescriptor,
    partition_columns: Vec<aws_sdk_glue::types::Column>,
    comment: Option<&str>,
    properties: Vec<(String, String)>,
) -> CatalogResult<TableInput> {
    let parameters: Option<HashMap<String, String>> = if properties.is_empty() {
        None
    } else {
        Some(properties.into_iter().collect())
    };

    let mut builder = TableInput::builder()
        .name(table_name)
        .storage_descriptor(storage_descriptor);

    if let Some(desc) = comment {
        builder = builder.description(desc);
    }

    if !partition_columns.is_empty() {
        builder = builder.set_partition_keys(Some(partition_columns));
    }

    if let Some(params) = parameters {
        builder = builder.set_parameters(Some(params));
    }

    builder
        .build()
        .map_err(|e| CatalogError::InvalidArgument(format!("Failed to build table input: {e}")))
}
