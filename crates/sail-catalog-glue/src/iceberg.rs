/// Iceberg table creation logic for AWS Glue Data Catalog.
use std::collections::{HashMap, HashSet};

use aws_sdk_glue::types::{
    CreateIcebergTableInput, IcebergInput, IcebergPartitionField, IcebergPartitionSpec,
    IcebergSchema, IcebergStructField, IcebergStructTypeEnum, MetadataOperation,
    OpenTableFormatInput, SerDeInfo, StorageDescriptor, TableInput,
};
use aws_sdk_glue::Client;
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::hive_format::HiveStorageFormat;
use sail_catalog::provider::{
    CatalogPartitionField, CatalogProvider, CreateTableColumnOptions, CreateTableOptions,
    Namespace, PartitionTransform,
};
use sail_common_datafusion::catalog::iceberg::{
    is_iceberg_table_properties, ICEBERG_CLASSIFICATION_KEY, ICEBERG_TABLE_TYPE_KEY,
    ICEBERG_TABLE_TYPE_VALUE,
};
use sail_common_datafusion::catalog::TableStatus;

use crate::data_type::{arrow_to_glue_type, arrow_to_iceberg_type};
use crate::GlueCatalogProvider;

/// Default Iceberg table spec version for new tables (v2 is the modern standard).
const DEFAULT_ICEBERG_FORMAT_VERSION: &str = "2";

pub(crate) fn is_iceberg_parameters(parameters: Option<&HashMap<String, String>>) -> bool {
    parameters.is_some_and(|props| {
        is_iceberg_table_properties(
            props
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
    })
}

/// Validated options for Iceberg table creation.
pub(crate) struct ValidatedIcebergOptions {
    pub columns: Vec<CreateTableColumnOptions>,
    pub location: String,
    pub partition_by: Vec<CatalogPartitionField>,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

/// Creates an Iceberg table in AWS Glue using the OpenTableFormatInput API.
pub(crate) async fn create_iceberg_table(
    provider: &GlueCatalogProvider,
    client: &Client,
    database: &Namespace,
    table: &str,
    options: CreateTableOptions,
) -> CatalogResult<TableStatus> {
    let database_name = GlueCatalogProvider::database_name(database)?;

    let options = validate_iceberg_options(options)?;

    let iceberg_schema = build_iceberg_schema(&options.columns)?;
    let partition_spec = build_iceberg_partition_spec(&options.partition_by, &options.columns)?;

    let mut iceberg_table_builder = CreateIcebergTableInput::builder()
        .location(&options.location)
        .schema(iceberg_schema);

    if let Some(spec) = partition_spec {
        iceberg_table_builder = iceberg_table_builder.partition_spec(spec);
    }

    // Extract format version from properties or use default
    let format_version = options
        .properties
        .iter()
        .find(|(k, _)| k == "format-version" || k == "metadata.format-version")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| DEFAULT_ICEBERG_FORMAT_VERSION.to_string());

    if !options.properties.is_empty() {
        let props: HashMap<String, String> = options.properties.iter().cloned().collect();
        iceberg_table_builder = iceberg_table_builder.set_properties(Some(props));
    }

    let create_iceberg_table = iceberg_table_builder.build().map_err(|e| {
        CatalogError::InvalidArgument(format!("Failed to build CreateIcebergTableInput: {e}"))
    })?;

    let iceberg_input = IcebergInput::builder()
        .metadata_operation(MetadataOperation::Create)
        .version(&format_version)
        .create_iceberg_table_input(create_iceberg_table)
        .build()
        .map_err(|e| CatalogError::InvalidArgument(format!("Failed to build IcebergInput: {e}")))?;

    let open_format_input = OpenTableFormatInput::builder()
        .iceberg_input(iceberg_input)
        .build();

    let result = client
        .create_table()
        .database_name(&database_name)
        .name(table)
        .open_table_format_input(open_format_input)
        .send()
        .await;

    match result {
        Ok(_) => provider.get_table(database, table).await,
        Err(sdk_err) => {
            let service_err = sdk_err.into_service_error();
            if service_err.is_already_exists_exception() {
                if options.if_not_exists {
                    provider.get_table(database, table).await
                } else {
                    Err(CatalogError::AlreadyExists(
                        CatalogObject::Table,
                        table.to_string(),
                    ))
                }
            } else if should_fallback_to_table_input(
                provider.has_custom_endpoint(),
                &service_err.to_string(),
            ) {
                create_iceberg_table_via_table_input(provider, client, database, table, options)
                    .await
            } else {
                Err(CatalogError::External(format!(
                    "Failed to create table: {service_err}"
                )))
            }
        }
    }
}

async fn create_iceberg_table_via_table_input(
    provider: &GlueCatalogProvider,
    client: &Client,
    database: &Namespace,
    table: &str,
    options: ValidatedIcebergOptions,
) -> CatalogResult<TableStatus> {
    let database_name = GlueCatalogProvider::database_name(database)?;
    let partition_columns: Vec<String> = options
        .partition_by
        .iter()
        .filter(|field| matches!(field.transform, None | Some(PartitionTransform::Identity)))
        .map(|field| field.column.clone())
        .collect();
    let partition_set: HashSet<_> = partition_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect();
    let mut regular_columns = Vec::new();
    let mut glue_partition_columns = Vec::new();
    for column in options.columns {
        let glue_type = arrow_to_glue_type(&column.data_type)?;
        let glue_column = aws_sdk_glue::types::Column::builder()
            .name(&column.name)
            .r#type(glue_type)
            .set_comment(column.comment.clone())
            .build()
            .map_err(|e| CatalogError::External(format!("Failed to build column: {e}")))?;
        if partition_set.contains(&column.name.to_ascii_lowercase()) {
            glue_partition_columns.push(glue_column);
        } else {
            regular_columns.push(glue_column);
        }
    }

    let format = HiveStorageFormat::parquet();
    let serde_info = SerDeInfo::builder()
        .serialization_library(format.serde_library)
        .build();
    let storage_descriptor = StorageDescriptor::builder()
        .set_columns(Some(regular_columns))
        .input_format(format.input_format)
        .output_format(format.output_format)
        .serde_info(serde_info)
        .location(options.location)
        .build();
    let mut parameters: HashMap<String, String> = options.properties.into_iter().collect();
    parameters.insert(
        ICEBERG_TABLE_TYPE_KEY.to_string(),
        ICEBERG_TABLE_TYPE_VALUE.to_string(),
    );
    parameters.insert("EXTERNAL".to_string(), "TRUE".to_string());
    parameters
        .entry(ICEBERG_CLASSIFICATION_KEY.to_string())
        .or_insert_with(|| ICEBERG_TABLE_TYPE_VALUE.to_string());

    let table_input = TableInput::builder()
        .name(table)
        .table_type("EXTERNAL_TABLE")
        .storage_descriptor(storage_descriptor)
        .set_partition_keys(Some(glue_partition_columns))
        .set_parameters(Some(parameters))
        .build()
        .map_err(|e| CatalogError::InvalidArgument(format!("Failed to build table input: {e}")))?;

    let result = client
        .create_table()
        .database_name(&database_name)
        .table_input(table_input)
        .send()
        .await;

    match result {
        Ok(_) => provider.get_table(database, table).await,
        Err(sdk_err) => {
            let service_err = sdk_err.into_service_error();
            if service_err.is_already_exists_exception() {
                if options.if_not_exists {
                    provider.get_table(database, table).await
                } else {
                    Err(CatalogError::AlreadyExists(
                        CatalogObject::Table,
                        table.to_string(),
                    ))
                }
            } else {
                Err(CatalogError::External(format!(
                    "Failed to create Glue Iceberg compatibility table: {service_err}"
                )))
            }
        }
    }
}

fn should_fallback_to_table_input(has_custom_endpoint: bool, error: &str) -> bool {
    if !has_custom_endpoint {
        return false;
    }
    let error = error.to_ascii_lowercase();
    error.contains("opentableformat")
        || error.contains("open_table_format")
        || error.contains("unknown parameter")
        || error.contains("not implemented")
        || error.contains("not yet implemented")
        || error.contains("unhandled error")
}

/// Validates CreateTableOptions for Iceberg tables.
pub(crate) fn validate_iceberg_create_table_options(
    options: &CreateTableOptions,
) -> CatalogResult<()> {
    if options.mode.is_replace() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support REPLACE".to_string(),
        ));
    }
    if !options.constraints.is_empty() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support CONSTRAINT".to_string(),
        ));
    }
    if !options.sort_by.is_empty() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support SORT BY".to_string(),
        ));
    }
    if options.bucket_by.is_some() {
        return Err(CatalogError::NotSupported(
            "AWS Glue catalog does not support BUCKET BY".to_string(),
        ));
    }

    if !options.is_external {
        return Err(CatalogError::InvalidArgument(
            "Location is required for Iceberg tables".to_string(),
        ));
    }

    if options.location.is_none() {
        return Err(CatalogError::InvalidArgument(
            "Location is required for Iceberg tables".to_string(),
        ));
    }

    Ok(())
}

fn validate_iceberg_options(options: CreateTableOptions) -> CatalogResult<ValidatedIcebergOptions> {
    validate_iceberg_create_table_options(&options)?;
    let CreateTableOptions {
        columns,
        comment,
        constraints: _,
        location,
        format: _,
        partition_by,
        sort_by: _,
        bucket_by: _,
        mode,
        properties,
        is_external: _,
        is_write_precondition: _,
    } = options;

    let location = location.ok_or_else(|| {
        CatalogError::InvalidArgument("Location is required for Iceberg tables".to_string())
    })?;

    // Preserve comment as a table property (OpenTableFormatInput API doesn't have native comment support)
    let mut final_properties = properties;
    if let Some(c) = comment {
        final_properties.push(("comment".to_string(), c));
    }

    Ok(ValidatedIcebergOptions {
        columns,
        location,
        partition_by,
        if_not_exists: mode.ignore_if_exists(),
        properties: final_properties,
    })
}

/// Builds an Iceberg schema from column options.
fn build_iceberg_schema(columns: &[CreateTableColumnOptions]) -> CatalogResult<IcebergSchema> {
    let fields: CatalogResult<Vec<IcebergStructField>> = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let type_doc = arrow_to_iceberg_type(&col.data_type)?;
            let mut builder = IcebergStructField::builder()
                .id((idx + 1) as i32)
                .name(&col.name)
                .required(!col.nullable)
                .r#type(type_doc);
            if let Some(comment) = &col.comment {
                builder = builder.doc(comment);
            }
            builder.build().map_err(|e| {
                CatalogError::InvalidArgument(format!("Failed to build Iceberg field: {e}"))
            })
        })
        .collect();

    IcebergSchema::builder()
        .schema_id(0)
        .r#type(IcebergStructTypeEnum::Struct)
        .set_fields(Some(fields?))
        .build()
        .map_err(|e| CatalogError::InvalidArgument(format!("Failed to build Iceberg schema: {e}")))
}

/// Builds an Iceberg partition spec from partition fields.
fn build_iceberg_partition_spec(
    partition_by: &[CatalogPartitionField],
    columns: &[CreateTableColumnOptions],
) -> CatalogResult<Option<IcebergPartitionSpec>> {
    if partition_by.is_empty() {
        return Ok(None);
    }

    let column_name_to_id: HashMap<String, i32> = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.name.clone(), (idx + 1) as i32))
        .collect();

    let fields: CatalogResult<Vec<IcebergPartitionField>> = partition_by
        .iter()
        .map(|p| {
            let source_id = column_name_to_id.get(&p.column).ok_or_else(|| {
                CatalogError::InvalidArgument(format!(
                    "Partition column '{}' not found in schema",
                    p.column
                ))
            })?;
            let (transform, name) = partition_transform_to_string(p);
            IcebergPartitionField::builder()
                .name(name)
                .source_id(*source_id)
                .transform(transform)
                .build()
                .map_err(|e| {
                    CatalogError::InvalidArgument(format!(
                        "Failed to build Iceberg partition field: {e}"
                    ))
                })
        })
        .collect();

    let spec = IcebergPartitionSpec::builder()
        .spec_id(0)
        .set_fields(Some(fields?))
        .build()
        .map_err(|e| {
            CatalogError::InvalidArgument(format!("Failed to build Iceberg partition spec: {e}"))
        })?;

    Ok(Some(spec))
}

/// Converts a CatalogPartitionField to an Iceberg transform string and partition name.
fn partition_transform_to_string(field: &CatalogPartitionField) -> (String, String) {
    match &field.transform {
        None | Some(PartitionTransform::Identity) => ("identity".to_string(), field.column.clone()),
        Some(PartitionTransform::Year) => ("year".to_string(), format!("{}_year", field.column)),
        Some(PartitionTransform::Month) => ("month".to_string(), format!("{}_month", field.column)),
        Some(PartitionTransform::Day) => ("day".to_string(), format!("{}_day", field.column)),
        Some(PartitionTransform::Hour) => ("hour".to_string(), format!("{}_hour", field.column)),
        Some(PartitionTransform::Bucket(n)) => {
            (format!("bucket[{n}]"), format!("{}_bucket", field.column))
        }
        Some(PartitionTransform::Truncate(w)) => {
            (format!("truncate[{w}]"), format!("{}_trunc", field.column))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_glue_open_table_format_errors_do_not_fallback() {
        assert!(
            !should_fallback_to_table_input(false, "OpenTableFormatInput is not supported"),
            "real AWS Glue OpenTableFormat failures must not use generic TableInput fallback"
        );
    }

    #[test]
    fn custom_endpoint_known_open_table_format_errors_fallback() {
        assert!(should_fallback_to_table_input(
            true,
            "OpenTableFormatInput is not implemented"
        ));
        assert!(should_fallback_to_table_input(
            true,
            "unknown parameter: open_table_format_input"
        ));
        assert!(should_fallback_to_table_input(true, "unhandled error"));
    }

    #[test]
    fn custom_endpoint_unrelated_errors_do_not_fallback() {
        assert!(!should_fallback_to_table_input(
            true,
            "AccessDeniedException: not authorized to create table"
        ));
    }
}
