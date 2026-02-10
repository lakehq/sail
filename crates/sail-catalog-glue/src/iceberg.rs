/// Iceberg table creation logic for AWS Glue Data Catalog.
use std::collections::HashMap;

use aws_sdk_glue::types::{
    CreateIcebergTableInput, IcebergInput, IcebergPartitionField, IcebergPartitionSpec,
    IcebergSchema, IcebergStructField, IcebergStructTypeEnum, MetadataOperation,
    OpenTableFormatInput,
};
use aws_sdk_glue::Client;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogPartitionField, CatalogProvider, CreateTableColumnOptions, CreateTableOptions,
    Namespace, PartitionTransform,
};
use sail_common_datafusion::catalog::TableStatus;

use crate::data_type::arrow_to_iceberg_type;
use crate::GlueCatalogProvider;

/// Default Iceberg table spec version for new tables (v2 is the modern standard).
const DEFAULT_ICEBERG_FORMAT_VERSION: &str = "2";

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
    let db_name = database.to_string();

    let ValidatedIcebergOptions {
        columns,
        location,
        partition_by,
        if_not_exists,
        properties,
    } = validate_iceberg_options(options)?;

    let iceberg_schema = build_iceberg_schema(&columns)?;
    let partition_spec = build_iceberg_partition_spec(&partition_by, &columns)?;

    let mut iceberg_table_builder = CreateIcebergTableInput::builder()
        .location(&location)
        .schema(iceberg_schema);

    if let Some(spec) = partition_spec {
        iceberg_table_builder = iceberg_table_builder.partition_spec(spec);
    }

    // Extract format version from properties or use default
    let format_version = properties
        .iter()
        .find(|(k, _)| k == "format-version" || k == "metadata.format-version")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| DEFAULT_ICEBERG_FORMAT_VERSION.to_string());

    if !properties.is_empty() {
        let props: HashMap<String, String> = properties.into_iter().collect();
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
        .database_name(&db_name)
        .name(table)
        .open_table_format_input(open_format_input)
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

/// Validates CreateTableOptions for Iceberg tables.
fn validate_iceberg_options(options: CreateTableOptions) -> CatalogResult<ValidatedIcebergOptions> {
    let CreateTableOptions {
        columns,
        comment,
        constraints,
        location,
        format: _,
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
        if_not_exists,
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
