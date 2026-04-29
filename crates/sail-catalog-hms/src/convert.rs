use std::collections::HashMap;

use chrono::Utc;
use hive_metastore::{Database, FieldSchema, PrincipalType, SerDeInfo, StorageDescriptor, Table};
use pilota::{AHashMap, FastStr};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::hive_format::{HiveDetectedFormat, HiveStorageFormat};
use sail_catalog::provider::{
    CreateDatabaseOptions, CreateTableColumnOptions, CreateViewColumnOptions, CreateViewOptions,
    Namespace,
};
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::catalog::{
    identity_partition_fields, DatabaseStatus, TableColumnStatus, TableKind, TableStatus,
};

use crate::data_type::{arrow_to_hive_type, hive_type_to_arrow};

pub(crate) const COMMENT_KEY: &str = "comment";
pub(crate) const EXTERNAL_KEY: &str = "EXTERNAL";
pub(crate) const EXTERNAL_TRUE: &str = "TRUE";
pub(crate) const SPARK_DATASOURCE_PROVIDER_KEY: &str = "spark.sql.sources.provider";
pub(crate) const MANAGED_TABLE_TYPE: &str = "MANAGED_TABLE";
pub(crate) const EXTERNAL_TABLE_TYPE: &str = "EXTERNAL_TABLE";
pub(crate) const VIRTUAL_VIEW_TYPE: &str = "VIRTUAL_VIEW";
/// Placeholder owner matching Spark/Hive convention for unauthenticated contexts.
/// Used as a literal owner name when no authenticated principal is available.
pub(crate) const DEFAULT_OWNER: &str = "user.name";

pub(crate) struct GenericTableFormat<'a> {
    pub logical_format: &'a str,
    pub storage: &'a HiveStorageFormat,
}

pub(crate) fn validate_namespace(namespace: &Namespace) -> CatalogResult<String> {
    if !namespace.tail.is_empty() {
        return Err(CatalogError::InvalidArgument(format!(
            "Hive Metastore only supports flat databases, got namespace '{}'",
            quote_namespace_if_needed(namespace)
        )));
    }
    Ok(namespace.head.to_string())
}

pub(crate) fn database_to_status(
    catalog: &str,
    database: &Database,
) -> CatalogResult<DatabaseStatus> {
    let name = database
        .name
        .as_ref()
        .ok_or_else(|| CatalogError::External("Database is missing a name".to_string()))?;

    Ok(DatabaseStatus {
        catalog: catalog.to_string(),
        database: vec![name.to_string()],
        comment: database.description.as_ref().map(ToString::to_string),
        location: database.location_uri.as_ref().map(ToString::to_string),
        properties: map_to_vec(database.parameters.as_ref()),
    })
}

pub(crate) fn table_to_status(
    catalog: &str,
    database: &Namespace,
    table: &Table,
) -> CatalogResult<TableStatus> {
    let name = table
        .table_name
        .as_ref()
        .ok_or_else(|| CatalogError::External("Table is missing a name".to_string()))?
        .to_string();
    let properties = map_to_vec(table.parameters.as_ref());
    let comment = extract_property(table.parameters.as_ref(), COMMENT_KEY);
    let storage = table.sd.as_ref();
    let columns = columns_from_hms(storage, table.partition_keys.as_ref())?;
    let location = storage
        .and_then(|sd| sd.location.as_ref())
        .map(ToString::to_string);
    let format = table_provider_format(table.parameters.as_ref()).unwrap_or_else(|| {
        detect_hms_logical_format(
            storage
                .and_then(|sd| sd.serde_info.as_ref())
                .and_then(|serde| serde.serialization_lib.as_deref()),
            storage.and_then(|sd| sd.input_format.as_deref()),
            storage.and_then(|sd| sd.output_format.as_deref()),
        )
    });
    let partition_by = table
        .partition_keys
        .as_ref()
        .map(|keys| identity_partition_fields(&field_names(keys)))
        .unwrap_or_default();

    Ok(TableStatus {
        catalog: Some(catalog.to_string()),
        database: database.clone().into(),
        name,
        kind: TableKind::Table {
            columns,
            comment,
            constraints: vec![],
            location,
            format,
            partition_by,
            sort_by: vec![],
            bucket_by: None,
            properties,
        },
    })
}

fn field_names(keys: &[FieldSchema]) -> Vec<String> {
    keys.iter()
        .filter_map(|field| field.name.as_ref().map(ToString::to_string))
        .collect()
}

pub(crate) fn view_to_status(
    catalog: &str,
    database: &Namespace,
    table: &Table,
) -> CatalogResult<TableStatus> {
    let name = table
        .table_name
        .as_ref()
        .ok_or_else(|| CatalogError::External("View is missing a name".to_string()))?
        .to_string();
    let definition = table
        .view_original_text
        .as_ref()
        .or(table.view_expanded_text.as_ref())
        .ok_or_else(|| CatalogError::External("View is missing a definition".to_string()))?
        .to_string();

    Ok(TableStatus {
        catalog: Some(catalog.to_string()),
        database: database.clone().into(),
        name,
        kind: TableKind::View {
            definition,
            columns: columns_from_hms(table.sd.as_ref(), None)?,
            comment: extract_property(table.parameters.as_ref(), COMMENT_KEY),
            properties: map_to_vec(table.parameters.as_ref()),
        },
    })
}

pub(crate) fn build_database(
    namespace: &Namespace,
    options: CreateDatabaseOptions,
) -> CatalogResult<Database> {
    let CreateDatabaseOptions {
        comment,
        location,
        if_not_exists: _,
        properties,
    } = options;

    let parameters = vec_to_map(properties);

    Ok(Database {
        name: Some(validate_namespace(namespace)?.into()),
        description: comment.map(Into::into),
        location_uri: location.map(Into::into),
        parameters,
        owner_name: Some(DEFAULT_OWNER.into()),
        owner_type: Some(PrincipalType::USER),
        ..Default::default()
    })
}

pub(crate) fn build_generic_table(
    database_name: &str,
    table_name: &str,
    columns: Vec<CreateTableColumnOptions>,
    partition_columns: Vec<String>,
    location: Option<String>,
    format: GenericTableFormat<'_>,
    comment: Option<String>,
    properties: Vec<(String, String)>,
) -> CatalogResult<Table> {
    let (regular_columns, partition_keys) = build_columns(columns, &partition_columns)?;
    let mut parameters = vec_to_map(properties);
    if let Some(comment) = comment {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(COMMENT_KEY),
            FastStr::from_string(comment),
        );
    }
    if format.logical_format == "delta" {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(SPARK_DATASOURCE_PROVIDER_KEY),
            FastStr::from_static_str("delta"),
        );
    }

    let table_type = if location.is_some() {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(EXTERNAL_KEY),
            FastStr::from_static_str(EXTERNAL_TRUE),
        );
        EXTERNAL_TABLE_TYPE
    } else {
        MANAGED_TABLE_TYPE
    };

    Ok(Table {
        db_name: Some(database_name.to_string().into()),
        table_name: Some(table_name.to_string().into()),
        owner: Some(DEFAULT_OWNER.into()),
        create_time: Some(current_time_secs()?),
        last_access_time: Some(current_time_secs()?),
        table_type: Some(table_type.into()),
        sd: Some(StorageDescriptor {
            cols: Some(regular_columns),
            location: location.map(Into::into),
            input_format: Some(format.storage.input_format.into()),
            output_format: Some(format.storage.output_format.into()),
            serde_info: Some(SerDeInfo {
                serialization_lib: Some(format.storage.serde_library.into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        partition_keys: if partition_keys.is_empty() {
            None
        } else {
            Some(partition_keys)
        },
        parameters,
        ..Default::default()
    })
}

pub(crate) fn build_view(
    database_name: &str,
    view_name: &str,
    options: CreateViewOptions,
) -> CatalogResult<Table> {
    let CreateViewOptions {
        columns,
        definition,
        if_not_exists: _,
        replace: _,
        comment,
        properties,
    } = options;

    let mut parameters = vec_to_map(properties);
    if let Some(comment) = comment {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(COMMENT_KEY),
            FastStr::from_string(comment),
        );
    }

    Ok(Table {
        db_name: Some(database_name.to_string().into()),
        table_name: Some(view_name.to_string().into()),
        owner: Some(DEFAULT_OWNER.into()),
        create_time: Some(current_time_secs()?),
        last_access_time: Some(current_time_secs()?),
        table_type: Some(VIRTUAL_VIEW_TYPE.into()),
        view_original_text: Some(definition.clone().into()),
        view_expanded_text: Some(definition.into()),
        sd: Some(StorageDescriptor {
            cols: Some(
                columns
                    .into_iter()
                    .map(view_column_to_field_schema)
                    .collect::<CatalogResult<Vec<_>>>()?,
            ),
            input_format: Some("org.apache.hadoop.mapred.TextInputFormat".into()),
            output_format: Some(
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".into(),
            ),
            serde_info: Some(SerDeInfo {
                name: Some(format!("{database_name}.{view_name}").into()),
                serialization_lib: Some(
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe".into(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }),
        parameters,
        ..Default::default()
    })
}

pub(crate) fn is_view_table(table: &Table) -> bool {
    table.table_type.as_deref() == Some(VIRTUAL_VIEW_TYPE)
}

pub(crate) fn extract_property(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
    key: &str,
) -> Option<String> {
    parameters.and_then(|props| props.get(key).map(ToString::to_string))
}

fn table_provider_format(parameters: Option<&AHashMap<FastStr, FastStr>>) -> Option<String> {
    let provider = extract_property(parameters, SPARK_DATASOURCE_PROVIDER_KEY)?;
    match provider.trim().to_ascii_lowercase().as_str() {
        "delta" | "deltalake" => Some("delta".to_string()),
        _ => None,
    }
}

fn detect_hms_logical_format(
    serde_library: Option<&str>,
    input_format: Option<&str>,
    output_format: Option<&str>,
) -> String {
    match HiveDetectedFormat::detect(serde_library, input_format, output_format) {
        HiveDetectedFormat::Csv => "csv".to_string(),
        detected => detected.as_str().to_string(),
    }
}

pub(crate) fn vec_to_map(values: Vec<(String, String)>) -> Option<AHashMap<FastStr, FastStr>> {
    if values.is_empty() {
        None
    } else {
        Some(
            values
                .into_iter()
                .map(|(key, value)| (FastStr::from_string(key), FastStr::from_string(value)))
                .collect(),
        )
    }
}

pub(crate) fn map_to_vec(values: Option<&AHashMap<FastStr, FastStr>>) -> Vec<(String, String)> {
    let mut values: Vec<_> = values
        .into_iter()
        .flat_map(|map| {
            map.iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
        })
        .collect();
    values.sort();
    values
}

fn build_columns(
    columns: Vec<CreateTableColumnOptions>,
    partition_columns: &[String],
) -> CatalogResult<(Vec<FieldSchema>, Vec<FieldSchema>)> {
    let mut partition_columns: HashMap<_, _> = partition_columns
        .iter()
        .map(|column| (column.to_lowercase(), column.as_str()))
        .collect();

    let mut regular = Vec::new();
    let mut partition = Vec::new();
    for column in columns {
        let schema = column_to_field_schema(column)?;
        if partition_columns
            .remove(&schema.name.as_deref().unwrap_or_default().to_lowercase())
            .is_some()
        {
            partition.push(schema);
        } else {
            regular.push(schema);
        }
    }

    if !partition_columns.is_empty() {
        let mut missing: Vec<_> = partition_columns.into_values().collect();
        missing.sort_unstable();
        return Err(CatalogError::InvalidArgument(format!(
            "Partition columns not found in table schema: {}",
            missing.join(", ")
        )));
    }

    Ok((regular, partition))
}

fn columns_from_hms(
    storage: Option<&StorageDescriptor>,
    partition_keys: Option<&Vec<FieldSchema>>,
) -> CatalogResult<Vec<TableColumnStatus>> {
    let regular = storage
        .and_then(|sd| sd.cols.as_ref())
        .into_iter()
        .flatten()
        .map(|field| field_schema_to_status(field, false))
        .collect::<CatalogResult<Vec<_>>>()?;

    let partition = partition_keys
        .into_iter()
        .flatten()
        .map(|field| field_schema_to_status(field, true))
        .collect::<CatalogResult<Vec<_>>>()?;

    Ok(regular.into_iter().chain(partition).collect())
}

fn field_schema_to_status(
    field: &FieldSchema,
    is_partition: bool,
) -> CatalogResult<TableColumnStatus> {
    let name = field
        .name
        .as_ref()
        .ok_or_else(|| CatalogError::External("Field schema is missing a name".to_string()))?;
    let data_type = hive_type_to_arrow(field.r#type.as_deref().unwrap_or("string"))?;
    Ok(TableColumnStatus {
        name: name.to_string(),
        data_type,
        nullable: true,
        comment: field.comment.as_ref().map(ToString::to_string),
        default: None,
        generated_always_as: None,
        is_partition,
        is_bucket: false,
        is_cluster: false,
    })
}

fn column_to_field_schema(column: CreateTableColumnOptions) -> CatalogResult<FieldSchema> {
    Ok(FieldSchema {
        name: Some(column.name.into()),
        r#type: Some(arrow_to_hive_type(&column.data_type)?.into()),
        comment: column.comment.map(Into::into),
    })
}

fn view_column_to_field_schema(column: CreateViewColumnOptions) -> CatalogResult<FieldSchema> {
    Ok(FieldSchema {
        name: Some(column.name.into()),
        r#type: Some(arrow_to_hive_type(&column.data_type)?.into()),
        comment: column.comment.map(Into::into),
    })
}

fn current_time_secs() -> CatalogResult<i32> {
    // HMS Thrift uses i32 timestamps (same 2038 limitation as the HMS protocol itself).
    Utc::now().timestamp().try_into().map_err(|_| {
        CatalogError::Internal("Current time is out of range for HMS i32 timestamps".to_string())
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use arrow::datatypes::DataType;
    use sail_catalog::hive_format::HiveStorageFormat;
    use sail_catalog::provider::{
        CreateTableColumnOptions, CreateViewColumnOptions, CreateViewOptions,
    };

    use super::{
        build_generic_table, build_view, database_to_status, is_view_table, map_to_vec,
        validate_namespace, GenericTableFormat, COMMENT_KEY, SPARK_DATASOURCE_PROVIDER_KEY,
        VIRTUAL_VIEW_TYPE,
    };

    #[test]
    fn test_validate_namespace_rejects_nested_namespaces() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["a", "b"]).unwrap();
        assert!(validate_namespace(&namespace).is_err());
    }

    #[test]
    fn test_database_to_status_reads_properties() {
        let database = hive_metastore::Database {
            name: Some("default".into()),
            description: Some("test".into()),
            parameters: Some([("k".into(), "v".into())].into_iter().collect()),
            ..Default::default()
        };

        let status = database_to_status("hms", &database).unwrap();
        assert_eq!(status.catalog, "hms");
        assert_eq!(status.database, vec!["default".to_string()]);
        assert_eq!(status.comment.as_deref(), Some("test"));
        assert_eq!(status.properties, vec![("k".to_string(), "v".to_string())]);
    }

    #[test]
    fn test_build_generic_table_marks_external_when_location_present() {
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
            }],
            vec![],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            Some("comment".to_string()),
            vec![],
        )
        .unwrap();

        assert_eq!(
            table.table_type.as_deref(),
            Some(super::EXTERNAL_TABLE_TYPE)
        );
        let properties = map_to_vec(table.parameters.as_ref());
        assert!(properties
            .iter()
            .any(|(k, v)| k == super::EXTERNAL_KEY && v == super::EXTERNAL_TRUE));
        assert!(properties
            .iter()
            .any(|(k, v)| k == COMMENT_KEY && v == "comment"));
    }

    #[test]
    fn test_build_generic_table_marks_delta_provider() {
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
            }],
            vec![],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "delta",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
        )
        .unwrap();

        let properties = map_to_vec(table.parameters.as_ref());
        assert!(properties
            .iter()
            .any(|(k, v)| k == SPARK_DATASOURCE_PROVIDER_KEY && v == "delta"));
    }

    #[test]
    fn test_build_generic_table_rejects_missing_partition_columns() {
        let error = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
            }],
            vec!["missing_partition".to_string()],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            sail_catalog::error::CatalogError::InvalidArgument(_)
        ));
        assert!(error.to_string().contains("missing_partition"));
    }

    #[test]
    fn test_table_to_status_reports_textfile_format() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
            }],
            vec![],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "textfile",
                storage: &HiveStorageFormat::textfile(),
            },
            None,
            vec![],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { format, .. } => {
                assert_eq!(format, "textfile");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_converts_partition_columns_to_identity_fields() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![
                CreateTableColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "day".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
            ],
            vec!["day".to_string()],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { partition_by, .. } => {
                assert_eq!(
                    partition_by,
                    vec![sail_common_datafusion::catalog::CatalogPartitionField {
                        column: "day".to_string(),
                        transform: None,
                    }]
                );
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_build_view_marks_virtual_view() {
        let table = build_view(
            "default",
            "v",
            CreateViewOptions {
                columns: vec![CreateViewColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    comment: None,
                }],
                definition: "select 1 as id".to_string(),
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![],
            },
        )
        .unwrap();

        assert_eq!(table.table_type.as_deref(), Some(VIRTUAL_VIEW_TYPE));
        assert!(is_view_table(&table));
        let storage = table.sd.as_ref().expect("view storage descriptor");
        assert_eq!(
            storage.input_format.as_deref(),
            Some("org.apache.hadoop.mapred.TextInputFormat")
        );
        assert_eq!(
            storage.output_format.as_deref(),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
        );
        let serde = storage.serde_info.as_ref().expect("view serde info");
        assert_eq!(
            serde.serialization_lib.as_deref(),
            Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        );
        assert_eq!(serde.name.as_deref(), Some("default.v"));
    }
}
