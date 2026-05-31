use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Field};
use chrono::Utc;
use hive_metastore::{Database, FieldSchema, PrincipalType, SerDeInfo, StorageDescriptor, Table};
use pilota::{AHashMap, FastStr};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::hive_format::{HiveDetectedFormat, HiveStorageFormat};
use sail_catalog::provider::{
    namespace_location_from_properties, CreateDatabaseOptions, CreateTableColumnOptions,
    CreateViewColumnOptions, CreateViewOptions, Namespace,
};
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::catalog::{
    identity_partition_fields, DatabaseStatus, TableColumnStatus, TableKind, TableStatus,
};

use crate::data_type::{
    arrow_to_hive_type, hive_type_to_arrow, spark_struct_json_from_fields,
    spark_struct_json_to_fields,
};

pub(crate) const COMMENT_KEY: &str = "comment";
pub(crate) const EXTERNAL_KEY: &str = "EXTERNAL";
pub(crate) const EXTERNAL_TRUE: &str = "TRUE";
pub(crate) const SPARK_DATASOURCE_PROVIDER_KEY: &str = "spark.sql.sources.provider";
pub(crate) const SPARK_SCHEMA_KEY: &str = "spark.sql.sources.schema";
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
    let properties = map_to_vec(database.parameters.as_ref());

    Ok(DatabaseStatus {
        catalog: catalog.to_string(),
        database: vec![name.to_string()],
        comment: database.description.as_ref().map(ToString::to_string),
        location: database
            .location_uri
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| {
                namespace_location_from_properties(
                    properties
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                )
            }),
        properties,
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
    let properties = filter_spark_properties(map_to_vec(table.parameters.as_ref()));
    let comment = extract_property(table.parameters.as_ref(), COMMENT_KEY);
    let storage = table.sd.as_ref();
    let partition_columns = table
        .partition_keys
        .as_ref()
        .map(|keys| field_names(keys))
        .unwrap_or_default();
    let columns = match columns_from_spark_properties(table.parameters.as_ref())? {
        Some(spark_columns) => reorder_spark_columns(spark_columns, &partition_columns)?,
        None => columns_from_hms(storage, table.partition_keys.as_ref())?,
    };
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
    let is_external = table.table_type.as_deref() == Some(EXTERNAL_TABLE_TYPE);
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
            is_external,
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
    reject_spark_properties(&properties)?;
    let (regular_columns, partition_keys) = build_columns(columns, &partition_columns)?;
    let mut parameters = vec_to_map(properties);
    insert_comment(&mut parameters, comment);
    if format.logical_format.eq_ignore_ascii_case("delta") {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(SPARK_DATASOURCE_PROVIDER_KEY),
            FastStr::from_static_str("delta"),
        );
    }

    parameters.get_or_insert_with(AHashMap::new).insert(
        FastStr::from_static_str(EXTERNAL_KEY),
        FastStr::from_static_str(EXTERNAL_TRUE),
    );
    let serde_parameters = location.as_ref().map(|location| {
        let mut parameters = AHashMap::new();
        parameters.insert(
            FastStr::from_static_str("path"),
            FastStr::from_string(location.clone()),
        );
        parameters
    });

    Ok(Table {
        db_name: Some(database_name.to_string().into()),
        table_name: Some(table_name.to_string().into()),
        owner: Some(DEFAULT_OWNER.into()),
        create_time: Some(current_time_secs()?),
        last_access_time: Some(current_time_secs()?),
        table_type: Some(EXTERNAL_TABLE_TYPE.into()),
        sd: Some(StorageDescriptor {
            cols: Some(regular_columns),
            location: location.map(Into::into),
            input_format: Some(format.storage.input_format.into()),
            output_format: Some(format.storage.output_format.into()),
            serde_info: Some(SerDeInfo {
                serialization_lib: Some(format.storage.serde_library.into()),
                parameters: serde_parameters,
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

/// Injects Spark-compatible metadata properties into an HMS table definition.
/// Spark writes datasource tables using `spark.sql.sources.*` properties and
/// expects them on read to recover provider and full schema information.
pub(crate) fn inject_spark_metadata(
    table: &mut Table,
    columns: &[CreateTableColumnOptions],
    partition_columns: &[String],
    format: &str,
) -> CatalogResult<()> {
    let parameters = table.parameters.get_or_insert_with(AHashMap::new);

    let fields: Vec<_> = columns
        .iter()
        .map(|col| {
            let mut metadata = std::collections::HashMap::new();
            if let Some(comment) = &col.comment {
                metadata.insert("comment".to_string(), comment.clone());
            }
            Field::new(&col.name, col.data_type.clone(), col.nullable).with_metadata(metadata)
        })
        .collect();
    let schema_value = spark_struct_json_from_fields(&fields)?;
    let schema_json = serde_json::to_string(&schema_value).map_err(|e| {
        CatalogError::External(format!("Failed to serialize Spark schema JSON: {e}"))
    })?;
    for (key, value) in split_large_table_prop(SPARK_SCHEMA_KEY, &schema_json, 4000) {
        parameters.insert(FastStr::from_string(key), FastStr::from_string(value));
    }

    if !partition_columns.is_empty() {
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numPartCols"),
            FastStr::from_string(partition_columns.len().to_string()),
        );
        for (i, col) in partition_columns.iter().enumerate() {
            parameters.insert(
                FastStr::from_string(format!("spark.sql.sources.schema.partCol.{i}")),
                FastStr::from_string(col.clone()),
            );
        }
    }

    parameters.insert(
        FastStr::from_static_str(SPARK_DATASOURCE_PROVIDER_KEY),
        FastStr::from_string(format.to_string()),
    );

    parameters.insert(
        FastStr::from_static_str("spark.sql.create.version"),
        FastStr::from_string(format!("sail-{}", env!("CARGO_PKG_VERSION"))),
    );

    Ok(())
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
    insert_comment(&mut parameters, comment);

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
    let provider = provider.trim().to_ascii_lowercase();
    Some(match provider.as_str() {
        "deltalake" => "delta".to_string(),
        _ => provider,
    })
}

pub(crate) fn reject_spark_properties(properties: &[(String, String)]) -> CatalogResult<()> {
    let invalid_keys: Vec<&str> = properties
        .iter()
        .filter(|(key, _)| is_reserved_spark_property_key(key))
        .map(|(key, _)| key.as_str())
        .collect();
    if !invalid_keys.is_empty() {
        return Err(CatalogError::InvalidArgument(format!(
            "Table properties may not contain Spark-internal keys: {}",
            invalid_keys.join(", ")
        )));
    }
    Ok(())
}

pub(crate) fn reject_spark_property_keys(keys: &[String]) -> CatalogResult<()> {
    let invalid_keys: Vec<&str> = keys
        .iter()
        .filter(|key| is_reserved_spark_property_key(key))
        .map(|key| key.as_str())
        .collect();
    if !invalid_keys.is_empty() {
        return Err(CatalogError::InvalidArgument(format!(
            "Table properties may not contain Spark-internal keys: {}",
            invalid_keys.join(", ")
        )));
    }
    Ok(())
}

fn is_reserved_spark_property_key(key: &str) -> bool {
    key.starts_with("spark.sql.") || key == EXTERNAL_KEY
}

fn filter_spark_properties(values: Vec<(String, String)>) -> Vec<(String, String)> {
    values
        .into_iter()
        .filter(|(key, _)| !key.starts_with("spark.sql."))
        .filter(|(key, _)| key != EXTERNAL_KEY)
        .collect()
}

fn split_large_table_prop(key: &str, value: &str, threshold: usize) -> Vec<(String, String)> {
    if value.chars().count() <= threshold {
        return vec![(key.to_string(), value.to_string())];
    }
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut char_count = 0;
    for c in value.chars() {
        if char_count >= threshold {
            parts.push(current);
            current = String::new();
            char_count = 0;
        }
        current.push(c);
        char_count += 1;
    }
    if !current.is_empty() {
        parts.push(current);
    }
    let mut output = Vec::with_capacity(parts.len() + 1);
    output.push((format!("{key}.numParts"), parts.len().to_string()));
    output.extend(
        parts
            .into_iter()
            .enumerate()
            .map(|(index, part)| (format!("{key}.part.{index}"), part)),
    );
    output
}

fn read_large_table_prop(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
    key: &str,
) -> CatalogResult<Option<String>> {
    let Some(parameters) = parameters else {
        return Ok(None);
    };
    if let Some(value) = parameters.get(key) {
        return Ok(Some(value.to_string()));
    }
    let num_parts_key = format!("{key}.numParts");
    let Some(num_parts) = parameters.get(num_parts_key.as_str()) else {
        return Ok(None);
    };
    let num_parts: usize = num_parts.parse().map_err(|_| {
        CatalogError::External(format!(
            "Invalid split property part count for {key}: {num_parts}"
        ))
    })?;
    if num_parts == 0 {
        return Err(CatalogError::External(format!(
            "Invalid split property part count for {key}: {num_parts}"
        )));
    }

    let estimated: usize = (0..num_parts)
        .filter_map(|index| parameters.get(format!("{key}.part.{index}").as_str()))
        .map(|part| part.len())
        .sum();
    let mut output = String::with_capacity(estimated);
    for index in 0..num_parts {
        let part_key = format!("{key}.part.{index}");
        let part = parameters.get(part_key.as_str()).ok_or_else(|| {
            CatalogError::External(format!("Missing split property part {part_key}"))
        })?;
        output.push_str(part);
    }
    Ok(Some(output))
}

fn columns_from_spark_properties(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
) -> CatalogResult<Option<Vec<TableColumnStatus>>> {
    let Some(schema) = read_large_table_prop(parameters, SPARK_SCHEMA_KEY)? else {
        return Ok(None);
    };
    let columns = spark_struct_json_to_fields(&schema)?
        .into_iter()
        .map(|field| TableColumnStatus {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            comment: field.metadata().get(COMMENT_KEY).cloned(),
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        })
        .collect();
    Ok(Some(columns))
}

fn reorder_spark_columns(
    columns: Vec<TableColumnStatus>,
    partition_columns: &[String],
) -> CatalogResult<Vec<TableColumnStatus>> {
    if partition_columns.is_empty() {
        return Ok(columns);
    }

    let mut seen = HashSet::new();
    for col in &columns {
        if !seen.insert(col.name.to_ascii_lowercase()) {
            return Err(CatalogError::External(format!(
                "Duplicate column name in Spark schema: {}",
                col.name
            )));
        }
    }

    let mut lookup = build_partition_lookup(partition_columns);
    let mut regular = Vec::new();
    let mut partition = Vec::new();
    for mut col in columns {
        if lookup.remove(&col.name.to_ascii_lowercase()).is_some() {
            col.is_partition = true;
            partition.push(col);
        } else {
            regular.push(col);
        }
    }
    check_unmatched_partitions(lookup, |missing| {
        CatalogError::External(format!(
            "Partition columns missing in Spark schema metadata: {missing}"
        ))
    })?;
    Ok(regular.into_iter().chain(partition).collect())
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

fn insert_comment(parameters: &mut Option<AHashMap<FastStr, FastStr>>, comment: Option<String>) {
    if let Some(comment) = comment {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(COMMENT_KEY),
            FastStr::from_string(comment),
        );
    }
}

fn build_partition_lookup(partition_columns: &[String]) -> HashMap<String, String> {
    partition_columns
        .iter()
        .map(|name| (name.to_ascii_lowercase(), name.clone()))
        .collect()
}

fn check_unmatched_partitions(
    lookup: HashMap<String, String>,
    make_error: impl FnOnce(String) -> CatalogError,
) -> CatalogResult<()> {
    if lookup.is_empty() {
        return Ok(());
    }
    let mut missing: Vec<_> = lookup.into_values().collect();
    missing.sort();
    Err(make_error(missing.join(", ")))
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
    let mut lookup = build_partition_lookup(partition_columns);
    let mut regular = Vec::new();
    let mut partition = Vec::new();
    for column in columns {
        let schema = column_to_field_schema(column)?;
        let name = schema.name.as_deref().unwrap_or_default();
        if lookup.remove(&name.to_ascii_lowercase()).is_some() {
            partition.push(schema);
        } else {
            regular.push(schema);
        }
    }
    check_unmatched_partitions(lookup, |missing| {
        CatalogError::InvalidArgument(format!(
            "Partition columns not found in table schema: {missing}"
        ))
    })?;
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

fn create_field_schema(
    name: String,
    data_type: &DataType,
    comment: Option<String>,
) -> CatalogResult<FieldSchema> {
    Ok(FieldSchema {
        name: Some(name.into()),
        r#type: Some(arrow_to_hive_type(data_type)?.into()),
        comment: comment.map(Into::into),
    })
}

fn column_to_field_schema(column: CreateTableColumnOptions) -> CatalogResult<FieldSchema> {
    create_field_schema(column.name, &column.data_type, column.comment)
}

fn view_column_to_field_schema(column: CreateViewColumnOptions) -> CatalogResult<FieldSchema> {
    create_field_schema(column.name, &column.data_type, column.comment)
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

    use arrow::datatypes::{DataType, Field, Fields};
    use hive_metastore::{Database, FieldSchema, SerDeInfo, StorageDescriptor, Table};
    use pilota::{AHashMap, FastStr};
    use sail_catalog::hive_format::HiveStorageFormat;
    use sail_catalog::provider::{
        CreateTableColumnOptions, CreateViewColumnOptions, CreateViewOptions, Namespace,
    };
    use sail_common_datafusion::catalog::TableKind;

    use super::{
        build_generic_table, build_view, columns_from_spark_properties, database_to_status,
        inject_spark_metadata, is_view_table, map_to_vec, table_to_status, validate_namespace,
        GenericTableFormat, COMMENT_KEY, SPARK_DATASOURCE_PROVIDER_KEY, SPARK_SCHEMA_KEY,
        VIRTUAL_VIEW_TYPE,
    };

    #[test]
    fn test_validate_namespace_rejects_nested_namespaces() {
        let namespace = Namespace::try_from(vec!["a", "b"]).unwrap();
        assert!(validate_namespace(&namespace).is_err());
    }

    #[test]
    fn test_database_to_status_reads_properties() {
        let database = Database {
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
    fn test_database_to_status_preserves_file_uri_locations() {
        let database = Database {
            name: Some("default".into()),
            location_uri: Some("file:/tmp/warehouse/default.db".into()),
            ..Default::default()
        };

        let status = database_to_status("hms", &database).unwrap();
        assert_eq!(
            status.location.as_deref(),
            Some("file:/tmp/warehouse/default.db")
        );
    }

    #[test]
    fn test_database_to_status_falls_back_to_namespace_properties() {
        let database = Database {
            name: Some("default".into()),
            parameters: Some(
                [
                    ("warehouse".into(), "s3://warehouse/default.db".into()),
                    ("path".into(), "s3://path/default.db".into()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        };

        let status = database_to_status("hms", &database).unwrap();
        assert_eq!(
            status.location.as_deref(),
            Some("s3://warehouse/default.db")
        );
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
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
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

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table { format, .. } => {
                assert_eq!(format, "textfile");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_converts_partition_columns_to_identity_fields() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
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

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table { partition_by, .. } => {
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
    fn test_inject_spark_metadata_writes_schema_and_partition_properties() {
        let mut table = build_generic_table(
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
            vec![("owner".to_string(), "alice".to_string())],
        )
        .unwrap();

        inject_spark_metadata(
            &mut table,
            &[
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
            &["day".to_string()],
            "parquet",
        )
        .unwrap();

        let props = map_to_vec(table.parameters.as_ref());
        assert!(props.iter().any(|(k, v)| k == "owner" && v == "alice"));
        assert!(props.iter().any(|(k, _)| k == SPARK_SCHEMA_KEY));
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.sources.schema.partCol.0" && v == "day"));
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.sources.provider" && v == "parquet"));
        assert!(props.iter().any(|(k, _)| k == "spark.sql.create.version"));
    }

    #[test]
    fn test_table_to_status_uses_spark_schema_and_hides_internal_properties() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let mut table = build_generic_table(
            "default",
            "items",
            vec![
                CreateTableColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: Some("pk".to_string()),
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
            Some("hello".to_string()),
            vec![("owner".to_string(), "alice".to_string())],
        )
        .unwrap();

        inject_spark_metadata(
            &mut table,
            &[
                CreateTableColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: Some("pk".to_string()),
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
            &["day".to_string()],
            "parquet",
        )
        .unwrap();

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table {
                format,
                columns,
                properties,
                ..
            } => {
                assert_eq!(format, "parquet");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "day");
                assert!(columns[1].is_partition);
                assert!(properties.iter().any(|(k, v)| k == "owner" && v == "alice"));
                assert!(!properties.iter().any(|(k, _)| k.starts_with("spark.sql.")));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_reads_split_spark_schema_metadata() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "payload",
                DataType::Struct(Fields::from(vec![Field::new(
                    "tags",
                    DataType::List(std::sync::Arc::new(Field::new_list_field(
                        DataType::Utf8,
                        true,
                    ))),
                    true,
                )])),
                true,
            ),
            Field::new("day", DataType::Utf8, false),
        ];
        let schema = serde_json::to_string(
            &crate::data_type::spark_struct_json_from_fields(&fields).unwrap(),
        )
        .unwrap();
        let split_at = schema.len() / 2;
        let mut parameters = AHashMap::new();
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numParts"),
            FastStr::from_static_str("2"),
        );
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.part.0"),
            FastStr::from_string(schema[..split_at].to_string()),
        );
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.part.1"),
            FastStr::from_string(schema[split_at..].to_string()),
        );
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.provider"),
            FastStr::from_static_str("deltalake"),
        );
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numPartCols"),
            FastStr::from_static_str("1"),
        );
        parameters.insert(
            FastStr::from_static_str("spark.sql.sources.schema.partCol.0"),
            FastStr::from_static_str("day"),
        );
        parameters.insert(
            FastStr::from_static_str(super::EXTERNAL_KEY),
            FastStr::from_static_str(super::EXTERNAL_TRUE),
        );
        parameters.insert(
            FastStr::from_static_str("owner"),
            FastStr::from_static_str("alice"),
        );
        let table = Table {
            table_name: Some("items".into()),
            table_type: Some(super::EXTERNAL_TABLE_TYPE.into()),
            sd: Some(StorageDescriptor {
                location: Some("s3://warehouse/items".into()),
                cols: Some(vec![
                    FieldSchema {
                        name: Some("id".into()),
                        r#type: Some("bigint".into()),
                        ..Default::default()
                    },
                    FieldSchema {
                        name: Some("payload".into()),
                        r#type: Some("string".into()),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            partition_keys: Some(vec![FieldSchema {
                name: Some("day".into()),
                r#type: Some("string".into()),
                ..Default::default()
            }]),
            parameters: Some(parameters),
            ..Default::default()
        };

        let status = table_to_status("hms", &namespace, &table).unwrap();

        match status.kind {
            TableKind::Table {
                format,
                columns,
                properties,
                partition_by,
                ..
            } => {
                assert_eq!(format, "delta");
                assert_eq!(
                    columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
                    ["id", "payload", "day"]
                );
                assert!(matches!(columns[1].data_type, DataType::Struct(_)));
                assert!(columns[2].is_partition);
                assert_eq!(partition_by[0].column, "day");
                assert_eq!(properties, vec![("owner".to_string(), "alice".to_string())]);
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_rejects_spark_schema_missing_partition_column() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let schema = serde_json::to_string(
            &crate::data_type::spark_struct_json_from_fields(&[Field::new(
                "id",
                DataType::Int64,
                false,
            )])
            .unwrap(),
        )
        .unwrap();
        let table = Table {
            table_name: Some("items".into()),
            sd: Some(StorageDescriptor::default()),
            partition_keys: Some(vec![FieldSchema {
                name: Some("day".into()),
                r#type: Some("string".into()),
                ..Default::default()
            }]),
            parameters: Some(AHashMap::from_iter([(
                FastStr::from_static_str(SPARK_SCHEMA_KEY),
                FastStr::from_string(schema),
            )])),
            ..Default::default()
        };

        let error = table_to_status("hms", &namespace, &table).unwrap_err();

        assert!(error
            .to_string()
            .contains("Partition columns missing in Spark schema metadata: day"));
    }

    #[test]
    fn test_table_to_status_uses_hms_partition_keys_when_spark_partition_metadata_disagrees() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let schema = serde_json::to_string(
            &crate::data_type::spark_struct_json_from_fields(&[
                Field::new("id", DataType::Int64, false),
                Field::new("day", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, false),
            ])
            .unwrap(),
        )
        .unwrap();
        let table = Table {
            table_name: Some("items".into()),
            table_type: Some(super::EXTERNAL_TABLE_TYPE.into()),
            sd: Some(StorageDescriptor::default()),
            partition_keys: Some(vec![FieldSchema {
                name: Some("day".into()),
                r#type: Some("string".into()),
                ..Default::default()
            }]),
            parameters: Some(AHashMap::from_iter([
                (
                    FastStr::from_static_str(SPARK_SCHEMA_KEY),
                    FastStr::from_string(schema),
                ),
                (
                    FastStr::from_static_str("spark.sql.sources.provider"),
                    FastStr::from_static_str("parquet"),
                ),
                (
                    FastStr::from_static_str("spark.sql.sources.schema.numPartCols"),
                    FastStr::from_static_str("1"),
                ),
                (
                    FastStr::from_static_str("spark.sql.sources.schema.partCol.0"),
                    FastStr::from_static_str("region"),
                ),
            ])),
            ..Default::default()
        };

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table {
                columns,
                partition_by,
                ..
            } => {
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].column, "day");
                assert_eq!(
                    columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>(),
                    ["id", "region", "day"]
                );
                assert_eq!(
                    columns
                        .iter()
                        .filter(|column| column.is_partition)
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>(),
                    ["day"]
                );
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_rejects_incomplete_split_spark_schema_metadata() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let table = Table {
            table_name: Some("items".into()),
            sd: Some(StorageDescriptor::default()),
            parameters: Some(AHashMap::from_iter([(
                FastStr::from_static_str("spark.sql.sources.schema.numParts"),
                FastStr::from_static_str("2"),
            )])),
            ..Default::default()
        };

        let error = table_to_status("hms", &namespace, &table).unwrap_err();

        assert!(error
            .to_string()
            .contains("Missing split property part spark.sql.sources.schema.part.0"));
    }

    #[test]
    fn test_table_to_status_falls_back_to_hms_columns_without_spark_schema() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let table = Table {
            table_name: Some("events".into()),
            table_type: Some(super::EXTERNAL_TABLE_TYPE.into()),
            sd: Some(StorageDescriptor {
                location: Some("s3://warehouse/events".into()),
                cols: Some(vec![
                    FieldSchema {
                        name: Some("id".into()),
                        r#type: Some("bigint".into()),
                        comment: Some("identifier".into()),
                    },
                    FieldSchema {
                        name: Some("tags".into()),
                        r#type: Some("array<string>".into()),
                        comment: Some("label tags".into()),
                    },
                    FieldSchema {
                        name: Some("attrs".into()),
                        r#type: Some("map<string,int>".into()),
                        comment: None,
                    },
                    FieldSchema {
                        name: Some("payload".into()),
                        r#type: Some("struct<flag:boolean,score:int>".into()),
                        comment: Some("nested payload".into()),
                    },
                ]),
                input_format: Some(HiveStorageFormat::parquet().input_format.into()),
                output_format: Some(HiveStorageFormat::parquet().output_format.into()),
                serde_info: Some(SerDeInfo {
                    serialization_lib: Some(HiveStorageFormat::parquet().serde_library.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            partition_keys: Some(vec![
                FieldSchema {
                    name: Some("day".into()),
                    r#type: Some("string".into()),
                    comment: Some("partition day".into()),
                },
                FieldSchema {
                    name: Some("event_date".into()),
                    r#type: Some("date".into()),
                    comment: Some("partition date".into()),
                },
            ]),
            parameters: Some(AHashMap::from_iter([(
                FastStr::from_static_str("owner"),
                FastStr::from_static_str("alice"),
            )])),
            ..Default::default()
        };

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table {
                columns,
                partition_by,
                properties,
                ..
            } => {
                assert_eq!(
                    columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
                    vec!["id", "tags", "attrs", "payload", "day", "event_date"]
                );
                assert_eq!(columns[0].comment.as_deref(), Some("identifier"));
                assert!(matches!(columns[1].data_type, DataType::List(_)));
                assert!(matches!(columns[2].data_type, DataType::Map(_, _)));
                assert!(matches!(columns[3].data_type, DataType::Struct(_)));
                assert!(columns[4].is_partition);
                assert!(columns[5].is_partition);
                assert!(matches!(columns[5].data_type, DataType::Date32));
                assert_eq!(
                    partition_by
                        .iter()
                        .map(|field| field.column.clone())
                        .collect::<Vec<_>>(),
                    vec!["day".to_string(), "event_date".to_string()]
                );
                assert_eq!(properties, vec![("owner".to_string(), "alice".to_string())]);
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_split_large_table_prop_round_trips_unicode_value() {
        let original = "αβγδ😎こんにちは🚀".repeat(40);
        let split = super::split_large_table_prop(SPARK_SCHEMA_KEY, &original, 13);
        assert!(split.len() > 2);

        let parameters = AHashMap::from_iter(
            split
                .into_iter()
                .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v))),
        );

        let decoded = super::read_large_table_prop(Some(&parameters), SPARK_SCHEMA_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_split_large_table_prop_round_trips_empty_value() {
        let split = super::split_large_table_prop(SPARK_SCHEMA_KEY, "", 13);
        assert_eq!(split, vec![(SPARK_SCHEMA_KEY.to_string(), String::new())]);

        let parameters = AHashMap::from_iter(
            split
                .into_iter()
                .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v))),
        );

        let decoded = super::read_large_table_prop(Some(&parameters), SPARK_SCHEMA_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(decoded, "");
    }

    #[test]
    fn test_read_large_table_prop_rejects_zero_num_parts() {
        let parameters = AHashMap::from_iter([(
            FastStr::from_static_str("spark.sql.sources.schema.numParts"),
            FastStr::from_static_str("0"),
        )]);
        let result = super::read_large_table_prop(Some(&parameters), SPARK_SCHEMA_KEY);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_large_table_prop_does_not_split_at_exact_threshold() {
        let value = "a".repeat(13);
        let split = super::split_large_table_prop(SPARK_SCHEMA_KEY, &value, 13);
        assert_eq!(split, vec![(SPARK_SCHEMA_KEY.to_string(), value)]);
        let value_unicode = "😎".repeat(5);
        let split_unicode = super::split_large_table_prop(SPARK_SCHEMA_KEY, &value_unicode, 5);
        assert_eq!(
            split_unicode,
            vec![(SPARK_SCHEMA_KEY.to_string(), value_unicode)]
        );
    }

    #[test]
    fn test_read_large_table_prop_rejects_missing_mid_sequence_part() {
        let parameters = AHashMap::from_iter([
            (
                FastStr::from_static_str("spark.sql.sources.schema.numParts"),
                FastStr::from_static_str("3"),
            ),
            (
                FastStr::from_static_str("spark.sql.sources.schema.part.0"),
                FastStr::from_static_str("present"),
            ),
            (
                FastStr::from_static_str("spark.sql.sources.schema.part.2"),
                FastStr::from_static_str("present"),
            ),
        ]);
        let result = super::read_large_table_prop(Some(&parameters), SPARK_SCHEMA_KEY);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("part.1"),
            "Expected error mentioning part.1, got: {err}"
        );
    }

    #[test]
    fn test_columns_from_spark_properties_rejects_malformed_json() {
        let parameters = AHashMap::from_iter([(
            FastStr::from_string(SPARK_SCHEMA_KEY.to_string()),
            FastStr::from_string("{invalid json}".to_string()),
        )]);
        let result = columns_from_spark_properties(Some(&parameters));
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Failed to parse Spark schema JSON"),
            "Expected parse error, got: {err}"
        );
    }

    #[test]
    fn test_columns_from_spark_properties_rejects_non_struct_json() {
        let parameters = AHashMap::from_iter([(
            FastStr::from_string(SPARK_SCHEMA_KEY.to_string()),
            FastStr::from_string(r#""just a string""#.to_string()),
        )]);
        let result = columns_from_spark_properties(Some(&parameters));
        assert!(result.is_err());
    }

    #[test]
    fn test_inject_spark_metadata_with_empty_columns() {
        let mut table = build_generic_table(
            "default",
            "empty_cols",
            vec![],
            vec![],
            Some("s3://warehouse/empty_cols".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
        )
        .unwrap();

        inject_spark_metadata(&mut table, &[], &[], "parquet").unwrap();

        let props = table.parameters.as_ref().unwrap();
        let schema_json = props.get(SPARK_SCHEMA_KEY).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(schema_json.as_str()).unwrap();
        assert_eq!(parsed, serde_json::json!({"type": "struct", "fields": []}));

        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table { columns, .. } => {
                assert!(
                    columns.is_empty(),
                    "Expected empty columns, got {columns:?}"
                );
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_injected_spark_metadata_preserves_mixed_complex_columns_and_partitions() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let mut table = build_generic_table(
            "default",
            "mixed_events",
            vec![
                CreateTableColumnOptions {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: Some("primary key".to_string()),
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "payload".to_string(),
                    data_type: DataType::Struct(Fields::from(vec![
                        Field::new(
                            "items",
                            DataType::List(std::sync::Arc::new(Field::new(
                                "item",
                                DataType::Struct(Fields::from(vec![
                                    Field::new("label", DataType::Utf8, true),
                                    Field::new(
                                        "attrs",
                                        DataType::Map(
                                            std::sync::Arc::new(Field::new(
                                                "entries",
                                                DataType::Struct(Fields::from(vec![
                                                    Field::new("keys", DataType::Utf8, false),
                                                    Field::new(
                                                        "values",
                                                        DataType::List(std::sync::Arc::new(
                                                            Field::new_list_field(
                                                                DataType::Int32,
                                                                true,
                                                            ),
                                                        )),
                                                        true,
                                                    ),
                                                ])),
                                                false,
                                            )),
                                            false,
                                        ),
                                        true,
                                    ),
                                ])),
                                true,
                            ))),
                            true,
                        ),
                        Field::new("active", DataType::Boolean, true),
                    ])),
                    nullable: true,
                    comment: Some("nested payload".to_string()),
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    comment: Some("category partition".to_string()),
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "event_date".to_string(),
                    data_type: DataType::Date32,
                    nullable: false,
                    comment: Some("date partition".to_string()),
                    default: None,
                    generated_always_as: None,
                },
            ],
            vec!["category".to_string(), "event_date".to_string()],
            Some("s3://warehouse/mixed_events".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            Some("mixed events table".to_string()),
            vec![("owner".to_string(), "alice".to_string())],
        )
        .unwrap();

        let all_columns = vec![
            CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: Some("primary key".to_string()),
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "payload".to_string(),
                data_type: DataType::Struct(Fields::from(vec![
                    Field::new(
                        "items",
                        DataType::List(std::sync::Arc::new(Field::new(
                            "item",
                            DataType::Struct(Fields::from(vec![
                                Field::new("label", DataType::Utf8, true),
                                Field::new(
                                    "attrs",
                                    DataType::Map(
                                        std::sync::Arc::new(Field::new(
                                            "entries",
                                            DataType::Struct(Fields::from(vec![
                                                Field::new("keys", DataType::Utf8, false),
                                                Field::new(
                                                    "values",
                                                    DataType::List(std::sync::Arc::new(
                                                        Field::new_list_field(
                                                            DataType::Int32,
                                                            true,
                                                        ),
                                                    )),
                                                    true,
                                                ),
                                            ])),
                                            false,
                                        )),
                                        false,
                                    ),
                                    true,
                                ),
                            ])),
                            true,
                        ))),
                        true,
                    ),
                    Field::new("active", DataType::Boolean, true),
                ])),
                nullable: true,
                comment: Some("nested payload".to_string()),
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "category".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                comment: Some("category partition".to_string()),
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "event_date".to_string(),
                data_type: DataType::Date32,
                nullable: false,
                comment: Some("date partition".to_string()),
                default: None,
                generated_always_as: None,
            },
        ];

        inject_spark_metadata(
            &mut table,
            &all_columns,
            &["category".to_string(), "event_date".to_string()],
            "parquet",
        )
        .unwrap();

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table {
                columns,
                partition_by,
                properties,
                ..
            } => {
                assert_eq!(
                    columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
                    vec!["id", "payload", "category", "event_date"]
                );
                assert_eq!(columns[0].comment.as_deref(), Some("primary key"));
                assert!(!columns[0].nullable);
                assert_eq!(columns[1].comment.as_deref(), Some("nested payload"));
                assert!(columns[1].nullable);
                assert!(matches!(columns[1].data_type, DataType::Struct(_)));
                assert!(columns[2].is_partition);
                assert!(columns[3].is_partition);
                assert!(matches!(columns[3].data_type, DataType::Date32));
                assert_eq!(
                    partition_by
                        .iter()
                        .map(|field| field.column.clone())
                        .collect::<Vec<_>>(),
                    vec!["category".to_string(), "event_date".to_string()]
                );
                assert!(properties.iter().any(|(k, v)| k == "owner" && v == "alice"));
                assert!(!properties.iter().any(|(k, _)| k.starts_with("spark.sql.")));
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

    #[test]
    fn test_build_generic_table_rejects_spark_sql_properties() {
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
            vec![],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                ("spark.sql.foo".to_string(), "bar".to_string()),
                ("owner".to_string(), "alice".to_string()),
            ],
        )
        .unwrap_err();
        assert!(error.to_string().contains("spark.sql.foo"));
        assert!(error.to_string().contains("Spark-internal"));
    }

    #[test]
    fn test_build_generic_table_rejects_external_key() {
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
            vec![],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![("EXTERNAL".to_string(), "TRUE".to_string())],
        )
        .unwrap_err();
        assert!(error.to_string().contains("EXTERNAL"));
        assert!(error.to_string().contains("Spark-internal"));
    }

    #[test]
    fn test_table_to_status_preserves_file_uri_locations() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let table = Table {
            table_name: Some("items".into()),
            sd: Some(StorageDescriptor {
                location: Some("file:/tmp/warehouse/default.db/items".into()),
                cols: Some(vec![FieldSchema {
                    name: Some("id".into()),
                    r#type: Some("int".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table { location, .. } => {
                assert_eq!(
                    location.as_deref(),
                    Some("file:/tmp/warehouse/default.db/items")
                );
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_preserves_non_local_uri_locations() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let table = Table {
            table_name: Some("items".into()),
            sd: Some(StorageDescriptor {
                location: Some("s3://warehouse/default.db/items".into()),
                cols: Some(vec![FieldSchema {
                    name: Some("id".into()),
                    r#type: Some("int".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let status = table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            TableKind::Table { location, .. } => {
                assert_eq!(location.as_deref(), Some("s3://warehouse/default.db/items"));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }
}
