use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Field};
use chrono::Utc;
use hive_metastore::{Database, FieldSchema, PrincipalType, SerDeInfo, StorageDescriptor, Table};
use pilota::{AHashMap, FastStr};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::hive_format::{HiveDetectedFormat, HiveStorageFormat};
use sail_catalog::provider::{
    CreateDatabaseOptions, CreateTableColumnOptions, CreateViewColumnOptions, CreateViewOptions,
    Namespace, PartitionFilter, PartitionPredicate, PartitionPredicateOp, PartitionSpec,
    PartitionStatus,
};
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::catalog::{
    identity_partition_fields, CatalogTableBucketBy, CatalogTableSort, ColumnStatistics,
    DatabaseStatus, TableColumnStatus, TableKind, TableStatistics, TableStatus,
};

use crate::data_type::{arrow_to_hive_type, hive_type_to_arrow, spark_struct_json_to_fields};

pub(crate) const COMMENT_KEY: &str = "comment";
pub(crate) const EXTERNAL_KEY: &str = "EXTERNAL";
pub(crate) const EXTERNAL_TRUE: &str = "TRUE";
pub(crate) const SPARK_DATASOURCE_PROVIDER_KEY: &str = "spark.sql.sources.provider";
pub(crate) const SPARK_SCHEMA_KEY: &str = "spark.sql.sources.schema";
pub(crate) const SPARK_STATS_TOTAL_SIZE_KEY: &str = "spark.sql.statistics.totalSize";
pub(crate) const SPARK_STATS_NUM_ROWS_KEY: &str = "spark.sql.statistics.numRows";
pub(crate) const HMS_TOTAL_SIZE_KEY: &str = "TOTAL_SIZE";
pub(crate) const HMS_RAW_DATA_SIZE_KEY: &str = "RAW_DATA_SIZE";
pub(crate) const HMS_ROW_COUNT_KEY: &str = "ROW_COUNT";
pub(crate) const HMS_DDL_TIME_KEY: &str = "transient_lastDdlTime";
pub(crate) const HIVE_DEFAULT_PARTITION_NAME: &str = "__HIVE_DEFAULT_PARTITION__";
pub(crate) const EXTERNAL_TABLE_TYPE: &str = "EXTERNAL_TABLE";
pub(crate) const MANAGED_TABLE_TYPE: &str = "MANAGED_TABLE";
pub(crate) const VIRTUAL_VIEW_TYPE: &str = "VIRTUAL_VIEW";
/// Placeholder owner matching Spark/Hive convention for unauthenticated contexts.
/// Used as a literal owner name when no authenticated principal is available.
pub(crate) const DEFAULT_OWNER: &str = "user.name";

pub(crate) struct GenericTableFormat<'a> {
    pub logical_format: &'a str,
    pub storage: &'a HiveStorageFormat,
}

pub(crate) struct GenericTableLocation {
    pub value: Option<String>,
    pub is_external: bool,
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
    let statistics = table_statistics_from_properties(table.parameters.as_ref())?;
    let properties = filter_spark_properties(map_to_vec(table.parameters.as_ref()));
    let comment = extract_property(table.parameters.as_ref(), COMMENT_KEY);
    let storage = table.sd.as_ref();
    let partition_columns = partition_columns_for_table(table)?;
    let spark_columns = columns_from_spark_properties(table.parameters.as_ref())?;
    let hms_columns = columns_from_hms(storage, table.partition_keys.as_ref())?;
    let columns = if let Some(spark_cols) = spark_columns {
        // When the SD contains Spark's sentinel placeholder (col:array<string>),
        // skip reconciliation — the real schema is always in properties.
        // Otherwise, reconcile: if column names diverged, the table was
        // altered externally in Hive, so fall back to HMS columns unless
        // the respectSparkSchema storage property explicitly overrides this.
        let respect_spark_schema = storage
            .and_then(|sd| sd.serde_info.as_ref())
            .and_then(|serde| serde.parameters.as_ref())
            .and_then(|params| params.get("respectSparkSchema"))
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if is_sentinel_storage_descriptor(storage)
            || respect_spark_schema
            || schemas_match_ignoring_case_and_nullability(&spark_cols, &hms_columns)
        {
            reorder_spark_columns(spark_cols, &partition_columns)?
        } else {
            hms_columns
        }
    } else {
        hms_columns
    };
    let location = table_location(storage, table.parameters.as_ref())
        .map(|location| normalize_location_uri(&location));
    let format = table_provider_format(table.parameters.as_ref()).unwrap_or_else(|| {
        detect_hms_logical_format(
            storage
                .and_then(|sd| sd.serde_info.as_ref())
                .and_then(|serde| serde.serialization_lib.as_deref()),
            storage.and_then(|sd| sd.input_format.as_deref()),
            storage.and_then(|sd| sd.output_format.as_deref()),
        )
    });
    let partition_by = identity_partition_fields(&partition_columns);
    let (sort_by, bucket_by) = restore_bucket_sort_from_properties(table.parameters.as_ref())?;
    let table_type = match table.table_type.as_deref() {
        Some(EXTERNAL_TABLE_TYPE) => Some("EXTERNAL".to_string()),
        Some(MANAGED_TABLE_TYPE) => Some("MANAGED".to_string()),
        _ => None,
    };

    Ok(TableStatus {
        catalog: Some(catalog.to_string()),
        database: database.clone().into(),
        name,
        statistics,
        kind: TableKind::Table {
            table_type,
            columns,
            comment,
            constraints: vec![],
            location,
            format,
            partition_by,
            sort_by,
            bucket_by,
            properties,
        },
    })
}

fn normalize_location_uri(location: &str) -> String {
    if location.starts_with("file:/") && !location.starts_with("file://") {
        format!("file:///{}", location.trim_start_matches("file:/"))
    } else {
        location.to_string()
    }
}

pub(crate) fn normalize_location_uri_for_hms_write(location: &str) -> String {
    if location.starts_with("file:///") {
        format!("file:/{}", location.trim_start_matches("file:///"))
    } else {
        location.to_string()
    }
}

fn table_location(
    storage: Option<&StorageDescriptor>,
    parameters: Option<&AHashMap<FastStr, FastStr>>,
) -> Option<String> {
    storage.and_then(|sd| {
        let path = sd
            .serde_info
            .as_ref()
            .and_then(|serde| storage_path_property(serde.parameters.as_ref()));
        let location = sd.location.as_ref().map(ToString::to_string);
        if table_provider_format(parameters).is_some() {
            path.or(location)
        } else {
            location.or(path)
        }
    })
}

fn storage_path_property(parameters: Option<&AHashMap<FastStr, FastStr>>) -> Option<String> {
    parameters.and_then(|parameters| {
        parameters
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("path"))
            .map(|(_, value)| value.to_string())
    })
}

fn field_names(keys: &[FieldSchema]) -> Vec<String> {
    keys.iter()
        .filter_map(|field| field.name.as_ref().map(ToString::to_string))
        .collect()
}

pub(crate) fn partition_columns_for_table(table: &Table) -> CatalogResult<Vec<String>> {
    if let Some(partition_columns) =
        spark_partition_columns_from_properties(table.parameters.as_ref())?
    {
        return Ok(partition_columns);
    }
    Ok(table
        .partition_keys
        .as_ref()
        .map(|keys| field_names(keys))
        .unwrap_or_default())
}

fn spark_partition_columns_from_properties(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
) -> CatalogResult<Option<Vec<String>>> {
    let Some(params) = parameters else {
        return Ok(None);
    };
    if !params.contains_key("spark.sql.sources.schema.numPartCols") {
        return Ok(None);
    }
    Ok(Some(get_column_names_by_type(
        params,
        "Part",
        "part",
        "partitioning columns",
    )?))
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
        statistics: None,
        kind: TableKind::View {
            definition,
            columns: columns_from_hms(table.sd.as_ref(), None)?,
            comment: extract_property(table.parameters.as_ref(), COMMENT_KEY),
            properties: filter_spark_properties(map_to_vec(table.parameters.as_ref())),
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
    let location = location.map(|location| normalize_location_uri_for_hms_write(&location));

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

#[cfg(test)]
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
    build_generic_table_with_location_kind(
        database_name,
        table_name,
        columns,
        partition_columns,
        GenericTableLocation {
            value: location.clone(),
            is_external: location.is_some(),
        },
        format,
        comment,
        properties,
    )
}

pub(crate) fn build_generic_table_with_location_kind(
    database_name: &str,
    table_name: &str,
    columns: Vec<CreateTableColumnOptions>,
    partition_columns: Vec<String>,
    location: GenericTableLocation,
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
    if location.is_external {
        parameters.get_or_insert_with(AHashMap::new).insert(
            FastStr::from_static_str(EXTERNAL_KEY),
            FastStr::from_static_str(EXTERNAL_TRUE),
        );
    }

    let storage_location = location
        .value
        .map(|location| normalize_location_uri_for_hms_write(&location));
    let serde_parameters = storage_location.as_ref().map(|location| {
        [(
            FastStr::from_static_str("path"),
            FastStr::from_string(location.clone()),
        )]
        .into_iter()
        .collect()
    });

    Ok(Table {
        db_name: Some(database_name.to_string().into()),
        table_name: Some(table_name.to_string().into()),
        owner: Some(DEFAULT_OWNER.into()),
        create_time: Some(current_time_secs()?),
        last_access_time: Some(current_time_secs()?),
        table_type: Some(
            if location.is_external {
                EXTERNAL_TABLE_TYPE
            } else {
                MANAGED_TABLE_TYPE
            }
            .into(),
        ),
        sd: Some(StorageDescriptor {
            cols: Some(regular_columns),
            location: storage_location.clone().map(Into::into),
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

/// Injects Spark-compatible metadata properties into an HMS `Table` for the
/// write path. Called after `build_generic_table` and before `create_hms_table`.
pub(crate) fn inject_spark_metadata(
    table: &mut Table,
    columns: &[CreateTableColumnOptions],
    partition_columns: &[String],
    format: &str,
) -> CatalogResult<()> {
    use crate::data_type::spark_struct_json_from_fields;

    let parameters = table.parameters.get_or_insert_with(AHashMap::new);

    // Serialize schema to Spark StructType JSON
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

    // Split large schema into parts if needed
    let schema_props = split_large_table_prop(SPARK_SCHEMA_KEY, &schema_json, 4000);
    for (key, value) in schema_props {
        parameters.insert(FastStr::from_string(key), FastStr::from_string(value));
    }

    // Partition column metadata (preserves original case to match Spark's
    // tableMetaToTableProps which writes partitionColumnNames as-is;
    // Spark's reorderSchema does exact-match: schema.find(_.name == partCol)).
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

    // Provider
    parameters.insert(
        FastStr::from_static_str(SPARK_DATASOURCE_PROVIDER_KEY),
        FastStr::from_string(format.to_string()),
    );

    // Partition provider (indicates partition management is handled by the
    // catalog rather than the data source, matching Spark's convention).
    if !partition_columns.is_empty() {
        parameters.insert(
            FastStr::from_static_str("spark.sql.partitionProvider"),
            FastStr::from_static_str("catalog"),
        );
    }

    // Create version
    let version = env!("CARGO_PKG_VERSION");
    parameters.insert(
        FastStr::from_static_str("spark.sql.create.version"),
        FastStr::from_string(format!("sail-{version}")),
    );

    Ok(())
}

pub(crate) fn extract_property(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
    key: &str,
) -> Option<String> {
    parameters.and_then(|props| props.get(key).map(ToString::to_string))
}

fn restore_bucket_sort_from_properties(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
) -> CatalogResult<(Vec<CatalogTableSort>, Option<CatalogTableBucketBy>)> {
    let Some(params) = parameters else {
        return Ok((vec![], None));
    };

    let sort_by = restore_sort_from_properties(params)?;
    let bucket_by = restore_bucket_from_properties(params)?;
    Ok((sort_by, bucket_by))
}

fn restore_sort_from_properties(
    params: &AHashMap<FastStr, FastStr>,
) -> CatalogResult<Vec<CatalogTableSort>> {
    if !params.contains_key("spark.sql.sources.schema.numSortCols") {
        return Ok(vec![]);
    }
    let sort_cols = get_column_names_by_type(params, "Sort", "sort", "sorting columns")?;
    Ok(sort_cols
        .into_iter()
        .map(|column| CatalogTableSort {
            column,
            ascending: true,
        })
        .collect())
}

fn restore_bucket_from_properties(
    params: &AHashMap<FastStr, FastStr>,
) -> CatalogResult<Option<CatalogTableBucketBy>> {
    let Some(num_buckets) = parse_usize_property(params, "spark.sql.sources.schema.numBuckets")?
    else {
        return Ok(None);
    };
    if !params.contains_key("spark.sql.sources.schema.numBucketCols") {
        return Err(CatalogError::External(
            "Missing bucketing columns count property spark.sql.sources.schema.numBucketCols"
                .to_string(),
        ));
    }
    let columns = get_column_names_by_type(params, "Bucket", "bucket", "bucketing columns")?;
    Ok(Some(CatalogTableBucketBy {
        columns,
        num_buckets,
    }))
}

fn table_provider_format(parameters: Option<&AHashMap<FastStr, FastStr>>) -> Option<String> {
    extract_property(parameters, SPARK_DATASOURCE_PROVIDER_KEY)
        .filter(|provider| !provider.trim().eq_ignore_ascii_case("hive"))
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

pub(crate) fn map_to_hashmap(
    values: Option<&AHashMap<FastStr, FastStr>>,
) -> HashMap<String, String> {
    values
        .into_iter()
        .flat_map(|map| {
            map.iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
        })
        .collect()
}

pub(crate) fn filter_spark_properties(values: Vec<(String, String)>) -> Vec<(String, String)> {
    values
        .into_iter()
        .filter(|(key, _)| !key.starts_with("spark.sql.") && key != HMS_DDL_TIME_KEY)
        .filter(|(key, _)| key != EXTERNAL_KEY)
        .collect()
}

fn table_statistics_from_properties(
    parameters: Option<&AHashMap<FastStr, FastStr>>,
) -> CatalogResult<Option<TableStatistics>> {
    let Some(parameters) = parameters else {
        return Ok(None);
    };

    let spark_size = parse_u64_property(parameters, SPARK_STATS_TOTAL_SIZE_KEY)?;
    let spark_rows = parse_u64_property(parameters, SPARK_STATS_NUM_ROWS_KEY)?;
    let has_spark_col_stats = parameters
        .keys()
        .any(|key| key.starts_with(SPARK_COL_STATS_PREFIX));
    if spark_size.is_some() {
        let col_stats = parse_col_stats_from_properties(parameters)?;
        return Ok(Some(TableStatistics {
            size_in_bytes: spark_size,
            row_count: spark_rows,
            col_stats,
        }));
    }
    if spark_rows.is_some() || has_spark_col_stats {
        return Ok(None);
    }

    let native_size = parse_u64_property(parameters, HMS_TOTAL_SIZE_KEY)?
        .or(parse_u64_property(parameters, HMS_RAW_DATA_SIZE_KEY)?);
    let native_rows = parse_u64_property(parameters, HMS_ROW_COUNT_KEY)?;
    if native_size.is_some() || native_rows.is_some() {
        return Ok(Some(TableStatistics {
            size_in_bytes: native_size,
            row_count: native_rows,
            col_stats: Default::default(),
        }));
    }

    Ok(None)
}

fn parse_u64_property(
    parameters: &AHashMap<FastStr, FastStr>,
    key: &str,
) -> CatalogResult<Option<u64>> {
    parameters
        .get(key)
        .map(|value| {
            value.parse().map_err(|_| {
                CatalogError::External(format!("Invalid unsigned integer property {key}: {value}"))
            })
        })
        .transpose()
}

fn parse_usize_property(
    parameters: &AHashMap<FastStr, FastStr>,
    key: &str,
) -> CatalogResult<Option<usize>> {
    parameters
        .get(key)
        .map(|value| {
            value.parse().map_err(|_| {
                CatalogError::External(format!("Invalid unsigned integer property {key}: {value}"))
            })
        })
        .transpose()
}

fn get_column_names_by_type(
    parameters: &AHashMap<FastStr, FastStr>,
    col_type_capitalized: &str,
    col_type_lower: &str,
    type_name: &str,
) -> CatalogResult<Vec<String>> {
    let num_cols_key = format!("spark.sql.sources.schema.num{col_type_capitalized}Cols");
    let Some(num_cols) = parse_usize_property(parameters, &num_cols_key)? else {
        return Ok(vec![]);
    };
    let mut cols = Vec::with_capacity(num_cols);
    for index in 0..num_cols {
        let key = format!("spark.sql.sources.schema.{col_type_lower}Col.{index}");
        let col = parameters.get(key.as_str()).ok_or_else(|| {
            CatalogError::External(format!(
                "Missing {type_name} property {key} (index {index} of {num_cols})"
            ))
        })?;
        cols.push(col.to_string());
    }
    Ok(cols)
}

pub(crate) const SPARK_COL_STATS_PREFIX: &str = "spark.sql.statistics.colStats.";

/// Parses `spark.sql.statistics.colStats.{col}.{stat}` properties into a
/// per-column stats map. Follows Spark's `CatalogColumnStat.fromMap` protocol:
/// columns are discovered by finding `version` keys, then scalar fields are
/// read. Histograms are silently skipped (deferred phase).
fn parse_col_stats_from_properties(
    parameters: &AHashMap<FastStr, FastStr>,
) -> CatalogResult<HashMap<String, ColumnStatistics>> {
    let mut col_stats = HashMap::new();

    for (key, value) in parameters {
        let Some(rest) = key.strip_prefix(SPARK_COL_STATS_PREFIX) else {
            continue;
        };
        // We only discover columns from the version key to avoid partial reads
        let Some(col_name) = rest.strip_suffix(".version") else {
            continue;
        };
        if col_name.is_empty() {
            continue;
        }

        let version: i32 = value.parse().map_err(|_| {
            CatalogError::External(format!("Invalid colStats version for {col_name}: {value}"))
        })?;

        let prefix = format!("{SPARK_COL_STATS_PREFIX}{col_name}.");
        let distinct_count = parameters
            .get(format!("{prefix}distinctCount").as_str())
            .and_then(|v| v.parse().ok());
        let min = parameters
            .get(format!("{prefix}min").as_str())
            .map(|v| v.to_string());
        let max = parameters
            .get(format!("{prefix}max").as_str())
            .map(|v| v.to_string());
        let null_count = parameters
            .get(format!("{prefix}nullCount").as_str())
            .and_then(|v| v.parse().ok());
        let avg_len = parameters
            .get(format!("{prefix}avgLen").as_str())
            .and_then(|v| v.parse().ok());
        let max_len = parameters
            .get(format!("{prefix}maxLen").as_str())
            .and_then(|v| v.parse().ok());
        // histogram is intentionally ignored for this phase

        col_stats.insert(
            col_name.to_string(),
            ColumnStatistics {
                version: Some(version),
                distinct_count,
                min,
                max,
                null_count,
                avg_len,
                max_len,
            },
        );
    }

    Ok(col_stats)
}

/// Serializes `TableStatistics` into `spark.sql.statistics.*` properties for the
/// write path. Returns key-value pairs ready to insert into HMS table parameters.
pub(crate) fn table_statistics_to_properties(
    stats: &TableStatistics,
) -> CatalogResult<Vec<(String, String)>> {
    if stats.size_in_bytes.is_none() && (stats.row_count.is_some() || !stats.col_stats.is_empty()) {
        return Err(CatalogError::InvalidArgument(
            "Spark statistics require spark.sql.statistics.totalSize when rowCount or column statistics are present".to_string(),
        ));
    }

    let mut props = Vec::new();

    if let Some(size) = stats.size_in_bytes {
        props.push((SPARK_STATS_TOTAL_SIZE_KEY.to_string(), size.to_string()));
    }
    if let Some(rows) = stats.row_count {
        props.push((SPARK_STATS_NUM_ROWS_KEY.to_string(), rows.to_string()));
    }

    for (col_name, col_stat) in &stats.col_stats {
        let prefix = format!("{SPARK_COL_STATS_PREFIX}{col_name}.");
        props.push((format!("{prefix}version"), "2".to_string()));
        if let Some(distinct_count) = col_stat.distinct_count {
            props.push((format!("{prefix}distinctCount"), distinct_count.to_string()));
        }
        if let Some(min) = &col_stat.min {
            props.push((format!("{prefix}min"), min.clone()));
        }
        if let Some(max) = &col_stat.max {
            props.push((format!("{prefix}max"), max.clone()));
        }
        if let Some(null_count) = col_stat.null_count {
            props.push((format!("{prefix}nullCount"), null_count.to_string()));
        }
        if let Some(avg_len) = col_stat.avg_len {
            props.push((format!("{prefix}avgLen"), avg_len.to_string()));
        }
        if let Some(max_len) = col_stat.max_len {
            props.push((format!("{prefix}maxLen"), max_len.to_string()));
        }
    }

    Ok(props)
}

pub(crate) fn clear_table_statistics_properties(parameters: &mut AHashMap<FastStr, FastStr>) {
    parameters.retain(|key, _| {
        !key.starts_with("spark.sql.statistics.")
            && !matches!(
                key.as_str(),
                HMS_TOTAL_SIZE_KEY | HMS_RAW_DATA_SIZE_KEY | HMS_ROW_COUNT_KEY
            )
    });
}

pub(crate) fn split_large_table_prop(
    key: &str,
    value: &str,
    threshold: usize,
) -> Vec<(String, String)> {
    if value.chars().count() <= threshold {
        return vec![(key.to_string(), value.to_string())];
    }

    let parts: Vec<String> = value
        .chars()
        .collect::<Vec<_>>()
        .chunks(threshold)
        .map(|chunk| chunk.iter().collect())
        .collect();
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

pub(crate) fn read_large_table_prop(
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

    let mut output = String::new();
    for index in 0..num_parts {
        let part_key = format!("{key}.part.{index}");
        let part = parameters.get(part_key.as_str()).ok_or_else(|| {
            CatalogError::External(format!("Missing split property part {part_key}"))
        })?;
        output.push_str(part);
    }
    Ok(Some(output))
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

/// Detects Spark's sentinel schema: a single column named "col" of type
/// `array<string>` in the HMS `StorageDescriptor`. This placeholder is used
/// when the real schema contains Hive-incompatible types and is stored in
/// `spark.sql.sources.schema` properties instead.
fn is_sentinel_storage_descriptor(storage: Option<&StorageDescriptor>) -> bool {
    let Some(sd) = storage else {
        return false;
    };
    let Some(cols) = sd.cols.as_ref() else {
        return false;
    };
    if cols.len() != 1 {
        return false;
    }
    let Some(name) = cols[0].name.as_deref() else {
        return false;
    };
    let Some(r#type) = cols[0].r#type.as_deref() else {
        return false;
    };
    name == "col" && r#type == "array<string>"
}

/// Compares Spark property columns against HMS SD columns by checking column
/// count, names (case-insensitive) and data types (ignoring nullability) match.
/// This mirrors Spark's `DataTypeUtils.equalsIgnoreCaseNullabilityAndCollation`
/// which verifies structural shape including types, while ignoring field name
/// case, nullability, and collation.
fn schemas_match_ignoring_case_and_nullability(
    spark_cols: &[TableColumnStatus],
    hms_cols: &[TableColumnStatus],
) -> bool {
    if spark_cols.len() != hms_cols.len() {
        return false;
    }
    spark_cols.iter().zip(hms_cols.iter()).all(|(sc, hc)| {
        sc.name.eq_ignore_ascii_case(&hc.name)
            && data_types_equal_ignoring_nullability(&sc.data_type, &hc.data_type)
    })
}

/// Recursively compares two Arrow data types, ignoring nullability on nested
/// struct/list/map fields. This matches Spark's structural type comparison
/// which ignores nullability and collation but requires matching type kinds.
fn data_types_equal_ignoring_nullability(a: &DataType, b: &DataType) -> bool {
    use arrow::datatypes::DataType::*;
    match (a, b) {
        // Primitive types: exact discriminant match
        (Null, Null)
        | (Boolean, Boolean)
        | (Int8, Int8)
        | (Int16, Int16)
        | (Int32, Int32)
        | (Int64, Int64)
        | (UInt8, UInt8)
        | (UInt16, UInt16)
        | (UInt32, UInt32)
        | (UInt64, UInt64)
        | (Float16, Float16)
        | (Float32, Float32)
        | (Float64, Float64)
        | (Utf8, Utf8)
        | (LargeUtf8, LargeUtf8)
        | (Utf8View, Utf8View)
        | (Binary, Binary)
        | (LargeBinary, LargeBinary)
        | (BinaryView, BinaryView)
        | (Date32, Date32)
        | (Date64, Date64) => true,

        (Decimal128(p1, s1), Decimal128(p2, s2)) | (Decimal256(p1, s1), Decimal256(p2, s2)) => {
            p1 == p2 && s1 == s2
        }

        (Timestamp(u1, _), Timestamp(u2, _)) => u1 == u2,
        (Time32(u1), Time32(u2)) => u1 == u2,
        (Time64(u1), Time64(u2)) => u1 == u2,
        (Duration(u1), Duration(u2)) => u1 == u2,
        (Interval(u1), Interval(u2)) => u1 == u2,

        (FixedSizeBinary(n1), FixedSizeBinary(n2)) => n1 == n2,

        // Nested types: recurse, ignoring field nullability
        (List(f1), List(f2)) | (LargeList(f1), LargeList(f2)) | (ListView(f1), ListView(f2)) => {
            f1.name().eq_ignore_ascii_case(f2.name())
                && data_types_equal_ignoring_nullability(f1.data_type(), f2.data_type())
        }
        (FixedSizeList(f1, n1), FixedSizeList(f2, n2)) => {
            n1 == n2
                && f1.name().eq_ignore_ascii_case(f2.name())
                && data_types_equal_ignoring_nullability(f1.data_type(), f2.data_type())
        }
        (Struct(fields_a), Struct(fields_b)) => {
            fields_a.len() == fields_b.len()
                && fields_a.iter().zip(fields_b.iter()).all(|(fa, fb)| {
                    fa.name().eq_ignore_ascii_case(fb.name())
                        && data_types_equal_ignoring_nullability(fa.data_type(), fb.data_type())
                })
        }
        (Map(f1, sorted1), Map(f2, sorted2)) => {
            sorted1 == sorted2
                && data_types_equal_ignoring_nullability(f1.data_type(), f2.data_type())
        }
        (Union(fields_a, mode_a), Union(fields_b, mode_b)) => {
            mode_a == mode_b
                && fields_a.len() == fields_b.len()
                && fields_a
                    .iter()
                    .zip(fields_b.iter())
                    .all(|((id_a, fa), (id_b, fb))| {
                        id_a == id_b
                            && fa.name().eq_ignore_ascii_case(fb.name())
                            && data_types_equal_ignoring_nullability(fa.data_type(), fb.data_type())
                    })
        }
        (Dictionary(k1, v1), Dictionary(k2, v2)) => {
            data_types_equal_ignoring_nullability(k1, k2)
                && data_types_equal_ignoring_nullability(v1, v2)
        }
        (RunEndEncoded(r1, v1), RunEndEncoded(r2, v2)) => {
            data_types_equal_ignoring_nullability(r1.data_type(), r2.data_type())
                && data_types_equal_ignoring_nullability(v1.data_type(), v2.data_type())
        }

        // Different discriminants
        _ => false,
    }
}

/// Reorders Spark property columns to put partition columns at the end and
/// marks them with `is_partition: true`, matching Spark's `reorderSchema`.
fn reorder_spark_columns(
    columns: Vec<TableColumnStatus>,
    partition_columns: &[String],
) -> CatalogResult<Vec<TableColumnStatus>> {
    if partition_columns.is_empty() {
        return Ok(columns);
    }

    for part_col in partition_columns {
        if !columns.iter().any(|c| c.name == *part_col) {
            return Err(CatalogError::External(format!(
                "Spark schema is missing partition column '{part_col}'"
            )));
        }
    }

    let mut partition_fields = HashMap::new();
    let mut regular_fields = Vec::new();
    for mut column in columns {
        if partition_columns.contains(&column.name) {
            column.is_partition = true;
            partition_fields.insert(column.name.clone(), column);
        } else {
            regular_fields.push(column);
        }
    }

    for part_col in partition_columns {
        let field = partition_fields.remove(part_col).ok_or_else(|| {
            CatalogError::External(format!("Missing partition column field '{part_col}'"))
        })?;
        regular_fields.push(field);
    }

    Ok(regular_fields)
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

/// Converts an HMS `Partition` to a `PartitionStatus`.
/// `partition_columns` provides the partition column names to pair with the
/// positional HMS partition values. If not provided, the spec will use empty keys.
pub(crate) fn partition_to_status(
    partition: &hive_metastore::Partition,
    partition_columns: Option<&[String]>,
) -> CatalogResult<PartitionStatus> {
    let values: Vec<String> = partition
        .values
        .as_deref()
        .map(|vals| vals.iter().map(|v| v.to_string()).collect())
        .unwrap_or_default();

    let spec = match partition_columns {
        Some(cols) => cols
            .iter()
            .zip(values.iter())
            .map(|(col, val)| (col.clone(), val.clone()))
            .collect(),
        None => values.into_iter().map(|v| (String::new(), v)).collect(),
    };

    let location = partition
        .sd
        .as_ref()
        .and_then(|sd| sd.location.as_ref())
        .map(ToString::to_string);
    let parameters = map_to_hashmap(partition.parameters.as_ref());
    let create_time = partition.create_time.map(|t| t as i64);
    let last_access_time = partition.last_access_time.map(|t| t as i64);
    let statistics = table_statistics_from_properties(partition.parameters.as_ref())?;

    Ok(PartitionStatus {
        spec,
        location,
        parameters,
        create_time,
        last_access_time,
        statistics,
    })
}

/// Converts a `PartitionStatus` to an HMS `Partition` for write operations.
pub(crate) fn status_to_partition(
    database: &str,
    table: &str,
    status: &PartitionStatus,
    partition_columns: &[String],
    template_partition: Option<&hive_metastore::Partition>,
    table_storage_descriptor: Option<&StorageDescriptor>,
) -> CatalogResult<hive_metastore::Partition> {
    let canonical_spec = canonicalize_partition_spec(&status.spec, partition_columns)?;
    let mut partition = template_partition.cloned().unwrap_or_default();
    let values: Vec<FastStr> = canonical_spec
        .iter()
        .map(|(_col, v)| FastStr::from_string(v.clone()))
        .collect();

    let mut sd = partition
        .sd
        .clone()
        .or_else(|| table_storage_descriptor.cloned());
    if let Some(location) = &status.location {
        let descriptor = sd.get_or_insert_with(Default::default);
        descriptor.location = Some(location.clone().into());
    } else if template_partition.is_none() && !canonical_spec.is_empty() {
        if let Some(descriptor) = sd.as_mut() {
            if let Some(table_location) = descriptor.location.as_ref() {
                let table_location = table_location.trim_end_matches('/');
                if !table_location.is_empty() {
                    descriptor.location = Some(
                        format!(
                            "{}/{}",
                            table_location,
                            partition_spec_to_name(&canonical_spec)
                        )
                        .into(),
                    );
                }
            }
        }
    }

    let parameters = {
        let mut params = partition.parameters.clone().unwrap_or_default();
        for (k, v) in &status.parameters {
            params.insert(
                FastStr::from_string(k.clone()),
                FastStr::from_string(v.clone()),
            );
        }
        if let Some(stats) = &status.statistics {
            params.retain(|key, _| !key.starts_with("spark.sql.statistics."));
            let stats_props = table_statistics_to_properties(stats)?;
            for (key, value) in stats_props {
                params.insert(key.into(), value.into());
            }
        }
        (!params.is_empty()).then_some(params)
    };

    if !canonical_spec.is_empty() {
        partition.values = Some(values);
    }
    partition.db_name = Some(database.to_string().into());
    partition.table_name = Some(table.to_string().into());
    partition.create_time = status
        .create_time
        .and_then(|t| i32::try_from(t).ok())
        .or(partition.create_time);
    partition.last_access_time = status
        .last_access_time
        .and_then(|t| i32::try_from(t).ok())
        .or(partition.last_access_time);
    partition.sd = sd;
    partition.parameters = parameters;

    Ok(partition)
}

pub(crate) fn canonicalize_partition_spec(
    spec: &PartitionSpec,
    partition_columns: &[String],
) -> CatalogResult<PartitionSpec> {
    if partition_columns.is_empty() {
        if spec.is_empty() {
            return Ok(vec![]);
        }
        return Err(CatalogError::InvalidArgument(format!(
            "Partition spec has extra columns not in table partition schema: {}",
            spec.iter()
                .map(|(k, _)| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let mut values_by_index: Vec<Option<String>> = vec![None; partition_columns.len()];
    let mut seen_indices = HashSet::new();
    let mut extra_columns = Vec::new();
    let mut duplicate_columns = Vec::new();

    for (column, value) in spec {
        let index = partition_columns
            .iter()
            .position(|expected| expected.eq_ignore_ascii_case(column));
        let Some(index) = index else {
            extra_columns.push(column.clone());
            continue;
        };

        if !seen_indices.insert(index) {
            duplicate_columns.push(column.clone());
            continue;
        }

        values_by_index[index] = Some(value.clone());
    }

    let missing_columns: Vec<String> = partition_columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| values_by_index[idx].is_none().then_some(col.clone()))
        .collect();

    if !missing_columns.is_empty() || !extra_columns.is_empty() || !duplicate_columns.is_empty() {
        let mut details = Vec::new();
        if !missing_columns.is_empty() {
            details.push(format!("missing columns: {}", missing_columns.join(", ")));
        }
        if !extra_columns.is_empty() {
            details.push(format!("extra columns: {}", extra_columns.join(", ")));
        }
        if !duplicate_columns.is_empty() {
            details.push(format!(
                "duplicate columns: {}",
                duplicate_columns.join(", ")
            ));
        }
        return Err(CatalogError::InvalidArgument(format!(
            "Invalid partition spec; {}",
            details.join("; ")
        )));
    }

    let mut canonical = Vec::with_capacity(partition_columns.len());
    for (idx, col) in partition_columns.iter().enumerate() {
        let value = values_by_index[idx].clone().ok_or_else(|| {
            CatalogError::Internal(format!(
                "partition spec normalization invariant violated for column '{col}'"
            ))
        })?;
        canonical.push((col.clone(), value));
    }

    Ok(canonical)
}

/// Renders a `PartitionFilter` to an HMS JDOQL filter string for
/// `get_partitions_by_filter`. Returns `Err(NotSupported)` for unsupported
/// expressions rather than falling back silently.
pub(crate) fn render_partition_filter(filter: &PartitionFilter) -> CatalogResult<String> {
    match filter {
        PartitionFilter::All => Ok(String::new()),
        PartitionFilter::Spec(spec) => {
            let clauses: Vec<String> = spec
                .iter()
                .map(|(col, val)| format!("{col} = \"{}\"", escape_jdoql_value(val)))
                .collect();
            Ok(clauses.join(" AND "))
        }
        PartitionFilter::Predicate(pred) => render_predicate(pred),
    }
}

fn render_predicate(pred: &PartitionPredicate) -> CatalogResult<String> {
    match pred {
        PartitionPredicate::Compare { column, op, value } => {
            let op_str = match op {
                PartitionPredicateOp::Eq => "=",
                PartitionPredicateOp::NotEq => "!=",
                PartitionPredicateOp::Lt => "<",
                PartitionPredicateOp::LtEq => "<=",
                PartitionPredicateOp::Gt => ">",
                PartitionPredicateOp::GtEq => ">=",
            };
            Ok(format!(
                "{column} {op_str} \"{}\"",
                escape_jdoql_value(value)
            ))
        }
        PartitionPredicate::And(exprs) => {
            let rendered: Vec<String> = exprs
                .iter()
                .map(render_predicate)
                .collect::<CatalogResult<Vec<_>>>()?;
            Ok(format!("({})", rendered.join(" AND ")))
        }
        PartitionPredicate::Or(exprs) => {
            let rendered: Vec<String> = exprs
                .iter()
                .map(render_predicate)
                .collect::<CatalogResult<Vec<_>>>()?;
            Ok(format!("({})", rendered.join(" OR ")))
        }
    }
}

/// Escapes special characters in a JDOQL string literal value.
/// JDOQL uses double-quote delimiters for string values, so internal
/// double-quotes and backslashes must be escaped.
fn escape_jdoql_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out
}

/// Escapes partition path components using Spark/Hive-style `%XX` escaping.
/// Mirrors Spark's ExternalCatalogUtils.escapePathName behavior for the
/// relevant ASCII control/special characters in partition path segments.
fn escape_partition_path_name(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if path.is_empty() {
        return path.to_string();
    }
    let mut out = String::with_capacity(path.len() + 16);
    let mut changed = false;
    for c in path.chars() {
        let should_escape = matches!(
            c,
            '\u{0001}'
                ..='\u{001F}'
                    | '"'
                    | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '\u{007F}'
                    | '{'
                    | '['
                    | ']'
                    | '^'
        ) || (cfg!(windows) && matches!(c, ' ' | '<' | '>' | '|'));
        if should_escape && (c as u32) <= 0xFF {
            changed = true;
            let byte = c as u32;
            out.push('%');
            out.push(HEX[((byte >> 4) & 0xF) as usize] as char);
            out.push(HEX[(byte & 0xF) as usize] as char);
        } else {
            out.push(c);
        }
    }
    if changed {
        out
    } else {
        path.to_string()
    }
}

/// Renders a `PartitionSpec` to an HMS partition name string.
/// HMS uses `key=value/key=value` format for partition names.
/// Partition column names preserve canonical casing from the caller.
pub(crate) fn partition_spec_to_name(spec: &PartitionSpec) -> String {
    spec.iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                escape_partition_path_name(k),
                partition_value_to_path_string(v)
            )
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn partition_value_to_path_string(value: &str) -> String {
    if value.is_empty() {
        HIVE_DEFAULT_PARTITION_NAME.to_string()
    } else {
        escape_partition_path_name(value)
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use hive_metastore::{FieldSchema, SerDeInfo, StorageDescriptor};
    use pilota::{AHashMap, FastStr};
    use sail_catalog::hive_format::HiveStorageFormat;
    use sail_catalog::provider::{
        CreateDatabaseOptions, CreateTableColumnOptions, CreateViewColumnOptions,
        CreateViewOptions, PartitionFilter, PartitionPredicate, PartitionPredicateOp,
        PartitionSpec, PartitionStatus,
    };
    use sail_common_datafusion::catalog::{ColumnStatistics, TableStatistics};

    use super::{
        build_database, build_generic_table, build_generic_table_with_location_kind, build_view,
        database_to_status, inject_spark_metadata, is_view_table, map_to_vec,
        read_large_table_prop, split_large_table_prop, table_statistics_from_properties,
        table_statistics_to_properties, validate_namespace, vec_to_map, GenericTableFormat,
        COMMENT_KEY, SPARK_DATASOURCE_PROVIDER_KEY, SPARK_SCHEMA_KEY, VIRTUAL_VIEW_TYPE,
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
    fn test_build_database_normalizes_file_uri_for_hms_write() {
        let database = build_database(
            &sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap(),
            CreateDatabaseOptions {
                comment: None,
                location: Some("file:///tmp/custom-db".to_string()),
                if_not_exists: false,
                properties: vec![],
            },
        )
        .unwrap();

        assert_eq!(
            database.location_uri.as_deref(),
            Some("file:/tmp/custom-db")
        );
    }

    #[test]
    fn test_build_generic_table_marks_explicit_location_as_external_with_storage_path() {
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
        assert_eq!(
            table.sd.as_ref().and_then(|sd| sd.location.as_deref()),
            Some("s3://warehouse/items")
        );
        assert_eq!(
            table
                .sd
                .as_ref()
                .and_then(|sd| sd.serde_info.as_ref())
                .and_then(|serde| serde.parameters.as_ref())
                .and_then(|parameters| parameters.get("path").map(|value| value.as_str())),
            Some("s3://warehouse/items")
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
    fn test_build_generic_table_can_write_managed_table_with_spark_storage_path() {
        let table = build_generic_table_with_location_kind(
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
            super::GenericTableLocation {
                value: Some("s3://warehouse/default/items".to_string()),
                is_external: false,
            },
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
        )
        .unwrap();

        assert_eq!(table.table_type.as_deref(), Some(super::MANAGED_TABLE_TYPE));
        assert_eq!(
            table.sd.as_ref().and_then(|sd| sd.location.as_deref()),
            Some("s3://warehouse/default/items")
        );
        assert_eq!(
            table
                .sd
                .as_ref()
                .and_then(|sd| sd.serde_info.as_ref())
                .and_then(|serde| serde.parameters.as_ref())
                .and_then(|parameters| parameters.get("path").map(|value| value.as_str())),
            Some("s3://warehouse/default/items")
        );
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
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "parquet".to_string(),
            )],
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
    fn test_table_to_status_normalizes_spark_file_location_uri() {
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
            Some("file:/tmp/sail-hms/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "parquet".to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { location, .. } => {
                assert_eq!(location.as_deref(), Some("file:///tmp/sail-hms/items"));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_build_generic_table_writes_spark_style_storage_path_uri() {
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
            Some("file:///tmp/sail-hms/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "parquet".to_string(),
            )],
        )
        .unwrap();

        assert_eq!(
            table.sd.as_ref().and_then(|sd| sd.location.as_deref()),
            Some("file:/tmp/sail-hms/items")
        );
        assert_eq!(
            table
                .sd
                .as_ref()
                .and_then(|sd| sd.serde_info.as_ref())
                .and_then(|serde| serde.parameters.as_ref())
                .and_then(|parameters| parameters.get("path").map(|value| value.as_str())),
            Some("file:/tmp/sail-hms/items")
        );
    }

    #[test]
    fn test_table_to_status_prefers_spark_storage_path_over_sd_location() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let mut table = build_generic_table(
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
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "parquet".to_string(),
            )],
        )
        .unwrap();
        table.sd.as_mut().unwrap().location = Some("s3://warehouse/placeholder".into());

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { location, .. } => {
                assert_eq!(location.as_deref(), Some("s3://warehouse/items"));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_preserves_spark_datasource_provider_format() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "json".to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { format, .. } => {
                assert_eq!(format, "json");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_preserves_raw_deltalake_provider_value() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "deltalake".to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { format, .. } => {
                assert_eq!(format, "deltalake");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_reads_spark_stats_and_native_hms_fallback() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let spark_table = build_generic_table(
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
                (
                    "spark.sql.statistics.totalSize".to_string(),
                    "100".to_string(),
                ),
                ("spark.sql.statistics.numRows".to_string(), "7".to_string()),
                ("TOTAL_SIZE".to_string(), "999".to_string()),
                ("ROW_COUNT".to_string(), "99".to_string()),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &spark_table).unwrap();
        let statistics = status.statistics.unwrap();
        assert_eq!(statistics.size_in_bytes, Some(100));
        assert_eq!(statistics.row_count, Some(7));

        let native_table = build_generic_table(
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
                ("TOTAL_SIZE".to_string(), "999".to_string()),
                ("ROW_COUNT".to_string(), "99".to_string()),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &native_table).unwrap();
        let statistics = status.statistics.unwrap();
        assert_eq!(statistics.size_in_bytes, Some(999));
        assert_eq!(statistics.row_count, Some(99));
    }

    #[test]
    fn test_clear_table_statistics_properties_removes_spark_and_native_hms_stats() {
        let mut parameters = AHashMap::from_iter([
            (
                FastStr::from_static_str(super::SPARK_STATS_TOTAL_SIZE_KEY),
                FastStr::from_static_str("100"),
            ),
            (
                FastStr::from_static_str(super::SPARK_STATS_NUM_ROWS_KEY),
                FastStr::from_static_str("5"),
            ),
            (
                FastStr::from_static_str("spark.sql.statistics.colStats.id.version"),
                FastStr::from_static_str("2"),
            ),
            (
                FastStr::from_static_str(super::HMS_TOTAL_SIZE_KEY),
                FastStr::from_static_str("999"),
            ),
            (
                FastStr::from_static_str(super::HMS_RAW_DATA_SIZE_KEY),
                FastStr::from_static_str("888"),
            ),
            (
                FastStr::from_static_str(super::HMS_ROW_COUNT_KEY),
                FastStr::from_static_str("77"),
            ),
            (
                FastStr::from_static_str("owner"),
                FastStr::from_static_str("sail"),
            ),
        ]);

        super::clear_table_statistics_properties(&mut parameters);

        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters.get("owner").map(|v| v.as_str()), Some("sail"));
    }

    #[test]
    fn test_table_to_status_restores_columns_from_spark_schema_properties() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "customerid".to_string(),
                data_type: DataType::Int64, // HMS SD type matches Spark's "long" -> Int64
                nullable: true,
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark property has CustomerID:long — same type as HMS but different case
                r#"{"type":"struct","fields":[{"name":"CustomerID","type":"long","nullable":false,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        // Types match (both Int64/long), names match ignoring case -> Spark schema wins,
        // preserving case as CustomerID and non-nullable from the Spark properties.
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "CustomerID");
        assert_eq!(columns[0].data_type, DataType::Int64);
        assert!(!columns[0].nullable);
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
    fn test_table_and_view_status_filter_spark_internal_properties() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                ("owner".to_string(), "sail".to_string()),
                ("transient_lastDdlTime".to_string(), "1714399200".to_string()),
                (
                    SPARK_SCHEMA_KEY.to_string(),
                    r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#
                        .to_string(),
                ),
                ("spark.sql.create.version".to_string(), "4.0.0".to_string()),
            ],
        )
        .unwrap();

        let table_status = super::table_to_status("hms", &namespace, &table).unwrap();
        let sail_common_datafusion::catalog::TableKind::Table { properties, .. } =
            table_status.kind
        else {
            panic!("expected table");
        };
        assert_eq!(properties, vec![("owner".to_string(), "sail".to_string())]);

        let view = build_view(
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
                properties: vec![
                    ("owner".to_string(), "sail".to_string()),
                    ("spark.sql.view.foo".to_string(), "bar".to_string()),
                ],
            },
        )
        .unwrap();
        let view_status = super::view_to_status("hms", &namespace, &view).unwrap();
        let sail_common_datafusion::catalog::TableKind::View { properties, .. } = view_status.kind
        else {
            panic!("expected view");
        };
        assert_eq!(properties, vec![("owner".to_string(), "sail".to_string())]);
    }

    #[test]
    fn test_large_table_property_round_trips_direct_and_split_forms() {
        let direct = split_large_table_prop(SPARK_SCHEMA_KEY, "abc", 4);
        assert_eq!(
            direct,
            vec![("spark.sql.sources.schema".to_string(), "abc".to_string())]
        );
        let direct_map = vec_to_map(direct);
        assert_eq!(
            read_large_table_prop(direct_map.as_ref(), SPARK_SCHEMA_KEY).unwrap(),
            Some("abc".to_string())
        );

        let split = split_large_table_prop(SPARK_SCHEMA_KEY, "abcdefghi", 4);
        assert_eq!(
            split,
            vec![
                (
                    "spark.sql.sources.schema.numParts".to_string(),
                    "3".to_string()
                ),
                (
                    "spark.sql.sources.schema.part.0".to_string(),
                    "abcd".to_string()
                ),
                (
                    "spark.sql.sources.schema.part.1".to_string(),
                    "efgh".to_string()
                ),
                (
                    "spark.sql.sources.schema.part.2".to_string(),
                    "i".to_string()
                ),
            ]
        );
        let split_map = vec_to_map(split);
        assert_eq!(
            read_large_table_prop(split_map.as_ref(), SPARK_SCHEMA_KEY).unwrap(),
            Some("abcdefghi".to_string())
        );
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
    fn test_table_to_status_detects_sentinel_and_uses_spark_schema() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let mut table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "col".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                nullable: true,
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                r#"{"type":"struct","fields":[{"name":"real_col","type":"integer","nullable":true,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        // Force the sentinel: sd.cols = [col: array<string>]
        table.sd = Some(StorageDescriptor {
            cols: Some(vec![FieldSchema {
                name: Some("col".into()),
                r#type: Some("array<string>".into()),
                comment: None,
            }]),
            location: Some("s3://warehouse/items".into()),
            input_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".into(),
            ),
            output_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".into(),
            ),
            serde_info: Some(SerDeInfo {
                serialization_lib: Some(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".into(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        });

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "real_col");
        assert_eq!(columns[0].data_type, DataType::Int32);
        // The sentinel sd.cols had col:array<string>, but real_col:int came from properties.
        // This proves sentinel SD was overridden by Spark schema properties.
    }

    #[test]
    fn test_table_to_status_serde_reconciliation_falls_back_to_hms_when_diverged() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark property says column "a:integer" but HMS SD has "b:string"
                r#"{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        // Spark schema and HMS schema diverged (different column names), so
        // reconciliation falls back to the HMS StorageDescriptor columns.
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "b");
        assert_eq!(columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn test_table_to_status_datasource_reconciliation_falls_back_to_hms_when_diverged() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
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
                (
                    SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                    "parquet".to_string(),
                ),
                (
                    SPARK_SCHEMA_KEY.to_string(),
                    r#"{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}"#
                        .to_string(),
                ),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "b");
        assert_eq!(columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn test_table_to_status_hive_provider_reconciliation_falls_back_to_hms_when_diverged() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
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
                (
                    SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                    "hive".to_string(),
                ),
                (
                    SPARK_SCHEMA_KEY.to_string(),
                    r#"{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}"#
                        .to_string(),
                ),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "b");
        assert_eq!(columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn test_table_to_status_serde_uses_spark_schema_when_matching() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "CustomerID".to_string(),
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark property has CustomerID:long, HMS SD has customerid:bigint (same type, different case)
                r#"{"type":"struct","fields":[{"name":"CustomerID","type":"long","nullable":false,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        // Schema matches ignoring case — Spark properties win, preserving case.
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "CustomerID");
        assert_eq!(columns[0].data_type, DataType::Int64);
    }

    #[test]
    fn test_table_to_status_respect_spark_schema_overrides_reconciliation() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "b".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark property says column "a:integer" but HMS SD has "b:string" (diverged)
                r#"{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        // Add respectSparkSchema=true to the SerDe info
        let mut table = table;
        let sd = table.sd.get_or_insert_with(StorageDescriptor::default);
        let serde = sd.serde_info.get_or_insert_with(SerDeInfo::default);
        let params = serde.parameters.get_or_insert_with(AHashMap::new);
        params.insert(
            FastStr::from_static_str("respectSparkSchema"),
            FastStr::from_static_str("true"),
        );

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        // Normally diverged schemas fall back to HMS, but respectSparkSchema=true
        // forces the Spark property schema to be used.
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "a");
        assert_eq!(columns[0].data_type, DataType::Int32);
    }

    #[test]
    fn test_table_to_status_serde_reconciliation_falls_back_when_types_diverge() {
        // When column names match (case-insensitive) but types differ, Spark's
        // equalsIgnoreCaseNullabilityAndCollation returns false and falls back
        // to HMS. Our schemas_match_ignoring_case_and_nullability must also
        // compare data types to match this behavior.
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: DataType::Int64, // HMS SD says bigint
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark property says id:integer but HMS SD has id:bigint
                r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        // Same name but different type => schemas don't match => fall back to HMS
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "id");
        assert_eq!(
            columns[0].data_type,
            DataType::Int64,
            "Should fall back to HMS type (bigint/Int64) when types diverge"
        );
    }

    #[test]
    fn test_table_to_status_reorders_partition_columns_to_end_from_spark_schema() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        // Build a table with no regular SD cols (sentinel) and partition key "day",
        // so Spark schema is the only source of truth and must be reordered.
        let mut table = build_generic_table(
            "default",
            "items",
            vec![
                CreateTableColumnOptions {
                    name: "day".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "reg_col".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
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
            vec![(
                SPARK_SCHEMA_KEY.to_string(),
                // Spark schema has partition col "day" first, then "reg_col"
                r#"{"type":"struct","fields":[{"name":"day","type":"string","nullable":false,"metadata":{}},{"name":"reg_col","type":"long","nullable":true,"metadata":{}}]}"#
                    .to_string(),
            )],
        )
        .unwrap();

        // Force sentinel so Spark schema always wins
        table.sd = Some(StorageDescriptor {
            cols: Some(vec![FieldSchema {
                name: Some("col".into()),
                r#type: Some("array<string>".into()),
                comment: None,
            }]),
            location: Some("s3://warehouse/items".into()),
            input_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".into(),
            ),
            output_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".into(),
            ),
            serde_info: Some(SerDeInfo {
                serialization_lib: Some(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".into(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        });

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let columns = status.kind.columns();

        assert_eq!(columns.len(), 2);
        // Regular column first, partition column last (reordered from Spark schema)
        assert_eq!(columns[0].name, "reg_col");
        assert!(!columns[0].is_partition);
        assert_eq!(columns[1].name, "day");
        assert!(columns[1].is_partition);
    }

    #[test]
    fn test_table_to_status_restores_partition_case_from_spark_partcol_properties() {
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
                    name: "dt".to_string(),
                    data_type: DataType::Date32,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
            ],
            vec!["dt".to_string()],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (
                    SPARK_SCHEMA_KEY.to_string(),
                    r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"Dt","type":"date","nullable":false,"metadata":{}}]}"#.to_string(),
                ),
                (
                    "spark.sql.sources.schema.numPartCols".to_string(),
                    "1".to_string(),
                ),
                (
                    "spark.sql.sources.schema.partCol.0".to_string(),
                    "Dt".to_string(),
                ),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table {
                columns,
                partition_by,
                ..
            } => {
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].column, "Dt");
                assert_eq!(columns.last().map(|c| c.name.as_str()), Some("Dt"));
                assert!(columns.last().is_some_and(|c| c.is_partition));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_restores_bucket_and_sort_from_properties() {
        use sail_common_datafusion::catalog::CatalogTableBucketBy;

        let namespace = sail_catalog::provider::Namespace::try_from(vec!["default"]).unwrap();
        let table = build_generic_table(
            "default",
            "items",
            vec![CreateTableColumnOptions {
                name: "user_id".to_string(),
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
                (SPARK_SCHEMA_KEY.to_string(), r#"{"type":"struct","fields":[{"name":"user_id","type":"long","nullable":false,"metadata":{}}]}"#.to_string()),
                ("spark.sql.sources.schema.numBuckets".to_string(), "4".to_string()),
                ("spark.sql.sources.schema.numBucketCols".to_string(), "1".to_string()),
                ("spark.sql.sources.schema.bucketCol.0".to_string(), "user_id".to_string()),
                ("spark.sql.sources.schema.numSortCols".to_string(), "1".to_string()),
                ("spark.sql.sources.schema.sortCol.0".to_string(), "ts".to_string()),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table {
                bucket_by, sort_by, ..
            } => {
                let bucket = bucket_by.expect("expected bucket_by from properties");
                assert_eq!(
                    bucket,
                    CatalogTableBucketBy {
                        columns: vec!["user_id".to_string()],
                        num_buckets: 4,
                    }
                );
                assert_eq!(sort_by.len(), 1);
                assert_eq!(sort_by[0].column, "ts");
                assert!(sort_by[0].ascending);
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_to_status_errors_on_missing_partcol_entry() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (SPARK_SCHEMA_KEY.to_string(), r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#.to_string()),
                ("spark.sql.sources.schema.numPartCols".to_string(), "1".to_string()),
            ],
        )
        .unwrap();

        let err = super::table_to_status("hms", &namespace, &table).unwrap_err();
        assert!(
            err.to_string().contains("partCol") || err.to_string().contains("partitioning columns")
        );
    }

    #[test]
    fn test_table_to_status_errors_on_missing_bucket_and_sort_entries() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (SPARK_SCHEMA_KEY.to_string(), r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#.to_string()),
                ("spark.sql.sources.schema.numBuckets".to_string(), "4".to_string()),
                ("spark.sql.sources.schema.numBucketCols".to_string(), "1".to_string()),
                ("spark.sql.sources.schema.numSortCols".to_string(), "1".to_string()),
            ],
        )
        .unwrap();

        let err = super::table_to_status("hms", &namespace, &table).unwrap_err();
        assert!(
            err.to_string().contains("bucketCol")
                || err.to_string().contains("sortCol")
                || err.to_string().contains("bucketing")
                || err.to_string().contains("sorting")
        );
    }

    #[test]
    fn test_table_to_status_errors_when_partcol_missing_in_spark_schema() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (SPARK_SCHEMA_KEY.to_string(), r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#.to_string()),
                ("spark.sql.sources.schema.numPartCols".to_string(), "1".to_string()),
                ("spark.sql.sources.schema.partCol.0".to_string(), "dt".to_string()),
            ],
        )
        .unwrap();

        let err = super::table_to_status("hms", &namespace, &table).unwrap_err();
        assert!(err.to_string().contains("partition"));
    }

    #[test]
    fn test_table_to_status_provider_hive_uses_serde_detection() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![(
                SPARK_DATASOURCE_PROVIDER_KEY.to_string(),
                "hive".to_string(),
            )],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        match status.kind {
            sail_common_datafusion::catalog::TableKind::Table { format, .. } => {
                assert_eq!(format, "parquet");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn test_table_statistics_parses_col_stats_from_spark_properties() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (
                    "spark.sql.statistics.totalSize".to_string(),
                    "1000".to_string(),
                ),
                ("spark.sql.statistics.numRows".to_string(), "50".to_string()),
                // Column stats for "mycol"
                (
                    "spark.sql.statistics.colStats.mycol.version".to_string(),
                    "2".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.distinctCount".to_string(),
                    "100".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.min".to_string(),
                    "0".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.max".to_string(),
                    "999".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.nullCount".to_string(),
                    "5".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.avgLen".to_string(),
                    "4".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.mycol.maxLen".to_string(),
                    "4".to_string(),
                ),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let stats = status.statistics.unwrap();
        assert_eq!(stats.size_in_bytes, Some(1000));
        assert_eq!(stats.row_count, Some(50));

        let col_stat = stats
            .col_stats
            .get("mycol")
            .expect("expected mycol col_stats");
        assert_eq!(
            col_stat,
            &ColumnStatistics {
                version: Some(2),
                distinct_count: Some(100),
                min: Some("0".to_string()),
                max: Some("999".to_string()),
                null_count: Some(5),
                avg_len: Some(4),
                max_len: Some(4),
            }
        );
    }

    #[test]
    fn test_table_statistics_skips_histogram_in_col_stats() {
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
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![
                (
                    "spark.sql.statistics.totalSize".to_string(),
                    "500".to_string(),
                ),
                ("spark.sql.statistics.numRows".to_string(), "10".to_string()),
                (
                    "spark.sql.statistics.colStats.x.version".to_string(),
                    "2".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.x.distinctCount".to_string(),
                    "3".to_string(),
                ),
                // Histogram should be silently ignored
                (
                    "spark.sql.statistics.colStats.x.histogram.numParts".to_string(),
                    "1".to_string(),
                ),
                (
                    "spark.sql.statistics.colStats.x.histogram.part.0".to_string(),
                    "{}".to_string(),
                ),
            ],
        )
        .unwrap();

        let status = super::table_to_status("hms", &namespace, &table).unwrap();
        let stats = status.statistics.unwrap();
        let col_stat = stats.col_stats.get("x").expect("expected x col_stats");
        assert_eq!(col_stat.distinct_count, Some(3));
        // histogram is not a field on ColumnStatistics in this phase
    }

    #[test]
    fn test_table_statistics_ignores_spark_stats_without_total_size() {
        let params: AHashMap<FastStr, FastStr> = [
            ("spark.sql.statistics.numRows".to_string(), "10".to_string()),
            (
                "spark.sql.statistics.colStats.x.version".to_string(),
                "2".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.x.distinctCount".to_string(),
                "3".to_string(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v)))
        .collect();

        let stats = super::table_statistics_from_properties(Some(&params)).unwrap();
        assert!(stats.is_none());
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

        // Existing user property preserved
        assert!(props.iter().any(|(k, v)| k == "owner" && v == "alice"));
        // Schema property present
        assert!(props.iter().any(|(k, _)| k == SPARK_SCHEMA_KEY));
        // Partition column metadata (preserves original case to match Spark's
        // tableMetaToTableProps which writes partitionColumnNames as-is)
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.sources.schema.partCol.0" && v == "day"));
        // Provider
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.sources.provider" && v == "parquet"));
        // Create version
        assert!(props.iter().any(|(k, _)| k == "spark.sql.create.version"));
        // Partition provider
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.partitionProvider" && v == "catalog"));
    }

    #[test]
    fn test_inject_spark_metadata_preserves_partition_column_case() {
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
                    name: "DayCol".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
            ],
            vec!["DayCol".to_string()],
            Some("s3://warehouse/items".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            None,
            vec![],
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
                    name: "DayCol".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
            ],
            &["DayCol".to_string()],
            "parquet",
        )
        .unwrap();

        let props = map_to_vec(table.parameters.as_ref());
        // partCol values preserve original case (DayCol stays DayCol)
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.sources.schema.partCol.0" && v == "DayCol"));
        // partitionProvider should be set
        assert!(props
            .iter()
            .any(|(k, v)| k == "spark.sql.partitionProvider" && v == "catalog"));
    }

    #[test]
    fn test_table_statistics_to_properties_writes_spark_keys() {
        let stats = TableStatistics {
            size_in_bytes: Some(1024),
            row_count: Some(42),
            col_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "mycol".to_string(),
                    ColumnStatistics {
                        version: Some(2),
                        distinct_count: Some(100),
                        min: Some("0".to_string()),
                        max: Some("999".to_string()),
                        null_count: Some(5),
                        avg_len: Some(4),
                        max_len: Some(4),
                    },
                );
                m
            },
        };

        let props = table_statistics_to_properties(&stats).unwrap();
        let props_map: HashMap<&str, &str> = props
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        assert_eq!(
            props_map.get("spark.sql.statistics.totalSize"),
            Some(&"1024")
        );
        assert_eq!(props_map.get("spark.sql.statistics.numRows"), Some(&"42"));
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.version"),
            Some(&"2")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.distinctCount"),
            Some(&"100")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.min"),
            Some(&"0")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.max"),
            Some(&"999")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.nullCount"),
            Some(&"5")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.avgLen"),
            Some(&"4")
        );
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.maxLen"),
            Some(&"4")
        );
    }

    #[test]
    fn test_table_statistics_to_properties_forces_col_stat_version_2() {
        let stats = TableStatistics {
            size_in_bytes: Some(256),
            row_count: None,
            col_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "mycol".to_string(),
                    ColumnStatistics {
                        version: Some(7),
                        distinct_count: Some(1),
                        min: None,
                        max: None,
                        null_count: None,
                        avg_len: None,
                        max_len: None,
                    },
                );
                m
            },
        };

        let props = table_statistics_to_properties(&stats).unwrap();
        let props_map: HashMap<&str, &str> = props
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            props_map.get("spark.sql.statistics.colStats.mycol.version"),
            Some(&"2")
        );
    }

    #[test]
    fn test_table_statistics_to_properties_omits_optional_fields_when_size_present() {
        let stats = TableStatistics {
            size_in_bytes: Some(10),
            row_count: None,
            col_stats: HashMap::new(),
        };

        let props = table_statistics_to_properties(&stats).unwrap();
        let keys: Vec<&str> = props.iter().map(|(k, _)| k.as_str()).collect();

        assert!(keys.iter().any(|k| k.contains("totalSize")));
        assert!(!keys.iter().any(|k| k.contains("numRows")));
    }

    #[test]
    fn test_stats_round_trip_write_then_read() {
        let stats = TableStatistics {
            size_in_bytes: Some(2048),
            row_count: Some(100),
            col_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "age".to_string(),
                    ColumnStatistics {
                        version: Some(2),
                        distinct_count: Some(50),
                        min: Some("1".to_string()),
                        max: Some("120".to_string()),
                        null_count: Some(3),
                        avg_len: Some(4),
                        max_len: Some(4),
                    },
                );
                m
            },
        };

        let props = table_statistics_to_properties(&stats).unwrap();
        let params: AHashMap<FastStr, FastStr> = props
            .into_iter()
            .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v)))
            .collect();

        let round_tripped = table_statistics_from_properties(Some(&params))
            .unwrap()
            .expect("expected Some stats");

        assert_eq!(round_tripped.size_in_bytes, Some(2048));
        assert_eq!(round_tripped.row_count, Some(100));
        let col_stat = round_tripped
            .col_stats
            .get("age")
            .expect("expected age col_stats");
        assert_eq!(col_stat.version, Some(2));
        assert_eq!(col_stat.distinct_count, Some(50));
        assert_eq!(col_stat.min, Some("1".to_string()));
        assert_eq!(col_stat.max, Some("120".to_string()));
        assert_eq!(col_stat.null_count, Some(3));
        assert_eq!(col_stat.avg_len, Some(4));
        assert_eq!(col_stat.max_len, Some(4));
    }

    #[test]
    fn test_table_statistics_to_properties_errors_without_total_size() {
        let stats = TableStatistics {
            size_in_bytes: None,
            row_count: None,
            col_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "age".to_string(),
                    ColumnStatistics {
                        version: None,
                        distinct_count: Some(50),
                        min: Some("1".to_string()),
                        max: Some("120".to_string()),
                        null_count: Some(3),
                        avg_len: None,
                        max_len: None,
                    },
                );
                m
            },
        };

        let err = table_statistics_to_properties(&stats).unwrap_err();
        assert!(err.to_string().contains("totalSize"));
    }

    #[test]
    fn test_partition_spec_to_name_renders_hms_format() {
        let spec: PartitionSpec = vec![
            ("dt".to_string(), "2024-01-01".to_string()),
            ("country".to_string(), "US".to_string()),
        ];
        let name = super::partition_spec_to_name(&spec);
        assert_eq!(name, "dt=2024-01-01/country=US");
    }

    #[test]
    fn test_partition_spec_to_name_preserves_column_name_case() {
        let spec: PartitionSpec = vec![
            ("Dt".to_string(), "2024-01-01".to_string()),
            ("Country".to_string(), "US".to_string()),
        ];
        let name = super::partition_spec_to_name(&spec);
        assert_eq!(name, "Dt=2024-01-01/Country=US");
    }

    #[test]
    fn test_partition_spec_to_name_escapes_path_chars() {
        let spec: PartitionSpec = vec![
            ("D:t".to_string(), "a/b".to_string()),
            ("country".to_string(), "U=S".to_string()),
            ("hash".to_string(), "a%b".to_string()),
        ];
        let name = super::partition_spec_to_name(&spec);
        assert_eq!(name, "D%3At=a%2Fb/country=U%3DS/hash=a%25b");
    }

    #[test]
    fn test_partition_spec_to_name_uses_default_partition_token_for_empty_values() {
        let spec: PartitionSpec = vec![
            ("dt".to_string(), "".to_string()),
            ("country".to_string(), "US".to_string()),
        ];
        let name = super::partition_spec_to_name(&spec);
        assert_eq!(name, "dt=__HIVE_DEFAULT_PARTITION__/country=US");
    }

    #[test]
    fn test_render_partition_filter_all_is_empty() {
        let filter = PartitionFilter::All;
        let result = super::render_partition_filter(&filter).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_render_partition_filter_spec() {
        let spec: PartitionSpec = vec![
            ("dt".to_string(), "2024-01-01".to_string()),
            ("country".to_string(), "US".to_string()),
        ];
        let filter = PartitionFilter::Spec(spec);
        let result = super::render_partition_filter(&filter).unwrap();
        assert_eq!(result, r#"dt = "2024-01-01" AND country = "US""#);
    }

    #[test]
    fn test_render_partition_filter_predicate_compare() {
        let pred = PartitionPredicate::Compare {
            column: "dt".to_string(),
            op: PartitionPredicateOp::GtEq,
            value: "2024-01-01".to_string(),
        };
        let filter = PartitionFilter::Predicate(pred);
        let result = super::render_partition_filter(&filter).unwrap();
        assert_eq!(result, r#"dt >= "2024-01-01""#);
    }

    #[test]
    fn test_render_partition_filter_predicate_and_or() {
        let pred = PartitionPredicate::And(vec![
            PartitionPredicate::Compare {
                column: "dt".to_string(),
                op: PartitionPredicateOp::Eq,
                value: "2024-01-01".to_string(),
            },
            PartitionPredicate::Or(vec![
                PartitionPredicate::Compare {
                    column: "country".to_string(),
                    op: PartitionPredicateOp::Eq,
                    value: "US".to_string(),
                },
                PartitionPredicate::Compare {
                    column: "country".to_string(),
                    op: PartitionPredicateOp::Eq,
                    value: "UK".to_string(),
                },
            ]),
        ]);
        let filter = PartitionFilter::Predicate(pred);
        let result = super::render_partition_filter(&filter).unwrap();
        assert_eq!(
            result,
            r#"(dt = "2024-01-01" AND (country = "US" OR country = "UK"))"#
        );
    }

    #[test]
    fn test_render_partition_filter_escapes_special_chars() {
        let spec: PartitionSpec = vec![("path".to_string(), r#"data"file\test"#.to_string())];
        let filter = PartitionFilter::Spec(spec);
        let result = super::render_partition_filter(&filter).unwrap();
        assert_eq!(result, r#"path = "data\"file\\test""#);
    }

    #[test]
    fn test_partition_to_status_pairs_values_with_column_names() {
        let partition = hive_metastore::Partition {
            values: Some(vec!["2024-01-01".into(), "US".into()]),
            sd: Some(StorageDescriptor {
                location: Some("s3://bucket/dt=2024-01-01/country=US".into()),
                ..Default::default()
            }),
            parameters: Some([("numRows".into(), "42".into())].into_iter().collect()),
            create_time: Some(1700000000),
            last_access_time: Some(1700000001),
            ..Default::default()
        };

        let status = super::partition_to_status(
            &partition,
            Some(&["dt".to_string(), "country".to_string()]),
        )
        .unwrap();

        assert_eq!(
            status.spec,
            vec![
                ("dt".to_string(), "2024-01-01".to_string()),
                ("country".to_string(), "US".to_string()),
            ]
        );
        assert_eq!(
            status.location,
            Some("s3://bucket/dt=2024-01-01/country=US".to_string())
        );
        assert_eq!(status.parameters.get("numRows"), Some(&"42".to_string()));
        assert_eq!(status.create_time, Some(1700000000));
        assert_eq!(status.last_access_time, Some(1700000001));
    }

    #[test]
    fn test_status_to_partition_converts_spec_to_positional_values() {
        let status = PartitionStatus {
            spec: vec![
                ("dt".to_string(), "2024-01-01".to_string()),
                ("country".to_string(), "US".to_string()),
            ],
            location: Some("s3://bucket/dt=2024-01-01/country=US".to_string()),
            parameters: HashMap::new(),
            create_time: Some(1700000000),
            last_access_time: None,
            statistics: None,
        };

        let partition = super::status_to_partition(
            "mydb",
            "mytable",
            &status,
            &["dt".to_string(), "country".to_string()],
            None,
            None,
        )
        .unwrap();

        // HMS values are positional (values only, no keys)
        assert_eq!(
            partition.values,
            Some(vec!["2024-01-01".into(), "US".into()])
        );
        assert_eq!(partition.db_name, Some("mydb".into()));
        assert_eq!(partition.table_name, Some("mytable".into()));
        assert_eq!(partition.create_time, Some(1700000000));
        assert!(partition.sd.as_ref().unwrap().location.is_some());
    }

    #[test]
    fn test_status_to_partition_preserves_template_storage_descriptor_metadata() {
        let status = PartitionStatus {
            spec: vec![("dt".to_string(), "2024-01-01".to_string())],
            location: None,
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let template = hive_metastore::Partition {
            values: Some(vec!["2023-12-31".into()]),
            sd: Some(StorageDescriptor {
                input_format: Some("in".into()),
                output_format: Some("out".into()),
                serde_info: Some(SerDeInfo {
                    serialization_lib: Some("serde".into()),
                    ..Default::default()
                }),
                location: Some("s3://template".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let part = super::status_to_partition(
            "db",
            "tbl",
            &status,
            &["dt".to_string()],
            Some(&template),
            None,
        )
        .unwrap();
        let sd = part.sd.unwrap();
        assert_eq!(sd.input_format.as_deref(), Some("in"));
        assert_eq!(sd.output_format.as_deref(), Some("out"));
        assert_eq!(
            sd.serde_info
                .as_ref()
                .and_then(|s| s.serialization_lib.as_deref()),
            Some("serde")
        );
        // location inherited from template when not explicitly set in status
        assert_eq!(sd.location.as_deref(), Some("s3://template"));
    }

    #[test]
    fn test_status_to_partition_derives_location_from_table_location_when_missing() {
        let status = PartitionStatus {
            spec: vec![
                ("dt".to_string(), "2024-01-01".to_string()),
                ("Country".to_string(), "U/S".to_string()),
            ],
            location: None,
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let table_sd = StorageDescriptor {
            input_format: Some("in".into()),
            output_format: Some("out".into()),
            serde_info: Some(SerDeInfo {
                serialization_lib: Some("serde".into()),
                ..Default::default()
            }),
            location: Some("s3://warehouse/items/".into()),
            ..Default::default()
        };

        let part = super::status_to_partition(
            "db",
            "tbl",
            &status,
            &["dt".to_string(), "country".to_string()],
            None,
            Some(&table_sd),
        )
        .unwrap();
        let sd = part.sd.unwrap();

        assert_eq!(
            sd.location.as_deref(),
            Some("s3://warehouse/items/dt=2024-01-01/country=U%2FS")
        );
        assert_eq!(sd.input_format.as_deref(), Some("in"));
        assert_eq!(sd.output_format.as_deref(), Some("out"));
    }

    #[test]
    fn test_status_to_partition_derives_location_with_default_partition_token() {
        let status = PartitionStatus {
            spec: vec![
                ("dt".to_string(), "".to_string()),
                ("country".to_string(), "US".to_string()),
            ],
            location: None,
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let table_sd = StorageDescriptor {
            location: Some("s3://warehouse/items/".into()),
            ..Default::default()
        };

        let part = super::status_to_partition(
            "db",
            "tbl",
            &status,
            &["dt".to_string(), "country".to_string()],
            None,
            Some(&table_sd),
        )
        .unwrap();
        let sd = part.sd.unwrap();

        assert_eq!(
            sd.location.as_deref(),
            Some("s3://warehouse/items/dt=__HIVE_DEFAULT_PARTITION__/country=US")
        );
        assert_eq!(part.values, Some(vec!["".into(), "US".into()]));
    }

    #[test]
    fn test_status_to_partition_reorders_out_of_order_spec_case_insensitively() {
        let status = PartitionStatus {
            spec: vec![
                ("COUNTRY".to_string(), "US".to_string()),
                ("day".to_string(), "2024-01-01".to_string()),
            ],
            location: None,
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let table_sd = StorageDescriptor {
            location: Some("s3://warehouse/items".into()),
            ..Default::default()
        };

        let part = super::status_to_partition(
            "db",
            "tbl",
            &status,
            &["Day".to_string(), "Country".to_string()],
            None,
            Some(&table_sd),
        )
        .unwrap();

        assert_eq!(part.values, Some(vec!["2024-01-01".into(), "US".into()]));
        assert_eq!(
            part.sd
                .as_ref()
                .and_then(|sd| sd.location.as_ref())
                .map(|v| v.to_string()),
            Some("s3://warehouse/items/Day=2024-01-01/Country=US".to_string())
        );
    }

    #[test]
    fn test_partition_spec_to_name_uses_canonical_mixed_case_keys() {
        let canonical_spec: PartitionSpec = vec![
            ("Day".to_string(), "2024-01-01".to_string()),
            ("Country".to_string(), "U/S".to_string()),
        ];
        let name = super::partition_spec_to_name(&canonical_spec);
        assert_eq!(name, "Day=2024-01-01/Country=U%2FS");
    }

    #[test]
    fn test_status_to_partition_keeps_raw_values_and_escapes_only_location() {
        let status = PartitionStatus {
            spec: vec![
                ("Path".to_string(), "a/b".to_string()),
                ("Empty".to_string(), "".to_string()),
            ],
            location: None,
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        };
        let table_sd = StorageDescriptor {
            location: Some("s3://warehouse/raw".into()),
            ..Default::default()
        };

        let part = super::status_to_partition(
            "db",
            "tbl",
            &status,
            &["path".to_string(), "empty".to_string()],
            None,
            Some(&table_sd),
        )
        .unwrap();

        assert_eq!(part.values, Some(vec!["a/b".into(), "".into()]));
        assert_eq!(
            part.sd
                .as_ref()
                .and_then(|sd| sd.location.as_ref())
                .map(|v| v.to_string()),
            Some("s3://warehouse/raw/path=a%2Fb/empty=__HIVE_DEFAULT_PARTITION__".to_string())
        );
    }

    #[test]
    fn test_canonicalize_partition_spec_rejects_missing_extra_and_duplicate_columns() {
        let err = super::canonicalize_partition_spec(
            &vec![
                ("dt".to_string(), "2024-01-01".to_string()),
                ("DT".to_string(), "2024-01-02".to_string()),
                ("region".to_string(), "us".to_string()),
            ],
            &["dt".to_string(), "country".to_string()],
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("missing columns: country"));
        assert!(message.contains("extra columns: region"));
        assert!(message.contains("duplicate columns: DT"));
    }

    /// Full end-to-end round-trip: create_table injects Spark metadata, then
    /// table_to_status reconstructs it. Verifies schema, partition columns,
    /// stats, bucket/sort, provider, create-version, and property filtering
    /// without needing a live HMS server.
    #[test]
    fn test_full_table_metadata_round_trip() {
        let namespace = sail_catalog::provider::Namespace::try_from(vec!["mydb"]).unwrap();

        // Build a table with inject_spark_metadata (write path)
        let mut table = build_generic_table(
            "mydb",
            "orders",
            vec![
                CreateTableColumnOptions {
                    name: "order_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: Some("order identifier".to_string()),
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "CustomerID".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "amount".to_string(),
                    data_type: DataType::Decimal128(10, 2),
                    nullable: true,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
                CreateTableColumnOptions {
                    name: "Dt".to_string(),
                    data_type: DataType::Date32,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                },
            ],
            vec!["Dt".to_string()],
            Some("s3://warehouse/orders".to_string()),
            GenericTableFormat {
                logical_format: "parquet",
                storage: &HiveStorageFormat::parquet(),
            },
            Some("test orders table".to_string()),
            vec![("owner".to_string(), "sail".to_string())],
        )
        .unwrap();

        let columns_for_inject = vec![
            CreateTableColumnOptions {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: Some("order identifier".to_string()),
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "CustomerID".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "amount".to_string(),
                data_type: DataType::Decimal128(10, 2),
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
            },
            CreateTableColumnOptions {
                name: "Dt".to_string(),
                data_type: DataType::Date32,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
            },
        ];
        inject_spark_metadata(
            &mut table,
            &columns_for_inject,
            &["Dt".to_string()],
            "parquet",
        )
        .unwrap();

        // Simulate stats written by alter_table_stats
        let stats = TableStatistics {
            size_in_bytes: Some(4096),
            row_count: Some(100),
            col_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "amount".to_string(),
                    ColumnStatistics {
                        version: Some(2),
                        distinct_count: Some(50),
                        min: Some("0.00".to_string()),
                        max: Some("999.99".to_string()),
                        null_count: Some(5),
                        avg_len: Some(8),
                        max_len: Some(8),
                    },
                );
                m
            },
        };
        let stats_props = super::table_statistics_to_properties(&stats).unwrap();
        let params = table.parameters.get_or_insert_with(AHashMap::new);
        for (key, value) in stats_props {
            params.insert(key.into(), value.into());
        }

        // Simulate bucket/sort from a Spark-created table
        params.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numBuckets"),
            FastStr::from_static_str("8"),
        );
        params.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numBucketCols"),
            FastStr::from_static_str("1"),
        );
        params.insert(
            FastStr::from_static_str("spark.sql.sources.schema.bucketCol.0"),
            FastStr::from_static_str("order_id"),
        );
        params.insert(
            FastStr::from_static_str("spark.sql.sources.schema.numSortCols"),
            FastStr::from_static_str("1"),
        );
        params.insert(
            FastStr::from_static_str("spark.sql.sources.schema.sortCol.0"),
            FastStr::from_static_str("CustomerID"),
        );

        // READ PATH: table_to_status
        let status = super::table_to_status("hms", &namespace, &table).unwrap();

        // Verify basic info
        assert_eq!(status.name, "orders");
        assert_eq!(status.catalog, Some("hms".to_string()));

        // Verify statistics
        let read_stats = status.statistics.unwrap();
        assert_eq!(read_stats.size_in_bytes, Some(4096));
        assert_eq!(read_stats.row_count, Some(100));
        let col_stat = read_stats.col_stats.get("amount").unwrap();
        assert_eq!(col_stat.version, Some(2));
        assert_eq!(col_stat.distinct_count, Some(50));
        assert_eq!(col_stat.min, Some("0.00".to_string()));
        assert_eq!(col_stat.max, Some("999.99".to_string()));

        // Verify columns from Spark schema (exact case preserved)
        let kind = status.kind;
        let (columns, partition_by, sort_by, bucket_by, format, properties) = match &kind {
            sail_common_datafusion::catalog::TableKind::Table {
                columns,
                partition_by,
                sort_by,
                bucket_by,
                format,
                properties,
                ..
            } => (
                columns,
                partition_by,
                sort_by,
                bucket_by,
                format,
                properties,
            ),
            other => panic!("expected table, got {other:?}"),
        };

        // Schema reconstructed from Spark properties: 3 regular + 1 partition at end
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0].name, "order_id");
        assert!(!columns[0].nullable);
        // Comments are preserved in Spark JSON metadata and survive the round-trip.
        assert_eq!(columns[0].comment, Some("order identifier".to_string()));
        assert_eq!(columns[1].name, "CustomerID"); // exact case preserved
        assert!(columns[1].nullable);
        assert_eq!(columns[2].name, "amount");
        // Partition column at end (reordered from Spark schema)
        assert_eq!(columns[3].name, "Dt");
        assert!(columns[3].is_partition);

        // Partition columns
        assert_eq!(partition_by.len(), 1);
        assert_eq!(partition_by[0].column, "Dt");

        // Bucket/sort restored
        let bucket = bucket_by.as_ref().unwrap();
        assert_eq!(bucket.num_buckets, 8);
        assert_eq!(bucket.columns, vec!["order_id".to_string()]);
        assert_eq!(sort_by.len(), 1);
        assert_eq!(sort_by[0].column, "CustomerID");

        // Format from provider property
        assert_eq!(format, "parquet");

        // spark.sql.* properties filtered from user-visible output
        assert!(properties.iter().any(|(k, v)| k == "owner" && v == "sail"));
        assert!(!properties.iter().any(|(k, _)| k.starts_with("spark.sql.")));
    }

    /// Verifies that Spark external string formats for column stat min/max are
    /// preserved through the read and write paths. The caller is responsible for
    /// producing Spark-compatible external strings (e.g. ISO-8601 dates,
    /// timestamp strings, decimal formatting). The catalog stores them as-is.
    #[test]
    fn test_col_stats_min_max_external_string_formats() {
        let props: AHashMap<FastStr, FastStr> = [
            (
                "spark.sql.statistics.totalSize".to_string(),
                "1000".to_string(),
            ),
            ("spark.sql.statistics.numRows".to_string(), "10".to_string()),
            // Integer column
            (
                "spark.sql.statistics.colStats.id.version".to_string(),
                "2".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.id.distinctCount".to_string(),
                "100".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.id.min".to_string(),
                "1".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.id.max".to_string(),
                "999".to_string(),
            ),
            // Date column (Spark external format: ISO-8601 date)
            (
                "spark.sql.statistics.colStats.dt.version".to_string(),
                "2".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.dt.min".to_string(),
                "2024-01-01".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.dt.max".to_string(),
                "2024-12-31".to_string(),
            ),
            // Decimal column (Spark external format: plain decimal string)
            (
                "spark.sql.statistics.colStats.price.version".to_string(),
                "2".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.price.min".to_string(),
                "0.99".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.price.max".to_string(),
                "9999.99".to_string(),
            ),
            // Timestamp column (Spark external format: ISO-8601 timestamp)
            (
                "spark.sql.statistics.colStats.ts.version".to_string(),
                "2".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.ts.min".to_string(),
                "2024-01-01T00:00:00.000000Z".to_string(),
            ),
            (
                "spark.sql.statistics.colStats.ts.max".to_string(),
                "2024-12-31T23:59:59.999999Z".to_string(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v)))
        .collect();

        let stats = super::table_statistics_from_properties(Some(&props))
            .unwrap()
            .unwrap();

        // Integer
        let id = stats.col_stats.get("id").unwrap();
        assert_eq!(id.min, Some("1".to_string()));
        assert_eq!(id.max, Some("999".to_string()));

        // Date (external string format preserved as-is)
        let dt = stats.col_stats.get("dt").unwrap();
        assert_eq!(dt.min, Some("2024-01-01".to_string()));
        assert_eq!(dt.max, Some("2024-12-31".to_string()));

        // Decimal
        let price = stats.col_stats.get("price").unwrap();
        assert_eq!(price.min, Some("0.99".to_string()));
        assert_eq!(price.max, Some("9999.99".to_string()));

        // Timestamp
        let ts = stats.col_stats.get("ts").unwrap();
        assert_eq!(ts.min, Some("2024-01-01T00:00:00.000000Z".to_string()));
        assert_eq!(ts.max, Some("2024-12-31T23:59:59.999999Z".to_string()));

        // Round-trip through write path
        let written = super::table_statistics_to_properties(&stats).unwrap();
        let written_map: AHashMap<FastStr, FastStr> = written
            .into_iter()
            .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v)))
            .collect();

        let round_tripped = super::table_statistics_from_properties(Some(&written_map))
            .unwrap()
            .unwrap();
        assert_eq!(
            round_tripped.col_stats["dt"].min,
            Some("2024-01-01".to_string())
        );
        assert_eq!(
            round_tripped.col_stats["price"].max,
            Some("9999.99".to_string())
        );
        assert_eq!(
            round_tripped.col_stats["ts"].min,
            Some("2024-01-01T00:00:00.000000Z".to_string())
        );
    }

    /// Stats deletion: passing None removes all spark.sql.statistics.* properties.
    /// This tests the write-path helper's role in the delete scenario.
    #[test]
    fn test_stats_deletion_clears_all_stats_properties() {
        // Build a table with stats
        let stats = TableStatistics {
            size_in_bytes: Some(100),
            row_count: Some(5),
            col_stats: HashMap::new(),
        };
        let props = super::table_statistics_to_properties(&stats).unwrap();
        let mut params: AHashMap<FastStr, FastStr> = props
            .into_iter()
            .map(|(k, v)| (FastStr::from_string(k), FastStr::from_string(v)))
            .collect();
        // Add a user property to verify it survives
        params.insert(
            FastStr::from_static_str("owner"),
            FastStr::from_static_str("sail"),
        );

        // Simulate deletion: retain non-stats properties
        params.retain(|key, _| !key.starts_with("spark.sql.statistics."));

        // Verify stats are gone, user property survives
        assert!(!params
            .keys()
            .any(|k| k.starts_with("spark.sql.statistics.")));
        assert_eq!(
            params.get("owner").map(|v| v.to_string()),
            Some("sail".to_string())
        );

        // Verify reading stats from remaining properties returns None
        let result = super::table_statistics_from_properties(Some(&params)).unwrap();
        assert!(result.is_none());
    }
}
