use std::sync::Arc;

use arrow_schema::{ArrowError, Schema, SchemaRef};
use datafusion_common::Result;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

/// Metadata keys for bucketing information embedded in Parquet files.
///
/// Schema-level metadata (shared across all files of a bucketed table):
///   - `COLUMNS`: Comma-separated bucket column names
///   - `NUM_BUCKETS`: Number of buckets
///   - `HASH`: Hash function used for bucketing
///   - `SORT_COLUMNS`: Sort order within each bucket (if specified)
///
/// File-level metadata (per Parquet file, in `WriterProperties` key-value pairs):
///   - `BUCKET_ID`: The bucket index this file belongs to
///   - `ROW_COUNT`: Total rows in this file
///   - `BYTE_SIZE`: File size in bytes
pub mod metadata_keys {
    pub const COLUMNS: &str = "sail.bucket.columns";
    pub const NUM_BUCKETS: &str = "sail.bucket.num_buckets";
    pub const HASH: &str = "sail.bucket.hash";
    pub const SORT_COLUMNS: &str = "sail.bucket.sort_columns";
    pub const BUCKET_ID: &str = "sail.bucket.id";
    pub const ROW_COUNT: &str = "sail.bucket.row_count";
    pub const BYTE_SIZE: &str = "sail.bucket.byte_size";
}

/// Hash function identifier for DataFusion's native hash.
pub const HASH_DATAFUSION: &str = "datafusion";

/// Configuration for a bucketed write operation.
#[derive(Debug, Clone)]
pub struct BucketingConfig {
    /// Column names used for bucketing (hash key).
    pub columns: Vec<String>,
    /// Number of buckets.
    pub num_buckets: usize,
    /// Sort order within each bucket: `(column_name, ascending)`.
    pub sort_columns: Vec<(String, bool)>,
    /// Hash function identifier.
    pub hash_function: String,
}

/// Parsed bucketing metadata read back from a Parquet file.
#[derive(Debug, Clone)]
pub struct BucketingMetadata {
    pub columns: Vec<String>,
    pub num_buckets: usize,
    pub hash_function: String,
    pub sort_columns: Vec<(String, bool)>,
    pub bucket_id: usize,
    pub row_count: Option<u64>,
    pub byte_size: Option<u64>,
}

/// Inject bucketing metadata into an Arrow schema's metadata map.
///
/// Returns a new schema with the `sail.bucket.*` keys added.
pub fn inject_schema_metadata(schema: &SchemaRef, config: &BucketingConfig) -> SchemaRef {
    let mut metadata = schema.metadata().clone();
    metadata.insert(metadata_keys::COLUMNS.to_string(), config.columns.join(","));
    metadata.insert(
        metadata_keys::NUM_BUCKETS.to_string(),
        config.num_buckets.to_string(),
    );
    metadata.insert(
        metadata_keys::HASH.to_string(),
        config.hash_function.clone(),
    );
    if !config.sort_columns.is_empty() {
        let sort_str = config
            .sort_columns
            .iter()
            .map(|(name, asc)| {
                if *asc {
                    format!("{name}:asc")
                } else {
                    format!("{name}:desc")
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        metadata.insert(metadata_keys::SORT_COLUMNS.to_string(), sort_str);
    }
    Arc::new(schema.as_ref().clone().with_metadata(metadata))
}

/// Parse bucketing metadata from an Arrow schema.
///
/// Returns `None` if the schema does not contain bucketing metadata.
pub fn parse_schema_metadata(schema: &Schema) -> Option<BucketingMetadata> {
    let metadata = schema.metadata();
    let columns_str = metadata.get(metadata_keys::COLUMNS)?;
    let num_buckets_str = metadata.get(metadata_keys::NUM_BUCKETS)?;
    let num_buckets = num_buckets_str.parse::<usize>().ok()?;

    let columns = columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let hash_function = metadata
        .get(metadata_keys::HASH)
        .cloned()
        .unwrap_or_else(|| HASH_DATAFUSION.to_string());

    let sort_columns = metadata
        .get(metadata_keys::SORT_COLUMNS)
        .map(|s| parse_sort_columns(s))
        .unwrap_or_default();

    Some(BucketingMetadata {
        columns,
        num_buckets,
        hash_function,
        sort_columns,
        bucket_id: 0,
        row_count: None,
        byte_size: None,
    })
}

/// Parse file-level bucketing metadata from Parquet key-value pairs.
///
/// Merges per-file info (bucket_id, row_count, byte_size) into the given
/// schema-level metadata.
pub fn parse_file_metadata(kv_metadata: &[KeyValue], base: &mut BucketingMetadata) {
    for kv in kv_metadata {
        match kv.key.as_str() {
            metadata_keys::BUCKET_ID => {
                if let Some(v) = kv.value.as_deref().and_then(|s| s.parse::<usize>().ok()) {
                    base.bucket_id = v;
                }
            }
            metadata_keys::ROW_COUNT => {
                base.row_count = kv.value.as_deref().and_then(|s| s.parse::<u64>().ok());
            }
            metadata_keys::BYTE_SIZE => {
                base.byte_size = kv.value.as_deref().and_then(|s| s.parse::<u64>().ok());
            }
            _ => {}
        }
    }
}

/// Create Parquet file-level key-value metadata for a single bucket file.
pub fn create_file_kv_metadata(bucket_id: usize, row_count: u64) -> Vec<KeyValue> {
    vec![
        KeyValue::new(metadata_keys::BUCKET_ID.to_string(), bucket_id.to_string()),
        KeyValue::new(metadata_keys::ROW_COUNT.to_string(), row_count.to_string()),
    ]
}

/// Generate the file name for a bucket.
pub fn bucket_file_name(bucket_id: usize) -> String {
    format!("bucket_{bucket_id:05}.parquet")
}

/// Build `WriterProperties` for a bucketed Parquet file.
///
/// Extends the base properties with:
/// - File-level key-value metadata (bucket ID, real row count)
/// - Bloom filters on bucket columns AND sort columns
/// - Page-level statistics
pub fn create_bucketed_writer_properties(
    base: &WriterProperties,
    config: &BucketingConfig,
    bucket_id: usize,
    row_count: u64,
) -> WriterProperties {
    let file_kv = create_file_kv_metadata(bucket_id, row_count);
    let mut builder = WriterPropertiesBuilder::from(base.clone())
        .set_key_value_metadata(Some(file_kv))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page);

    // Enable bloom filters on bucket columns for fast key lookups.
    for col_name in &config.columns {
        let col_path = ColumnPath::new(vec![col_name.clone()]);
        builder = builder
            .set_column_bloom_filter_enabled(col_path.clone(), true)
            .set_column_bloom_filter_fpp(col_path, 0.01);
    }

    // Enable bloom filters on sort columns too — helps predicate pushdown
    // when filtering by sorted columns within a bucket.
    for (col_name, _ascending) in &config.sort_columns {
        // Skip if already a bucket column (avoid duplicate settings).
        if config.columns.iter().any(|c| c == col_name) {
            continue;
        }
        let col_path = ColumnPath::new(vec![col_name.clone()]);
        builder = builder
            .set_column_bloom_filter_enabled(col_path.clone(), true)
            .set_column_bloom_filter_fpp(col_path, 0.01);
    }

    builder.build()
}

/// Resolve bucket column indices from the schema.
///
/// Returns the column indices in the schema for the given bucket column names.
pub fn resolve_bucket_column_indices(
    schema: &Schema,
    bucket_columns: &[String],
) -> Result<Vec<usize>> {
    bucket_columns
        .iter()
        .map(|col_name| {
            schema.index_of(col_name).map_err(|_| {
                ArrowError::SchemaError(format!("bucket column '{col_name}' not found in schema"))
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))
}

/// Parse a sort columns string like `"amount:desc,created_at:asc"`.
fn parse_sort_columns(s: &str) -> Vec<(String, bool)> {
    s.split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let (name, ascending) = if let Some(name) = part.strip_suffix(":desc") {
                (name.trim(), false)
            } else if let Some(name) = part.strip_suffix(":asc") {
                (name.trim(), true)
            } else {
                (part, true)
            };
            Some((name.to_string(), ascending))
        })
        .collect()
}

#[expect(clippy::expect_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_inject_and_parse_schema_metadata() {
        let schema = test_schema();
        let config = BucketingConfig {
            columns: vec!["id".to_string(), "name".to_string()],
            num_buckets: 16,
            sort_columns: vec![("amount".to_string(), false)],
            hash_function: HASH_DATAFUSION.to_string(),
        };

        let enriched = inject_schema_metadata(&schema, &config);
        let metadata = enriched.metadata();
        assert_eq!(
            metadata.get(metadata_keys::COLUMNS).map(String::as_str),
            Some("id,name")
        );
        assert_eq!(
            metadata.get(metadata_keys::NUM_BUCKETS).map(String::as_str),
            Some("16")
        );
        assert_eq!(
            metadata.get(metadata_keys::HASH).map(String::as_str),
            Some("datafusion")
        );
        assert_eq!(
            metadata
                .get(metadata_keys::SORT_COLUMNS)
                .map(String::as_str),
            Some("amount:desc")
        );

        let parsed = parse_schema_metadata(&enriched).expect("should parse");
        assert_eq!(parsed.columns, vec!["id", "name"]);
        assert_eq!(parsed.num_buckets, 16);
        assert_eq!(parsed.hash_function, "datafusion");
        assert_eq!(parsed.sort_columns, vec![("amount".to_string(), false)]);
    }

    #[test]
    fn test_parse_schema_metadata_missing() {
        let schema = test_schema();
        assert!(parse_schema_metadata(&schema).is_none());
    }

    #[test]
    fn test_bucket_file_name() {
        assert_eq!(bucket_file_name(0), "bucket_00000.parquet");
        assert_eq!(bucket_file_name(42), "bucket_00042.parquet");
        assert_eq!(bucket_file_name(99999), "bucket_99999.parquet");
    }

    #[test]
    fn test_file_kv_metadata_roundtrip() {
        let kv = create_file_kv_metadata(7, 15000);
        assert_eq!(kv.len(), 2);

        let config = BucketingConfig {
            columns: vec!["id".to_string()],
            num_buckets: 8,
            sort_columns: vec![],
            hash_function: HASH_DATAFUSION.to_string(),
        };
        let mut base = BucketingMetadata {
            columns: config.columns.clone(),
            num_buckets: config.num_buckets,
            hash_function: config.hash_function.clone(),
            sort_columns: vec![],
            bucket_id: 0,
            row_count: None,
            byte_size: None,
        };
        parse_file_metadata(&kv, &mut base);
        assert_eq!(base.bucket_id, 7);
        assert_eq!(base.row_count, Some(15000));
        assert_eq!(base.byte_size, None);
    }

    #[test]
    fn test_resolve_bucket_column_indices() {
        let schema = test_schema();
        let indices =
            resolve_bucket_column_indices(&schema, &["name".to_string(), "id".to_string()])
                .expect("should resolve");
        assert_eq!(indices, vec![1, 0]);
    }

    #[test]
    fn test_resolve_bucket_column_missing() {
        let schema = test_schema();
        let result = resolve_bucket_column_indices(&schema, &["nonexistent".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sort_columns() {
        assert_eq!(
            parse_sort_columns("amount:desc,created_at:asc"),
            vec![
                ("amount".to_string(), false),
                ("created_at".to_string(), true)
            ]
        );
        assert_eq!(parse_sort_columns("name"), vec![("name".to_string(), true)]);
        assert!(parse_sort_columns("").is_empty());
    }
}
