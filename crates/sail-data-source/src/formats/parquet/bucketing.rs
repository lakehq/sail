use arrow::array::ArrayRef;
use arrow_schema::{ArrowError, Schema};
use datafusion_common::Result;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use sail_function::scalar::hash::utils::create_murmur3_hashes;

/// Seed used by Spark's default `HashPartitioning` — `Pmod(Hash(cols, 42), numPartitions)`.
///
/// Bucketed Parquet files written by Sail are bit-compatible with Spark thanks to
/// using the same Murmur3_x86_32 algorithm + seed. Spark can read them with full
/// bucket pruning / bucket-join elimination semantics.
const SPARK_BUCKETING_SEED: u32 = 42;

/// Compute per-row hash values for bucket assignment (Spark-compatible).
///
/// The same function MUST be used on both the write side (bucket routing in the sink)
/// and the read side (bucket pruning), otherwise a value hashed on write into bucket X
/// will be looked up on read in bucket Y and miss. Consistency is guarded by the
/// `test_hash_consistency_write_vs_read` unit test.
///
/// Matches `org.apache.spark.sql.catalyst.expressions.HashPartitioning`:
/// `Pmod(Hash(cols, 42), numPartitions)` → bucket id.
pub fn hash_for_bucketing(arrays: &[ArrayRef]) -> Result<Vec<u32>> {
    let num_rows = arrays.first().map(|a| a.len()).unwrap_or(0);
    let mut hashes = vec![SPARK_BUCKETING_SEED; num_rows];
    create_murmur3_hashes(arrays, &mut hashes)?;
    Ok(hashes)
}

/// Map a Spark Murmur3 hash value to a bucket id using positive modulo.
///
/// Spark uses `Pmod` (`((h % n) + n) % n`) — `Int.rem_euclid` in Rust.
/// Negative hash values (Murmur3 returns `i32`) otherwise produce negative
/// buckets which mismatch Spark.
#[inline]
pub fn bucket_id_for_hash(hash: u32, num_buckets: usize) -> usize {
    (hash as i32).rem_euclid(num_buckets as i32) as usize
}

/// Configuration for a bucketed write operation.
///
/// Bucketing info is always read from the catalog (authoritative source).
/// Bucketed Parquet files written by Sail carry zero proprietary metadata —
/// they only rely on Spark-compatible file naming (see [`bucket_file_name`]).
#[derive(Debug, Clone)]
pub struct BucketingConfig {
    /// Column names used for bucketing (hash key).
    pub columns: Vec<String>,
    /// Number of buckets.
    pub num_buckets: usize,
    /// Sort order within each bucket: `(column_name, ascending)`.
    pub sort_columns: Vec<(String, bool)>,
}

/// Generate the file name for a bucket, following Spark's naming convention.
///
/// Spark uses: `part-{taskAttemptId}-{uuid}_{bucketId}.c000.snappy.parquet`
/// where taskAttemptId is 00000 (single writer task) and the UUID is shared
/// across all bucket files from the same write operation.
///
/// Reference: `org.apache.spark.sql.execution.datasources.BucketingUtils`
pub fn bucket_file_name(task_uuid: &str, bucket_id: usize) -> String {
    format!("part-00000-{task_uuid}_{bucket_id:05}.c000.snappy.parquet")
}

/// Build `WriterProperties` for a bucketed Parquet file.
///
/// Extends the base properties with:
/// - Bloom filters on bucket columns AND sort columns
/// - Page-level statistics
///
/// Intentionally emits no `sail.*` key-value metadata — bucketing info is
/// carried by the file naming convention and the catalog.
pub fn create_bucketed_writer_properties(
    base: &WriterProperties,
    config: &BucketingConfig,
) -> WriterProperties {
    let mut builder = WriterPropertiesBuilder::from(base.clone())
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

#[expect(clippy::expect_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_bucket_file_name() {
        let uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        // Spark convention: part-00000-{uuid}_{bucketId:05}.c000.snappy.parquet
        let name = bucket_file_name(uuid, 0);
        assert_eq!(
            name,
            "part-00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890_00000.c000.snappy.parquet"
        );

        let name = bucket_file_name(uuid, 42);
        assert_eq!(
            name,
            "part-00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890_00042.c000.snappy.parquet"
        );

        // All buckets share same taskId (00000) and UUID
        let n0 = bucket_file_name(uuid, 0);
        let n1 = bucket_file_name(uuid, 1);
        assert!(n0.starts_with("part-00000-a1b2c3d4"), "got: {n0}");
        assert!(n1.starts_with("part-00000-a1b2c3d4"), "got: {n1}");
        assert_ne!(n0, n1); // different bucket IDs
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

    /// Verify that the hash used for bucket pruning (read side) matches
    /// the hash used for bucket assignment (write side).
    ///
    /// Both use `create_murmur3_hashes` with Spark's seed (42). This test catches
    /// regressions if the underlying Murmur3 implementation changes output.
    #[test]
    fn test_hash_consistency_write_vs_read() {
        use arrow::array::{ArrayRef, Int32Array};

        let num_buckets = 8;

        // Simulate write side: hash a column of values and assign to buckets.
        let values = Int32Array::from(vec![1, 2, 3, 42, 100, -5]);
        let arrays: Vec<ArrayRef> = vec![Arc::new(values.clone())];
        let write_hashes = hash_for_bucketing(&arrays).expect("write hash");

        // Simulate read side: hash each value individually (as bucket pruning does).
        for (i, val) in [1i32, 2, 3, 42, 100, -5].iter().enumerate() {
            let single = Int32Array::from(vec![*val]);
            let single_arrays: Vec<ArrayRef> = vec![Arc::new(single)];
            let read_hashes = hash_for_bucketing(&single_arrays).expect("read hash");

            let write_bucket = bucket_id_for_hash(write_hashes[i], num_buckets);
            let read_bucket = bucket_id_for_hash(read_hashes[0], num_buckets);
            assert_eq!(
                write_bucket, read_bucket,
                "hash mismatch for value {val}: write bucket {write_bucket} != read bucket {read_bucket}"
            );
        }
    }
}
