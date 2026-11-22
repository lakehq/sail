// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::Session;
use datafusion::datasource::listing::helpers::{
    evaluate_partition_prefix, parse_partitions_for_path,
};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, lit, utils};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::stats::Precision;
use datafusion_common::{
    ColumnStatistics, DFSchema, Result, ScalarValue, Statistics, TableReference,
    assert_or_internal_err,
};
use datafusion_datasource::{FileExtensions, ListingTableUrl, PartitionedFile};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

/// The name of the directory of the rows whose partition value is NULL
/// or an empty string. This is the name that Hive and Spark use
/// (`ExternalCatalogUtils.DEFAULT_PARTITION_NAME` in Spark).
pub const DEFAULT_PARTITION_NAME: &str = "__HIVE_DEFAULT_PARTITION__";

// TODO: Remove the functions below after the upgrade to DataFusion 56, and use the functions
//   of DataFusion again. DataFusion 56 reads the default partition name as NULL
//   (https://github.com/apache/datafusion/pull/26310).

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/77611de53e6a5300a0b7888ad093739e3065a84a/datafusion/catalog-listing/src/helpers.rs#L295-L460
pub fn filter_partitioned_file(
    pf: PartitionedFile,
    filters: &[Expr],
    df_schema: &DFSchema,
) -> Result<Option<PartitionedFile>> {
    if pf.partition_values.is_empty() && !filters.is_empty() {
        return Ok(None);
    } else if filters.is_empty() {
        return Ok(Some(pf));
    }

    let arrays = pf
        .partition_values
        .iter()
        .map(|v| v.to_array())
        .collect::<Result<_, _>>()?;

    let batch = RecordBatch::try_new(Arc::clone(df_schema.inner()), arrays)?;

    let filter = utils::conjunction(filters.iter().cloned()).unwrap_or_else(|| lit(true));
    let props = ExecutionProps::new();
    let expr = create_physical_expr(
        &filter,
        df_schema,
        &props,
        &PhysicalPlanningContext::default(),
    )?;

    // Since we're only operating on a single file, our batch and resulting "array" holds only one
    // value indicating if the input file matches the provided filters. A NULL is not a match:
    // a null partition value makes that reachable, as `part = 1` is NULL for a null partition.
    let matches = expr.evaluate(&batch)?.into_array(1)?;
    let matches = matches.as_boolean();
    if matches.is_valid(0) && matches.value(0) {
        return Ok(Some(pf));
    }

    Ok(None)
}

/// Returns `Ok(None)` when the file is not inside a valid partition path
/// (e.g. a stale file in the table root directory), as there is no value to
/// assign for non-partitioned files.
///
/// A partition directory named `null_partition_name` is read as NULL.
fn try_into_partitioned_file(
    object_meta: ObjectMeta,
    partition_cols: &[(String, DataType)],
    table_path: &ListingTableUrl,
    null_partition_name: &str,
) -> Result<Option<PartitionedFile>> {
    let cols = partition_cols.iter().map(|(name, _)| name.as_str());
    let parsed = parse_partitions_for_path(table_path, &object_meta.location, cols);

    let Some(parsed) = parsed else {
        // parse_partitions_for_path already logs a debug message
        return Ok(None);
    };

    let partition_values = parsed
        .into_iter()
        .zip(partition_cols)
        .map(|(parsed, (_, datatype))| {
            if !null_partition_name.is_empty() && parsed == null_partition_name {
                return ScalarValue::try_new_null(datatype);
            }
            ScalarValue::try_from_string(parsed.into_owned(), datatype)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut pf: PartitionedFile = object_meta.into();
    pf.partition_values = partition_values;
    pf.table_reference.clone_from(table_path.get_table_ref());

    Ok(Some(pf))
}

/// Discover the partitions on the given path and prune out files
/// that belong to irrelevant partitions using `filters` expressions.
/// `filters` should only contain expressions that can be evaluated
/// using only the partition columns.
pub async fn pruned_partition_list<'a>(
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    let prefix = if !partition_cols.is_empty() {
        evaluate_partition_prefix(partition_cols, filters)
    } else {
        None
    };

    let objects = table_path
        .list_prefixed_files(ctx, store, prefix, file_extension)
        .await?
        .try_filter(|object_meta| futures::future::ready(object_meta.size > 0));

    if partition_cols.is_empty() {
        assert_or_internal_err!(
            filters.is_empty(),
            "Got partition filters for unpartitioned table {}",
            table_path
        );

        // if no partition col => list all the files
        Ok(objects
            .try_filter_map(|object_meta| {
                futures::future::ready(object_meta_to_partitioned_file(
                    object_meta,
                    table_path.get_table_ref().as_ref(),
                ))
            })
            .boxed())
    } else {
        let df_schema = DFSchema::from_unqualified_fields(
            partition_cols
                .iter()
                .map(|(n, d)| Field::new(n, d.clone(), true))
                .collect(),
            Default::default(),
        )?;

        let null_partition_name = DEFAULT_PARTITION_NAME;

        Ok(objects
            .try_filter_map(move |object_meta| {
                futures::future::ready(try_into_partitioned_file(
                    object_meta,
                    partition_cols,
                    table_path,
                    null_partition_name,
                ))
            })
            .try_filter_map(move |pf| {
                futures::future::ready(filter_partitioned_file(pf, filters, &df_schema))
            })
            .boxed())
    }
}

fn object_meta_to_partitioned_file(
    object_meta: ObjectMeta,
    table_ref: Option<&TableReference>,
) -> Result<Option<PartitionedFile>> {
    Ok(Some(PartitionedFile {
        object_meta,
        arrow_schema: None,
        partition_values: vec![],
        range: None,
        statistics: None,
        ordering: None,
        extensions: FileExtensions::new(),
        metadata_size_hint: None,
        table_reference: table_ref.cloned(),
    }))
}
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/77611de53e6a5300a0b7888ad093739e3065a84a/datafusion/datasource/src/mod.rs#L328-L373
/// Update the statistics for this file.
///
/// The provided `statistics` should cover only the file schema columns.
/// This method will automatically append exact statistics for partition columns
/// based on `partition_values`:
/// - `min = max = partition_value` (all rows have the same value)
/// - `null_count = 0` (all rows have a non-null value)
/// - `distinct_count = 1` (all rows have the same partition value)
///
/// A null partition value (e.g. a Hive default partition) instead gives
/// `null_count = num_rows`, `distinct_count = 0` and absent `min`/`max`.
pub fn with_statistics(
    mut file: PartitionedFile,
    file_statistics: Arc<Statistics>,
) -> PartitionedFile {
    if file.partition_values.is_empty() {
        // No partition columns, use stats as-is
        file.statistics = Some(file_statistics);
    } else {
        // Extend stats with exact partition column statistics
        let mut stats = Arc::unwrap_or_clone(file_statistics);
        for partition_value in &file.partition_values {
            let (null_count, min_max, distinct_count) = if partition_value.is_null() {
                (stats.num_rows, Precision::Absent, Precision::Exact(0))
            } else {
                (
                    Precision::Exact(0),
                    Precision::Exact(partition_value.clone()),
                    Precision::Exact(1),
                )
            };
            let col_stats = ColumnStatistics {
                null_count,
                max_value: min_max.clone(),
                min_value: min_max,
                distinct_count,
                sum_value: Precision::Absent,
                byte_size: partition_value
                    .data_type()
                    .primitive_width()
                    .map(|w| stats.num_rows.multiply(&Precision::Exact(w)))
                    .unwrap_or_else(|| Precision::Absent),
            };
            stats.column_statistics.push(col_stats);
        }
        file.statistics = Some(Arc::new(stats));
    }
    file
}
// SPDX-SnippetEnd

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::logical_expr::{Operator, binary_expr, col, not};
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    /// Lists the files of the directories `table/<directory>/` and returns the directory
    /// and the partition values of each file that matches the filters.
    async fn list_partitions(
        directories: &[&str],
        partition_cols: &[(&str, DataType)],
        filters: &[Expr],
    ) -> Result<Vec<(String, Vec<ScalarValue>)>> {
        let state = SessionContext::new().state();
        let store = InMemory::new();
        for directory in directories {
            store
                .put(
                    &Path::from(format!("table/{directory}/file.parquet")),
                    PutPayload::from_static(b"x"),
                )
                .await?;
        }
        let url = ListingTableUrl::parse("memory:///table/")?;
        let partition_cols: Vec<_> = partition_cols
            .iter()
            .map(|(name, data_type)| (name.to_string(), data_type.clone()))
            .collect();
        let mut files: Vec<_> =
            pruned_partition_list(&state, &store, &url, filters, "", &partition_cols)
                .await?
                .map_ok(|file| {
                    let directory = file
                        .object_meta
                        .location
                        .as_ref()
                        .trim_start_matches("table/")
                        .trim_end_matches("/file.parquet")
                        .to_string();
                    (directory, file.partition_values)
                })
                .try_collect()
                .await?;
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    #[tokio::test]
    async fn test_default_partition_is_read_as_null() -> Result<()> {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for (data_type, value) in [
            (DataType::Int8, "-1"),
            (DataType::Int16, "2"),
            (DataType::Int32, "0"),
            (DataType::Int64, "100"),
            (DataType::UInt8, "1"),
            (DataType::UInt16, "1"),
            (DataType::UInt32, "1"),
            (DataType::UInt64, "0"),
            (DataType::Float16, "1.5"),
            (DataType::Float32, "0"),
            (DataType::Float64, "NaN"),
            (DataType::Boolean, "false"),
            (DataType::Utf8, "a"),
            (DataType::LargeUtf8, "a"),
            (DataType::Utf8View, "a"),
            (dictionary, "a"),
            (DataType::Binary, "a"),
            (DataType::LargeBinary, "a"),
            (DataType::BinaryView, "a"),
            (DataType::Date32, "1970-01-01"),
            (DataType::Date64, "1970-01-01"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "2024-01-02 03:04:05",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "2024-01-02 03:04:05",
            ),
            (DataType::Decimal32(9, 2), "0.00"),
            (DataType::Decimal64(18, 2), "1.50"),
            (DataType::Decimal128(38, 2), "0"),
            (DataType::Decimal256(76, 2), "-2.25"),
        ] {
            let default = format!("k={DEFAULT_PARTITION_NAME}");
            let directory = format!("k={value}");
            let files = list_partitions(
                &[default.as_str(), directory.as_str()],
                &[("k", data_type.clone())],
                &[],
            )
            .await?;
            let mut expected = vec![
                (
                    directory.clone(),
                    vec![ScalarValue::try_from_string(value.to_string(), &data_type)?],
                ),
                (
                    default.clone(),
                    vec![ScalarValue::try_new_null(&data_type)?],
                ),
            ];
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(files, expected, "{data_type}");
            for (name, values) in &files {
                assert_eq!(values[0].is_null(), name == &default, "{data_type}");
                assert_eq!(values[0].data_type(), data_type);
            }

            // Only the default partition has rows without a value.
            for (filter, expected) in [
                (col("k").is_null(), &default),
                (col("k").is_not_null(), &directory),
            ] {
                let actual = list_partitions(
                    &[default.as_str(), directory.as_str()],
                    &[("k", data_type.clone())],
                    &[filter],
                )
                .await?;
                let actual: Vec<_> = actual.into_iter().map(|x| x.0).collect();
                assert_eq!(actual, vec![expected.clone()], "{data_type}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_default_partition_filters() -> Result<()> {
        let default = format!("k={DEFAULT_PARTITION_NAME}");
        let directories = ["k=0", "k=1", default.as_str()];
        let null = lit(ScalarValue::Int32(None));
        for (filter, expected) in [
            (None, vec!["k=0", "k=1", default.as_str()]),
            // The value of a row in the default partition is not the default value
            // of the type, whether the directories are listed by their prefix or not.
            (Some(col("k").eq(lit(0))), vec!["k=0"]),
            (Some(col("k").eq(lit(1))), vec!["k=1"]),
            (Some(col("k").lt_eq(lit(0))), vec!["k=0"]),
            (Some(col("k").lt(lit(1))), vec!["k=0"]),
            (Some((col("k") + lit(0)).eq(lit(0))), vec!["k=0"]),
            (Some(col("k").not_eq(lit(0))), vec!["k=1"]),
            (Some(not(col("k").eq(lit(1)))), vec!["k=0"]),
            (
                Some(col("k").in_list(vec![lit(0), null.clone()], false)),
                vec!["k=0"],
            ),
            (Some(col("k").eq(null.clone())), vec![]),
            (Some(col("k").is_null()), vec![default.as_str()]),
            (Some(col("k").is_not_null()), vec!["k=0", "k=1"]),
            (
                Some(binary_expr(
                    col("k"),
                    Operator::IsNotDistinctFrom,
                    null.clone(),
                )),
                vec![default.as_str()],
            ),
            (
                Some(binary_expr(col("k"), Operator::IsDistinctFrom, lit(0))),
                vec!["k=1", default.as_str()],
            ),
            (
                Some(col("k").is_null().or(col("k").eq(lit(1)))),
                vec!["k=1", default.as_str()],
            ),
        ] {
            let filters: Vec<Expr> = filter.into_iter().collect();
            let files = list_partitions(&directories, &[("k", DataType::Int32)], &filters).await?;
            let actual: Vec<_> = files.iter().map(|x| x.0.as_str()).collect();
            assert_eq!(actual, expected, "{filters:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_default_partition_of_strings_and_several_columns() -> Result<()> {
        let default = DEFAULT_PARTITION_NAME;
        let directories = [
            format!("a=1/b={default}"),
            format!("a={default}/b=x"),
            format!("a={default}/b={default}"),
            "a=1/b=x".to_string(),
        ];
        let directories: Vec<_> = directories.iter().map(|x| x.as_str()).collect();
        let partition_cols = [("a", DataType::Int64), ("b", DataType::Utf8)];
        let value = |a: Option<i64>, b: Option<&str>| {
            vec![
                ScalarValue::Int64(a),
                ScalarValue::Utf8(b.map(|x| x.to_string())),
            ]
        };
        let files = list_partitions(&directories, &partition_cols, &[]).await?;
        assert_eq!(
            files,
            vec![
                (directories[0].to_string(), value(Some(1), None)),
                (directories[3].to_string(), value(Some(1), Some("x"))),
                (directories[2].to_string(), value(None, None)),
                (directories[1].to_string(), value(None, Some("x"))),
            ]
        );
        // The name of the default partition is not a value of a string column.
        for (filter, expected) in [
            (col("b").eq(lit(default)), vec![]),
            (col("b").eq(lit("")), vec![]),
            (col("b").is_null(), vec![directories[0], directories[2]]),
            (
                col("a").is_null().and(col("b").eq(lit("x"))),
                vec![directories[1]],
            ),
            (
                col("a").eq(lit(1_i64)).and(col("b").is_null()),
                vec![directories[0]],
            ),
        ] {
            let files =
                list_partitions(&directories, &partition_cols, std::slice::from_ref(&filter))
                    .await?;
            let actual: Vec<_> = files.iter().map(|x| x.0.as_str()).collect();
            assert_eq!(actual, expected, "{filter}");
        }
        Ok(())
    }

    #[test]
    fn test_statistics_of_default_partition() {
        let mut file = PartitionedFile::new("table/k=1/file.parquet", 1);
        file.partition_values = vec![
            ScalarValue::Int32(None),
            ScalarValue::Utf8(Some("a".to_string())),
            ScalarValue::Utf8(None),
        ];
        let statistics = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        let file = with_statistics(file, Arc::new(statistics));
        let statistics = file.statistics.map(|x| x.column_statistics.clone());
        let columns = statistics.unwrap_or_default();
        assert_eq!(columns.len(), 4);
        for i in [1, 3] {
            assert_eq!(columns[i].null_count, Precision::Exact(10));
            assert_eq!(columns[i].min_value, Precision::Absent);
            assert_eq!(columns[i].max_value, Precision::Absent);
            assert_eq!(columns[i].distinct_count, Precision::Exact(0));
        }
        let value = Precision::Exact(ScalarValue::Utf8(Some("a".to_string())));
        assert_eq!(columns[2].null_count, Precision::Exact(0));
        assert_eq!(columns[2].min_value, value);
        assert_eq!(columns[2].max_value, value);
        assert_eq!(columns[2].distinct_count, Precision::Exact(1));

        // The number of rows without a value is not known when the number of rows is not.
        let mut file = PartitionedFile::new("table/k=1/file.parquet", 1);
        file.partition_values = vec![ScalarValue::Int32(None)];
        let statistics = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        let file = with_statistics(file, Arc::new(statistics));
        let statistics = file.statistics.map(|x| x.column_statistics.clone());
        let columns = statistics.unwrap_or_default();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].null_count, Precision::Absent);
        assert_eq!(columns[0].min_value, Precision::Absent);
    }
}
