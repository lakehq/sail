---
title: Spark 3.5.9 Test Failures
rank: 40
---

# Spark 3.5.9 Test Failures

This page analyzes the failures in the Spark 3.5.9 test report for the `test-connect` suite (75 failures) and the doctest suites (21 failures).
The failures were the same before and after the catalog changes in [#2801](https://github.com/lakehq/sail/pull/2801), so none of them are regressions from that pull request.

The catalog group was reproduced locally and its root causes are confirmed in the code.
The other groups are triaged from the CI test logs, and their root causes still need to be confirmed by reproducing each test.

## Reproducing the Failures

The failures were reproduced on Linux with a debug build of the Sail server and the Spark 3.5.9 Python tests.

1. Clone Spark 3.5.9 into `opt/spark` and apply the test patch to the Python sources.
   ```bash
   git clone --depth 1 --branch v3.5.9 https://github.com/apache/spark.git opt/spark
   git -C opt/spark apply --include='python/pyspark/*' ../../scripts/spark-tests/spark-3.5.9.patch
   ```
2. Install `pyspark[connect]==3.5.9` with `pandas<3`, `pyarrow>=24,<25`, and `numpy<2` in the test environment.
   NumPy 2 does not work with `pyspark.pandas` in Spark 3.5 (`np.NaN` was removed), and it is imported by `pyspark.testing`.
3. Copy `opt/spark/python/pyspark` and `opt/spark/python/test_support` over the installed `pyspark` package,
   since the PyPI package does not include the tests.
4. Add `spark-connect_2.12-3.5.9.jar` from Maven Central to `pyspark/jars`.
   The tests use the Spark Connect local mode, which starts a JVM with the Spark Connect plugin, and the patched client then connects to the port in `SPARK_TESTING_REMOTE_PORT` instead.
   These steps avoid building the patched PySpark package described in [Spark Setup](./spark-setup), which takes a while but is the standard setup.
5. Run the Sail server as in `scripts/spark-tests/run-server.sh`, and run the tests with the `plugins.spark` pytest plugin.
   ```bash
   pytest -p plugins.spark --pyargs pyspark.sql.tests.connect.test_parity_catalog pyspark.sql.tests.connect.test_parity_dataframe -k "catalog or cache"
   pytest -p plugins.spark --doctest-modules --pyargs pyspark.sql.catalog
   ```

The local run fails the same 7 `test-connect` tests and 8 catalog doctests as CI, with the same errors.

## Catalog and Cache

### Table and DataFrame Caching

| Test                                                            | Error                                                            |
| :-------------------------------------------------------------- | :--------------------------------------------------------------- |
| `CatalogParityTests.test_table_cache`                           | `PlanNode::IsCached`                                             |
| `CatalogParityTests.test_refresh_table`                         | `PlanNode::CacheTable`                                           |
| `DataFrameParityTests.test_cache_table`                         | `Temporary View not found: tab2` (masks `PlanNode::IsCached`)    |
| `DataFrameParityTests.test_cache_dataframe`                     | `StorageLevel(False, True, True, False, 1) != StorageLevel.NONE` |
| doctests `cacheTable`, `isCached`, `uncacheTable`, `clearCache` | `PlanNode::CacheTable`, `PlanNode::ClearCache`                   |
| doctests `refreshTable`, `refreshByPath`                        | `PlanNode::CacheTable` (the examples cache the table first)      |
| doctest `DataFrame.storageLevel`                                | the reported storage level does not match                        |

**Root cause.** Sail has no table or DataFrame cache.
The Spark Connect catalog messages are converted to `CommandNode::CacheTable`, `IsCached`, `UncacheTable`, `ClearCache`, `RefreshTable`, and `RefreshByPath`,
but the plan resolver returns `PlanError::todo` for all of them (`crates/sail-plan/src/resolver/command/mod.rs`).
`persist` and `unpersist` are accepted as no-ops, and `GetStorageLevel` always returns the same fixed storage level
(`use_memory` and `use_off_heap`, which is `OFF_HEAP`) in `crates/sail-spark-connect/src/service/plan_analyzer.rs`.
So `df.storageLevel` is `OFF_HEAP` for a DataFrame that was never cached, where Spark returns `NONE`.

`test_cache_table` reports a different error because of a second bug, described in [Dropping Temporary Views](#dropping-temporary-views).

**Proposed fix.** These tests check the cache bookkeeping rather than the cached data, except `test_refresh_table`.
A session-level cache registry in the catalog manager would make them pass without materializing anything:

- `cacheTable` and `persist` record the resolved table (or the plan) with its storage level, and fail with "not found" for a missing table, as Spark does.
- `isCached`, `uncacheTable`, `clearCache`, `unpersist`, and `storageLevel` read and update that registry.
  `storageLevel` returns `NONE` for a plan that is not cached.
- `refreshTable` and `refreshByPath` invalidate any provider caches for the table and drop it from the registry.

`test_refresh_table` additionally expects cached data to survive the deletion of the underlying files until the table is refreshed.
That requires materializing the cached plan (for example, as an in-memory table), which belongs to the caching design in [#1014](https://github.com/lakehq/sail/issues/1014).
A registry-only implementation would make `isCached` report `True` for data that is not actually cached,
so whether that is acceptable as a first step is a decision for the maintainers.

### Dropping Temporary Views

| Test                                    | Error                            |
| :-------------------------------------- | :------------------------------- |
| `DataFrameParityTests.test_cache_table` | `Temporary View not found: tab2` |

**Root cause.** `spark.catalog.dropTempView` on a view that does not exist raises an error, where Spark returns `False`.
`DropTempView` was converted with `if_exists: false` in `crates/sail-spark-connect/src/proto/plan.rs`,
and the catalog command always returned `True` for both `dropTempView` and `dropGlobalTempView`.
The test helper `tempView` drops every view in its `finally` block.
After `isCached("tab1")` fails, `tab2` was never created, so dropping it raised the error that the test reported, which hid the original failure.

**Fix (in this pull request).** `TemporaryViewManager::drop_view` now returns whether the view existed,
`DropTempView` uses `if_exists: true`, and the command returns the result.
This also fixes the known issue covered by the strict `xfail` on `test_drop_missing_global_temp_view_returns_false`, which now passes.
`test_cache_table` still fails, now with the `PlanNode::IsCached` error from the caching group.

### Persistent Functions

| Test                                      | Error                                                      |
| :---------------------------------------- | :--------------------------------------------------------- |
| `CatalogParityTests.test_function_exists` | `found FUNCTION at 7:15 expected 'DATABASE', ...`          |
| `CatalogParityTests.test_get_function`    | `found FUNCTION at 7:15 expected 'DATABASE', ...`          |
| `CatalogParityTests.test_list_functions`  | `not supported: qualified function name` (masks the above) |
| doctest `getFunction`                     | `found FUNCTION at 7:15 expected 'DATABASE', ...`          |

**Root cause.** All four tests create a persistent function backed by a JVM class, for example `CREATE FUNCTION func1 AS 'org.apache.spark.data.bricks'`.
The SQL parser has no `CREATE FUNCTION` statement (it supports `DROP`, `REFRESH`, `SHOW`, and `DESCRIBE FUNCTION`), so parsing fails at the `FUNCTION` keyword.
The catalog providers have no storage for functions either. User functions are kept only as session-level temporary functions in the catalog manager.

`test_list_functions` reported a different error.
The test helper `function("func1", "some_db.func2")` runs `DROP FUNCTION IF EXISTS some_db.func2` in its `finally` block,
and `CatalogManager::deregister_function` rejected every qualified name, even with `IF EXISTS`.

**Fix (in this pull request).** `DROP FUNCTION [IF EXISTS] db.f` now checks that the database exists, then succeeds with `IF EXISTS` and reports that the function is not found otherwise.
No persistent function can exist, so this matches Spark.
Qualified names are still rejected for `DROP TEMPORARY FUNCTION`.
`test_list_functions` now fails with the same `CREATE FUNCTION` parse error as the other tests.

**Proposed fix.** The tests only check function metadata (`functionExists`, `getFunction`, and `listFunctions`), and they never call the function.
Sail cannot run JVM classes, so there are two options:

1. Parse `CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS] name AS 'class' [USING JAR ...]` and return a clear "not supported" error that names JVM function classes.
   The tests keep failing, but with an accurate message.
2. Store persistent function metadata (name, database, and class name) in the memory catalog and report it from the catalog APIs, while invoking such a function fails.
   This makes the four tests pass, but it lets users create functions that cannot be called.

Option 1 is the smaller change, and option 2 needs a decision on whether metadata-only functions are acceptable.

### Recovering Partitions

| Test                        | Error                         |
| :-------------------------- | :---------------------------- |
| doctest `recoverPartitions` | `PlanNode::RecoverPartitions` |

**Root cause.** `CommandNode::RecoverPartitions` is not implemented in the plan resolver.

**Proposed fix.** For the memory catalog, partitions are discovered from the file listing when the table is read, so the command could succeed as a no-op after checking that the table exists and is partitioned.
For catalogs that store partitions (Hive Metastore and Glue), the command would need to list the table location and register the partitions it finds.

## Other Groups (Triage)

The groups below are triaged from the CI test logs and not yet reproduced locally.

| Group                  | Tests                                                                                                                                                                                                                                        | Error                                                                                            | Likely cause and direction                                                                                                |
| :--------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------ |
| Artifacts              | `test_artifact` (9 tests)                                                                                                                                                                                                                    | `handle add artifacts`, `handle artifact statuses`                                               | The `AddArtifacts` and `ArtifactStatus` RPCs are not implemented.                                                         |
| Plan analysis          | `test_same_semantics`, `test_semantic_hash`, doctest `sameSemantics`                                                                                                                                                                         | `handle analyze same semantics`, `handle analyze semantic hash`                                  | The analyze requests are not implemented. They could compare and hash the resolved logical plans.                         |
| Statistics             | `test_stat_approx_quantile`, `test_approxQuantile`, `test_stat_freq_items`, `test_freqItems`, `test_stat_sample_by`, doctests `sampleBy`                                                                                                     | `approx quantile`, `freq items`, sampled rows differ                                             | `approxQuantile` and `freqItems` relations are not implemented. `sampleBy` results depend on the random number generator. |
| Percentile functions   | `test_aggregation_functions`, doctests `approx_percentile`, `percentile_approx`                                                                                                                                                              | `approx_percentile_cont` expects 2 arguments, `approx_distinct` on `Float64`                     | The accuracy argument is not mapped, and `approx_distinct` lacks a `Float64` implementation.                              |
| Higher-order functions | doctests `zip_with`, `map_zip_with`, `test_lambda_functions`                                                                                                                                                                                 | `function: zip_with`, `function: map_zip_with`, unsupported cast from `Map` to `Null`            | These functions are not implemented.                                                                                      |
| Windows and streaming  | `test_time_window_functions`, doctest `session_window`, `test_deduplicate_within_watermark_in_batch`, `test_streaming_local_relation`                                                                                                        | `function: session_window`, `deduplicate within watermark`                                       | Session windows and watermark deduplication are not implemented.                                                          |
| Input files            | `test_input_files`, `test_udf_with_input_file_name` (3 tests), doctest `input_file_name`                                                                                                                                                     | wrong file count, empty file name                                                                | `input_file_name()` and `inputFiles()` do not report the files that were read.                                            |
| Hints                  | `test_join_hint`, `test_generic_hints`, `test_functions_broadcast`, doctest `hint`                                                                                                                                                           | hint not in the plan string                                                                      | Hints are accepted but do not appear in `explain()` output.                                                               |
| Duplicate field names  | `test_createDataFrame_duplicate_field_names`, `test_toPandas_duplicate_field_names`, `test_duplicate_field_names`, `test_assert_equal_nested_...`                                                                                            | `Converting to Python dictionary is not supported when duplicate field names are present`        | Struct values with duplicate field names are converted through Python dictionaries.                                       |
| User-defined types     | `test_simple_udt_from_read`, `test_parquet_with_udt`, `test_udf_with_udt`, `test_udt_with_none`, `test_complex_nested_udt_in_df`                                                                                                             | UDT read back as its SQL type, `user defined data type should only exist in a field`             | The UDT metadata is not preserved through reads and Python UDFs.                                                          |
| Data sources           | `test_orc`, `test_read_multiple_orc_file`, `test_jdbc`, `test_jdbc_format`, `test_bucketed_write`                                                                                                                                            | `No data source found for: orc`, JDBC error message, `bucketing for writing listing data source` | ORC and bucketed writes are not supported. The JDBC tests expect a different error message.                               |
| JSON and CSV           | `test_json`, `test_encoding_json`, `test_linesep_json`, `test_multiline_json`, `test_multi_paths`, `test_checking_csv_header`, `test_write_operations`                                                                                       | JSON parse errors, `_corrupt_record` missing, CSV header not validated                           | Options such as `encoding`, `lineSep`, `multiLine`, `enforceSchema`, and corrupt record handling differ from Spark.       |
| Expression results     | `test_normal_functions`, `test_string_functions_multi_args`, `test_window_functions`, `test_collection_functions`, `test_generator_functions`, `test_column_arithmetic_ops`, `test_decimal`, `test_math_functions`, `test_date_ts_functions` | DataFrames differ, `factorial` on decimals, date cast from `1997/02/28 10:30:00`                 | Individual function differences. Each test needs to be split by the expression that differs.                              |
| Schema and types       | `test_schema`, `test_to`, `test_to_pandas`                                                                                                                                                                                                   | nullability differs, `field not found in input schema: col1`, `datetime64[us]` instead of `[ns]` | Nullability inference, `DataFrame.to` column matching, and pandas timestamp resolution.                                   |
| Python UDFs            | `test_udf`, `test_udtf`, `test_scalar_iter_udf_init`                                                                                                                                                                                         | assertion errors, `'NoneType' object has no attribute 'partitionId'`                             | `TaskContext` is not available in Python UDF workers, plus UDF and UDTF behavior differences.                             |
| Other                  | `test_observe`, `test_distributed_sequence_id`, `test_random_split`, doctests `colRegex`, `randomSplit`                                                                                                                                      | `collect metrics`, `distributed_sequence_id`, random split results                               | `observe` and `distributed_sequence_id` are not implemented. `randomSplit` results depend on the random number generator. |
