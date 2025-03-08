# Changelog

## 0.2.2

_March 6, 2025_

- Switched to the built-in SQL parser ([#338](https://github.com/lakehq/sail/pull/338), [#358](https://github.com/lakehq/sail/pull/358), [#359](https://github.com/lakehq/sail/pull/359), and [#376](https://github.com/lakehq/sail/pull/376)).
- Supported the majority of Spark SQL syntax ([#378](https://github.com/lakehq/sail/pull/378), [#380](https://github.com/lakehq/sail/pull/380), [#382](https://github.com/lakehq/sail/pull/382), [#385](https://github.com/lakehq/sail/pull/385), [#387](https://github.com/lakehq/sail/pull/387), [#389](https://github.com/lakehq/sail/pull/389), and [#390](https://github.com/lakehq/sail/pull/390)).
- Expanded support for Spark SQL functions ([#364](https://github.com/lakehq/sail/pull/364), [#384](https://github.com/lakehq/sail/pull/384), and [#391](https://github.com/lakehq/sail/pull/391)).
- Fixed issues with `join()` in the Spark DataFrame API ([#392](https://github.com/lakehq/sail/pull/392)).
- Supported `NATURAL JOIN` in Spark SQL ([#396](https://github.com/lakehq/sail/pull/396)).
- Fixed an issue with SQL window expressions ([#386](https://github.com/lakehq/sail/pull/386)).
- Fixed result parity issues with derived TPC-DS queries ([#393](https://github.com/lakehq/sail/pull/393)).

## 0.2.1

_January 15, 2025_

- Supported SQL table functions and lateral views ([#326](https://github.com/lakehq/sail/pull/326) and [#327](https://github.com/lakehq/sail/pull/327)).
- Supported PySpark UDTFs ([#329](https://github.com/lakehq/sail/pull/329)).
- Improved literal and data type support ([#317](https://github.com/lakehq/sail/pull/317), [#328](https://github.com/lakehq/sail/pull/328), [#330](https://github.com/lakehq/sail/pull/330), and [#339](https://github.com/lakehq/sail/pull/339)).
- Supported `ANTI JOIN` and `SEMI JOIN` ([#337](https://github.com/lakehq/sail/pull/337)).
- Fixed a few PySpark UDF issues ([#343](https://github.com/lakehq/sail/pull/343)).
- Supported nested fields in SQL ([#340](https://github.com/lakehq/sail/pull/340)).
- Supported more queries in the derived TPC-DS benchmark ([#346](https://github.com/lakehq/sail/pull/346)).
- Supported more datetime functions ([#349](https://github.com/lakehq/sail/pull/349)).

## 0.2.0

_December 3, 2024_

We are excited to announce the first Sail release with the distributed processing capability. Spark SQL and DataFrame queries can now run on Kubernetes, powered by the Sail distributed compute engine. We also introduced a new Sail CLI and a configuration mechanism that will serve as the entrypoint for all Sail features moving forward.

We continued extending coverage for Spark SQL functions and the Spark DataFrame API. The changes are listed below.

- Supported the following DataFrame and SQL functions ([#278](https://github.com/lakehq/sail/pull/278) and [#305](https://github.com/lakehq/sail/pull/305)).
  - `DataFrame.crosstab`
  - `DataFrame.replace`
  - `DataFrame.to`
  - `reverse`
  - `aes_decrypt`
  - `aes_encrypt`
  - `try_aes_decrypt`
  - `base64`
  - `unbase64`
  - `weekofyear`
- Supported `mapInPandas()` and `mapInArrow()` for Spark DataFrame ([#310](https://github.com/lakehq/sail/pull/310)).
- Supported `applyInPandas()` for grouped and co-grouped Spark DataFrame ([#313](https://github.com/lakehq/sail/pull/313)).

### Breaking Changes

This release comes with the new Sail CLI, and the way to launch the Spark Connect server and PySpark shell is different from the 0.1.x versions. Please refer to the [Getting Started](/guide/getting-started/) page for the updated instructions.

## 0.1.7

_November 1, 2024_

- Expanded support for Spark DataFrame functions ([#268](https://github.com/lakehq/sail/pull/268) and [#261](https://github.com/lakehq/sail/pull/261)).
  Added full parity and coverage for the following DataFrame and SQL functions.
  - `DataFrame.summary`
  - `DataFrame.describe`
  - `DataFrame.corr`
  - `DataFrame.cov`
  - `DataFrame.stat`
  - `DataFrame.drop`
  - `corr`
  - `regr_avgx`
- Fixed most issues with `ORDER BY` in the derived TPC-DS benchmark, bringing total coverage to 74 out of the 99 queries ([#261](https://github.com/lakehq/sail/pull/261)).

We also made significant changes to the Sail internals to support **distributed processing**. We are targeting the 0.2.0 release in the next few weeks for an MVP (minimum viable product) of this exciting feature. Please stay tuned! If you are interested in the ongoing work, you can follow [#246](https://github.com/lakehq/sail/issues/246) in our GitHub repository to get the latest updates!

## 0.1.6

_October 23, 2024_

- Supported `UNION` by name ([#253](https://github.com/lakehq/sail/pull/253)).
- Fixed a few issues with column references ([#254](https://github.com/lakehq/sail/pull/254) and [#257](https://github.com/lakehq/sail/pull/257)).

## 0.1.5

_October 17, 2024_

- Expanded support for Spark SQL syntax and functions ([#239](https://github.com/lakehq/sail/pull/239) and [#247](https://github.com/lakehq/sail/pull/247)).
  Added full parity and coverage for the following SQL functions.
  - `current_catalog`
  - `current_database`
  - `current_schema`
  - `hash`
  - `hex`
  - `unhex`
  - `xxhash64`
  - `unix_timestamp`
- Fixed a few issues with `JOIN` ([#250](https://github.com/lakehq/sail/pull/250)).

## 0.1.4

_October 03, 2024_

- Enabled Avro in DataFusion ([#234](https://github.com/lakehq/sail/pull/234)).
- Expanded support for Spark SQL syntax and functions ([#213](https://github.com/lakehq/sail/pull/213) and [#207](https://github.com/lakehq/sail/pull/207)).
  Added full parity and coverage for the following SQL functions.
  - `array`
  - `date_format`
  - `get_json_object`
  - `json_array_length`
  - `overlay`
  - `replace`
  - `split_part`
  - `to_date`
  - `any_value`
  - `approx_count_distinct`
  - `current_timezone`
  - `first_value`
  - `greatest`
  - `last`
  - `last_value`
  - `least`
  - `map_contains_key`
  - `map_keys`
  - `map_values`
  - `min_by`
  - `substr`
  - `sum_distinct`
- Supported HDFS ([#196](https://github.com/lakehq/sail/pull/196)).
- Supported parsing value prefixes followed by whitespace ([#218](https://github.com/lakehq/sail/pull/218) and [lakehq/sqlparser-rs#6](https://github.com/lakehq/sqlparser-rs/pull/6)).
- Added basic support for Python UDAF ([#214](https://github.com/lakehq/sail/pull/214)).

### Contributors

Huge thanks to our first community contributor, [@skewballfox](https://github.com/skewballfox) for adding support for HDFS!!

## 0.1.3

_September 18, 2024_

- Supported column positions in `GROUP BY` and `ORDER BY` ([#205](https://github.com/lakehq/sail/pull/205)).
- Expanded support for `INSERT` statements ([#195](https://github.com/lakehq/sail/pull/195)).
- Fixed issues with Spark configuration ([#192](https://github.com/lakehq/sail/pull/192)).
- Expanded support for `CREATE` and `REPLACE` statements ([#183](https://github.com/lakehq/sail/pull/183)).
- Supported `GROUPING SETS` aggregation ([#184](https://github.com/lakehq/sail/pull/184/files)).
- Integrated [fastrace](https://github.com/fast/fastrace) for more performant logging and tracing ([#166](https://github.com/lakehq/sail/pull/166)).
- Enabled gzip and zstd compression in Tonic ([#166](https://github.com/lakehq/sail/pull/166)).

## 0.1.2

_September 10, 2024_

- Fixed issues with aggregation queries.
- Extended support for SQL functions.
- Added support for temporary views and global temporary views.

## 0.1.1

_September 03, 2024_

- Extended support for SQL statements and SQL functions.
- Fixed a performance issue for the PySpark DataFrame `show()` method.

## 0.1.0

_August 29, 2024_

This is the first Sail release.
