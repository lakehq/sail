# Changelog

## 0.1.7

## _November 1, 2024_

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
