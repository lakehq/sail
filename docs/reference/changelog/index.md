# Changelog

## 0.1.4

_October 03, 2024_

- Enable Avro in DataFusion [#234](https://github.com/lakehq/sail/pull/234).
- Expanded support for Spark SQL syntax and functions [#213](https://github.com/lakehq/sail/pull/213) and [#207](https://github.com/lakehq/sail/pull/207).
  Added full parity and coverage for the following SQL functions:
  `array`, `date_format`, `get_json_object`, `json_array_length`, `overlay`, `replace`, `split_part`, `to_date`,
  `any_value`, `approx_count_distinct`, `current_timezone`, `first_value`, `greatest`, `last`, `last_value`, `least`,
  `map_contains_key`, `map_keys`, `map_values`, `min_by`, `substr`, `sum_distinct`.
- HDFS support [#196](https://github.com/lakehq/sail/pull/196).
- Support parsing value prefixes followed by whitespace [#218](https://github.com/lakehq/sail/pull/218) and [#6](https://github.com/lakehq/sqlparser-rs/pull/6).
- Python UDAF support [#214](https://github.com/lakehq/sail/pull/214).

### Contributors

Huge thanks to our first community contributor, [@skewballfox](https://github.com/skewballfox) for adding support for HDFS!!

## 0.1.3

_September 18, 2024_

- Support Group By and Order By Column Positions [#205](https://github.com/lakehq/sail/pull/205).
- Expanded support for `INSERT` statements [#195](https://github.com/lakehq/sail/pull/195).
- Fix issues with Spark config [#192](https://github.com/lakehq/sail/pull/192).
- Expanded Support for `CREATE` and `REPLACE` statement syntax [#183](https://github.com/lakehq/sail/pull/183).
- Support `Grouping SETS` Aggregation [#184](https://github.com/lakehq/sail/pull/184/files).
- Fastrace integration for improved and more performant logging and tracing [#166](https://github.com/lakehq/sail/pull/166).
- Enable `GZIP` and `ZSTD` in Tonic [#166](https://github.com/lakehq/sail/pull/166).

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
