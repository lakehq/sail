---
prev: false
next: false
---

# Changelog

## 0.4.5

_December 22, 2025_

- Added basic support for the Delta Lake merge operation ([#1093](https://github.com/lakehq/sail/pull/1093), [#1133](https://github.com/lakehq/sail/pull/1133), [#1139](https://github.com/lakehq/sail/pull/1139), and [#1144](https://github.com/lakehq/sail/pull/1144)).
- Improved distributed query execution ([#1128](https://github.com/lakehq/sail/pull/1128), [#1134](https://github.com/lakehq/sail/pull/1134), [#1135](https://github.com/lakehq/sail/pull/1135), and [#1137](https://github.com/lakehq/sail/pull/1137)).
- Improved Spark Connect server logic ([#1126](https://github.com/lakehq/sail/pull/1126) and [#1140](https://github.com/lakehq/sail/pull/1140)).
- Supported removing sessions ([#1125](https://github.com/lakehq/sail/pull/1125)).
- Supported metrics and checkpoints for Delta Lake ([#1136](https://github.com/lakehq/sail/pull/1136)).
- Improved OpenTelemetry metric reporting ([#1119](https://github.com/lakehq/sail/pull/1119)).
- Improved the following SQL functions ([#1105](https://github.com/lakehq/sail/pull/1105)):
  - `make_dt_interval`
  - `make_interval`
  - `hex`
  - `elt`
- Updated Parquet configuration options ([#1141](https://github.com/lakehq/sail/pull/1141)).
- Updated the Spark Connect protocol for Spark 4.1 ([#1145](https://github.com/lakehq/sail/pull/1145) and [#1148](https://github.com/lakehq/sail/pull/1148)).
- Fixed an issue with the `EXPLAIN` statement output ([#1147](https://github.com/lakehq/sail/pull/1147)).

### Contributors

Huge thanks to [@davidlghellin](https://github.com/davidlghellin) for your contributions!

## 0.4.4

_December 12, 2025_

- Improved Delta Lake and Iceberg integration ([#1098](https://github.com/lakehq/sail/pull/1098), [#1095](https://github.com/lakehq/sail/pull/1095), [#1108](https://github.com/lakehq/sail/pull/1108), [#1115](https://github.com/lakehq/sail/pull/1115), [#1109](https://github.com/lakehq/sail/pull/1109), and [#1117](https://github.com/lakehq/sail/pull/1117)).
- Supported exporting logs, metrics, and traces to OpenTelemetry collectors ([#1097](https://github.com/lakehq/sail/pull/1097), [#1104](https://github.com/lakehq/sail/pull/1104), and [#1116](https://github.com/lakehq/sail/pull/1116)).
- Added a Python example for reporting Sail compatibility for PySpark code ([#1075](https://github.com/lakehq/sail/pull/1075)).
- Supported customizing pod labels for Sail workers in Kubernetes deployments ([#1103](https://github.com/lakehq/sail/pull/1103)).
- Added support for the following SQL functions ([#1106](https://github.com/lakehq/sail/pull/1106)):
  - `shuffle`
  - `bitwise_not`
  - `format_string`
- Improved the output of the `EXPLAIN` statement ([#1110](https://github.com/lakehq/sail/pull/1110)).
- Fixed a few shuffle planning issues in distributed query execution ([#1111](https://github.com/lakehq/sail/pull/1111)).
- Fixed an issue with the `LIMIT` clause in distributed query execution ([#1121](https://github.com/lakehq/sail/pull/1121)).
- Improved data source implementation ([#1099](https://github.com/lakehq/sail/pull/1099)).

### Contributors

Huge thanks to [@davidlghellin](https://github.com/davidlghellin), [@zemin-piao](https://github.com/zemin-piao), [keen85](https://github.com/keen85) (_first-time contributor_), [@YichiZhang0613](https://github.com/YichiZhang0613) (_first-time contributor_), and [@gstvg](https://github.com/gstvg) (_first-time contributor_) for your contributions!

## 0.4.3

_November 26, 2025_

- Added schema evolution support for Iceberg ([#1048](https://github.com/lakehq/sail/pull/1048)).
- Improved the following SQL functions ([#1049](https://github.com/lakehq/sail/pull/1049), [#1056](https://github.com/lakehq/sail/pull/1056), [#1057](https://github.com/lakehq/sail/pull/1057), and [#1077](https://github.com/lakehq/sail/pull/1077)):
  - `max_by`
  - `min_by`
  - `signum`
  - `greatest`
  - `least`
  - `div`
- Added support for `EXPLAIN` in SQL statements ([#1078](https://github.com/lakehq/sail/pull/1078)).

### Contributors

Huge thanks to [@davidlghellin](https://github.com/davidlghellin) for your contributions!

## 0.4.2

_November 13, 2025_

- Supported column mapping for Delta Lake ([#985](https://github.com/lakehq/sail/pull/985)).
- Supported time travel for Iceberg ([#1039](https://github.com/lakehq/sail/pull/1039)).
- Supported Unity Catalog ([#1005](https://github.com/lakehq/sail/pull/1005)).
- Improved Iceberg integration ([#1006](https://github.com/lakehq/sail/pull/1006), [#1009](https://github.com/lakehq/sail/pull/1009), and [#1042](https://github.com/lakehq/sail/pull/1042)).
- Added the `luhn_check` SQL function ([#909](https://github.com/lakehq/sail/pull/909)).
- Improved the following SQL functions ([#909](https://github.com/lakehq/sail/pull/909) and [#1024](https://github.com/lakehq/sail/pull/1024)):
  - `bit_count`
  - `bit_get`
  - `getbit`
  - `crc32`
  - `sha`
  - `sha1`
  - `expm1`
  - `pmod`
  - `width_bucket`
  - `bitmap_count`
  - `to_date`
- Added the `try_avg` SQL aggregate function ([#1012](https://github.com/lakehq/sail/pull/1012)).
- Supported the `try_sum` and `try_avg` SQL aggregate functions in window expressions ([#1040](https://github.com/lakehq/sail/pull/1040)).

### Contributors

Huge thanks to [@davidlghellin](https://github.com/davidlghellin) for the contribution!

## 0.4.1

_November 2, 2025_

- Supported writing partitioned Iceberg tables ([#1003](https://github.com/lakehq/sail/pull/1003)).
- Added the `try_sum` SQL aggregate function ([#960](https://github.com/lakehq/sail/pull/960)).
- Fixed a filter pushdown performance issue ([#1008](https://github.com/lakehq/sail/pull/1008)).

### Contributors

Huge thanks to [@davidlghellin](https://github.com/davidlghellin) for the contribution!

## 0.4.0

_October 29, 2025_

- Added basic support for reading and writing Iceberg tables ([#944](https://github.com/lakehq/sail/pull/944), [#987](https://github.com/lakehq/sail/pull/987), [#976](https://github.com/lakehq/sail/pull/976), [#994](https://github.com/lakehq/sail/pull/994), and [#997](https://github.com/lakehq/sail/pull/997)).
- Supported Iceberg REST catalog ([#961](https://github.com/lakehq/sail/pull/961), [#974](https://github.com/lakehq/sail/pull/974), [#993](https://github.com/lakehq/sail/pull/993), and [#995](https://github.com/lakehq/sail/pull/995)).
- Improved Delta Lake integration ([#921](https://github.com/lakehq/sail/pull/921)).
- Supported multiple arguments for the `count_distinct` SQL function ([#957](https://github.com/lakehq/sail/pull/957)).
- Added guide for HDFS Kerberos authentication ([#992](https://github.com/lakehq/sail/pull/992)).
- Updated a few execution configuration options ([#975](https://github.com/lakehq/sail/pull/975)).
- Fixed a cost estimation issue with the join reorder optimizer ([#969](https://github.com/lakehq/sail/pull/969)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster), [@davidlghellin](https://github.com/davidlghellin), and [@zemin-piao](https://github.com/zemin-piao) (_first-time contributor_) for the contributions!

## 0.3.7

_October 3, 2025_

- Improved error reporting for the SQL parser ([#938](https://github.com/lakehq/sail/pull/938)).
- Supported the `df.unpivot()` method in the Spark DataFrame API ([#948](https://github.com/lakehq/sail/pull/948)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster) for the continued contributions!

## 0.3.6

_September 30, 2025_

- Supported the binary file format ([#853](https://github.com/lakehq/sail/pull/853)).
- Implemented an experimental join reorder physical optimizer using the DPhyp algorithm ([#810](https://github.com/lakehq/sail/pull/810) and [#917](https://github.com/lakehq/sail/pull/917)). This optimizer is not enabled by default but can be enabled via configuration options.
- Supported file metadata caching to improve read performance for the Parquet data source ([#928](https://github.com/lakehq/sail/pull/928)).
- Supported the PySpark UDF `applyInArrow()` method in the Spark DataFrame API for grouped and cogrouped data ([#886](https://github.com/lakehq/sail/pull/886) and [#887](https://github.com/lakehq/sail/pull/887)).
- Supported time travel for Delta Lake ([#854](https://github.com/lakehq/sail/pull/854)).
- Supported the delete operation for Delta Lake ([#856](https://github.com/lakehq/sail/pull/856)).
- Improved Delta Lake integration ([#848](https://github.com/lakehq/sail/pull/848) and [#916](https://github.com/lakehq/sail/pull/916)).
- Added support for the following SQL functions ([#820](https://github.com/lakehq/sail/pull/820), [#841](https://github.com/lakehq/sail/pull/841), [#824](https://github.com/lakehq/sail/pull/824), [#843](https://github.com/lakehq/sail/pull/843), [#835](https://github.com/lakehq/sail/pull/835), [#855](https://github.com/lakehq/sail/pull/855), [#859](https://github.com/lakehq/sail/pull/859), and [#860](https://github.com/lakehq/sail/pull/860)):
  - `elt`
  - `inline`
  - `inline_outer`
  - `try_parse_url`
  - `stack`
  - `make_dt_interval`
  - `version`
  - `months_between`
  - `user`
  - `session_user`
- Improved the following SQL functions ([#841](https://github.com/lakehq/sail/pull/841), [#847](https://github.com/lakehq/sail/pull/847), [#878](https://github.com/lakehq/sail/pull/878), [#920](https://github.com/lakehq/sail/pull/920), [#926](https://github.com/lakehq/sail/pull/926), and [#914](https://github.com/lakehq/sail/pull/914)):
  - `array`
  - `try_multiply`
  - `map_from_arrays`
  - `map_from_entries`
  - `approx_count_distinct`
- Added support for using all aggregate functions in window expressions ([#861](https://github.com/lakehq/sail/pull/861)).
- Fixed issues with sorting by aggregate expressions ([#915](https://github.com/lakehq/sail/pull/915)).
- Fixed issues with session key generation when the user ID is missing on the Windows platform ([#849](https://github.com/lakehq/sail/pull/849)).
- Continued the work for data streaming support ([#832](https://github.com/lakehq/sail/pull/832)).
- Added batch view creation endpoints in the MCP server ([#875](https://github.com/lakehq/sail/pull/875)).
- Added an example of using Kustomize with pod templates for Sail workers ([#833](https://github.com/lakehq/sail/pull/833)).
- Fixed input repartitioning issues for PySpark UDTFs ([#662](https://github.com/lakehq/sail/pull/662)).
- Fixed issues with the `DataFrame.replace()` method in the Spark DataFrame API ([#891](https://github.com/lakehq/sail/pull/891)).
- Supported the `REAL` data type in the SQL parser ([#892](https://github.com/lakehq/sail/pull/892)).
- Fixed various literal parsing issues in the SQL parser ([#868](https://github.com/lakehq/sail/pull/868), [#872](https://github.com/lakehq/sail/pull/872), and [#873](https://github.com/lakehq/sail/pull/873)).
- Fixed issues with PySpark UDFs with no arguments ([#895](https://github.com/lakehq/sail/pull/895)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster), [@davidlghellin](https://github.com/davidlghellin), and [@rafafrdz](https://github.com/rafafrdz) for the continued contributions!

## 0.3.5

_September 5, 2025_

- Fixed issues with writing partitioned data to Delta Lake tables ([#837](https://github.com/lakehq/sail/pull/837)).
- Improved type inference for `NULL` map values in the `VALUES` SQL clause ([#829](https://github.com/lakehq/sail/pull/829)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster) for the continued contributions!

## 0.3.4

_September 3, 2025_

- Supported the text file format ([#737](https://github.com/lakehq/sail/pull/737) and [#813](https://github.com/lakehq/sail/pull/813)).
- Supported the Spark DataFrame streaming API and added a few data sources/sinks for testing purposes ([#751](https://github.com/lakehq/sail/pull/751)). This provides a foundation for streaming support in Sail but is not ready for general use yet.
- Improved the internals of the Delta Lake integration ([#768](https://github.com/lakehq/sail/pull/768) and [#794](https://github.com/lakehq/sail/pull/794)).
- Improved idle session handling ([#761](https://github.com/lakehq/sail/pull/761) and [#818](https://github.com/lakehq/sail/pull/818)).
- Fixed performance issues with the `DataFrame.show()` method in the Spark DataFrame API ([#790](https://github.com/lakehq/sail/pull/790)).
- Fixed issues with reading and writing compressed files ([#760](https://github.com/lakehq/sail/pull/760)).
- Fixed SQL parsing issues with negated predicates ([#776](https://github.com/lakehq/sail/pull/776)).
- Fixed issues with the `DataFrame.withColumnsRenamed` method in the Spark DataFrame API ([#764](https://github.com/lakehq/sail/pull/764)).
- Fixed issues with the `DataFrame.withColumns` method in the Spark DataFrame API ([#814](https://github.com/lakehq/sail/pull/814)).
- Added support for the following SQL functions ([#727](https://github.com/lakehq/sail/pull/727), [#682](https://github.com/lakehq/sail/pull/682), [#774](https://github.com/lakehq/sail/pull/774), [#777](https://github.com/lakehq/sail/pull/777), [#779](https://github.com/lakehq/sail/pull/779), [#787](https://github.com/lakehq/sail/pull/787), [#762](https://github.com/lakehq/sail/pull/762), [#795](https://github.com/lakehq/sail/pull/795), and [#798](https://github.com/lakehq/sail/pull/798)):
  - `try_mod`
  - `make_interval`
  - `map_entries`
  - `map_from_entries`
  - `map_concat`
  - `str_to_map`
  - `width_bucket`
  - `regexp_instr`
- Improved the following SQL functions ([#682](https://github.com/lakehq/sail/pull/682), [#767](https://github.com/lakehq/sail/pull/767), [#769](https://github.com/lakehq/sail/pull/769), [#777](https://github.com/lakehq/sail/pull/777), [#722](https://github.com/lakehq/sail/pull/722), [#785](https://github.com/lakehq/sail/pull/785), [#789](https://github.com/lakehq/sail/pull/789), [#795](https://github.com/lakehq/sail/pull/795), [#801](https://github.com/lakehq/sail/pull/801), and [#806](https://github.com/lakehq/sail/pull/806)):
  - `try_add`
  - `try_divide`
  - `try_multiply`
  - `try_subtract`
  - `nth_value`
  - `median`
  - `map`
  - `map_from_arrays`
  - `split`
  - `element_at`
  - `try_element_at`
  - `position`
  - `locate`
  - `get_json_object`
  - `json_object_keys`
  - `collect_list`
- Improved a few window functions to return the correct types of integers ([#765](https://github.com/lakehq/sail/pull/765)).
- Improved the implementation of array functions ([#786](https://github.com/lakehq/sail/pull/786)).
- Improved the implementation of string functions ([#798](https://github.com/lakehq/sail/pull/798)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster), [@davidlghellin](https://github.com/davidlghellin), and [@rafafrdz](https://github.com/rafafrdz) for the continued contributions!

## 0.3.3

_August 14, 2025_

- Fixed issues with physical planning to avoid performance degradation when querying Delta Lake tables ([#750](https://github.com/lakehq/sail/pull/750)).
- Fixed issues with the `Catalog.getTable()` method in the Spark DataFrame API ([#752](https://github.com/lakehq/sail/pull/752)).
- Added support for `NaN` values in `VALUES` ([#739](https://github.com/lakehq/sail/pull/739)).
- Fixed issues with the `parquet.bloom_filter_on_write` configuration option not being respected ([#735](https://github.com/lakehq/sail/pull/735)).
- Added support for the following SQL functions ([#670](https://github.com/lakehq/sail/pull/670) and [#725](https://github.com/lakehq/sail/pull/725)):
  - `try_to_number`
  - `convert_timezone`
  - `make_timestamp_ltz`
- Improved the following SQL functions ([#725](https://github.com/lakehq/sail/pull/725), [#730](https://github.com/lakehq/sail/pull/730), [#734](https://github.com/lakehq/sail/pull/734), [#743](https://github.com/lakehq/sail/pull/743), [#754](https://github.com/lakehq/sail/pull/754), and [#756](https://github.com/lakehq/sail/pull/756)):
  - `make_timestamp`
  - `from_utc_timestamp`
  - `to_utc_timestamp`
  - `skewness`
  - `kurtosis`
  - `ln`
  - `log`
  - `log10`
  - `log1p`
  - `log2`
  - `acos`
  - `acosh`
  - `asin`
  - `asinh`
  - `atan`
  - `atan2`
  - `atanh`
  - `cbrt`
  - `cos`
  - `cosh`
  - `cot`
  - `csc`
  - `degrees`
  - `exp`
  - `radians`
  - `sec`
  - `sin`
  - `sinh`
  - `sqrt`
  - `tan`
  - `tanh`
  - `json_array_length`
  - `map_contains_key`

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster) and [@rafafrdz](https://github.com/rafafrdz) for the continued contributions!

## 0.3.2

_August 8, 2025_

- Added support for reading and writing Delta Lake tables ([#578](https://github.com/lakehq/sail/pull/578), [#634](https://github.com/lakehq/sail/pull/634), [#677](https://github.com/lakehq/sail/pull/677), [#680](https://github.com/lakehq/sail/pull/680), [#716](https://github.com/lakehq/sail/issues/716), [#717](https://github.com/lakehq/sail/pull/717), and [#723](https://github.com/lakehq/sail/pull/723)).
- Added support for Azure storage services and Google Cloud Storage (GCS), and improved support for S3 ([#616](https://github.com/lakehq/sail/pull/616) and [#706](https://github.com/lakehq/sail/pull/706)).
- Added support for file listing cache and file statistics cache ([#709](https://github.com/lakehq/sail/pull/709) and [#712](https://github.com/lakehq/sail/pull/712)).
- Added support for the following SQL functions and operators ([#529](https://github.com/lakehq/sail/pull/529), [#580](https://github.com/lakehq/sail/pull/580), [#633](https://github.com/lakehq/sail/pull/633), [#645](https://github.com/lakehq/sail/pull/645), [#638](https://github.com/lakehq/sail/pull/638), [#654](https://github.com/lakehq/sail/pull/654), [#539](https://github.com/lakehq/sail/pull/539), [#661](https://github.com/lakehq/sail/pull/661), [#629](https://github.com/lakehq/sail/pull/629), [#676](https://github.com/lakehq/sail/pull/676), [#672](https://github.com/lakehq/sail/pull/672), [#635](https://github.com/lakehq/sail/pull/635), [#683](https://github.com/lakehq/sail/pull/683), [#702](https://github.com/lakehq/sail/pull/702), [#698](https://github.com/lakehq/sail/pull/698), [#708](https://github.com/lakehq/sail/pull/708), [#713](https://github.com/lakehq/sail/pull/713), and [#719](https://github.com/lakehq/sail/issues/719)):
  - `from_csv`
  - `bround`
  - `conv`
  - `csc`
  - `sec`
  - `bit_count`
  - `bit_get`
  - `getbit`
  - `shiftrightunsigned`
  - `>>>`
  - `~`
  - `array_insert`
  - `listagg`
  - `string_agg`
  - `parse_url`
  - `url_decode`
  - `url_encode`
  - `bitmap_bit_position`
  - `bitmap_bucket_number`
  - `bitmap_count`
  - `to_number`
  - `to_utc_timestamp`
  - `try_add`
  - `try_divide`
  - `try_multiply`
  - `try_subtract`
  - `monthname`
  - `arrays_zip`
  - `is_valid_utf8`
  - `try_validate_utf8`
  - `validate_utf8`
  - `make_valid_utf8`
- Added support for the `Column.try_cast()` method in the Spark DataFrame API ([#694](https://github.com/lakehq/sail/pull/694)).
- Improved the following SQL functions ([#609](https://github.com/lakehq/sail/pull/609), [#613](https://github.com/lakehq/sail/pull/613), [#619](https://github.com/lakehq/sail/pull/619), [#621](https://github.com/lakehq/sail/pull/621), [#623](https://github.com/lakehq/sail/pull/623), [#617](https://github.com/lakehq/sail/pull/617), [#640](https://github.com/lakehq/sail/pull/640), [#644](https://github.com/lakehq/sail/pull/644), [#642](https://github.com/lakehq/sail/pull/642), [#643](https://github.com/lakehq/sail/pull/643), [#647](https://github.com/lakehq/sail/pull/647), [#660](https://github.com/lakehq/sail/pull/660), [#666](https://github.com/lakehq/sail/pull/666), [#674](https://github.com/lakehq/sail/pull/674), and [#701](https://github.com/lakehq/sail/pull/701)):
  - `date_part`
  - `datepart`
  - `extract`
  - `nullifzero`
  - `zeroifnull`
  - `array_contains`
  - `array_position`
  - `array_append`
  - `array_prepend`
  - `array_size`
  - `cardinality`
  - `size`
  - `array_agg`
  - `collect_set`
  - `flatten`
  - `arrays_overlap`
  - `concat`
  - `map`
  - `ltrim`
  - `rtrim`
  - `trim`
  - `avg`
  - `to_unix_timestamp`
- Fixed issues with the `DataFrame.na.drop()` and `DataFrame.dropna()` methods in the Spark DataFrame API ([#693](https://github.com/lakehq/sail/pull/693)).
- Fixed issues with casting timestamp and interval values from and to numeric values ([#691](https://github.com/lakehq/sail/pull/691)).
- Fixed incorrect eager execution behavior of the `CASE` expression ([#649](https://github.com/lakehq/sail/pull/649)).
- Fixed issues with PySpark UDF and UDTF execution ([#652](https://github.com/lakehq/sail/pull/652) and [#658](https://github.com/lakehq/sail/pull/658)).
- Fixed issues with expression naming ([#668](https://github.com/lakehq/sail/pull/668) and [#685](https://github.com/lakehq/sail/pull/685)).
- Improved the implementation of SQL math functions ([#699](https://github.com/lakehq/sail/pull/699)).
- Improved the internals of catalog management, data reader, and data writer ([#592](https://github.com/lakehq/sail/pull/592), [#615](https://github.com/lakehq/sail/pull/615), [#628](https://github.com/lakehq/sail/pull/628), [#632](https://github.com/lakehq/sail/pull/632), [#681](https://github.com/lakehq/sail/pull/681), [#688](https://github.com/lakehq/sail/pull/688), [#705](https://github.com/lakehq/sail/pull/705), and [#707](https://github.com/lakehq/sail/pull/707)).

### Contributors

Shoutout to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster) for contributions across bug fixes, features, and enhancements! Huge thanks to [@rafafrdz](https://github.com/rafafrdz), [@davidlghellin](https://github.com/davidlghellin), [@anhvdq](https://github.com/anhvdq) (_first-time contributor_), and [@jamesfricker](https://github.com/jamesfricker) (_first-time contributor_), for helping to further extend our parity with Spark SQL functions!

## 0.3.1

_July 7, 2025_

- Added support for the following SQL functions ([#570](https://github.com/lakehq/sail/pull/570), [#571](https://github.com/lakehq/sail/pull/571), [#582](https://github.com/lakehq/sail/pull/582), [#585](https://github.com/lakehq/sail/pull/585), and [#586](https://github.com/lakehq/sail/pull/586)):
  - `dayname`
  - `nullifzero`
  - `zeroifnull`
  - `split` (partial support)
  - `collect_set`
  - `count_if`
- Fixed issues with the `from_utc_timestamp` SQL function ([#596](https://github.com/lakehq/sail/pull/596)).
- Added support for the `DataFrame.sampleBy` method in the Spark DataFrame API ([#547](https://github.com/lakehq/sail/pull/547)).
- Added support for the following SQL statements ([#588](https://github.com/lakehq/sail/pull/588)):
  - `SHOW COLUMNS`
  - `SHOW DATABASES`
  - `SHOW TABLES`
  - `SHOW VIEWS`
- Improved data source listing performance ([#579](https://github.com/lakehq/sail/pull/579)).
- Improved the internal logic of data source options ([#587](https://github.com/lakehq/sail/pull/587) and [#598](https://github.com/lakehq/sail/pull/598)).
- Updated gRPC server TCP and HTTP configuration ([#593](https://github.com/lakehq/sail/pull/593)).

### Contributors

Huge thanks to [@SparkApplicationMaster](https://github.com/SparkApplicationMaster) for the first contributions related to SQL functions!
Huge thanks to [@davidlghellin](https://github.com/davidlghellin) for the continued contributions related to the Spark DataFrame API!

## 0.3.0

_June 28, 2025_

The 0.3.0 release introduces support for Spark 4.0 in Sail, alongside the existing support for Spark 3.5. One of the most notable changes in Spark 4.0 is the new `pyspark-client` package, a lightweight PySpark client. When using Sail in your PySpark applications, you can now choose to install this client package, instead of the full `pyspark` package that includes all the JAR files.

Here is a summary of the new features and improvements in this release.

- Improved remote data access performance by caching object stores ([#515](https://github.com/lakehq/sail/pull/515)).
- Added support for data reader and writer configuration ([#466](https://github.com/lakehq/sail/pull/466) and [#535](https://github.com/lakehq/sail/pull/535)).
- Added support for the following SQL functions ([#527](https://github.com/lakehq/sail/pull/527)):
  - `crc32`
  - `sha`
  - `sha1`
- Fixed issues with casting integers to timestamps ([#533](https://github.com/lakehq/sail/pull/533)).
- Fixed issues with the `random` and `randn` SQL functions ([#530](https://github.com/lakehq/sail/pull/530)).
- Added support for the `DataFrame.sample` method in the Spark DataFrame API ([#496](https://github.com/lakehq/sail/pull/496)).
- Added support for Spark 4.0 ([#467](https://github.com/lakehq/sail/pull/467), [#498](https://github.com/lakehq/sail/pull/498), and [#559](https://github.com/lakehq/sail/pull/559)).
- Updated the default value of a few configuration options ([#565](https://github.com/lakehq/sail/pull/565)).

### Breaking Changes

The `spark` "extra" has been removed from the `pysail` package. As a result, you can no longer use commands like `pip install pysail[spark]` to install Sail along with Spark. Instead, you must install the PySpark package separately in your Python environment.

This change allows you to freely choose the PySpark version when using Sail. Depending on your requirements, you can opt for either the `pyspark` package (Spark 3.5 or later) or the `pyspark-client` package (introduced in Spark 4.0).

### Contributors

We are thrilled by the growing interest from the community.
Huge thanks to [@rafafrdz](https://github.com/rafafrdz), [@davidlghellin](https://github.com/davidlghellin), [@lonless9](https://github.com/lonless9), and [@pimlie](https://github.com/pimlie) for making their first contributions to Sail!

## 0.2.6

_May 14, 2025_

- Improved temporal data type casting and display ([#448](https://github.com/lakehq/sail/pull/448)).
- Corrected the time unit for reading `INT96` timestamp data from Parquet files ([#444](https://github.com/lakehq/sail/pull/444)).
- Fixed issues with column metadata in the Spark DataFrame API ([#447](https://github.com/lakehq/sail/pull/447)).
- Supported referring to aliased aggregation expressions in Spark SQL `GROUP BY` and `HAVING` clauses ([#456](https://github.com/lakehq/sail/pull/456)).
- Supported more data formats and added directory listing endpoints in the MCP server ([#455](https://github.com/lakehq/sail/pull/455) and [#458](https://github.com/lakehq/sail/pull/458)).

## 0.2.5

_April 22, 2025_

- Corrected Spark session default time zone configuration and fixed various issues for timestamp data types ([#438](https://github.com/lakehq/sail/pull/438)).
- Improved object store setup and cluster mode task execution ([#432](https://github.com/lakehq/sail/pull/432)).

## 0.2.4

_April 10, 2025_

- Improved MCP server logging ([#421](https://github.com/lakehq/sail/pull/421)).
- Improved AWS S3 data access ([#426](https://github.com/lakehq/sail/pull/426)).
- Supported AWS credential caching ([#430](https://github.com/lakehq/sail/pull/430)).
- Fixed issues with cluster mode task execution ([#429](https://github.com/lakehq/sail/pull/429)).
- Supported `exceptAll()` and `tail()` in the Spark DataFrame API ([#417](https://github.com/lakehq/sail/pull/417)).

## 0.2.3

_March 21, 2025_

- Implemented MCP (Model Context Protocol) server ([#410](https://github.com/lakehq/sail/pull/410)).
- Supported the `hf://` protocol for reading Hugging Face datasets ([#412](https://github.com/lakehq/sail/pull/412)).
- Supported glob patterns in data source URLs ([#415](https://github.com/lakehq/sail/pull/415)).
- Supported a few data reader and writer options for CSV files ([#414](https://github.com/lakehq/sail/pull/414)).
- Fixed a few issues with SQL temporary views ([#413](https://github.com/lakehq/sail/pull/413)).
- Improved task error reporting in cluster mode ([#409](https://github.com/lakehq/sail/pull/409)).

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

This release comes with the new Sail CLI, and the way to launch the Spark Connect server and PySpark shell is different from the 0.1.x versions. Please refer to the [Getting Started](/introduction/getting-started/) page for the updated instructions.

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
