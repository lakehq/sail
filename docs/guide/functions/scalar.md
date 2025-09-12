---
title: Scalar Functions
rank: 1
---

# Built-in Spark Scalar Functions

## Array

| Function             | Supported          |
|----------------------|--------------------|
| `array`              | :white_check_mark: |
| `array_append`       | :white_check_mark: |
| `array_compact`      | :white_check_mark: |
| `array_contains`     | :white_check_mark: |
| `array_contains_all` | :white_check_mark: |
| `array_distinct`     | :white_check_mark: |
| `array_except`       | :white_check_mark: |
| `array_insert`       | :white_check_mark: |
| `array_intersect`    | :white_check_mark: |
| `array_join`         | :white_check_mark: |
| `array_max`          | :white_check_mark: |
| `array_min`          | :white_check_mark: |
| `array_position`     | :white_check_mark: |
| `array_prepend`      | :white_check_mark: |
| `array_remove`       | :white_check_mark: |
| `array_repeat`       | :white_check_mark: |
| `array_size`         | :white_check_mark: |
| `array_union`        | :white_check_mark: |
| `arrays_overlap`     | :white_check_mark: |
| `arrays_zip`         | :white_check_mark: |
| `flatten`            | :white_check_mark: |
| `get`                | :white_check_mark: |
| `sequence`           | :white_check_mark: |
| `slice`              | :white_check_mark: |
| `sort_array`         | :white_check_mark: |
| `shuffle`            | :construction:     |

## Bitwise

| Function                                      | Supported          |
|-----------------------------------------------|--------------------|
| `&` (ampersand sign) operator                 | :white_check_mark: |
| `^` (caret sign) operator                     | :white_check_mark: |
| `bit_count`                                   | :white_check_mark: |
| `bit_get`                                     | :white_check_mark: |
| `getbit`                                      | :white_check_mark: |
| `shiftleft`                                   | :white_check_mark: |
| `<<` (bitwise left shift) operator            | :white_check_mark: |
| `shiftright`                                  | :white_check_mark: |
| `>>` (bitwise right shift) operator           | :white_check_mark: |
| `shiftrightunsigned`                          | :white_check_mark: |
| `>>>` (bitwise unsigned right shift) operator | :white_check_mark: |
| `\|` (pipe sign) operator                     | :white_check_mark: |
| `~` (tilde sign) operator                     | :white_check_mark: |

## Collection

| Function         | Supported          |
|------------------|--------------------|
| `cardinality`    | :white_check_mark: |
| `deep_size`      | :white_check_mark: |
| `element_at`     | :white_check_mark: |
| `size`           | :white_check_mark: |
| `concat`         | :white_check_mark: |
| `reverse`        | :white_check_mark: |
| `try_element_at` | :white_check_mark: |

## Conditional

| Function     | Supported          |
|--------------|--------------------|
| `coalesce`   | :white_check_mark: |
| `if`         | :white_check_mark: |
| `ifnull`     | :white_check_mark: |
| `nanvl`      | :white_check_mark: |
| `nullif`     | :white_check_mark: |
| `nullifzero` | :white_check_mark: |
| `nvl`        | :white_check_mark: |
| `nvl2`       | :white_check_mark: |
| `zeroifnull` | :white_check_mark: |
| `when`       | :white_check_mark: |
| `case`       | :white_check_mark: |

## Conversion

| Function    | Supported          |
|-------------|--------------------|
| `bigint`    | :white_check_mark: |
| `binary`    | :white_check_mark: |
| `boolean`   | :white_check_mark: |
| `cast`      | :white_check_mark: |
| `date`      | :white_check_mark: |
| `decimal`   | :white_check_mark: |
| `double`    | :white_check_mark: |
| `float`     | :white_check_mark: |
| `int`       | :white_check_mark: |
| `smallint`  | :white_check_mark: |
| `string`    | :white_check_mark: |
| `timestamp` | :white_check_mark: |
| `tinyint`   | :white_check_mark: |

## CSV

| Function        | Supported          |
|-----------------|--------------------|
| `from_csv`      | :white_check_mark: |
| `schema_of_csv` | :construction:     |
| `to_csv`        | :construction:     |

## Datetime

| Function                 | Supported          |
|--------------------------|--------------------|
| `add_years`              | :white_check_mark: |
| `add_months`             | :white_check_mark: |
| `add_days`               | :white_check_mark: |
| `convert_timezone`       | :white_check_mark: |
| `curdate`                | :white_check_mark: |
| `current_date`           | :white_check_mark: |
| `current_timestamp`      | :white_check_mark: |
| `current_timezone`       | :white_check_mark: |
| `date_add`               | :white_check_mark: |
| `date_diff`              | :white_check_mark: |
| `date_format`            | :white_check_mark: |
| `date_from_unix_date`    | :white_check_mark: |
| `date_part`              | :white_check_mark: |
| `date_sub`               | :white_check_mark: |
| `date_trunc`             | :white_check_mark: |
| `dateadd`                | :white_check_mark: |
| `datediff`               | :white_check_mark: |
| `datepart`               | :white_check_mark: |
| `day`                    | :white_check_mark: |
| `dayname`                | :white_check_mark: |
| `dayofmonth`             | :white_check_mark: |
| `dayofweek`              | :white_check_mark: |
| `dayofyear`              | :white_check_mark: |
| `extract`                | :white_check_mark: |
| `from_unixtime`          | :white_check_mark: |
| `from_utc_timestamp`     | :white_check_mark: |
| `hour`                   | :white_check_mark: |
| `last_day`               | :white_check_mark: |
| `localtimestamp`         | :white_check_mark: |
| `make_date`              | :white_check_mark: |
| `make_dt_interval`       | :white_check_mark: |
| `make_interval`          | :white_check_mark: |
| `make_timestamp`         | :white_check_mark: |
| `make_timestamp_ltz`     | :white_check_mark: |
| `make_timestamp_ntz`     | :white_check_mark: |
| `make_ym_interval`       | :white_check_mark: |
| `minute`                 | :white_check_mark: |
| `month`                  | :white_check_mark: |
| `monthname`              | :white_check_mark: |
| `months_between`         | :white_check_mark: |
| `next_day`               | :white_check_mark: |
| `now`                    | :white_check_mark: |
| `quarter`                | :white_check_mark: |
| `second`                 | :white_check_mark: |
| `timestamp_micros`       | :white_check_mark: |
| `timestamp_millis`       | :white_check_mark: |
| `timestamp_seconds`      | :white_check_mark: |
| `to_date`                | :white_check_mark: |
| `to_timestamp`           | :white_check_mark: |
| `to_timestamp_ltz`       | :white_check_mark: |
| `to_timestamp_ntz`       | :white_check_mark: |
| `to_unix_timestamp`      | :white_check_mark: |
| `to_utc_timestamp`       | :white_check_mark: |
| `trunc`                  | :white_check_mark: |
| `try_to_timestamp`       | :white_check_mark: |
| `unix_date`              | :white_check_mark: |
| `unix_micros`            | :white_check_mark: |
| `unix_millis`            | :white_check_mark: |
| `unix_seconds`           | :white_check_mark: |
| `unix_timestamp`         | :white_check_mark: |
| `weekday`                | :white_check_mark: |
| `weekofyear`             | :white_check_mark: |
| `year`                   | :white_check_mark: |
| `session_window`         | :construction:     |
| `window`                 | :construction:     |
| `window_time`            | :construction:     |
| `try_make_interval`      | :construction:     |
| `try_make_timestamp`     | :construction:     |
| `try_make_timestamp_ltz` | :construction:     |
| `try_make_timestamp_ntz` | :construction:     |

## Hash

| Function   | Supported          |
|------------|--------------------|
| `crc32`    | :white_check_mark: |
| `hash`     | :white_check_mark: |
| `md5`      | :white_check_mark: |
| `sha`      | :white_check_mark: |
| `sha1`     | :white_check_mark: |
| `sha2`     | :white_check_mark: |
| `xxhash64` | :white_check_mark: |

## JSON

| Function            | Supported          |
|---------------------|--------------------|
| `get_json_object`   | :white_check_mark: |
| `json_array_length` | :white_check_mark: |
| `json_object_keys`  | :white_check_mark: |
| `from_json`         | :construction:     |
| `json_tuple`        | :construction:     |
| `schema_of_json`    | :construction:     |
| `to_json`           | :construction:     |

## Lambda

| Function           | Supported      |
|--------------------|----------------|
| `aggregate`        | :construction: |
| `array_sort`       | :construction: |
| `exists`           | :construction: |
| `filter`           | :construction: |
| `forall`           | :construction: |
| `map_filter`       | :construction: |
| `map_zip_with`     | :construction: |
| `reduce`           | :construction: |
| `transform`        | :construction: |
| `transform_keys`   | :construction: |
| `transform_values` | :construction: |
| `zip_with`         | :construction: |

## Map

| Function           | Supported          |
|--------------------|--------------------|
| `map`              | :white_check_mark: |
| `map_concat`       | :white_check_mark: |
| `map_contains_key` | :white_check_mark: |
| `map_entries`      | :white_check_mark: |
| `map_from_arrays`  | :white_check_mark: |
| `map_from_entries` | :white_check_mark: |
| `map_keys`         | :white_check_mark: |
| `map_values`       | :white_check_mark: |
| `str_to_map`       | :white_check_mark: |

## Math

| Function                     | Supported          |
|------------------------------|--------------------|
| `%` (percent sign) operator  | :white_check_mark: |
| `*` (asterisk sign) operator | :white_check_mark: |
| `+` (plus sign) operator     | :white_check_mark: |
| `-` (minus sign) operator    | :white_check_mark: |
| `/` (slash sign) operator    | :white_check_mark: |
| `abs`                        | :white_check_mark: |
| `acos`                       | :white_check_mark: |
| `acosh`                      | :white_check_mark: |
| `asin`                       | :white_check_mark: |
| `asinh`                      | :white_check_mark: |
| `atan`                       | :white_check_mark: |
| `atan2`                      | :white_check_mark: |
| `atanh`                      | :white_check_mark: |
| `bin`                        | :white_check_mark: |
| `bround`                     | :white_check_mark: |
| `cbrt`                       | :white_check_mark: |
| `ceil`                       | :white_check_mark: |
| `ceiling`                    | :white_check_mark: |
| `conv`                       | :white_check_mark: |
| `cos`                        | :white_check_mark: |
| `cosh`                       | :white_check_mark: |
| `cot`                        | :white_check_mark: |
| `csc`                        | :white_check_mark: |
| `degrees`                    | :white_check_mark: |
| `div`                        | :white_check_mark: |
| `e`                          | :white_check_mark: |
| `exp`                        | :white_check_mark: |
| `expm1`                      | :white_check_mark: |
| `factorial`                  | :white_check_mark: |
| `floor`                      | :white_check_mark: |
| `greatest`                   | :white_check_mark: |
| `hex`                        | :white_check_mark: |
| `hypot`                      | :white_check_mark: |
| `least`                      | :white_check_mark: |
| `ln`                         | :white_check_mark: |
| `log`                        | :white_check_mark: |
| `log10`                      | :white_check_mark: |
| `log1p`                      | :white_check_mark: |
| `log2`                       | :white_check_mark: |
| `mod`                        | :white_check_mark: |
| `negative`                   | :white_check_mark: |
| `pi`                         | :white_check_mark: |
| `pmod`                       | :white_check_mark: |
| `positive`                   | :white_check_mark: |
| `pow`                        | :white_check_mark: |
| `power`                      | :white_check_mark: |
| `radians`                    | :white_check_mark: |
| `rand`                       | :white_check_mark: |
| `random_poisson`             | :white_check_mark: |
| `randn`                      | :white_check_mark: |
| `random`                     | :white_check_mark: |
| `rint`                       | :white_check_mark: |
| `round`                      | :white_check_mark: |
| `sec`                        | :white_check_mark: |
| `sign`                       | :white_check_mark: |
| `signum`                     | :white_check_mark: |
| `sin`                        | :white_check_mark: |
| `sinh`                       | :white_check_mark: |
| `sqrt`                       | :white_check_mark: |
| `tan`                        | :white_check_mark: |
| `tanh`                       | :white_check_mark: |
| `try_add`                    | :white_check_mark: |
| `try_divide`                 | :white_check_mark: |
| `try_multiply`               | :white_check_mark: |
| `try_mod`                    | :white_check_mark: |
| `try_subtract`               | :white_check_mark: |
| `unhex`                      | :white_check_mark: |
| `width_bucket`               | :white_check_mark: |
| `uniform`                    | :construction:     |

## Misc

| Function                      | Supported          |
|-------------------------------|--------------------|
| `aes_decrypt`                 | :white_check_mark: |
| `aes_encrypt`                 | :white_check_mark: |
| `assert_true`                 | :white_check_mark: |
| `bitmap_bit_position`         | :white_check_mark: |
| `bitmap_bucket_number`        | :white_check_mark: |
| `bitmap_count`                | :white_check_mark: |
| `current_catalog`             | :white_check_mark: |
| `current_database`            | :white_check_mark: |
| `current_schema`              | :white_check_mark: |
| `current_user`                | :white_check_mark: |
| `equal_null`                  | :white_check_mark: |
| `raise_error`                 | :white_check_mark: |
| `session_user`                | :white_check_mark: |
| `try_aes_encrypt`             | :white_check_mark: |
| `try_aes_decrypt`             | :white_check_mark: |
| `typeof`                      | :white_check_mark: |
| `user`                        | :white_check_mark: |
| `uuid`                        | :white_check_mark: |
| `version`                     | :white_check_mark: |
| `hll_sketch_estimate`         | :construction:     |
| `hll_union`                   | :construction:     |
| `input_file_block_length`     | :construction:     |
| `input_file_block_start`      | :construction:     |
| `input_file_name`             | :construction:     |
| `monotonically_increasing_id` | :construction:     |
| `spark_partition_id`          | :construction:     |
| `from_avro`                   | :construction:     |
| `from_protobuf`               | :construction:     |
| `schema_of_avro`              | :construction:     |
| `to_avro`                     | :construction:     |
| `to_protobuf`                 | :construction:     |
| `reflect`                     | :x:                |
| `try_reflect`                 | :x:                |
| `java_method`                 | :x:                |

## Predicate

| Function                       | Supported          |
|--------------------------------|--------------------|
| `!` (bang sign) operator       | :white_check_mark: |
| `!=` (bang eq sign) operator   | :white_check_mark: |
| `<` (lt sign) operator         | :white_check_mark: |
| `<=` (lt eq sign) operator     | :white_check_mark: |
| `<=>` (lt eq gt sign) operator | :white_check_mark: |
| `=` (eq sign) operator         | :white_check_mark: |
| `==` (eq eq sign) operator     | :white_check_mark: |
| `>` (gt sign) operator         | :white_check_mark: |
| `>=` (gt eq sign) operator     | :white_check_mark: |
| `and`                          | :white_check_mark: |
| `ilike`                        | :white_check_mark: |
| `in`                           | :white_check_mark: |
| `isnan`                        | :white_check_mark: |
| `isnotnull`                    | :white_check_mark: |
| `isnull`                       | :white_check_mark: |
| `like`                         | :white_check_mark: |
| `not`                          | :white_check_mark: |
| `or`                           | :white_check_mark: |
| `regexp`                       | :white_check_mark: |
| `regexp_like`                  | :white_check_mark: |
| `rlike`                        | :white_check_mark: |

## String

| Function             | Supported          |
|----------------------|--------------------|
| `ascii`              | :white_check_mark: |
| `base64`             | :white_check_mark: |
| `bit_length`         | :white_check_mark: |
| `btrim`              | :white_check_mark: |
| `char`               | :white_check_mark: |
| `char_length`        | :white_check_mark: |
| `character_length`   | :white_check_mark: |
| `chr`                | :white_check_mark: |
| `concat_ws`          | :white_check_mark: |
| `contains`           | :white_check_mark: |
| `decode`             | :white_check_mark: |
| `elt`                | :white_check_mark: |
| `encode`             | :white_check_mark: |
| `endswith`           | :white_check_mark: |
| `find_in_set`        | :white_check_mark: |
| `initcap`            | :white_check_mark: |
| `instr`              | :white_check_mark: |
| `is_valid_utf8`      | :white_check_mark: |
| `lcase`              | :white_check_mark: |
| `left`               | :white_check_mark: |
| `len`                | :white_check_mark: |
| `length`             | :white_check_mark: |
| `levenshtein`        | :white_check_mark: |
| `locate`             | :white_check_mark: |
| `lower`              | :white_check_mark: |
| `lpad`               | :white_check_mark: |
| `ltrim`              | :white_check_mark: |
| `make_valid_utf8`    | :white_check_mark: |
| `mask`               | :white_check_mark: |
| `octet_length`       | :white_check_mark: |
| `overlay`            | :white_check_mark: |
| `position`           | :white_check_mark: |
| `regexp_count`       | :white_check_mark: |
| `regexp_instr`       | :white_check_mark: |
| `regexp_replace`     | :white_check_mark: |
| `repeat`             | :white_check_mark: |
| `replace`            | :white_check_mark: |
| `right`              | :white_check_mark: |
| `rpad`               | :white_check_mark: |
| `rtrim`              | :white_check_mark: |
| `space`              | :white_check_mark: |
| `split`              | :white_check_mark: |
| `split_part`         | :white_check_mark: |
| `startswith`         | :white_check_mark: |
| `substr`             | :white_check_mark: |
| `substring`          | :white_check_mark: |
| `substring_index`    | :white_check_mark: |
| `to_binary`          | :white_check_mark: |
| `to_number`          | :white_check_mark: |
| `translate`          | :white_check_mark: |
| `trim`               | :white_check_mark: |
| `try_to_binary`      | :white_check_mark: |
| `try_to_number`      | :white_check_mark: |
| `try_validate_utf8`  | :white_check_mark: |
| `ucase`              | :white_check_mark: |
| `unbase64`           | :white_check_mark: |
| `upper`              | :white_check_mark: |
| `validate_utf8`      | :white_check_mark: |
| `strpos`             | :white_check_mark: |
| `format_number`      | :construction:     |
| `format_string`      | :construction:     |
| `luhn_check`         | :construction:     |
| `printf`             | :construction:     |
| `regexp_extract`     | :construction:     |
| `regexp_extract_all` | :construction:     |
| `regexp_substr`      | :construction:     |
| `sentences`          | :construction:     |
| `soundex`            | :construction:     |
| `to_char`            | :construction:     |
| `to_varchar`         | :construction:     |
| `collate`            | :construction:     |
| `collation`          | :construction:     |
| `randstr`            | :construction:     |

## Struct

| Function       | Supported          |
|----------------|--------------------|
| `named_struct` | :white_check_mark: |
| `struct`       | :white_check_mark: |

## URL

| Function         | Supported          |
|------------------|--------------------|
| `parse_url`      | :white_check_mark: |
| `try_parse_url`  | :white_check_mark: |
| `try_url_decode` | :white_check_mark: |
| `url_decode`     | :white_check_mark: |
| `url_encode`     | :white_check_mark: |

## Variant

| Function                | Supported      |
|-------------------------|----------------|
| `is_variant_null`       | :construction: |
| `parse_json`            | :construction: |
| `schema_of_variant`     | :construction: |
| `schema_of_variant_agg` | :construction: |
| `to_variant_object`     | :construction: |
| `try_parse_json`        | :construction: |
| `try_variant_get`       | :construction: |
| `variant_explode`       | :construction: |
| `variant_explode_outer` | :construction: |
| `variant_get`           | :construction: |

## XML

| Function        | Supported      |
|-----------------|----------------|
| `xpath`         | :construction: |
| `xpath_boolean` | :construction: |
| `xpath_double`  | :construction: |
| `xpath_float`   | :construction: |
| `xpath_int`     | :construction: |
| `xpath_long`    | :construction: |
| `xpath_number`  | :construction: |
| `xpath_short`   | :construction: |
| `xpath_string`  | :construction: |
| `from_xml`      | :construction: |
| `schema_of_xml` | :construction: |
| `to_xml`        | :construction: |