---
title: Scalar Functions
rank: 1
---

# List of Supported Built-in Spark Scalar Functions

## Array

- `array`
- `array_append`
- `array_compact`
- `array_contains`
- `array_contains_all`
- `array_distinct`
- `array_except`
- `array_insert`
- `array_intersect`
- `array_join`
- `array_max`
- `array_min`
- `array_position`
- `array_prepend`
- `array_remove`
- `array_repeat`
- `array_size`
- `array_union`
- `arrays_overlap`
- `arrays_zip`
- `flatten`
- `get`
- `sequence`
- `slice`
- `sort_array`

## Bitwise

- `&` (ampersand sign) operator
- `^` (caret sign) operator
- `bit_count`
- `bit_get`
- `getbit`
- `shiftleft`
- `<<` (bitwise left shift) operator
- `shiftright`
- `>>` (bitwise right shift) operator
- `shiftrightunsigned`
- `>>>` (bitwise unsigned right shift) operator
- `|` (pipe sign) operator
- `~` (tilde sign) operator

## Collection

- `cardinality`
- `deep_size`
- `element_at`
- `size`
- `concat`
- `reverse`
- `try_element_at`

## Conditional

- `coalesce`
- `if`
- `ifnull`
- `nanvl`
- `nullif`
- `nullifzero`
- `nvl`
- `nvl2`
- `zeroifnull`
- `when`
- `case`

## Conversion

- `bigint`
- `binary`
- `boolean`
- `cast`
- `date`
- `decimal`
- `double`
- `float`
- `int`
- `smallint`
- `string`
- `timestamp`
- `tinyint`

## CSV

- `from_csv`

## Datetime

- `add_years`
- `add_months`
- `add_days`
- `convert_timezone`
- `curdate`
- `current_date`
- `current_timestamp`
- `current_timezone`
- `date_add`
- `date_diff`
- `date_format`
- `date_from_unix_date`
- `date_part`
- `date_sub`
- `date_trunc`
- `dateadd`
- `datediff`
- `datepart`
- `day`
- `dayname`
- `dayofmonth`
- `dayofweek`
- `dayofyear`
- `extract`
- `from_unixtime`
- `from_utc_timestamp`
- `hour`
- `last_day`
- `localtimestamp`
- `make_date`
- `make_dt_interval`
- `make_interval`
- `make_timestamp`
- `make_timestamp_ltz`
- `make_timestamp_ntz`
- `make_ym_interval`
- `minute`
- `month`
- `monthname`
- `months_between`
- `next_day`
- `now`
- `quarter`
- `second`
- `timestamp_micros`
- `timestamp_millis`
- `timestamp_seconds`
- `to_date`
- `to_timestamp`
- `to_timestamp_ltz`
- `to_timestamp_ntz`
- `to_unix_timestamp`
- `to_utc_timestamp`
- `trunc`
- `try_to_timestamp`
- `unix_date`
- `unix_micros`
- `unix_millis`
- `unix_seconds`
- `unix_timestamp`
- `weekday`
- `weekofyear`
- `year`

## Hash

- `crc32`
- `hash`
- `md5`
- `sha`
- `sha1`
- `sha2`
- `xxhash64`

## JSON

- `get_json_object`
- `json_array_length`
- `json_object_keys`

## Map

- `map`
- `map_concat`
- `map_contains_key`
- `map_entries`
- `map_from_arrays`
- `map_from_entries`
- `map_keys`
- `map_values`
- `str_to_map`

## Math

- `%` (percent sign) operator
- `*` (asterisk sign) operator
- `+` (plus sign) operator
- `-` (minus sign) operator
- `/` (slash sign) operator
- `abs`
- `acos`
- `acosh`
- `asin`
- `asinh`
- `atan`
- `atan2`
- `atanh`
- `bin`
- `bround`
- `cbrt`
- `ceil`
- `ceiling`
- `conv`
- `cos`
- `cosh`
- `cot`
- `csc`
- `degrees`
- `div`
- `e`
- `exp`
- `expm1`
- `factorial`
- `floor`
- `greatest`
- `hex`
- `hypot`
- `least`
- `ln`
- `log`
- `log10`
- `log1p`
- `log2`
- `mod`
- `negative`
- `pi`
- `pmod`
- `positive`
- `pow`
- `power`
- `radians`
- `rand`
- `random_poisson`
- `randn`
- `random`
- `rint`
- `round`
- `sec`
- `sign`
- `signum`
- `sin`
- `sinh`
- `sqrt`
- `tan`
- `tanh`
- `try_add`
- `try_divide`
- `try_multiply`
- `try_mod`
- `try_subtract`
- `unhex`
- `width_bucket`

## Misc

- `aes_decrypt`
- `aes_encrypt`
- `assert_true`
- `bitmap_bit_position`
- `bitmap_bucket_number`
- `bitmap_count`
- `current_catalog`
- `current_database`
- `current_schema`
- `current_user`
- `equal_null`
- `raise_error`
- `session_user`
- `try_aes_encrypt`
- `try_aes_decrypt`
- `typeof`
- `user`
- `uuid`
- `version`

## Predicate

- `!` (bang sign) operator
- `!=` (bang eq sign) operator
- `<` (lt sign) operator
- `<=` (lt eq sign) operator
- `<=>` (lt eq gt sign) operator
- `=` (eq sign) operator
- `==` (eq eq sign) operator
- `>` (gt sign) operator
- `>=` (gt eq sign) operator
- `and`
- `ilike`
- `in`
- `isnan`
- `isnotnull`
- `isnull`
- `like`
- `not`
- `or`
- `regexp`
- `regexp_like`
- `rlike`

## String

- `ascii`
- `base64`
- `bit_length`
- `btrim`
- `char`
- `char_length`
- `character_length`
- `chr`
- `concat_ws`
- `contains`
- `decode`
- `elt`
- `encode`
- `endswith`
- `find_in_set`
- `initcap`
- `instr`
- `is_valid_utf8`
- `lcase`
- `left`
- `len`
- `length`
- `levenshtein`
- `locate`
- `lower`
- `lpad`
- `ltrim`
- `make_valid_utf8`
- `mask`
- `octet_length`
- `overlay`
- `position`
- `regexp_count`
- `regexp_instr`
- `regexp_replace`
- `repeat`
- `replace`
- `right`
- `rpad`
- `rtrim`
- `space`
- `split`
- `split_part`
- `startswith`
- `substr`
- `substring`
- `substring_index`
- `to_binary`
- `to_number`
- `translate`
- `trim`
- `try_to_binary`
- `try_to_number`
- `try_validate_utf8`
- `ucase`
- `unbase64`
- `upper`
- `validate_utf8`
- `strpos`

## Struct

- `named_struct`
- `struct`

## URL

- `parse_url`
- `try_parse_url`
- `url_decode`
- `url_encode`
