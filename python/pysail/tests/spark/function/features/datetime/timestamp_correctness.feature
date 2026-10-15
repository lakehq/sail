Feature: Timestamp operations preserve instants and session-local calendar values

  Background:
    Given config spark.sql.session.timeZone = UTC
    And variable location for temporary directory timestamp_correctness
    And final statement
      """
      DROP TABLE IF EXISTS timestamp_correctness
      """
    And statement template
      """
      CREATE TABLE timestamp_correctness USING parquet LOCATION {{ location.sql }} AS
      SELECT id, wall, instant, array(instant) AS instants,
             named_struct('ts', instant) AS instant_struct,
             map('ts', instant) AS instant_map
      FROM VALUES
        (0, TIMESTAMP_NTZ '2020-03-08 02:30:00', TIMESTAMP '2020-03-08 10:30:00Z'),
        (1, TIMESTAMP_NTZ '2020-11-01 01:30:00', TIMESTAMP '2020-11-01 09:30:00Z'),
        (2, TIMESTAMP_NTZ '2020-01-01 00:30:00', TIMESTAMP '2020-01-01 00:30:00Z'),
        (3, TIMESTAMP_NTZ '1969-12-31 23:59:59.999999', TIMESTAMP '1969-12-31 23:59:59.999999Z'),
        (4, CAST(NULL AS TIMESTAMP_NTZ), CAST(NULL AS TIMESTAMP_LTZ))
        AS t(id, wall, instant)
      """

  Scenario: Stored instants use the current session timezone in calendar operations
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(instant AS STRING) AS displayed,
             CAST(instant AS DATE) AS local_date,
             CAST(instant AS TIMESTAMP_NTZ) AS local_time,
             hour(instant) AS hour,
             year(instant) AS year,
             date_format(instant, 'yyyy-MM-dd HH:mm:ss') AS formatted,
             CAST(date_trunc('DAY', instant) AS STRING) AS midnight
      FROM timestamp_correctness WHERE id = 2
      """
    Then query result
      | displayed           | local_date | local_time          | hour | year | formatted           | midnight            |
      | 2019-12-31 16:30:00 | 2019-12-31 | 2019-12-31 16:30:00 | 16   | 2019 | 2019-12-31 16:30:00 | 2019-12-31 00:00:00 |

  Scenario: Timestamp identity functions preserve the later occurrence of an overlap
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(to_timestamp(instant)) = unix_micros(instant) AS ordinary,
             unix_micros(to_timestamp_ltz(instant)) = unix_micros(instant) AS explicit_ltz,
             unix_micros(try_to_timestamp(instant)) = unix_micros(instant) AS tolerant,
             CAST(to_timestamp_ntz(instant) AS STRING) AS local_time
      FROM timestamp_correctness WHERE id = 1
      """
    Then query result
      | ordinary | explicit_ltz | tolerant | local_time          |
      | true     | true         | true     | 2020-11-01 01:30:00 |

  Scenario: Timestamp conversion functions resolve nonexistent local times like casts
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(CAST(wall AS TIMESTAMP_LTZ) AS STRING) AS cast_value,
             CAST(to_timestamp(wall) AS STRING) AS parsed_value,
             CAST(to_timestamp_ltz(wall) AS STRING) AS explicit_value,
             CAST(try_to_timestamp(wall) AS STRING) AS tolerant_value
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | cast_value          | parsed_value        | explicit_value      | tolerant_value      |
      | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 | 2020-03-08 03:30:00 |

  Scenario: Timestamp constructors resolve gaps without losing explicit-zone instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(make_timestamp(2020, 3, 8, 2, 30, 0) AS STRING) AS gap,
             unix_micros(make_timestamp_ltz(2020, 11, 1, 9, 30, 0, 'UTC')) AS overlap,
             CAST(try_make_timestamp(2020, 3, 8, 2, 30, 0) AS STRING) AS tolerant
      """
    Then query result
      | gap                 | overlap          | tolerant            |
      | 2020-03-08 03:30:00 | 1604223000000000 | 2020-03-08 03:30:00 |

  Scenario: Truncating a wall timestamp returns an instant after resolving a gap
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(date_trunc('HOUR', wall) AS STRING) AS truncated,
             typeof(date_trunc('HOUR', wall)) AS result_type
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | truncated           | result_type |
      | 2020-03-08 03:00:00 | timestamp   |

  Scenario: Epoch seconds and milliseconds round negative timestamps down
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_seconds(instant) AS seconds,
             unix_millis(instant) AS millis,
             unix_micros(instant) AS micros
      FROM timestamp_correctness WHERE id = 3
      """
    Then query result
      | seconds | millis | micros |
      | -1      | -1     | -1     |

  Scenario Outline: Mixed temporal <expression> values use an instant common type
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(<expression> AS STRING) AS value, typeof(<expression>) AS result_type
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | value               | result_type |
      | 2020-03-08 03:30:00 | timestamp   |

    Examples:
      | expression                              |
      | coalesce(wall, instant)                  |
      | ifnull(wall, instant)                    |
      | nvl(wall, instant)                       |
      | nvl2(wall, wall, instant)                |
      | if(id = 0, wall, instant)                |
      | CASE WHEN id = 0 THEN wall ELSE instant END |
      | greatest(wall, instant)                  |
      | least(wall, instant)                     |
      | array(wall, instant)[0]                  |
      | map(0, wall, 1, instant)[0]              |
      | coalesce(array(wall), array(instant))[0] |

  Scenario: Mixed timestamp predicates and NULLIF compare instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, wall IN (instant) AS scalar_in,
             array(wall) IN (array(instant)) AS array_in,
             CAST(nullif(wall, instant) AS STRING) AS distinct_wall,
             typeof(nullif(wall, instant)) AS result_type
      FROM timestamp_correctness WHERE id < 3 ORDER BY id
      """
    Then query result
      | id | scalar_in | array_in | distinct_wall       | result_type   |
      | 0  | true      | true     | NULL                | timestamp_ntz |
      | 1  | false     | false    | 2020-11-01 01:30:00 | timestamp_ntz |
      | 2  | false     | false    | 2020-01-01 00:30:00 | timestamp_ntz |

  Scenario: Array timestamp searches and set operations distinguish overlap instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT array_contains(array(wall), instant) AS contains_instant,
             array_position(array(wall), instant) AS position,
             arrays_overlap(array(wall), array(instant)) AS overlaps,
             size(array_union(array(wall), array(instant))) AS union_size,
             size(array_intersect(array(wall), array(instant))) AS intersect_size,
             size(array_except(array(wall), array(instant))) AS except_size,
             size(array_remove(array(wall), instant)) AS remaining
      FROM timestamp_correctness WHERE id = 1
      """
    Then query result
      | contains_instant | position | overlaps | union_size | intersect_size | except_size | remaining |
      | false            | 0        | false    | 2          | 0              | 1           | 1         |

  Scenario Outline: Array <operation> preserves both timestamp instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT transform(<operation>, x -> unix_micros(x)) AS instants
      FROM timestamp_correctness WHERE id = 1
      """
    Then query result
      | instants                             |
      | [1604219400000000, 1604223000000000] |

    Examples:
      | operation                             |
      | array(wall, instant)                  |
      | array_append(array(wall), instant)    |
      | array_prepend(array(instant), wall)   |
      | array_insert(array(wall), 2, instant) |

  Scenario: Formatted timestamp conversion preserves the stored microseconds
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(to_timestamp_ltz(wall, 'yyyy') AS STRING) AS formatted_ltz,
             CAST(to_timestamp_ntz(instant, 'yyyy') AS STRING) AS formatted_ntz,
             unix_micros(to_timestamp_ltz(instant, CAST(NULL AS STRING))) AS ignored_null_format,
             date_format(wall, 'yyyy-MM-dd HH:mm:ss') AS resolved_format
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | formatted_ltz       | formatted_ntz       | ignored_null_format | resolved_format     |
      | 2020-03-07 18:30:00 | 2020-03-08 10:30:00 | 1583663400000000     | 2020-03-08 03:30:00 |

  Scenario: Calendar days differ from elapsed hours across a DST transition
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(timestampadd(DAY, 1, ts) AS STRING) AS calendar_day,
             CAST(timestampadd(HOUR, 24, ts) AS STRING) AS elapsed_hours,
             CAST(ts + INTERVAL 24 HOURS AS STRING) AS interval_day,
             CAST(ts + make_interval(0, 0, 0, 0, 24) AS STRING) AS calendar_interval_hours,
             CAST(INTERVAL 1 DAY + ts AS STRING) AS reversed_day
      FROM VALUES (TIMESTAMP '2020-03-07 12:00:00') AS t(ts)
      """
    Then query result
      | calendar_day        | elapsed_hours       | interval_day        | calendar_interval_hours | reversed_day        |
      | 2020-03-08 12:00:00 | 2020-03-08 13:00:00 | 2020-03-08 12:00:00 | 2020-03-08 13:00:00     | 2020-03-08 12:00:00 |

  Scenario: Calendar subtraction preserves the wall time across a DST transition
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(instant - INTERVAL 1 DAY AS STRING) AS prior_day,
             unix_micros(instant + INTERVAL 0 MONTH) = unix_micros(instant) AS unchanged
      FROM timestamp_correctness WHERE id = 1
      """
    Then query result
      | prior_day           | unchanged |
      | 2020-10-31 01:30:00 | true      |

  Scenario: Month arithmetic resolves gaps and retains the preferred overlap offset
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(TIMESTAMP '2020-02-08 02:30:00' + INTERVAL 1 MONTH AS STRING) AS gap,
             unix_micros(TIMESTAMP '2020-12-01 01:30:00' - INTERVAL 1 MONTH) AS overlap
      """
    Then query result
      | gap                 | overlap          |
      | 2020-03-08 03:30:00 | 1604223000000000 |

  Scenario: Timestamp arithmetic preserves naive timestamp types
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT timestampadd(DAY, 1, wall) AS next_day,
             typeof(timestampadd(DAY, 1, wall)) AS result_type
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | next_day            | result_type   |
      | 2020-03-09 02:30:00 | timestamp_ntz |

  Scenario: Date arithmetic retains sub-day interval fields
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT DATE '2020-03-08' + INTERVAL 12 HOURS AS noon,
             DATE '2020-03-08' - INTERVAL 1 HOUR AS prior_hour,
             DATE '2020-03-08' + INTERVAL 1 DAY AS next_date,
             typeof(DATE '2020-03-08' + INTERVAL 1 DAY) AS date_type
      """
    Then query result
      | noon                | prior_hour          | next_date  | date_type |
      | 2020-03-08 13:00:00 | 2020-03-07 23:00:00 | 2020-03-09 | date      |

  Scenario: Timestamp differences count complete local units after resolving naive inputs
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT timestampdiff(DAY, wall, wall + INTERVAL 1 DAY) AS days,
             timestampdiff(HOUR, wall, wall + INTERVAL 1 DAY) AS hours,
             timestampdiff(MONTH, wall, wall + INTERVAL 1 MONTH) AS months,
             timestampdiff(DAY, instant, instant + INTERVAL 23 HOURS) AS partial_day,
             timestampdiff(MICROSECOND, instant, instant + INTERVAL 999 MICROSECONDS) AS micros,
             timestampdiff(MILLISECOND, instant, instant + INTERVAL 999 MICROSECONDS) AS millis,
             typeof(datediff(wall, DATE '2020-03-08')) AS datediff_type
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | days | hours | months | partial_day | micros | millis | datediff_type |
      | 0    | 23    | 0      | 0           | 999    | 0      | int           |

  Scenario: Timestamp subtraction compares session-local times
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT instant - wall AS mixed_difference,
             typeof(instant - DATE '2020-01-01') AS result_type,
             TIMESTAMP '2020-03-08 03:30:00' - TIMESTAMP '2020-03-08 01:30:00' AS gap_difference
      FROM timestamp_correctness WHERE id = 2
      """
    Then query result
      | mixed_difference                  | result_type            | gap_difference                  |
      | INTERVAL '-0 08:00:00' DAY TO SECOND | interval day to second | INTERVAL '0 02:00:00' DAY TO SECOND |

  Scenario: Month differences use elapsed seconds since local midnight
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT months_between(TIMESTAMP_NTZ '2020-03-08 12:00:00', TIMESTAMP_NTZ '2020-03-07 12:00:00') AS months
      """
    Then query result
      | months     |
      | 0.03091398 |

  Scenario: Integer timestamp addition keeps microseconds beyond floating point precision
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, unix_micros(timestampadd(MICROSECOND, 9007199254740993L + id, instant)) - unix_micros(instant) AS delta
      FROM timestamp_correctness WHERE id < 4
      ORDER BY id
      """
    Then query result
      | id | delta            |
      | 0  | 9007199254740993 |
      | 1  | 9007199254740994 |
      | 2  | 9007199254740995 |
      | 3  | 9007199254740996 |

  Scenario: Numeric timestamp functions keep seconds and fractional microseconds
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, unix_micros(to_timestamp(id - 1)) AS integer_seconds,
             unix_micros(timestamp_seconds(CAST(id AS DOUBLE) - 0.5D)) AS fractional_seconds,
             unix_micros(timestamp_seconds(CAST(9007199254.740991 AS DECIMAL(20,6)) + CAST(id AS DECIMAL(20,6)) / 1000000)) AS precise_micros
      FROM timestamp_correctness WHERE id < 3 ORDER BY id
      """
    Then query result
      | id | integer_seconds | fractional_seconds | precise_micros   |
      | 0  | -1000000       | -500000            | 9007199254740991 |
      | 1  | 0              | 500000             | 9007199254740992 |
      | 2  | 1000000        | 1500000            | 9007199254740993 |

  Scenario: Timestamp casts to integers floor negative fractional seconds
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(instant AS BIGINT) AS seconds, TRY_CAST(instant AS INT) AS int_seconds
      FROM timestamp_correctness WHERE id = 3
      """
    Then query result
      | seconds | int_seconds |
      | -1      | -1          |

  Scenario Outline: Naive timestamps cannot be explicitly cast to numeric values
    When query
      """
      SELECT <cast>(wall AS <type>) FROM timestamp_correctness
      """
    Then query error (?i)cannot cast|cannot resolve|data type

    Examples:
      | cast     | type   |
      | CAST     | BIGINT |
      | TRY_CAST | DOUBLE |

  Scenario: Stored nested instants format in the session timezone
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(instants AS STRING) AS array_value,
             CAST(instant_struct AS STRING) AS struct_value,
             CAST(instant_map AS STRING) AS map_value
      FROM timestamp_correctness WHERE id = 2
      """
    Then query result
      | array_value           | struct_value          | map_value                   |
      | [2019-12-31 16:30:00] | {2019-12-31 16:30:00} | {ts -> 2019-12-31 16:30:00} |

  Scenario Outline: Nested timestamp <expression> resolves a DST gap
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(<expression> AS STRING) AS value, typeof(<expression>) AS result_type
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | value               | result_type |
      | 2020-03-08 03:30:00 | timestamp   |

    Examples:
      | expression                                                                 |
      | CAST(array(wall) AS ARRAY<TIMESTAMP_LTZ>)[0]                                |
      | CAST(named_struct('ts', wall) AS STRUCT<ts: TIMESTAMP_LTZ>).ts              |
      | CAST(named_struct('old', wall) AS STRUCT<ts: TIMESTAMP_LTZ>).ts             |
      | coalesce(named_struct('ts', wall), named_struct('ts', instant)).ts          |

  Scenario: Nested timestamp comparisons use the same conversion as scalar comparisons
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, array(wall) = array(instant) AS array_equal,
             named_struct('ts', wall) = named_struct('ts', instant) AS struct_equal
      FROM timestamp_correctness WHERE id < 3 ORDER BY id
      """
    Then query result
      | id | array_equal | struct_equal |
      | 0  | true        | true         |
      | 1  | false       | false        |
      | 2  | false       | false        |

  Scenario: Union converts naive timestamps before combining them with instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT side, unix_micros(ts) AS micros FROM (
        SELECT 0 AS side, wall AS ts FROM timestamp_correctness WHERE id = 1
        UNION ALL
        SELECT 1 AS side, instant AS ts FROM timestamp_correctness WHERE id = 1
      ) ORDER BY side
      """
    Then query result
      | side | micros           |
      | 0    | 1604219400000000 |
      | 1    | 1604223000000000 |

  Scenario: Timestamp set operations compare converted instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT 'except' AS operation, count(*) AS n FROM (
        SELECT wall AS ts FROM timestamp_correctness WHERE id = 1
        EXCEPT
        SELECT instant AS ts FROM timestamp_correctness WHERE id = 1
      )
      UNION ALL
      SELECT 'intersect', count(*) FROM (
        SELECT wall AS ts FROM timestamp_correctness WHERE id = 0
        INTERSECT
        SELECT instant AS ts FROM timestamp_correctness WHERE id = 0
      )
      ORDER BY operation
      """
    Then query result
      | operation | n |
      | except    | 1 |
      | intersect | 1 |

  Scenario: Join using compares timestamps in their common instant type
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, typeof(ts) AS visible_key_type
      FROM (SELECT id, wall AS ts FROM timestamp_correctness WHERE id < 3) a
      JOIN (SELECT instant AS ts FROM timestamp_correctness WHERE id < 3) b USING (ts)
      ORDER BY id
      """
    Then query result
      | id | visible_key_type |
      | 0  | timestamp_ntz    |

  Scenario Outline: Insert converts <source> to <target> using the current session timezone
    Given variable destination for temporary directory timestamp_typed_insert
    And final statement
      """
      DROP TABLE IF EXISTS timestamp_typed_insert
      """
    And statement template
      """
      CREATE TABLE timestamp_typed_insert (ts <target>) USING parquet LOCATION {{ destination.sql }}
      """
    And config spark.sql.session.timeZone = America/Los_Angeles
    And statement
      """
      INSERT INTO timestamp_typed_insert SELECT <source> FROM timestamp_correctness WHERE id = <id>
      """
    When query
      """
      SELECT CAST(ts AS STRING) AS value FROM timestamp_typed_insert
      """
    Then query result
      | value   |
      | <value> |

    Examples:
      | source  | target        | id | value               |
      | wall    | TIMESTAMP_LTZ | 0  | 2020-03-08 03:30:00 |
      | instant | TIMESTAMP_NTZ | 2  | 2019-12-31 16:30:00 |

  Scenario: Date comparison resolves a skipped local date
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT DATE '2011-12-30' = TIMESTAMP '2011-12-31 00:00:00' AS same_instant
      """
    Then query result
      | same_instant |
      | true         |

  Scenario: JSON timestamp defaults distinguish instants from naive timestamps
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT to_json(named_struct('ts', instant)) AS instant_json,
             to_json(named_struct('ts', wall)) AS naive_json
      FROM timestamp_correctness WHERE id = 3
      """
    Then query result
      | instant_json                           | naive_json                       |
      | {"ts":"1969-12-31T15:59:59.999-08:00"} | {"ts":"1969-12-31T23:59:59.999"} |

  Scenario: JSON and CSV preserve historical IANA timezone offsets
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT to_json(named_struct('ts', TIMESTAMP '1900-01-01 00:00:00.123456Z')) AS json_value,
             to_csv(named_struct('ts', TIMESTAMP '1900-01-01 00:00:00.123456Z')) AS csv_value
      """
    Then query result
      | json_value                                | csv_value                           |
      | {"ts":"1899-12-31T12:33:04.123-11:26:56"} | 1899-12-31T12:33:04.123-11:26:56    |

  Scenario: JSON parsing leaves naive timestamp fields in wall-clock time
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT from_json('{"ts":"2020-03-08 02:30:00"}', 'ts TIMESTAMP_NTZ').ts AS naive,
             from_json('{"ts":"2020-03-08 02:30:00"}', 'ts TIMESTAMP_LTZ').ts AS instant,
             from_json('[{"ts":"2020-11-01 01:30:00"}]', 'ARRAY<STRUCT<ts: TIMESTAMP_NTZ>>')[0].ts AS nested_naive
      """
    Then query result
      | naive               | instant             | nested_naive        |
      | 2020-03-08 02:30:00 | 2020-03-08 03:30:00 | 2020-11-01 01:30:00 |

  Scenario: CSV parsing ignores the timezone option for naive timestamps
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT from_csv('2020-03-08 02:30:00', 'ts TIMESTAMP_NTZ', map('timeZone', 'America/Los_Angeles')).ts AS naive,
             from_csv('2020-03-08 02:30:00', 'ts TIMESTAMP_LTZ', map('timeZone', 'America/Los_Angeles')).ts AS instant
      """
    Then query result
      | naive               | instant             |
      | 2020-03-08 02:30:00 | 2020-03-08 03:30:00 |

  Scenario: Timestamp to small integer casts return null on overflow without ANSI
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(TIMESTAMP '1970-01-01 00:02:08Z' AS TINYINT) AS byte_overflow,
             CAST(TIMESTAMP '1970-01-01 09:06:08Z' AS SMALLINT) AS short_overflow,
             CAST(TIMESTAMP '2038-01-19 03:14:08Z' AS INT) AS int_overflow,
             CAST(timestamp_micros(-1) AS TINYINT) AS negative_fraction
      """
    Then query result
      | byte_overflow | short_overflow | int_overflow | negative_fraction |
      | NULL          | NULL           | NULL         | -1                |

  Scenario: Try cast returns null on timestamp to integer overflow with ANSI enabled
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT TRY_CAST(TIMESTAMP '2038-01-19 03:14:08Z' AS INT) AS overflow
      """
    Then query result
      | overflow |
      | NULL     |

  Scenario: UTC conversions and differences preserve the full microsecond domain
    When query
      """
      SELECT unix_micros(CAST(CAST(timestamp_micros(CAST('-9223372036854775808' AS BIGINT)) AS TIMESTAMP_NTZ) AS TIMESTAMP_LTZ)) AS minimum,
             unix_micros(CAST(CAST(timestamp_micros(9223372036854775807L) AS TIMESTAMP_NTZ) AS TIMESTAMP_LTZ)) AS maximum,
             timestampdiff(MICROSECOND, timestamp_micros(CAST('-9223372036854775808' AS BIGINT)), timestamp_micros(-9223372036854775807L)) AS difference
      """
    Then query result
      | minimum              | maximum             | difference |
      | -9223372036854775808 | 9223372036854775807 | 1          |

  Scenario: Map casts localize timestamp keys and values
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(CAST(map('ts', wall) AS MAP<STRING, TIMESTAMP_LTZ>) AS STRING) AS values,
             CAST(CAST(map(wall, 1) AS MAP<TIMESTAMP_LTZ, INT>) AS STRING) AS keys
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | values                       | keys                       |
      | {ts -> 2020-03-08 03:30:00} | {2020-03-08 03:30:00 -> 1} |

  Scenario: Map casts preserve entries when daylight saving conversion makes keys equal
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      WITH converted AS (
        SELECT CAST(map(
          TIMESTAMP_NTZ '2020-03-08 02:30:00', 1,
          TIMESTAMP_NTZ '2020-03-08 03:30:00', 2
        ) AS MAP<TIMESTAMP_LTZ, INT>) AS m
      )
      SELECT size(m) AS entries, map_values(m) AS values,
             map_keys(m)[0] = map_keys(m)[1] AS equal_keys
      FROM converted
      """
    Then query result
      | entries | values | equal_keys |
      | 2       | [1, 2] | true       |

  Scenario: Conditional maps and structs coerce timestamps alongside numeric fields
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id,
             CAST(CASE WHEN id = 0 THEN map('ts', wall) ELSE map('ts', instant) END AS STRING) AS m,
             CAST(CASE WHEN id = 0 THEN named_struct('ts', wall, 'n', 1) ELSE named_struct('ts', instant, 'n', 1L) END AS STRING) AS s
      FROM timestamp_correctness WHERE id IN (0, 2)
      ORDER BY id
      """
    Then query result
      | id | m                            | s                        |
      | 0  | {ts -> 2020-03-08 03:30:00} | {2020-03-08 03:30:00, 1} |
      | 2  | {ts -> 2019-12-31 16:30:00} | {2019-12-31 16:30:00, 1} |

  Scenario: Struct casts convert timestamps alongside numeric fields and renamed fields
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(CAST(named_struct('a', wall, 'b', 1) AS STRUCT<a: TIMESTAMP_LTZ, b: BIGINT>) AS STRING) AS same_names,
             CAST(CAST(named_struct('a', wall) AS STRUCT<b: TIMESTAMP_LTZ>) AS STRING) AS renamed
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | same_names               | renamed               |
      | {2020-03-08 03:30:00, 1} | {2020-03-08 03:30:00} |

  Scenario: Union coerces timestamp map values
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(m AS STRING) AS value FROM (
        SELECT map('ts', wall) AS m FROM timestamp_correctness WHERE id = 0
        UNION ALL
        SELECT map('ts', instant) AS m FROM timestamp_correctness WHERE id = 0
      )
      """
    Then query result
      | value                        |
      | {ts -> 2020-03-08 03:30:00} |
      | {ts -> 2020-03-08 03:30:00} |

  Scenario: Map concatenation and key lookup use timestamp common types
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(map_concat(map('a', wall), map('b', instant)) AS STRING) AS combined,
             map_contains_key(map(wall, 1), instant) AS found
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | combined                                             | found |
      | {a -> 2020-03-08 03:30:00, b -> 2020-03-08 03:30:00} | true  |

  Scenario: Nested timestamp coercion preserves null containers
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(CAST(NULL AS MAP<TIMESTAMP_NTZ, TIMESTAMP_NTZ>) AS MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>) IS NULL AS null_map,
             CAST(CAST(NULL AS ARRAY<STRUCT<ts: TIMESTAMP_NTZ>>) AS ARRAY<STRUCT<ts: TIMESTAMP_LTZ>>) IS NULL AS null_array,
             CAST(CAST(NULL AS STRUCT<ts: TIMESTAMP_NTZ>) AS STRUCT<ts: TIMESTAMP_LTZ>) IS NULL AS null_struct
      """
    Then query result
      | null_map | null_array | null_struct |
      | true     | true       | true        |

  Scenario: Wide UTC timestamp differences do not overflow intermediate microseconds
    When query
      """
      WITH bounds AS (
        SELECT timestamp_micros(CAST('-9223372036854775808' AS BIGINT)) AS lo,
               timestamp_micros(9223372036854775807L) AS hi
      )
      SELECT timestampdiff(MILLISECOND, lo, hi) AS millis,
             timestampdiff(SECOND, lo, hi) AS seconds,
             timestampdiff(DAY, lo, hi) AS days,
             timestampdiff(DAY, hi, lo) AS reverse_days
      FROM bounds
      """
    Then query result
      | millis            | seconds        | days      | reverse_days |
      | 18446744073709551 | 18446744073709 | 213503982 | -213503982  |

  Scenario: Timestamp conversion recurses through arrays of maps and structs
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(CAST(
        array(map('ts', named_struct('value', wall)))
        AS ARRAY<MAP<STRING, STRUCT<value: TIMESTAMP_LTZ>>>
      ) AS STRING) AS value
      FROM timestamp_correctness WHERE id = 0
      """
    Then query result
      | value                            |
      | [{ts -> {2020-03-08 03:30:00}}] |
