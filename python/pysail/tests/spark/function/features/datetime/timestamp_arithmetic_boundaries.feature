Feature: Timestamp arithmetic rejects overflow and retains supported wide dates

  Background:
    Given config spark.sql.session.timeZone = UTC

  Scenario Outline: Zero additions preserve <type> values in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT id,
             timestampadd(YEAR, 0, ts) <=> ts AS years,
             timestampadd(DAY, 0, ts) <=> ts AS days,
             timestampadd(HOUR, 0, ts) <=> ts AS hours,
             timestampadd(MICROSECOND, 0, ts) <=> ts AS micros,
             (ts + INTERVAL 0 MONTH) <=> ts AS month_interval,
             (ts + INTERVAL 0 SECOND) <=> ts AS second_interval,
             (ts + make_interval(0, 0, 0, 0, 0, 0, 0)) <=> ts AS calendar_interval
      FROM (
        SELECT id, CAST(CASE WHEN id = 0 THEN '2021-11-07 01:30:00'
                            WHEN id = 1 THEN '2020-03-08 02:30:00' END AS <type>) AS ts
        FROM range(3)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | years | days | hours | micros | month_interval | second_interval | calendar_interval |
      | 0  | true  | true | true  | true   | true           | true            | true              |
      | 1  | true  | true | true  | true   | true           | true            | true              |
      | 2  | true  | true | true  | true   | true           | true            | true              |

    Examples:
      | type          | timezone            |
      | TIMESTAMP     | UTC                 |
      | TIMESTAMP_NTZ | UTC                 |
      | TIMESTAMP     | America/Los_Angeles |
      | TIMESTAMP_NTZ | America/Los_Angeles |
      | TIMESTAMP     | Asia/Kathmandu      |
      | TIMESTAMP_NTZ | Asia/Kathmandu      |

  Scenario Outline: Microsecond offsets from the epoch retain the full range in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT id, unix_micros(ltz) AS ltz, CAST(ntz AS STRING) AS ntz,
             typeof(ltz) AS ltz_type, typeof(ntz) AS ntz_type
      FROM (
        SELECT id,
               timestampadd(MICROSECOND, quantity, TIMESTAMP '1970-01-01 00:00:00Z') AS ltz,
               timestampadd(MICROSECOND, quantity, TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ntz
        FROM VALUES (0, CAST('-9223372036854775808' AS BIGINT)), (1, -1L),
                    (2, 0L), (3, 1L), (4, 9223372036854775807L), (5, CAST(NULL AS BIGINT))
             AS t(id, quantity)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | ltz                  | ntz                           | ltz_type  | ntz_type      |
      | 0  | -9223372036854775808 | -290308-12-21 19:59:05.224192 | timestamp | timestamp_ntz |
      | 1  | -1                   | 1969-12-31 23:59:59.999999    | timestamp | timestamp_ntz |
      | 2  | 0                    | 1970-01-01 00:00:00           | timestamp | timestamp_ntz |
      | 3  | 1                    | 1970-01-01 00:00:00.000001    | timestamp | timestamp_ntz |
      | 4  | 9223372036854775807  | +294247-01-10 04:00:54.775807 | timestamp | timestamp_ntz |
      | 5  | NULL                 | NULL                          | timestamp | timestamp_ntz |

    Examples:
      | timezone            |
      | UTC                 |
      | America/Los_Angeles |
      | Asia/Kathmandu      |

  Scenario Outline: Integer epoch constructors reject <function> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT unix_micros(<function>(<value>))
      """
    Then query error (?i)overflow|out of range|cast

    Examples:
      | function          | value                | ansi  |
      | timestamp_seconds | 9223372036855L       | true  |
      | timestamp_seconds | -9223372036855L      | true  |
      | timestamp_millis  | 9223372036854776L    | true  |
      | timestamp_millis  | -9223372036854776L   | true  |
      | timestamp_seconds | 9223372036855L       | false |
      | timestamp_seconds | -9223372036855L      | false |
      | timestamp_millis  | 9223372036854776L    | false |
      | timestamp_millis  | -9223372036854776L   | false |

  Scenario Outline: Integer epoch constructor columns reject <function> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT unix_micros(<function>(id + <value>)) FROM range(1)
      """
    Then query error (?i)overflow|out of range|cast

    Examples:
      | function          | value               | ansi  |
      | timestamp_seconds | 9223372036855L      | true  |
      | timestamp_seconds | -9223372036855L     | true  |
      | timestamp_millis  | 9223372036854776L   | true  |
      | timestamp_millis  | -9223372036854776L  | true  |
      | timestamp_seconds | 9223372036855L      | false |
      | timestamp_seconds | -9223372036855L     | false |
      | timestamp_millis  | 9223372036854776L   | false |
      | timestamp_millis  | -9223372036854776L  | false |

  Scenario Outline: Integer epoch constructors retain <function> boundaries and NULLs with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, unix_micros(<function>(value)) AS micros, typeof(<function>(value)) AS type
      FROM VALUES (0, -<limit>L), (1, -1L), (2, 0L), (3, 1L),
                  (4, <limit>L), (5, CAST(NULL AS BIGINT)) AS t(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | micros         | type      |
      | 0  | -<micros>      | timestamp |
      | 1  | -<factor>      | timestamp |
      | 2  | 0              | timestamp |
      | 3  | <factor>       | timestamp |
      | 4  | <micros>       | timestamp |
      | 5  | NULL           | timestamp |

    Examples:
      | function          | limit            | micros              | factor  | ansi  |
      | timestamp_seconds | 9223372036854    | 9223372036854000000 | 1000000 | true  |
      | timestamp_seconds | 9223372036854    | 9223372036854000000 | 1000000 | false |
      | timestamp_millis  | 9223372036854775 | 9223372036854775000 | 1000    | true  |
      | timestamp_millis  | 9223372036854775 | 9223372036854775000 | 1000    | false |

  Scenario Outline: Integer epoch constructors defer unused <function> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT CASE WHEN false THEN unix_micros(<function>(<value>)) ELSE id END AS constant_case,
             CASE WHEN id = -1 THEN unix_micros(<function>(<value> + 0L)) ELSE id END AS column_case
      FROM range(2)
      ORDER BY constant_case
      """
    Then query result ordered
      | constant_case | column_case |
      | 0             | 0           |
      | 1             | 1           |

    Examples:
      | function          | value              | ansi  |
      | timestamp_seconds | 9223372036855L     | true  |
      | timestamp_seconds | 9223372036855L     | false |
      | timestamp_millis  | 9223372036854776L  | true  |
      | timestamp_millis  | 9223372036854776L  | false |

  Scenario Outline: Integer epoch constructors preserve <type> input nullability
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT timestamp_seconds(CAST(id AS <type>)) AS seconds,
             timestamp_millis(CAST(id AS <type>)) AS millis,
             timestamp_seconds(CASE WHEN id = 0 THEN CAST(id AS <type>) END) AS nullable_seconds,
             timestamp_millis(CASE WHEN id = 0 THEN CAST(id AS <type>) END) AS nullable_millis
      FROM range(2)
      """
    Then query schema
      """
      root
       |-- seconds: timestamp (nullable = false)
       |-- millis: timestamp (nullable = false)
       |-- nullable_seconds: timestamp (nullable = true)
       |-- nullable_millis: timestamp (nullable = true)
      """

    Examples:
      | type     |
      | TINYINT  |
      | SMALLINT |
      | INT      |
      | BIGINT   |

  Scenario Outline: Timestamp arithmetic rejects <expression> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT unix_micros(<expression>) FROM range(1)
      """
    Then query error (?i)overflow|out of range

    Examples:
      | expression                                                                                   | ansi  |
      | timestampadd(SECOND, id + 9223372036855L, TIMESTAMP '1970-01-01')                               | true  |
      | timestampadd(MILLISECOND, id + 9223372036854776L, TIMESTAMP '1970-01-01')                       | true  |
      | timestampadd(MICROSECOND, id + 1L, timestamp_micros(9223372036854775807L))                      | true  |
      | timestampadd(MICROSECOND, id - 1L, timestamp_micros(CAST('-9223372036854775808' AS BIGINT)))    | true  |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 MICROSECOND                            | true  |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 DAY                                    | true  |
      | timestampadd(SECOND, id + 9223372036855L, TIMESTAMP '1970-01-01')                               | false |
      | timestampadd(MILLISECOND, id + 9223372036854776L, TIMESTAMP '1970-01-01')                       | false |
      | timestampadd(MICROSECOND, id + 1L, timestamp_micros(9223372036854775807L))                      | false |
      | timestampadd(MICROSECOND, id - 1L, timestamp_micros(CAST('-9223372036854775808' AS BIGINT)))    | false |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 MICROSECOND                            | false |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 DAY                                    | false |

  Scenario: NULL timestamps suppress quantity overflow within mixed batches
    When query
      """
      SELECT id, unix_micros(timestampadd(SECOND, quantity, ts)) AS result
      FROM (
        SELECT id, CASE WHEN id = 0 THEN 9223372036854775807L ELSE 1L END AS quantity,
               CASE WHEN id = 0 THEN CAST(NULL AS TIMESTAMP)
                    ELSE TIMESTAMP '1970-01-01' END AS ts
        FROM range(0, 2, 1, 1)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | result  |
      | 0  | NULL    |
      | 1  | 1000000 |

  Scenario: Named-zone day arithmetic retains both timestamp extremes in columns
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, unix_micros(ts + INTERVAL 1 DAY) AS added,
             unix_micros(ts - INTERVAL 1 DAY) AS subtracted
      FROM (
        SELECT id, timestamp_micros(CASE WHEN id = 0 THEN -9000000000000000000L
                                         ELSE 9000000000000000000L END) AS ts
        FROM range(0, 2, 1, 1)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | added                | subtracted           |
      | 0  | -8999999913600000000 | -9000000086400000000 |
      | 1  | 9000000086400000000  | 8999999913600000000  |

  Scenario Outline: Checked elapsed arithmetic preserves supported session alias <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT unix_micros(timestampadd(HOUR, id + 1, timestamp_micros(0))) AS result
      FROM range(1)
      """
    Then query result
      | result     |
      | 3600000000 |

    Examples:
      | zone |
      | Z    |
      | UT   |
      | PST  |

  Scenario: Wide named-zone day arithmetic can return to an ordinary summer date
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(timestamp_micros(9000000000000000000L)
                        + make_dt_interval(-104148186)) AS result
      """
    Then query result
      | result           |
      | 1596729600000000 |

  @sail-bug
  Scenario: Calendar days retain recurring DST transitions beyond Chrono's range
    # FIXME: The preexisting elapsed-day fallback does not apply future recurring timezone rules.
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(timestamp_micros(8837530219800000000L) + INTERVAL 1 DAY) AS result
      """
    Then query result
      | result              |
      | 8837530302600000000 |

  Scenario Outline: Scalar <type> subtraction rejects minimum intervals with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <type> '1970-01-02' - make_dt_interval(-106751991, -4, 0, -54.775808)
      """
    Then query error (?is)\A(?!.*(?:panic|cancelled|rst_stream|stream removed)).*(?:interval_arithmetic_overflow|arithmetic overflow)

    Examples:
      | type          | ansi  |
      | DATE          | true  |
      | TIMESTAMP     | true  |
      | TIMESTAMP_NTZ | true  |
      | DATE          | false |
      | TIMESTAMP     | false |
      | TIMESTAMP_NTZ | false |

  Scenario Outline: Column <type> subtraction rejects minimum intervals with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <type> '1970-01-02'
             - make_dt_interval(CAST(id - 106751991 AS INT), -4, 0, -54.775808)
      FROM range(2)
      """
    Then query error (?is)\A(?!.*(?:panic|cancelled|rst_stream|stream removed)).*(?:interval_arithmetic_overflow|arithmetic overflow)

    Examples:
      | type          | ansi  |
      | DATE          | true  |
      | TIMESTAMP     | true  |
      | TIMESTAMP_NTZ | true  |
      | DATE          | false |
      | TIMESTAMP     | false |
      | TIMESTAMP_NTZ | false |

  Scenario Outline: Interval subtraction preserves valid boundaries and NULL with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
             unix_micros(TIMESTAMP '1970-01-01'
                         - make_dt_interval(-106751991, -4, 0, -54.775807)) AS boundary,
             unix_micros(TIMESTAMP '1970-01-01'
                         - CAST(NULL AS INTERVAL DAY TO SECOND)) AS scalar_null,
             unix_micros(TIMESTAMP '1970-01-01' - make_dt_interval(0, 0, 0, seconds)) AS result
      FROM VALUES (0, CAST(NULL AS INT)), (1, -1), (2, 0), (3, 1) AS t(id, seconds)
      ORDER BY id
      """
    Then query result ordered
      | id | boundary            | scalar_null | result   |
      | 0  | 9223372036854775807 | NULL        | NULL     |
      | 1  | 9223372036854775807 | NULL        | 1000000  |
      | 2  | 9223372036854775807 | NULL        | 0        |
      | 3  | 9223372036854775807 | NULL        | -1000000 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario: Negative calendar seconds round before arithmetic
    When query
      """
      SELECT id,
             unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, -0.0000009)) AS scalar,
             unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, -seconds)) AS added,
             unix_micros(TIMESTAMP '1970-01-01' - make_interval(0, 0, 0, 0, 0, 0, seconds)) AS subtracted
      FROM VALUES (0, CAST(NULL AS DECIMAL(8, 7))), (1, 0.0000009), (2, 1.0000009) AS t(id, seconds)
      ORDER BY id
      """
    Then query result ordered
      | id | scalar | added    | subtracted |
      | 0  | -1     | NULL     | NULL       |
      | 1  | -1     | -1       | -1         |
      | 2  | -1     | -1000001 | -1000001   |

  Scenario Outline: Calendar interval construction rounds submicrosecond seconds in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, -0.0000001)) AS added,
             unix_micros(TIMESTAMP '1970-01-01' - make_interval(0, 0, 0, 0, 0, 0, 0.0000001)) AS subtracted,
             unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, 0.0000001)) AS positive
      """
    Then query result
      | added   | subtracted | positive |
      | <epoch> | <epoch>    | <epoch>  |

    Examples:
      | timezone            | epoch       |
      | UTC                 | 0           |
      | America/Los_Angeles | 28800000000 |

  Scenario Outline: Calendar interval seconds round HALF_UP before arithmetic in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT id,
             unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, seconds))
               - unix_micros(TIMESTAMP '1970-01-01') AS added,
             unix_micros(TIMESTAMP '1970-01-01' - make_interval(0, 0, 0, 0, 0, 0, seconds))
               - unix_micros(TIMESTAMP '1970-01-01') AS subtracted,
             unix_micros(TIMESTAMP '1970-01-01' + make_interval(0, 0, 0, 0, 0, 0, CAST(seconds AS DOUBLE)))
               - unix_micros(TIMESTAMP '1970-01-01') AS double_added
      FROM VALUES (0, CAST(NULL AS DECIMAL(8, 7))), (1, -0.0000009), (2, -0.0000005),
                  (3, -0.0000004), (4, 0.0000004), (5, 0.0000005), (6, 0.0000009) AS t(id, seconds)
      ORDER BY id
      """
    Then query result ordered
      | id | added | subtracted | double_added |
      | 0  | NULL  | NULL       | NULL         |
      | 1  | -1    | 1          | -1           |
      | 2  | -1    | 1          | -1           |
      | 3  | 0     | 0          | 0            |
      | 4  | 0     | 0          | 0            |
      | 5  | 1     | -1         | 1            |
      | 6  | 1     | -1         | 1            |

    Examples:
      | timezone            |
      | UTC                 |
      | America/Los_Angeles |

  Scenario Outline: Non-null interval subtraction retains non-null output with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT TIMESTAMP '2020-01-01' - INTERVAL 1 SECOND AS day_time,
             TIMESTAMP '2020-01-01' - INTERVAL 1 MONTH AS year_month,
             TIMESTAMP_NTZ '2020-01-01' - INTERVAL 1 SECOND AS ntz,
             DATE '2020-01-01' - INTERVAL 1 SECOND AS date_time
      """
    Then query schema
      """
      root
       |-- day_time: timestamp (nullable = false)
       |-- year_month: timestamp (nullable = false)
       |-- ntz: timestamp_ntz (nullable = false)
       |-- date_time: timestamp (nullable = false)
      """

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario: Nullable interval subtraction retains nullable output
    When query
      """
      SELECT TIMESTAMP '2020-01-01' - dt AS day_time,
             TIMESTAMP '2020-01-01' - ym AS year_month,
             TIMESTAMP_NTZ '2020-01-01' - dt AS ntz
      FROM VALUES (INTERVAL 1 SECOND, INTERVAL 1 MONTH),
                  (CAST(NULL AS INTERVAL DAY TO SECOND), CAST(NULL AS INTERVAL YEAR TO MONTH)) AS t(dt, ym)
      """
    Then query schema
      """
      root
       |-- day_time: timestamp (nullable = true)
       |-- year_month: timestamp (nullable = true)
       |-- ntz: timestamp_ntz (nullable = true)
      """

  Scenario Outline: NULL <type> rows do not negate minimum intervals with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    # The minimum interval is valid; only negating it would overflow.
    When query
      """
      SELECT id, CAST(ts - iv AS STRING) AS result
      FROM (
        SELECT id,
               CASE WHEN id = 0 THEN CAST(NULL AS <type>)
                    ELSE <type> '1970-01-02' END AS ts,
               CASE WHEN id = 0 THEN make_dt_interval(-106751991, -4, 0, -54.775808)
                    ELSE make_dt_interval(0, 0, 0, 1) END AS iv
        FROM range(0, 2, 1, 1)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | result              |
      | 0  | NULL                |
      | 1  | 1970-01-01 23:59:59 |

    Examples:
      | type          | ansi  |
      | TIMESTAMP     | true  |
      | TIMESTAMP_NTZ | false |
      | DATE          | true  |

  Scenario Outline: Adjacent wide timestamp differences retain microseconds in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT CAST(end_ts - start_ts AS STRING) AS difference,
             timestampdiff(MICROSECOND, start_ts, end_ts) AS micros
      FROM (
        SELECT timestamp_micros(<end>) AS end_ts,
               timestamp_micros(<start>) AS start_ts
      )
      """
    Then query result
      | difference                            | micros   |
      | INTERVAL '<difference>' DAY TO SECOND | <micros> |

    Examples:
      | timezone            | end                                    | start                 | difference         | micros |
      | +01:00              | 9223372036854775807L                   | 9223372036854775806L  | 0 00:00:00.000001  | 1      |
      | -01:00              | CAST('-9223372036854775808' AS BIGINT) | -9223372036854775807L | -0 00:00:00.000001 | -1     |
      | America/Los_Angeles | 9000000000000000000L                   | 8999999999999999999L  | 0 00:00:00.000001  | 1      |

  Scenario Outline: Mixed wide timestamp differences preserve NULL rows in <timezone> with ANSI <ansi>
    Given config spark.sql.session.timeZone = <timezone>
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CAST(end_ts - start_ts AS STRING) AS difference,
             timestampdiff(SECOND, start_ts, end_ts) AS seconds
      FROM (
        SELECT id, timestamp_micros(end_micros) AS end_ts,
               timestamp_micros(start_micros) AS start_ts
        FROM VALUES (0, 9000000000000000000L, 1719835200000000L),
                    (1, CAST(NULL AS BIGINT), 1719835200000000L),
                    (2, 9000000000000000000L, CAST(NULL AS BIGINT))
             AS t(id, end_micros, start_micros)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | difference                                  | seconds       |
      | 0  | INTERVAL '104146761 04:00:00' DAY TO SECOND | 8998280164800 |
      | 1  | NULL                                        | NULL          |
      | 2  | NULL                                        | NULL          |

    Examples:
      | timezone            | ansi  |
      | Etc/GMT+8           | true  |
      | America/Los_Angeles | false |
      | Asia/Kolkata        | true  |

  Scenario: Constant NULL timestamp differences do not evaluate unused errors
    When query
      """
      SELECT CAST(NULL AS TIMESTAMP) - CAST(raise_error('unused') AS TIMESTAMP) AS difference,
             timestampdiff(SECOND, CAST(NULL AS TIMESTAMP),
                           CAST(raise_error('unused') AS TIMESTAMP)) AS seconds
      """
    Then query result
      | difference | seconds |
      | NULL       | NULL    |

  Scenario Outline: Timestamp differences reject overflow with ANSI <ansi>: <expression>
    Given config spark.sql.session.timeZone = UTC
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <expression> AS result
      FROM (
        SELECT timestamp_micros(CAST('-9223372036854775808' AS BIGINT)) AS lo,
               timestamp_micros(9223372036854775807L) AS hi
      )
      """
    Then query error (?i)overflow

    Examples:
      | expression                         | ansi  |
      | hi - lo                            | true  |
      | hi - lo                            | false |
      | timestampdiff(MICROSECOND, lo, hi) | true  |
      | timestampdiff(MICROSECOND, lo, hi) | false |
