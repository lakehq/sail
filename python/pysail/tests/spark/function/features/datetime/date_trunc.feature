Feature: DATE_TRUNC output type

  Rule: date_trunc always returns TIMESTAMP

    Scenario: date_trunc on a timestamp column returns timestamp
      When query
        """
        WITH t(ts) AS (VALUES (TIMESTAMP '2026-02-02 00:00:00 UTC'))
        SELECT date_trunc('YEAR', ts) AS result FROM t
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # `date_trunc` is declared to return TIMESTAMP regardless of the input type, so a
    # TIMESTAMP_NTZ argument is converted rather than preserved.
    @function(nullability)
    Scenario Outline: date_trunc on timestamp_ntz returns timestamp: <case>
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case    | query                                                                                                  |
        | column  | WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00')) SELECT date_trunc('YEAR', ts) AS result FROM t |
        | literal | SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result                                |

    Scenario: date_trunc YEAR on timestamp values
      When query
        """
        SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
        """
        SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-01 00:00:00 |

    Scenario: date_trunc DAY on timestamp with America/Los_Angeles timezone
      When query
        """
        SELECT date_trunc('DAY', TIMESTAMP '2026-03-15 02:30:00 America/Los_Angeles') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-15 00:00:00 |

    Scenario: date_trunc HOUR on timestamp with America/New_York timezone
      When query
        """
        SELECT date_trunc('HOUR', TIMESTAMP '2026-03-15 14:45:30 America/New_York') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-15 18:00:00 |

  Rule: date_trunc — the argument may come from a column

    @function(columnargs)
    Scenario: date_trunc with the argument as a literal
      When query
        """
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359') AS result
        """
      Then query result ordered
        | result              |
        | 2015-03-01 00:00:00 |

    @function(columnargs)
    Scenario Outline: Date_trunc: <case>
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                                                   | v1     | v2     | r1                  | r2                  |
        | date_trunc takes argument 1 from a column holding two different values | 'YEAR' | 'MM'   | 2015-01-01 00:00:00 | 2015-03-01 00:00:00 |
        | date_trunc takes argument 1 from a column                              | 'YEAR' | 'YEAR' | 2015-01-01 00:00:00 | 2015-01-01 00:00:00 |
        | date_trunc handles NULL and invalid column units                      | NULL   | 'bad'  | NULL                | NULL                |
        | date_trunc accepts mixed-case column units                            | 'dD'   | 'mOn'  | 2015-03-05 00:00:00 | 2015-03-01 00:00:00 |

    @function(columnargs) @sail-bug
    Scenario: a NULL column unit skips timestamp evaluation
      # FIXME: Scalar UDF arguments are evaluated before the truncation kernel can skip NULL units.
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, date_trunc(
          CASE WHEN id = 0 THEN CAST(NULL AS STRING) ELSE 'DAY' END,
          CAST(CASE WHEN id = 0 THEN raise_error('timestamp-evaluated')
                    ELSE '2024-06-15 12:34:56' END AS TIMESTAMP)) AS result
        FROM range(2)
        """
      Then query result
        | id | result              |
        | 0  | NULL                |
        | 1  | 2024-06-15 00:00:00 |

    Scenario: paired truncation units and timestamps retain the session timezone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT date_trunc(unit, ts) AS result FROM VALUES
          (1, 'DAY', TIMESTAMP '2024-03-10 03:30:00'),
          (2, 'HOUR', TIMESTAMP '2024-03-10 03:30:00'),
          (3, 'WEEK', TIMESTAMP '2024-03-10 03:30:00'),
          (4, 'MONTH', CAST(NULL AS TIMESTAMP)) AS t(id, unit, ts)
        ORDER BY id
        """
      Then query result ordered
        | result              |
        | 2024-03-10 00:00:00 |
        | 2024-03-10 03:00:00 |
        | 2024-03-04 00:00:00 |
        | NULL                |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null timestamp literal yields a timestamp
      When query
        """
        SELECT date_trunc('year', TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null timestamp column yields a timestamp
      When query
        """
        SELECT date_trunc('year', CAST(id AS TIMESTAMP)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT date_trunc('year', c) AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Scalar and column truncation share timestamp boundaries

    @sail-only
    Scenario Outline: native truncation alias <unit> works for scalar and column units
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT date_trunc('<unit>', ts) AS scalar_result, date_trunc(unit, ts) AS column_result
        FROM VALUES (1, '<unit>', TIMESTAMP '2024-06-15 12:34:56'),
                    (2, 'DAY', TIMESTAMP '2024-06-15 12:34:56') AS t(id, unit, ts)
        ORDER BY id
        """
      Then query result ordered
        | scalar_result | column_result       |
        | <result>      | <result>            |
        | <result>      | 2024-06-15 00:00:00 |

      Examples:
        | unit | result              |
        | mins | 2024-06-15 12:34:00 |
        | qtr  | 2024-04-01 00:00:00 |
        | yrs  | 2024-01-01 00:00:00 |

    @sail-only
    Scenario: scalar units preserve native Unicode case folding
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT date_trunc('weeK', TIMESTAMP '2024-02-29 12:34:56') AS result
        """
      Then query result
        | result              |
        | 2024-02-26 00:00:00 |

    Scenario Outline: an invalid scalar unit returns NULL: <unit>
      When query
        """
        SELECT date_trunc(<unit>, TIMESTAMP '2024-02-29 12:34:56') AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | unit                 |
        | 'bad'                |
        | CAST(NULL AS STRING) |

    Scenario Outline: scalar pre-epoch truncation floors to <unit>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc('<unit>', timestamp_micros(-1L))) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | unit        | result       |
        | MICROSECOND | -1           |
        | MILLISECOND | -1000        |
        | SECOND      | -1000000     |
        | MINUTE      | -60000000    |
        | HOUR        | -3600000000  |
        | DAY         | -86400000000 |

    Scenario: millisecond truncation floors timestamp columns and preserves NULL
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT v, unix_micros(date_trunc('MILLISECOND', timestamp_micros(v))) AS result
        FROM VALUES (-1001L), (-1L), (1L), (CAST(NULL AS BIGINT)) AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v     | result |
        | NULL  | NULL   |
        | -1001 | -2000  |
        | -1    | -1000  |
        | 1     | 0      |

    Scenario: dynamic units floor negative timestamps independently
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc(unit, timestamp_micros(value))) AS result
        FROM VALUES (1, 'MILLISECOND', -1L), (2, 'SECOND', -1L),
                    (3, 'MINUTE', -1L), (4, CAST(NULL AS STRING), -1L)
             AS t(id, unit, value)
        ORDER BY id
        """
      Then query result ordered
        | result    |
        | -1000     |
        | -1000000  |
        | -60000000 |
        | NULL      |

    Scenario Outline: scalar <unit> truncation preserves representable timestamp boundaries
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc('<unit>', timestamp_micros(CAST('<low_input>' AS BIGINT)))) AS lo,
               unix_micros(date_trunc('<unit>', timestamp_micros(9223372036854775807L))) AS hi
        """
      Then query result
        | lo           | hi            |
        | <low_result> | <high_result> |

      Examples:
        | unit        | low_input            | low_result           | high_result         |
        | MICROSECOND | -9223372036854775808 | -9223372036854775808 | 9223372036854775807 |
        | MILLISECOND | -9223372036854774999 | -9223372036854775000 | 9223372036854775000 |
        | SECOND      | -9223372036853999999 | -9223372036854000000 | 9223372036854000000 |
        | MINUTE      | -9223372036799999999 | -9223372036800000000 | 9223372036800000000 |

    Scenario Outline: column <unit> truncation preserves representable timestamp boundaries and NULL
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc('<unit>', timestamp_micros(v))) AS result
        FROM VALUES (1, CAST('<low_input>' AS BIGINT)), (2, 9223372036854775807L),
                    (3, CAST(NULL AS BIGINT)) AS t(id, v)
        ORDER BY id
        """
      Then query result ordered
        | result        |
        | <low_result>  |
        | <high_result> |
        | NULL          |

      Examples:
        | unit        | low_input            | low_result           | high_result         |
        | MICROSECOND | -9223372036854775808 | -9223372036854775808 | 9223372036854775807 |
        | MILLISECOND | -9223372036854774999 | -9223372036854775000 | 9223372036854775000 |
        | SECOND      | -9223372036853999999 | -9223372036854000000 | 9223372036854000000 |
        | MINUTE      | -9223372036799999999 | -9223372036800000000 | 9223372036800000000 |

    Scenario Outline: <unit> truncation rejects timestamp underflow for a <shape>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc('<unit>', timestamp_micros(<value>))) AS result
        <source>
        """
      Then query error (?i)(overflow|out of range)

      Examples:
        | unit        | shape  | value                                   | source                                                                            |
        | MILLISECOND | scalar | CAST('-9223372036854775808' AS BIGINT) |                                                                                   |
        | SECOND      | scalar | CAST('-9223372036854775808' AS BIGINT) |                                                                                   |
        | MINUTE      | scalar | CAST('-9223372036854775808' AS BIGINT) |                                                                                   |
        | MILLISECOND | column | v                                       | FROM VALUES (CAST('-9223372036854775808' AS BIGINT)), (0L) AS t(v) |
        | SECOND      | column | v                                       | FROM VALUES (CAST('-9223372036854775808' AS BIGINT)), (0L) AS t(v) |
        | MINUTE      | column | v                                       | FROM VALUES (CAST('-9223372036854775808' AS BIGINT)), (0L) AS t(v) |

  Rule: Microsecond timestamps retain the ordinary date range

    Scenario Outline: scalar calendar truncation accepts year <year>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(date_trunc('YEAR', TIMESTAMP '<year>-06-15 12:34:56') AS STRING) AS result
        """
      Then query result
        | result                 |
        | <year>-01-01 00:00:00 |

      Examples:
        | year |
        | 1000 |
        | 9999 |

    Scenario Outline: microsecond truncation preserves a scalar year <year>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(date_trunc('MICROSECOND', TIMESTAMP '<year>-06-15 12:34:56.123456'))
             = unix_micros(TIMESTAMP '<year>-06-15 12:34:56.123456') AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | year |
        | 1000 |
        | 9999 |

    Scenario: wide timestamp columns share the scalar calendar kernel
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(date_trunc('YEAR', ts) AS STRING) AS year_start,
               unix_micros(date_trunc('MICROSECOND', ts)) = unix_micros(ts) AS unchanged
        FROM VALUES (1, TIMESTAMP '1000-06-15 12:34:56.123456'),
                    (2, TIMESTAMP '9999-06-15 12:34:56.123456'),
                    (3, CAST(NULL AS TIMESTAMP)) AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | year_start          | unchanged |
        | 1000-01-01 00:00:00 | true      |
        | 9999-01-01 00:00:00 | true      |
        | NULL                | NULL      |

  Rule: Minute truncation retains the integer timestamp range

    Scenario: minute truncation preserves named-zone timestamp columns beyond the calendar range
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT v, unix_micros(date_trunc('MINUTE', timestamp_micros(v))) AS result
        FROM VALUES (9000000000000000000L), (9000000000001000000L),
                    (-1L), (CAST(NULL AS BIGINT)) AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v                    | result               |
        | NULL                 | NULL                 |
        | -1                  | -60000000           |
        | 9000000000000000000  | 9000000000000000000  |
        | 9000000000001000000  | 9000000000000000000  |

  Rule: Named-zone truncation respects historical and daylight saving boundaries

    @sail-bug
    Scenario: a historical minute is aligned in local time for scalar input
      # FIXME: The integer MINUTE path ignores historical second-resolution zone offsets.
      # A transition-aware replacement must preserve batch performance and the timestamp range.
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT CAST(date_trunc('MINUTE', TIMESTAMP '1900-01-01 00:00:35Z') AS STRING) AS result,
               unix_micros(date_trunc('MINUTE', TIMESTAMP '1900-01-01 00:00:35Z')) AS micros
        """
      Then query result
        | result              | micros            |
        | 1900-01-01 05:41:00 | -2208988816000000 |

    @sail-bug
    Scenario: historical minute columns include second-resolution zone offsets
      # FIXME: MINUTE retains the same pre-existing UTC-aligned boundary for timestamp columns.
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT unix_micros(date_trunc('MINUTE', ts)) AS result
        FROM VALUES (TIMESTAMP '1900-01-01 00:00:35Z'),
                    (TIMESTAMP '1900-01-01 00:01:35Z') AS t(ts)
        ORDER BY ts
        """
      Then query result ordered
        | result            |
        | -2208988816000000 |
        | -2208988756000000 |

    Scenario: month starts choose the earliest repeated midnight while DAY retains the input offset
      Given config spark.sql.session.timeZone = America/Havana
      When query
        """
        SELECT unix_micros(date_trunc('MONTH', TIMESTAMP '2015-11-15 12:00:00')) AS month_start,
               unix_micros(date_trunc('DAY', TIMESTAMP '2015-11-01 06:30:00Z')) AS day_start
        """
      Then query result
        | month_start     | day_start       |
        | 1446350400000000 | 1446354000000000 |

    Scenario: calendar units use the first valid time when midnight lies inside a gap
      Given config spark.sql.session.timeZone = America/Toronto
      When query
        """
        SELECT CAST(date_trunc('WEEK', TIMESTAMP '1919-03-31 12:00:00') AS STRING) AS week_start,
               CAST(date_trunc('DAY', TIMESTAMP '1919-03-31 12:00:00') AS STRING) AS day_start
        """
      Then query result
        | week_start          | day_start           |
        | 1919-03-31 00:30:00 | 1919-03-31 01:00:00 |

    Scenario Outline: truncation resolves a timezone gap: <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(date_trunc('<unit>', TIMESTAMP '<input>') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | unit | input               | result              |
        | Australia/Lord_Howe | HOUR | 2020-10-04 02:45:00 | 2020-10-04 02:30:00 |
        | America/Sao_Paulo   | DAY  | 2018-11-04 12:00:00 | 2018-11-04 01:00:00 |

  Rule: Invalid units skip timestamp expressions
    # FIXME: Skip timestamp evaluation for invalid units before evaluating scalar UDF arguments.

    @sail-bug
    Scenario: invalid and NULL column units skip a throwing timestamp
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, date_trunc(
          CASE WHEN id = 0 THEN 'invalid' WHEN id = 1 THEN CAST(NULL AS STRING) ELSE 'MM' END,
          CAST(CASE WHEN id < 2 THEN raise_error('timestamp-evaluated')
                    ELSE '2024-06-15 12:34:56' END AS TIMESTAMP)) AS result
        FROM range(3)
        """
      Then query result
        | id | result              |
        | 0  | NULL                |
        | 1  | NULL                |
        | 2  | 2024-06-01 00:00:00 |

    @sail-bug
    Scenario: an invalid literal unit skips a throwing timestamp
      When query
        """
        SELECT date_trunc('invalid', CAST(raise_error('timestamp-evaluated') AS TIMESTAMP)) AS result
        """
      Then query result
        | result |
        | NULL   |
