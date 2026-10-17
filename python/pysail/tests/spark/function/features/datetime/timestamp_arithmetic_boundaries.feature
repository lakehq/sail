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
