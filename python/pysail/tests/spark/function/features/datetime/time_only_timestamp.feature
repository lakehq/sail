Feature: Time-only strings as timestamps
  Time-only LTZ timestamps use today's date in the input timezone, or the session timezone
  when none is supplied. NTZ timestamps require a date.

  Background:
    Given config spark.sql.session.timeZone = UTC
    And config spark.sql.timestampType = TIMESTAMP_LTZ

  Scenario Outline: Ibis time delta under ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT DATEDIFF(HOUR, CAST('01:58:00' AS TIMESTAMP),
                           CAST('23:59:59' AS TIMESTAMP)) AS result
      """
    Then query result
      | result |
      | 22     |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Time-only timestamp uses the session date in <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT CAST('01:58:00' AS TIMESTAMP_LTZ) = expected AS cast_matches,
             to_timestamp('01:58:00') = expected AS function_matches,
             try_to_timestamp('01:58:00') = expected AS try_matches,
             to_timestamp_ltz('01:58:00') = expected AS ltz_matches
      FROM (SELECT CAST(concat(substring(CAST(current_timestamp() AS STRING), 1, 10),
                               ' 01:58:00') AS TIMESTAMP_LTZ) AS expected)
      """
    Then query result
      | cast_matches | function_matches | try_matches | ltz_matches |
      | true         | true             | true        | true        |

    Examples:
      | zone                |
      | UTC                 |
      | America/Los_Angeles |
      | +14:00              |
      | -12:00              |

  Scenario Outline: Time-only timestamp syntax <input>
    When query
      """
      SELECT CAST('<input>' AS TIMESTAMP_LTZ) =
             CAST(concat(current_date(), ' <time>') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |

    Examples:
      | input                | time            |
      | 1:2                  | 01:02:00        |
      | 1:2:3                | 01:02:03        |
      | T1                   | 01:00:00        |
      | T01:58               | 01:58:00        |
      | T01:58:00            | 01:58:00        |
      | 01:58:00.123456789    | 01:58:00.123456 |
      | 00:00:00             | 00:00:00        |
      | 23:59:59.999999       | 23:59:59.999999 |

  Scenario Outline: Explicit timezone determines the date for <zone>
    When query
      """
      SELECT CAST('01:58:00 <zone>' AS TIMESTAMP_LTZ) =
             CAST(concat(<date_expression>, ' 01:58:00 <zone>') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |

    Examples:
      | zone                | date_expression                                                                  |
      | UTC                 | current_date()                                                                   |
      | +14:00              | date_format(current_timestamp() + INTERVAL 14 HOURS, 'yyyy-MM-dd')               |
      | -12:00              | date_format(current_timestamp() - INTERVAL 12 HOURS, 'yyyy-MM-dd')               |
      | America/Los_Angeles | CAST(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles') AS DATE)    |

  Scenario: Time-only timestamp columns preserve nulls and full timestamps
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(value AS TIMESTAMP_LTZ) <=> CAST(expected AS TIMESTAMP_LTZ) AS cast_matches,
             to_timestamp(value) <=> CAST(expected AS TIMESTAMP_LTZ) AS function_matches,
             try_to_timestamp(value) <=> CAST(expected AS TIMESTAMP_LTZ) AS try_matches
      FROM VALUES
        (1, '01:58:00', concat(current_date(), ' 01:58:00')),
        (2, 'T23:59:59', concat(current_date(), ' 23:59:59')),
        (3, '2000-02-29 12:34:56.123456', '2000-02-29 12:34:56.123456'),
        (4, '24:00:00', NULL),
        (5, CAST(NULL AS STRING), NULL) AS t(id, value, expected)
      ORDER BY id
      """
    Then query result
      | id | cast_matches | function_matches | try_matches |
      | 1  | true         | true             | true        |
      | 2  | true         | true             | true        |
      | 3  | true         | true             | true        |
      | 4  | true         | true             | true        |
      | 5  | true         | true             | true        |

  Scenario Outline: Malformed time-only timestamps return null without ANSI for <input>
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST('<input>' AS TIMESTAMP_LTZ) AS cast_result,
             to_timestamp('<input>') AS function_result,
             try_to_timestamp('<input>') AS try_result
      """
    Then query result
      | cast_result | function_result | try_result |
      | NULL        | NULL            | NULL       |

    Examples:
      | input       |
      | 24:00:00    |
      | 12:60:00    |
      | 23:59:60    |
      | 001:00:00   |
      | 12:34.5     |
      | 12:34Z      |
      | T           |
      | -01:58:00   |

  Scenario Outline: ANSI errors for invalid time-only timestamps through <expression>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> AS result
      """
    Then query error .*

    Examples:
      | expression                           |
      | CAST('24:00:00' AS TIMESTAMP_LTZ)     |
      | to_timestamp('24:00:00')             |
      | CAST('01:58:00' AS TIMESTAMP_NTZ)     |
      | to_timestamp_ntz('01:58:00')         |

  Scenario: Try conversion remains safe under ANSI
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT try_to_timestamp('24:00:00') AS function_result,
             try_cast('01:58:00' AS TIMESTAMP_NTZ) AS ntz_result
      """
    Then query result
      | function_result | ntz_result |
      | NULL            | NULL       |

  Scenario Outline: NTZ rejects time-only strings with default type <timestamp_type>
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.timestampType = <timestamp_type>
    When query
      """
      SELECT CAST('01:58:00' AS TIMESTAMP_NTZ) AS cast_result,
             to_timestamp_ntz('01:58:00') AS function_result
      """
    Then query result
      | cast_result | function_result |
      | NULL        | NULL            |

    Examples:
      | timestamp_type |
      | TIMESTAMP_LTZ  |
      | TIMESTAMP_NTZ  |

  Scenario: Default NTZ functions continue to reject time-only strings
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.timestampType = TIMESTAMP_NTZ
    When query
      """
      SELECT CAST('01:58:00' AS TIMESTAMP) AS cast_result,
             to_timestamp('01:58:00') AS function_result,
             try_to_timestamp('01:58:00') AS try_result
      """
    Then query result
      | cast_result | function_result | try_result |
      | NULL        | NULL            | NULL       |

  Scenario: Explicit time patterns retain their epoch date
    When query
      """
      SELECT to_timestamp('01:58', 'HH:mm') AS ltz_result,
             to_timestamp_ntz('01:58', 'HH:mm') AS ntz_result,
             try_to_timestamp('01:58', 'HH:mm') AS try_result
      """
    Then query result
      | ltz_result          | ntz_result          | try_result          |
      | 1970-01-01 01:58:00 | 1970-01-01 01:58:00 | 1970-01-01 01:58:00 |

  Scenario Outline: Time-only timestamp preserves Spark fraction and whitespace syntax for <input>
    When query
      """
      SELECT CAST(<input> AS TIMESTAMP_LTZ) =
             CAST(concat(current_date(), ' <time>') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |

    Examples:
      | input                          | time            |
      | '01:58:00.12345678901234567890' | 01:58:00.123456 |
      | '01:58:00.'                    | 01:58:00        |
      | ' 01:58:00 '                   | 01:58:00        |

  Scenario Outline: Time-only timezone spelling <suffix>
    When query
      """
      SELECT CAST('01:58:00<suffix>' AS TIMESTAMP_LTZ) =
             CAST(concat(<date_expression>, ' 01:58:00 <zone>') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |

    Examples:
      | suffix     | zone   | date_expression                                                       |
      | Z          | UTC    | current_date()                                                        |
      | UTC+05:30  | +05:30 | date_format(current_timestamp() + INTERVAL 330 MINUTES, 'yyyy-MM-dd') |
      | UTC-12:00  | -12:00 | date_format(current_timestamp() - INTERVAL 12 HOURS, 'yyyy-MM-dd')    |
      | +18:00     | +18:00 | date_format(current_timestamp() + INTERVAL 18 HOURS, 'yyyy-MM-dd')    |

  Scenario Outline: Time-only timestamp rejects invalid marker or offset <input>
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST('<input>' AS TIMESTAMP_LTZ) AS cast_result,
             to_timestamp('<input>') AS function_result,
             try_to_timestamp('<input>') AS try_result
      """
    Then query result
      | cast_result | function_result | try_result |
      | NULL        | NULL            | NULL       |

    Examples:
      | input             |
      | t01:58:00         |
      | 01:58:00z         |
      | 01:58:00+01:60    |
      | 01:58:00+19:00    |
      | 01:58:00+18:01    |

  # TODO: Resolve Java short timezone aliases such as PST before using Arrow's timezone parser.
  @sail-bug
  Scenario: Time-only timestamp with a Java timezone alias uses the aliased region date
    When query
      """
      SELECT CAST('01:58:00 PST' AS TIMESTAMP_LTZ) =
             CAST(concat(CAST(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles') AS DATE),
                         ' 01:58:00 America/Los_Angeles') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |

  Scenario: Timestamp function and ANSI coalesce parse time-only strings
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT timestamp('01:58:00') = expected AS function_matches,
             coalesce(CAST(NULL AS TIMESTAMP_LTZ), '01:58:00') = expected AS coalesce_matches
      FROM (SELECT CAST(concat(current_date(), ' 01:58:00') AS TIMESTAMP_LTZ) AS expected)
      """
    Then query result
      | function_matches | coalesce_matches |
      | true             | true             |

  Scenario: Day-time interval arithmetic parses time-only strings
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT '01:58:00' + INTERVAL 1 DAY =
             CAST(concat(date_add(current_date(), 1), ' 01:58:00') AS TIMESTAMP_LTZ) AS result
      """
    Then query result
      | result |
      | true   |
