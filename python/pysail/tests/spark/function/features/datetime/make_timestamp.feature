Feature: make_timestamp_ntz and try_make_timestamp_ntz functions

  Rule: Basic timestamp creation with 6 arguments

    Scenario Outline: Six arguments: <case>
      When query
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | args                            | result                     |
        | create timestamp from date and time components | 2014, 12, 28, 6, 30, 45.887     | 2014-12-28 06:30:45.887    |
        | create timestamp at midnight                   | 2023, 12, 31, 0, 0, 0.0         | 2023-12-31 00:00:00        |
        | create timestamp near end of valid range       | 9999, 12, 31, 23, 58, 59.999999 | 9999-12-31 23:58:59.999999 |
        | sec=60 adds one minute                         | 2024, 6, 15, 14, 30, 60.0       | 2024-06-15 14:31:00        |
        | year zero                                     | 0, 1, 1, 0, 0, 0              | 0000-01-01 00:00:00        |
        | negative year                                 | -1, 1, 1, 0, 0, 0             | -0001-01-01 00:00:00       |
        | expanded year                                 | 10000, 1, 1, 0, 0, 0          | +10000-01-01 00:00:00      |

  Rule: Timestamp creation with date and time arguments

    Scenario Outline: Date and time: <case>
      When query
        """
        SELECT make_timestamp_ntz(DATE <date>, TIME <time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | date         | time              | result                     |
        | combine date and time                            | '2024-03-15' | '14:30:00'        | 2024-03-15 14:30:00        |
        | combine date and time with microsecond precision | '2024-01-01' | '12:34:56.123456' | 2024-01-01 12:34:56.123456 |

  Rule: try_make_timestamp_ntz with valid inputs

    Scenario: valid 6-argument call
      When query
        """
        SELECT try_make_timestamp_ntz(2024, 2, 14, 15, 45, 30.5) AS result
        """
      Then query result
        | result                |
        | 2024-02-14 15:45:30.5 |

    Scenario: valid date and time combination
      When query
        """
        SELECT try_make_timestamp_ntz(DATE '2024-07-04', TIME '18:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-07-04 18:00:00 |

  Rule: try_make_timestamp_ntz with invalid inputs returns NULL

    Scenario Outline: Invalid component: <case>
      When query
        """
        SELECT try_make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case           | args                   |
        | invalid month  | 2024, 13, 1, 0, 0, 0.0 |
        | invalid day    | 2024, 2, 30, 0, 0, 0.0 |
        | invalid hour   | 2024, 1, 1, 24, 0, 0.0 |
        | invalid minute | 2024, 1, 1, 0, 60, 0.0 |
        | invalid second | 2024, 1, 1, 0, 0, 61.0 |
        | fractional 60  | 2024, 1, 1, 0, 0, 60.0000006 |
        | negative month | 2024, -1, 1, 0, 0, 0.0 |
        | negative hour  | 2024, 1, 1, -1, 0, 0.0 |

    Scenario: a negative BIGINT component is invalid rather than an unsigned cast error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_make_timestamp_ntz(2024, CAST(-1 AS BIGINT), 1, 0, 0, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a safe constructor does not suppress a positive component cast overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_make_timestamp_ntz(2024, 4294967296L, 1, 0, 0, 0)
        """
      Then query error (?i)(overflow|can't cast)

  Rule: NULL handling

    Scenario: make_timestamp_ntz with null date
      When query
        """
        SELECT make_timestamp_ntz(CAST(NULL AS DATE), TIME '10:00:00') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_make_timestamp_ntz with null year
      When query
        """
        SELECT try_make_timestamp_ntz(CAST(NULL AS INT), 1, 1, 0, 0, 0.0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario Outline: a NULL year skips an unsigned component with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_timestamp_ntz(CAST(NULL AS INT), 4294967295L, 1, 0, 0, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Multiple rows with mixed valid and invalid inputs

    Scenario: try_make_timestamp_ntz on array of inputs
      When query
        """
        SELECT
          year,
          try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2024, 1, 1, 0, 0, 0.0),
          (2024, 13, 1, 0, 0, 0.0),
          (2024, 6, 15, 12, 30, 45.5)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year, month, day
        """
      Then query result ordered
        | year | result                |
        | 2024 | 2024-01-01 00:00:00   |
        | 2024 | 2024-06-15 12:30:45.5 |
        | 2024 | NULL                  |

  Rule: Per-element NULL propagation with 6 arguments over columns

    Scenario: try_make_timestamp_ntz null second only, rest valid, returns NULL not a valid timestamp
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null second only over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null year over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, 1, 1, 0, 0, 0.0) AS result
        FROM VALUES (2020), (CAST(NULL AS INT))
        AS t(year)
        ORDER BY year NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario Outline: NULL component rows skip unsigned components with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_timestamp_ntz(
          CASE WHEN id = 0 THEN CAST(NULL AS INT) ELSE 2024 END,
          CASE WHEN id = 0 THEN 4294967295L ELSE 1L END,
          1, 0, 0, 0
        ) AS result
        FROM range(2)
        ORDER BY id
        """
      Then query result ordered
        | result              |
        | NULL                |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: try_make_timestamp_ntz any null component over columns returns NULL
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (CAST(NULL AS INT), 1, 1, 0, 0, 0.0),
          (2020, CAST(NULL AS INT), 1, 0, 0, 0.0),
          (2020, 1, CAST(NULL AS INT), 0, 0, 0.0),
          (2020, 1, 1, CAST(NULL AS INT), 0, 0.0),
          (2020, 1, 1, 0, CAST(NULL AS INT), 0.0),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE)),
          (2020, 1, 1, 0, 0, 0.0)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year NULLS FIRST, month NULLS FIRST, day NULLS FIRST, hour NULLS FIRST, min NULLS FIRST, sec NULLS FIRST
        """
      Then query result ordered
        | result              |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | 2020-01-01 00:00:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: non-null components yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: non-null component columns yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable component column stays nullable
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, c) AS result FROM VALUES (CAST(0 AS INT)), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario Outline: nullable seconds conversion stays visible in the schema: <seconds>
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, <seconds>) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | seconds                  |
        | 1D                       |
        | 1F                       |
        | 1L                       |
        | '1'                      |
        | CAST(id AS DOUBLE)       |
        | CAST(1 AS DECIMAL(20, 0)) |
        | CAST(1 AS DECIMAL(17, 7)) |

    Scenario: a non-null decimal seconds value within the target precision stays non-nullable
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, CAST(1 AS DECIMAL(16, 6))) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: string component coercion stays nullable under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp('2024', 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: try_make_timestamp stays nullable for non-null components
      When query
        """
        SELECT try_make_timestamp(2024, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Invalid components in legacy mode

    Scenario: make_timestamp returns NULL for an invalid date when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp(2024, 2, 30, 0, 0, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario Outline: legacy <fn> handles valid, invalid, and NULL component rows
      Given config spark.sql.ansi.enabled = false
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, <fn>(2024, m, d, h, 0, 0) AS result
        FROM VALUES
          (1, 2, 29, 12),
          (2, 2, 30, 0),
          (3, -1, 1, 0),
          (4, 1, 1, -1),
          (5, CAST(NULL AS INT), 1, 0)
        AS t(i, m, d, h)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | 2024-02-29 12:00:00 |
        | 2 | NULL                |
        | 3 | NULL                |
        | 4 | NULL                |
        | 5 | NULL                |

      Examples:
        | fn                 |
        | make_timestamp     |
        | make_timestamp_ltz |
        | make_timestamp_ntz |

    @function(nullability)
    Scenario: legacy make_timestamp stays nullable for non-null literal and column inputs
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 0) AS literal_timestamp,
               make_timestamp(2024, 1, 1, 0, 0, CAST(id AS INT)) AS column_timestamp
        FROM range(3)
        """
      Then query schema
        """
        root
         |-- literal_timestamp: timestamp (nullable = true)
         |-- column_timestamp: timestamp (nullable = true)
        """

    Scenario Outline: legacy <fn> handles malformed component strings per row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, <fn>(y, 1, 1, 0, 0, s) AS result
        FROM VALUES
          (1, '2024', '0.5'),
          (2, 'bad', '0'),
          (3, '2024', 'bad')
        AS t(i, y, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                |
        | 1 | 2024-01-01 00:00:00.5 |
        | 2 | NULL                  |
        | 3 | NULL                  |

      Examples:
        | fn                     |
        | make_timestamp_ntz     |
        | try_make_timestamp_ntz |

  Rule: Floating-point seconds and ANSI errors

    Scenario Outline: numeric seconds round exact half-microseconds up: <seconds>
      When query
        """
        SELECT make_timestamp_ntz(2024, 1, 1, 0, 0, <seconds>) AS result
        """
      Then query result
        | result                    |
        | 2024-01-01 00:00:00.500001 |

      Examples:
        | seconds   |
        | 0.5000005D |
        | 0.5000005  |

    Scenario Outline: floating seconds are rounded and non-finite values become NULL with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT i, make_timestamp_ntz(2024, 1, 1, 0, 0, CAST(s AS DOUBLE)) AS result
        FROM VALUES
          (1, '0.5'), (2, 'NaN'), (3, 'Infinity'), (4, '-Infinity'), (5, CAST(NULL AS STRING)),
          (6, '59.9999996'), (7, '60.0000004'), (8, '-0.0000004')
        AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                |
        | 1 | 2024-01-01 00:00:00.5 |
        | 2 | NULL                  |
        | 3 | NULL                  |
        | 4 | NULL                  |
        | 5 | NULL                  |
        | 6 | 2024-01-01 00:01:00   |
        | 7 | 2024-01-01 00:01:00   |
        | 8 | 2024-01-01 00:00:00   |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: invalid components still raise under ANSI: <args>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp_ntz(<args>)
        """
      Then query error (?i)(invalid|out.of.range|can't cast)

      Examples:
        | args                         |
        | 2024, -1, 1, 0, 0, 0        |
        | 2024, 1, 1, 0, 0, 60.0000006 |

    Scenario Outline: <fn> does not suppress malformed argument coercion under ANSI: <args>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <fn>(<args>)
        """
      Then query error (?i)(CAST_INVALID_INPUT|cast.*bad|bad.*cast|parse.*bad)

      Examples:
        | fn                     | args                    |
        | make_timestamp_ntz     | 'bad', 1, 1, 0, 0, 0    |
        | make_timestamp_ntz     | 2024, 1, 1, 0, 0, 'bad' |
        | try_make_timestamp_ntz | 'bad', 1, 1, 0, 0, 0    |
        | try_make_timestamp_ntz | 2024, 1, 1, 0, 0, 'bad' |

  Rule: String seconds retain decimal precision

    Scenario Outline: <fn> parses string seconds per row with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT i, <fn>(2024, 1, 1, 0, 0, s) AS result
        FROM VALUES
          (1, '5e0'),
          (2, '1e-10000'),
          (3, '  +0.5  '),
          (4, '1.00000049999999999999999999999'),
          (5, '1.00000050000000000000000000001'),
          (6, '-0.0000004999999999999999999999'),
          (7, '59.9999995'),
          (8, '0.5000005'),
          (9, CAST(NULL AS STRING))
        AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                     |
        | 1 | 2024-01-01 00:00:05         |
        | 2 | 2024-01-01 00:00:00         |
        | 3 | 2024-01-01 00:00:00.5       |
        | 4 | 2024-01-01 00:00:01         |
        | 5 | 2024-01-01 00:00:01.000001  |
        | 6 | 2024-01-01 00:00:00         |
        | 7 | 2024-01-01 00:01:00         |
        | 8 | 2024-01-01 00:00:00.500001  |
        | 9 | NULL                       |

      Examples:
        | fn                     | ansi  |
        | make_timestamp_ntz     | true  |
        | make_timestamp_ntz     | false |
        | try_make_timestamp_ntz | true  |
        | try_make_timestamp_ntz | false |

    Scenario Outline: NULL components skip non-finite string seconds with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT i, make_timestamp_ntz(y, 1, 1, 0, 0, s) AS result
        FROM VALUES
          (1, CAST(NULL AS INT), 'NaN'),
          (2, CAST(NULL AS INT), 'Infinity'),
          (3, 2024, '5e0')
        AS t(i, y, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | NULL                |
        | 2 | NULL                |
        | 3 | 2024-01-01 00:00:05 |

      Examples:
        | ansi  |
        | true  |
        | false |

    @sail-only
    Scenario Outline: safe timestamp constructors retain non-finite string inputs with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT try_make_timestamp_ntz(2024, 1, 1, 0, 0, s) AS result
        FROM VALUES ('NaN'), ('nan'), ('+NaN'), ('-NaN'), ('Inf'), ('-inf'),
          ('Infinity'), ('+Infinity'), ('  Infinity  ') AS t(s)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

    @sail-only
    Scenario Outline: timestamp constructors retain zero and underflow with large exponents and ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_timestamp_ntz(2024, 1, 1, 0, 0, s) AS result
        FROM VALUES ('0e10000'), ('0e9999999999999999999999999'),
          ('1e-999999999999999999999999') AS t(s)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Exact numeric seconds and signed component boundaries

    Scenario Outline: numeric second columns preserve rounding boundaries for <type>
      When query
        """
        SELECT i, make_timestamp_ntz(2024, 1, 1, 0, 0, CAST(s AS <type>)) AS result
        FROM VALUES (0, '0.5000004'), (1, '0.5000005'), (2, '0.5000006'),
          (3, '59.9999995'), (4, '-0.0000004'), (5, CAST(NULL AS STRING)) AS t(i, s)
        ORDER BY i
        """
      Then query result ordered
        | i | result                     |
        | 0 | 2024-01-01 00:00:00.5      |
        | 1 | 2024-01-01 00:00:00.500001 |
        | 2 | 2024-01-01 00:00:00.500001 |
        | 3 | 2024-01-01 00:01:00        |
        | 4 | 2024-01-01 00:00:00        |
        | 5 | NULL                       |
      Examples:
        | type          |
        | DOUBLE        |
        | DECIMAL(16,7) |

    Scenario: negative BIGINT components remain invalid in columns
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT i, try_make_timestamp_ntz(2024, 1, 1, h, 0, 0) AS result
        FROM VALUES (0, -1L), (1, -2147483648L), (2, 0L), (3, CAST(NULL AS BIGINT)) AS t(i, h)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 0 | NULL                |
        | 1 | NULL                |
        | 2 | 2024-01-01 00:00:00 |
        | 3 | NULL                |

    Scenario: a safe constructor preserves negative component cast overflow
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_make_timestamp_ntz(2024, -2147483649L, 1, 0, 0, 0)
        """
      Then query error (?i)(overflow|can't cast)
