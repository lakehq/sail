Feature: from_utc_timestamp

  Rule: Type coercion

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `from_utc_timestamp` with coercible input
      When query
        """
        SELECT from_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 07:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 07:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 07:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 07:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 07:30:00 |
        | '2024-06-15 13:30:00+07:00'           | 2024-06-15 07:30:00 |
        | DATE '2024-06-15'                     | 2024-06-14 17:00:00 |

  Rule: Invalid time zone validation

    Scenario: `from_utc_timestamp` validates a constant zone for a runtime-null timestamp
      When query
        """
        SELECT from_utc_timestamp(
          CASE
            WHEN id = 0 THEN CAST(NULL AS TIMESTAMP)
            ELSE TIMESTAMP '2024-01-01 00:00:00'
          END,
          'Not/AZone'
        ) AS result
        FROM range(0, 1, 1, 1)
        """
      Then query error INVALID_TIMEZONE

    @sail-bug
    Scenario: `from_utc_timestamp` skips a dynamic zone for a null timestamp
      When query
        """
        SELECT from_utc_timestamp(source_ts, target_tz) AS result
        FROM VALUES
          (CAST(NULL AS TIMESTAMP), 'Not/AZone')
          AS t(source_ts, target_tz)
        """
      Then query result
        | result |
        | NULL   |

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `from_utc_timestamp` around daylight saving time transition
      When query
        """
        SELECT from_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | result              |
        | '2025-03-09 17:30:00' | 2025-03-09 09:30:00 |
        | '2025-03-09 18:30:00' | 2025-03-09 11:30:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to from_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null column input to from_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT from_utc_timestamp(CAST(id AS STRING), 'Asia/Seoul') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable column input to from_utc_timestamp stays nullable
      When query
        """
        SELECT from_utc_timestamp(c, 'Asia/Seoul') AS result FROM VALUES ('2016-08-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
