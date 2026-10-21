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

    # Spark's generated execution parses constant zones before checking the timestamp;
    # higher-order zones and interpreted execution check NULL first. Sail deliberately
    # checks NULL before parsing zones, while non-NULL timestamps still validate their zones.

    Scenario: `from_utc_timestamp` skips a constant zone for a runtime-null timestamp
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
      Then query result unless Spark raises INVALID_TIMEZONE
        | result |
        | NULL   |

    Scenario: `from_utc_timestamp` skips a query-time constant zone for a null timestamp
      When query
        """
        SELECT from_utc_timestamp(
          CASE WHEN id = 0 THEN CAST(NULL AS TIMESTAMP) ELSE TIMESTAMP '2024-01-01' END,
          concat('Not/AZone', substring(CAST(current_timestamp() AS STRING), 1, 0))
        ) AS result FROM range(1)
        """
      Then query result unless Spark raises INVALID_TIMEZONE
        | result |
        | NULL   |

    Scenario: `from_utc_timestamp` skips a constant zone containing convert_timezone for a null timestamp
      When query
        """
        SELECT from_utc_timestamp(
          CASE WHEN id = 0 THEN CAST(NULL AS TIMESTAMP) ELSE TIMESTAMP '2024-01-01' END,
          concat('Not/AZone', substring(
            CAST(convert_timezone('UTC', 'UTC', TIMESTAMP_NTZ '2024-01-01') AS STRING),
            1, 0
          ))
        ) AS result FROM range(1)
        """
      Then query result unless Spark raises INVALID_TIMEZONE
        | result |
        | NULL   |

    Scenario: `from_utc_timestamp` skips a higher-order zone for a null timestamp
      # Spark treats higher-order expressions as non-foldable, even with constant inputs.
      When query
        """
        SELECT from_utc_timestamp(
          CASE WHEN id = 0 THEN CAST(NULL AS TIMESTAMP) ELSE TIMESTAMP '2024-01-01' END,
          element_at(transform(array('Not/AZone'), x -> x), 1)
        ) AS result FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

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

    Scenario: dynamic zones are validated only for non-NULL timestamps in a batch
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT i, from_utc_timestamp(source_ts, target_tz) AS result
        FROM VALUES
          (1, CAST(NULL AS TIMESTAMP), 'Not/AZone'),
          (2, TIMESTAMP '2024-06-15 12:30:00', 'America/Los_Angeles'),
          (3, CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING)),
          (4, TIMESTAMP '2024-06-15 12:30:00', CAST(NULL AS STRING))
          AS t(i, source_ts, target_tz)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | NULL                |
        | 2 | 2024-06-15 05:30:00 |
        | 3 | NULL                |
        | 4 | NULL                |

    Scenario: a dynamic invalid zone still raises on a non-NULL timestamp
      When query
        """
        SELECT from_utc_timestamp(source_ts, target_tz) AS result
        FROM VALUES
          (CAST(NULL AS TIMESTAMP), 'Not/AZone'),
          (TIMESTAMP '2024-01-01 00:00:00', 'Not/AZone')
          AS t(source_ts, target_tz)
        """
      Then query error INVALID_TIMEZONE

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

  Rule: Time zone argument types

    # Skipping timezone-string validation for NULL timestamps must not skip type validation.
    Scenario Outline: `from_utc_timestamp` rejects ARRAY time zones with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT from_utc_timestamp(<timestamp>, array('UTC')) AS result
        """
      Then query error DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

      Examples:
        | timestamp                       | ansi  |
        | CAST(NULL AS TIMESTAMP)          | true  |
        | CAST(NULL AS TIMESTAMP)          | false |
        | TIMESTAMP '2024-01-01 00:00:00' | true  |
        | TIMESTAMP '2024-01-01 00:00:00' | false |

    Scenario Outline: `from_utc_timestamp` retains binary time zone coercion with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', CAST('UTC' AS BINARY)) AS result
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |
