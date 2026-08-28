Feature: Timestamp and string predicate coercion

  Rule: Timestamp comparisons use Spark coercion semantics

    Scenario: String-to-timestamp comparison uses the session time zone and microsecond precision
      Given config spark.sql.session.timeZone = Asia/Shanghai
      When query
        """
        SELECT
          TIMESTAMP '2024-05-01 12:00:00' > '2024-05-01 13:00:00' AS after,
          TIMESTAMP '2024-05-01 12:00:00' = CONCAT('2024-05-01 12:00:', '00') AS dynamic_match,
          TIMESTAMP '2024-05-01 12:00:00.123456' = '2024-05-01 12:00:00.123456789' AS precise_match
        """
      Then query result
        | after | dynamic_match | precise_match |
        | false | true          | true          |

    Scenario: Timestamp IN uses the ANSI common type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          TIMESTAMP '2024-05-01 12:00:00.123456'
            IN ('2024-05-01 12:00:00.123456789') AS matched,
          TIMESTAMP '2024-05-01 12:00:00'
            IN ('2024-05-01 12:00:00', 1) AS mixed_matched
        """
      Then query result
        | matched | mixed_matched |
        | false   | true          |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789')
        """
      Then query result
        | matched |
        | 0       |
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT TIMESTAMP '2024-05-01 12:00:00.123456'
          IN ('2024-05-01 12:00:00.123456789') AS matched
        """
      Then query result
        | matched |
        | true    |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789')
        """
      Then query result
        | matched |
        | 1       |

    Scenario: ANSI IN chooses one recursive datetime common type
      Given config spark.sql.session.timeZone = Asia/Shanghai
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          '2024-04-30 16:00:00Z' IN (
            TIMESTAMP_NTZ '2000-01-01 00:00:00',
            TIMESTAMP '2024-05-01 00:00:00'
          ) AS ltz_after_ntz,
          TIMESTAMP '2024-05-01 00:00:00' IN (
            '2000-01-01 00:00:00',
            DATE '2024-05-01'
          ) AS date_promoted,
          ARRAY(TIMESTAMP '2024-05-01 00:00:00.123456') IN (
            NULL,
            ARRAY('2024-05-01 00:00:00.123456789')
          ) AS array_match,
          ARRAY(ARRAY(TIMESTAMP '2024-05-01 00:00:00.123456')) IN (
            ARRAY(ARRAY('2024-05-01 00:00:00.123456789'))
          ) AS nested_array_match,
          named_struct(
            'x', TIMESTAMP '2024-05-01 00:00:00.123456'
          ) IN (
            NULL,
            named_struct('x', '2024-04-30 16:00:00.123456789Z')
          ) AS struct_match
        """
      Then query result
        | ltz_after_ntz | date_promoted | array_match | nested_array_match | struct_match |
        | true          | true          | true        | true               | true         |

    Scenario: Struct IN resolves fields positionally and with the configured resolver
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.caseSensitive = false
      When query
        """
        SELECT
          named_struct('x', TIMESTAMP '2024-05-01 00:00:00', 'x', 1) IN (
            named_struct('x', '2024-05-01 00:00:00', 'x', 2)
          ) AS duplicate_mismatch,
          named_struct('x', TIMESTAMP '2024-05-01 00:00:00.123456') IN (
            named_struct('X', '2024-05-01 00:00:00.123456789')
          ) AS case_insensitive_match
        """
      Then query result
        | duplicate_mismatch | case_insensitive_match |
        | false              | true                   |

    Scenario: Array and struct IN subqueries use recursive common types
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.caseSensitive = false
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (ARRAY(TIMESTAMP '2024-05-01 00:00:00.123456')) AS lhs(v)
        WHERE v IN (SELECT ARRAY('2024-05-01 00:00:00.123456789'))
        """
      Then query result
        | matched |
        | 0       |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (
          named_struct('x', TIMESTAMP '2024-05-01 00:00:00.123456')
        ) AS lhs(v)
        WHERE v IN (
          SELECT named_struct('X', '2024-05-01 00:00:00.123456789')
        )
        """
      Then query result
        | matched |
        | 0       |
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (ARRAY(TIMESTAMP '2024-05-01 00:00:00.123456')) AS lhs(v)
        WHERE v IN (SELECT ARRAY('2024-05-01 00:00:00.123456789'))
        """
      Then query result
        | matched |
        | 1       |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (
          named_struct('x', TIMESTAMP '2024-05-01 00:00:00.123456')
        ) AS lhs(v)
        WHERE v IN (
          SELECT named_struct('X', '2024-05-01 00:00:00.123456789')
        )
        """
      Then query result
        | matched |
        | 1       |

    Scenario: Legacy IN promotes nested timestamp and string values to string
      Given config spark.sql.session.timeZone = Asia/Shanghai
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          ARRAY(TIMESTAMP '2024-05-01 00:00:00') IN (
            NULL,
            ARRAY('2024-04-30 16:00:00Z')
          ) AS array_with_null,
          ARRAY(ARRAY(TIMESTAMP '2024-05-01 00:00:00')) IN (
            ARRAY(ARRAY('2024-04-30 16:00:00Z'))
          ) AS nested_array_match,
          named_struct('x', TIMESTAMP '2024-05-01 00:00:00') IN (
            NULL,
            named_struct('x', '2024-04-30 16:00:00Z')
          ) AS struct_with_null
        """
      Then query result
        | array_with_null | nested_array_match | struct_with_null |
        | NULL            | false              | NULL             |

    Scenario: Legacy IN uses Spark-compatible string rendering
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(1e18 AS DOUBLE) IN (
            '1.0E18',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS large_value,
          CAST(1e-7 AS DOUBLE) IN (
            '1.0E-7',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS small_value,
          CAST('-0.0' AS DOUBLE) IN (
            '-0.0',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS negative_zero,
          CAST('4.9E-324' AS DOUBLE) IN (
            '4.9E-324',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS min_double,
          CAST('1.4E-45' AS FLOAT) IN (
            '1.4E-45',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS min_float,
          INTERVAL 1 DAY IN (
            CONCAT('INTERVAL ', CHR(39), '1', CHR(39), ' DAY'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS day_interval
        """
      Then query result
        | large_value | small_value | negative_zero | min_double | min_float | day_interval |
        | true        | true        | true          | true       | true      | true         |

    Scenario: Legacy datetime ordering honors datetimeToString configuration
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.typeCoercion.datetimeToString.enabled = false
      When query
        """
        SELECT
          (TIMESTAMP '2024-01-01 00:00:00' > '9') IS NULL
            AS timestamp_ordering_is_null
        """
      Then query result
        | timestamp_ordering_is_null |
        | true                       |
      Given config spark.sql.legacy.typeCoercion.datetimeToString.enabled = true
      When query
        """
        SELECT
          TIMESTAMP '2024-01-01 00:00:00' > '9' AS timestamp_ordering,
          (TIMESTAMP '2024-01-01 00:00:00' = 'not-a-timestamp') IS NULL
            AS equality_still_parses
        """
      Then query result
        | timestamp_ordering | equality_still_parses |
        | false              | true                  |

    @function(nullability)
    Scenario: Null-safe timestamp comparisons remain non-nullable after coercion
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          TIMESTAMP '2024-01-01 00:00:00'
            IS DISTINCT FROM '2024-01-01 00:00:00' AS distinct_value,
          TIMESTAMP '2024-01-01 00:00:00'
            IS NOT DISTINCT FROM '2024-01-01 00:00:00' AS not_distinct_value
        """
      Then query result
        | distinct_value | not_distinct_value |
        | false          | true               |
      And query schema
        """
        root
         |-- distinct_value: boolean (nullable = false)
         |-- not_distinct_value: boolean (nullable = false)
        """

    Scenario: Multi-column timestamp IN subquery coerces every pair
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00', 7)
          AS lhs(timestamp_value, candidate_id)
        WHERE (timestamp_value, candidate_id) IN (
          SELECT candidate_time, candidate_id
          FROM VALUES ('2024-05-01T12:00:00Z', 7)
            AS rhs(candidate_time, candidate_id)
        )
        """
      Then query result
        | matched |
        | 0       |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES ('2024-05-01 12:00:00.123456789', 7)
          AS lhs(timestamp_text, candidate_id)
        WHERE (timestamp_text, candidate_id) IN (
          SELECT candidate_time, candidate_id
          FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456', 7)
            AS rhs(candidate_time, candidate_id)
        )
        """
      Then query result
        | matched |
        | 0       |
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00', 7)
          AS lhs(timestamp_value, candidate_id)
        WHERE (timestamp_value, candidate_id) IN (
          SELECT candidate_time, candidate_id
          FROM VALUES ('2024-05-01T12:00:00Z', 7)
            AS rhs(candidate_time, candidate_id)
        )
        """
      Then query result
        | matched |
        | 1       |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES ('2024-05-01 12:00:00.123456789', 7)
          AS lhs(timestamp_text, candidate_id)
        WHERE (timestamp_text, candidate_id) IN (
          SELECT candidate_time, candidate_id
          FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456', 7)
            AS rhs(candidate_time, candidate_id)
        )
        """
      Then query result
        | matched |
        | 1       |

    Scenario: Implicit comparisons use Spark unformatted timestamp semantics
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(current_date() AS TIMESTAMP)
              + INTERVAL 23 HOURS 59 MINUTES 59 SECONDS
              = '23:59:59' AS time_only,
          timestamp_micros(9223372036854775807L)
              = '294247-01-10T04:00:54.775807Z' AS max_year,
          timestamp_micros(CAST('-9223372036854775808' AS BIGINT))
              = '-290308-12-21 19:59:05.224192Z' AS min_year,
          TIMESTAMP '2024-01-01 08:00:00Z'
              = '2024-01-01 00:00:00 PST' AS short_zone,
          TIMESTAMP '2024-01-01 00:00:00Z'
              = '2024-01-01 01:00:00 GMT+01:00' AS prefixed_zone,
          TIMESTAMP '2024-05-01 12:00:00.123456'
              = CONCAT('  2024-05-01 12:00:00.123456789', '0  ')
                AS padded_long_fraction,
          (
            TIMESTAMP '-200000-01-01 00:00:00'
              = '-0200000-01-01 00:00:00'
          ) IS NULL AS seven_digit_year_rejected,
          (
            TIMESTAMP '2023-12-31 00:01:00Z'
              = '2024-01-01 00:00:00+23:59'
          ) IS NULL AS oversized_offset_rejected,
          (
            TIMESTAMP '2024-01-01 12:00:00'
              = '2024-01-01t12:00:00'
          ) IS NULL AS lowercase_t_rejected,
          (
            TIMESTAMP '2024-01-01 00:00:00Z'
              = '2024-01-01 00:00:00z'
          ) IS NULL AS lowercase_z_rejected
        """
      Then query result
        | time_only | max_year | min_year | short_zone | prefixed_zone | padded_long_fraction | seven_digit_year_rejected | oversized_offset_rejected | lowercase_t_rejected | lowercase_z_rejected |
        | true      | true     | true     | true       | true          | true                 | true                      | true                      | true                 | true                 |
