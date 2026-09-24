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
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789' AS candidate)
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
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789' AS candidate)
        """
      Then query result
        | matched |
        | 1       |

    @sail-bug
    Scenario: An unaliased string literal in an IN subquery is a PARSE error
      # Spark's `SELECT` is not a reserved keyword, so inside `IN (...)` the ANTLR grammar
      # resolves `SELECT '<string>'` through the `inList` alternative and reads the two
      # tokens as a typed literal (`identifier stringLit`, AstBuilder.visitTypeConstructor)
      # rather than as a subquery projection. The result is a parse-time
      # UNSUPPORTED_TYPED_LITERAL naming "SELECT" as the type.
      #
      # Sail's parser instead accepts it and evaluates it as a real subquery, returning 0 —
      # so Sail is a strict superset of Spark's grammar here, and any test written against
      # Sail alone reads as green.
      #
      # The three scenarios below are what make this discriminate. Only the FIRST is
      # rejected: `IN (SELECT 1)` proves IN-subqueries are supported at all, and the
      # aliased form proves the projection itself is fine — so a blanket "IN subqueries are
      # broken" reading is ruled out, and the defect is pinned to the bare string literal.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789')
        """
      Then query error Literals of the type "SELECT" are not supported

    Scenario: An IN subquery over a non-string literal parses fine
      # Control for the scenario above: an integer literal has no `identifier stringLit`
      # reading, so the same shape parses and runs.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (1) AS t(x)
        WHERE x IN (SELECT 1)
        """
      Then query result
        | matched |
        | 1       |

    Scenario: Aliasing the string literal makes the IN subquery parse
      # The fix, and the other control: adding the alias removes the
      # `identifier stringLit` reading and the query behaves exactly like the in-list form.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789' AS candidate)
        """
      Then query result
        | matched |
        | 0       |

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
          CAST('4.611686018427388E18' AS DOUBLE) IN (
            '4.6116860184273879E18',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS jdk17_double,
          CAST('-2.41156777E14' AS FLOAT) IN (
            '-2.41156777E14',
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS jdk17_float,
          INTERVAL 1 DAY IN (
            CONCAT('INTERVAL ', CHR(39), '1', CHR(39), ' DAY'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS day_interval
        """
      Then query result
        | large_value | small_value | negative_zero | min_double | min_float | jdk17_double | jdk17_float | day_interval |
        | true        | true        | true          | true       | true      | true         | true        | true         |
      When query
        """
        SELECT
          day_value IN (
            CONCAT('INTERVAL ', CHR(39), '1', CHR(39), ' DAY'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS day_column,
          day_second_value IN (
            CONCAT('INTERVAL ', CHR(39), '1 00:00:00', CHR(39), ' DAY TO SECOND'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS day_to_second_column,
          day_value + INTERVAL '1' DAY IN (
            CONCAT('INTERVAL ', CHR(39), '2', CHR(39), ' DAY'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS day_expression,
          month_value IN (
            CONCAT('INTERVAL ', CHR(39), '1', CHR(39), ' MONTH'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS month_column,
          year_month_value IN (
            CONCAT('INTERVAL ', CHR(39), '0-1', CHR(39), ' YEAR TO MONTH'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS year_to_month_column
        FROM VALUES (
          INTERVAL '1' DAY,
          INTERVAL '1 00:00:00' DAY TO SECOND,
          INTERVAL '1' MONTH,
          INTERVAL '0-1' YEAR TO MONTH
        ) AS intervals(day_value, day_second_value, month_value, year_month_value)
        """
      Then query result
        | day_column | day_to_second_column | day_expression | month_column | year_to_month_column |
        | true       | true                 | true           | true         | true                 |

    Scenario: Legacy IN uses zero-padded interval strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          INTERVAL '1:02' HOUR TO MINUTE IN (
            CONCAT('INTERVAL ', CHR(39), '01:02', CHR(39), ' HOUR TO MINUTE'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS literal_match,
          v IN (
            CONCAT('INTERVAL ', CHR(39), '01:02', CHR(39), ' HOUR TO MINUTE'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS column_match
        FROM VALUES (INTERVAL '1:02' HOUR TO MINUTE) AS t(v)
        """
      Then query result
        | literal_match | column_match |
        | true          | true         |

    Scenario: Floating-point literal names use Spark-compatible rendering
      When query
        """
        SELECT 1e18, 1e-7
        """
      Then query schema
        """
        root
         |-- 1.0E18: double (nullable = false)
         |-- 1.0E-7: double (nullable = false)
        """

    Scenario: Legacy IN preserves interval qualifiers through value expressions
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          (CASE
            WHEN true THEN INTERVAL '1' DAY
            ELSE INTERVAL '2' DAY
          END) IN (
            CONCAT('INTERVAL ', CHR(39), '1', CHR(39), ' DAY'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS case_interval,
          INTERVAL 0 YEAR 0 MONTH IN (
            CONCAT('INTERVAL ', CHR(39), '0-0', CHR(39), ' YEAR TO MONTH'),
            TIMESTAMP '2000-01-01 00:00:00'
          ) AS zero_year_month
        """
      Then query result
        | case_interval | zero_year_month |
        | true          | true            |

    Scenario: Legacy IN preserves interval qualifiers on nested casts
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(id AS INTERVAL DAY) IN (
          CONCAT('INTERVAL ', CHR(39), id, CHR(39), ' DAY'),
          TIMESTAMP '2000-01-01 00:00:00'
        ) AS matched
        FROM range(1, 3)
        """
      Then query result
        | matched |
        | true    |
        | true    |

    Scenario Outline: Legacy datetimeToString configuration accepts padded <value>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.typeCoercion.datetimeToString.enabled = {{ ' <value> ' }}
      When query
        """
        SELECT TIMESTAMP '2024-01-01 00:00:00' > '9' AS ordering
        """
      Then query result
        | ordering |
        | <result> |

      Examples:
        | value | result |
        | true  | false  |
        | TrUe  | false  |
        | false | NULL   |

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

    Scenario Outline: Timestamp NTZ ordering parses strings with datetimeToString <enabled>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.typeCoercion.datetimeToString.enabled = <enabled>
      When query
        """
        SELECT
          TIMESTAMP_NTZ '2024-01-01 00:00:00' > '9' AS timestamp_first,
          '9' < TIMESTAMP_NTZ '2024-01-01 00:00:00' AS string_first,
          NOT (TIMESTAMP_NTZ '2024-01-01 00:00:00' > '9') AS negated,
          TIMESTAMP_NTZ '2024-01-01 00:00:00' >= '2024-1-1' AS valid_date
        """
      Then query result
        | timestamp_first | string_first | negated | valid_date |
        | NULL            | NULL         | NULL    | true       |

      Examples:
        | enabled |
        | true    |
        | false   |

    @sail-only
    Scenario: Timestamp string comparison pushes a literal predicate into Parquet
      Given config spark.sql.session.timeZone = UTC
      And variable location for temporary directory timestamp_predicate_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS timestamp_predicate_pushdown
        """
      Given statement template
        """
        CREATE TABLE timestamp_predicate_pushdown USING PARQUET LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP '2024-01-01 00:00:00'),
          (TIMESTAMP '2024-01-02 00:00:00'),
          (TIMESTAMP '2024-01-03 00:00:00') AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM timestamp_predicate_pushdown WHERE ts >= '2024-01-02 00:00:00'
        """
      Then query plan matches snapshot
      When query
        """
        SELECT ts FROM timestamp_predicate_pushdown WHERE ts >= '2024-01-02 00:00:00'
        """
      Then query result
        | ts                  |
        | 2024-01-02 00:00:00 |
        | 2024-01-03 00:00:00 |

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
