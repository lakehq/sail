Feature: sequence output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a non-null column input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(CAST(id AS INT), 5) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to sequence stays nullable
      When query
        """
        SELECT sequence(c, 5) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: temporal sequence elements are non-nullable
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          sequence(DATE '2018-01-01', DATE '2018-01-02') AS dates,
          sequence(
            TIMESTAMP_NTZ '2018-01-01 00:00:00',
            TIMESTAMP_NTZ '2018-01-02 00:00:00'
          ) AS timestamps,
          sequence(
            TIMESTAMP_NTZ '2018-01-01 00:00:00',
            TIMESTAMP '2018-01-02 00:00:00'
          ) AS mixed_timestamps
        """
      Then query schema
        """
        root
         |-- dates: array (nullable = false)
         |    |-- element: date (containsNull = false)
         |-- timestamps: array (nullable = false)
         |    |-- element: timestamp_ntz (containsNull = false)
         |-- mixed_timestamps: array (nullable = false)
         |    |-- element: timestamp (containsNull = false)
        """

  Rule: Integral type coercion

    Scenario: sequence widens a literal start to a BIGINT column stop
      When query
        """
        SELECT
          n,
          typeof(sequence(1, n)) AS result_type,
          sequence(1, n) AS result
        FROM VALUES (CAST(1 AS BIGINT)), (3), (12) AS t(n)
        ORDER BY n
        """
      Then query result ordered
        | n  | result_type   | result                                  |
        | 1  | array<bigint> | [1]                                     |
        | 3  | array<bigint> | [1, 2, 3]                               |
        | 12 | array<bigint> | [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] |

    Scenario: sequence executes every Spark integral kernel
      When query
        """
        SELECT
          typeof(sequence(
            CAST(-1 AS TINYINT),
            CAST(-3 AS TINYINT),
            CAST(-1 AS TINYINT)
          )) AS tinyint_type,
          sequence(
            CAST(-1 AS TINYINT),
            CAST(-3 AS TINYINT),
            CAST(-1 AS TINYINT)
          ) AS tinyint_result,
          typeof(sequence(
            CAST(3 AS SMALLINT),
            CAST(-3 AS SMALLINT),
            CAST(-3 AS SMALLINT)
          )) AS smallint_type,
          sequence(
            CAST(3 AS SMALLINT),
            CAST(-3 AS SMALLINT),
            CAST(-3 AS SMALLINT)
          ) AS smallint_result,
          typeof(sequence(-3, 3, 3)) AS int_type,
          sequence(-3, 3, 3) AS int_result,
          typeof(sequence(1L, 3L, 1L)) AS bigint_type,
          sequence(1L, 3L, 1L) AS bigint_result
        """
      Then query result
        | tinyint_type   | tinyint_result | smallint_type   | smallint_result | int_type   | int_result | bigint_type   | bigint_result |
        | array<tinyint> | [-1, -2, -3]   | array<smallint> | [3, 0, -3]      | array<int> | [-3, 0, 3] | array<bigint> | [1, 2, 3]    |

    Scenario: sequence infers integral types through untyped NULL arguments
      When query
        """
        SELECT
          typeof(sequence(NULL, 1)) AS null_start_type,
          sequence(NULL, 1) AS null_start,
          typeof(sequence(NULL, NULL, 1)) AS null_bounds_type,
          sequence(NULL, NULL, 1) AS null_bounds,
          typeof(sequence(1, 3, NULL)) AS null_step_type,
          sequence(1, 3, NULL) AS null_step
        """
      Then query result
        | null_start_type | null_start | null_bounds_type | null_bounds | null_step_type | null_step |
        | array<int>      | NULL       | array<int>       | NULL        | array<int>     | NULL      |

    Scenario: ANSI sequence coercion parses a string in an integral context
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(sequence(1, ' 3 ')) AS result_type, sequence(1, ' 3 ') AS result
        """
      Then query result
        | result_type   | result    |
        | array<bigint> | [1, 2, 3] |

    Scenario: ANSI sequence coercion trims Spark control whitespace
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          typeof(sequence(
            1,
            concat(chr(9), '3', chr(10))
          )) AS result_type,
          sequence(
            1,
            concat(chr(9), '3', chr(10))
          ) AS result
        """
      Then query result
        | result_type   | result    |
        | array<bigint> | [1, 2, 3] |

    Scenario: ANSI sequence coercion trims Spark whitespace in an integral context
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sequence(1, '\t3\n') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario Outline: sequence rejects unresolved input families
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <call> AS result
        """
      Then query error DATATYPE_MISMATCH\.SEQUENCE_WRONG_INPUT_TYPES

      Examples:
        | ansi  | call                                                        |
        | true  | sequence(NULL, NULL)                                        |
        | true  | sequence(DATE '2018-01-01', DATE '2018-01-02', NULL)        |
        | false | sequence(1, '3')                                            |
        | true  | sequence('2018-01-01', '2018-01-02')                        |

  Rule: Integral execution semantics

    Scenario: sequence chooses its default step from the boundary direction
      When query
        """
        SELECT sequence(1, 3) AS ascending, sequence(3, 1) AS descending
        """
      Then query result
        | ascending | descending |
        | [1, 2, 3] | [3, 2, 1]  |

    Scenario: sequence applies row-level boundaries and null propagation
      When query
        """
        SELECT label, sequence(lo, hi, stride) AS result
        FROM VALUES
          ('ascending', 1L, 3L, 1L),
          ('descending', 3L, 1L, -1L),
          ('equal-zero', 1L, 1L, 0L),
          ('null-start', CAST(NULL AS BIGINT), 1L, 1L),
          ('null-stop', 1L, CAST(NULL AS BIGINT), 1L),
          ('null-step', 1L, 1L, CAST(NULL AS BIGINT))
          AS t(label, lo, hi, stride)
        ORDER BY label
        """
      Then query result ordered
        | label      | result    |
        | ascending  | [1, 2, 3] |
        | descending | [3, 2, 1] |
        | equal-zero | [1]       |
        | null-start | NULL      |
        | null-step  | NULL      |
        | null-stop  | NULL      |

    Scenario: sequence stops evaluating arguments after a NULL boundary
      When query
        """
        SELECT
          id,
          sequence(
            start,
            CASE
              WHEN start IS NULL
                THEN CAST(raise_error(CAST(id AS STRING)) AS BIGINT)
              ELSE stop
            END,
            CASE
              WHEN stop IS NULL
                THEN CAST(raise_error(CAST(id AS STRING)) AS BIGINT)
              ELSE 1L
            END
          ) AS result
        FROM VALUES
          (1, CAST(NULL AS BIGINT), 3L),
          (2, 1L, CAST(NULL AS BIGINT)),
          (3, 1L, 3L)
          AS t(id, start, stop)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | NULL      |
        | 2  | NULL      |
        | 3  | [1, 2, 3] |

    Scenario Outline: sequence rejects illegal integral boundaries
      When query
        """
        SELECT sequence(<start>, <stop>, <step>) AS result
        """
      Then query error Illegal sequence boundaries: <start> to <stop> by <step>

      Examples:
        | start | stop | step |
        | 1     | 2    | 0    |
        | 2     | 1    | 1    |
        | 1     | 2    | -1   |

    Scenario Outline: sequence rejects arrays beyond Spark's collection limit
      When query
        """
        SELECT <call> AS result
        """
      Then query error COLLECTION_SIZE_LIMIT_EXCEEDED\.PARAMETER

      Examples:
        | call                                                                                  |
        | sequence(-2147483648, 2147483647, 1)                                                  |
        | sequence(CAST(9223372036854775807 AS BIGINT), CAST(-1 AS BIGINT), CAST(-1 AS BIGINT)) |

    Scenario Outline: sequence reports Spark's collection limit message verbatim
      When query
        """
        SELECT <call> AS result
        """
      Then query error \[COLLECTION_SIZE_LIMIT_EXCEEDED\.PARAMETER\] Can't create array with <elements> elements which exceeding the array size limit 2147483632, the value of parameter\(s\) `count` in the function `sequence` is invalid\.

      Examples:
        | call                                                                                  | elements            |
        | sequence(-2147483648, 2147483647, 1)                                                  | 4294967296          |
        | sequence(CAST(9223372036854775807 AS BIGINT), CAST(-1 AS BIGINT), CAST(-1 AS BIGINT)) | 9223372036854775809 |

    Scenario: sequence preserves Spark's internal error after arithmetic overflow
      When query
        """
        SELECT sequence(
          CAST(-9223372036854775808 AS BIGINT),
          CAST(9223372036854775807 AS BIGINT),
          CAST(9223372036854775807 AS BIGINT)
        ) AS result
        """
      Then query error \[INTERNAL_ERROR\] Unreachable code reached\.
