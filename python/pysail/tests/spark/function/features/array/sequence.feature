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
          ) AS timestamps
        """
      Then query schema
        """
        root
         |-- dates: array (nullable = false)
         |    |-- element: date (containsNull = false)
         |-- timestamps: array (nullable = false)
         |    |-- element: timestamp_ntz (containsNull = false)
        """

    Scenario: a mixed temporal sequence is non-nullable after timestamp widening
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT sequence(
          TIMESTAMP_NTZ '2018-01-01 00:00:00',
          TIMESTAMP '2018-01-02 00:00:00'
        ) AS mixed_timestamps
        """
      Then query schema
        """
        root
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

    @sail-bug
    Scenario: ANSI sequence string bounds report Spark's cast error class
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sequence(1, 'abc') AS result
        """
      Then query error CAST_INVALID_INPUT

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

    @sail-bug
    Scenario: an earlier foldable NULL suppresses a later foldable sequence error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sequence(
          CAST(NULL AS BIGINT),
          CAST(concat('b', 'ad') AS BIGINT)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: sequence reports the first of two foldable errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sequence(
          CAST('bad-first' AS BIGINT),
          CAST(1 / 0 AS BIGINT)
        ) AS result
        """
      Then query error CAST_INVALID_INPUT

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

    @sail-bug
    Scenario: sequence extracts only the Python UDF subtree before short-circuiting
      Given scalar Python UDF one_udf returns 1
      When query
        """
        SELECT sequence(
          CASE WHEN id = 0 THEN CAST(NULL AS BIGINT) ELSE id END,
          one_udf(id) + CAST(raise_error('subtree-error') AS BIGINT)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence keeps common expressions behind a NULL boundary
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT sequence(
          CASE WHEN id = 0 THEN CAST(NULL AS BIGINT) ELSE CAST(1 / id AS BIGINT) END,
          CAST(1 / id AS BIGINT)
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence does not consume random values after a NULL boundary
      When query
        """
        SELECT id, sequence(start, CAST(rand(1) * 10 AS BIGINT)) AS result
        FROM VALUES
          (1, CAST(NULL AS BIGINT)),
          (2, 1L),
          (3, 1L)
          AS t(id, start)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | NULL                |
        | 2  | [1, 2, 3, 4, 5, 6] |
        | 3  | [1, 2, 3, 4, 5]    |

    Scenario: sequence does not swallow a batched child error during row recovery
      When query
        """
        SELECT id, sequence(
          1L,
          CASE
            WHEN rand(1) > 0.62 THEN 2L
            ELSE CAST(raise_error('random-error') AS BIGINT)
          END
        ) AS result
        FROM VALUES (1), (2) AS t(id)
        ORDER BY id
        """
      Then query error random-error

    Scenario: sequence preserves seeded random positions while locating a row error
      When query
        """
        SELECT sequence(
          1L,
          CASE
            WHEN rand(1) > 0.62 AND id = 2
              THEN CAST(raise_error('spurious-error') AS BIGINT)
            WHEN id = 3
              THEN CAST(raise_error('original-error') AS BIGINT)
            ELSE 2L
          END
        ) AS result
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error original-error

    Scenario: sequence reports an earlier row's boundary error before evaluating later rows
      When query
        """
        SELECT sequence(
          1L,
          2L,
          CASE
            WHEN id = 1 THEN 0L
            ELSE CAST(raise_error('later-row') AS BIGINT)
          END
        ) AS result
        FROM VALUES (1), (2) AS t(id)
        """
      Then query error Illegal sequence boundaries: 1 to 2 by 0

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

    Scenario: sequence composes inside another higher-order function lambda
      When query
        """
        SELECT transform(array(1, 3), x -> sequence(1, x)) AS result
        """
      Then query result
        | result           |
        | [[1], [1, 2, 3]] |

    Scenario: sequence does not shadow an enclosing lambda parameter
      When query
        """
        SELECT transform(array(1, 2), _sequence -> sequence(_sequence, 3)) AS result
        """
      Then query result
        | result               |
        | [[1, 2, 3], [2, 3]] |

  Rule: Integer sequences

    Scenario: Basic ascending integer sequence with default step
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: Ascending integer sequence with explicit step
      When query
        """
        SELECT sequence(1, 10, 2) AS result
        """
      Then query result
        | result              |
        | [1, 3, 5, 7, 9] |

    Scenario: Descending integer sequence with negative step
      When query
        """
        SELECT sequence(10, 1, -2) AS result
        """
      Then query result
        | result               |
        | [10, 8, 6, 4, 2] |

    Scenario: Descending integer sequence with default step
      When query
        """
        SELECT sequence(5, 1) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: Single element when start equals stop
      When query
        """
        SELECT sequence(3, 3) AS result
        """
      Then query result
        | result |
        | [3]    |

    Scenario: Single element when start equals stop with explicit step
      When query
        """
        SELECT sequence(1, 1, 1) AS result
        """
      Then query result
        | result |
        | [1]    |

    Scenario: Negative integer sequence ascending
      When query
        """
        SELECT sequence(-5, -1) AS result
        """
      Then query result
        | result                  |
        | [-5, -4, -3, -2, -1] |

    Scenario: Negative to positive integer sequence
      When query
        """
        SELECT sequence(-3, 3) AS result
        """
      Then query result
        | result                         |
        | [-3, -2, -1, 0, 1, 2, 3] |

    Scenario: Negative integer sequence descending with default step
      When query
        """
        SELECT sequence(-1, -5) AS result
        """
      Then query result
        | result                  |
        | [-1, -2, -3, -4, -5] |

    Scenario: Step overshoots the end value
      When query
        """
        SELECT sequence(1, 5, 10) AS result
        """
      Then query result
        | result |
        | [1]    |

    Scenario: Negative range with explicit step
      When query
        """
        SELECT sequence(-10, -1, 3) AS result
        """
      Then query result
        | result                  |
        | [-10, -7, -4, -1] |

    Scenario: Negative descending with explicit step
      When query
        """
        SELECT sequence(-1, -10, -3) AS result
        """
      Then query result
        | result              |
        | [-1, -4, -7, -10] |

    Scenario: Zero start ascending
      When query
        """
        SELECT sequence(0, 5) AS result
        """
      Then query result
        | result                |
        | [0, 1, 2, 3, 4, 5] |

    Scenario: Zero to zero returns single element
      When query
        """
        SELECT sequence(0, 0) AS result
        """
      Then query result
        | result |
        | [0]    |

    Scenario: Step exactly reaches end value
      When query
        """
        SELECT sequence(0, 10, 5) AS result
        """
      Then query result
        | result          |
        | [0, 5, 10] |

    Scenario: Step does not evenly divide range
      When query
        """
        SELECT sequence(1, 10, 3) AS result
        """
      Then query result
        | result           |
        | [1, 4, 7, 10] |

    Scenario: Large range returns correct size
      When query
        """
        SELECT size(sequence(1, 10000)) AS result
        """
      Then query result
        | result |
        | 10000  |

  Rule: Integer boundary values

    Scenario: INT_MAX boundary
      When query
        """
        SELECT sequence(2147483645, 2147483647) AS result
        """
      Then query result
        | result                                    |
        | [2147483645, 2147483646, 2147483647] |

    Scenario: INT_MIN boundary
      When query
        """
        SELECT sequence(-2147483648, -2147483646) AS result
        """
      Then query result
        | result                                          |
        | [-2147483648, -2147483647, -2147483646] |

    Scenario: Near INT_MAX with two elements
      When query
        """
        SELECT sequence(2147483646, 2147483647) AS result
        """
      Then query result
        | result                          |
        | [2147483646, 2147483647] |

    Scenario: BIGINT large values
      When query
        """
        SELECT sequence(CAST(9223372036854775805 AS BIGINT), CAST(9223372036854775807 AS BIGINT)) AS result
        """
      Then query result
        | result                                                                    |
        | [9223372036854775805, 9223372036854775806, 9223372036854775807] |

    Scenario: BIGINT with large step
      When query
        """
        SELECT sequence(CAST(0 AS BIGINT), CAST(100 AS BIGINT), CAST(25 AS BIGINT)) AS result
        """
      Then query result
        | result                  |
        | [0, 25, 50, 75, 100] |

  Rule: Integer type coercion

    Scenario: BIGINT sequence
      When query
        """
        SELECT sequence(CAST(1 AS BIGINT), CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: TINYINT sequence
      When query
        """
        SELECT sequence(CAST(1 AS TINYINT), CAST(5 AS TINYINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: SMALLINT step with INT start and end
      When query
        """
        SELECT sequence(1, 10, CAST(3 AS SMALLINT)) AS result
        """
      Then query result
        | result           |
        | [1, 4, 7, 10] |

    Scenario: TINYINT step with INT start and end
      When query
        """
        SELECT sequence(1, 10, CAST(3 AS TINYINT)) AS result
        """
      Then query result
        | result           |
        | [1, 4, 7, 10] |

    Scenario: BIGINT sequence with explicit step
      When query
        """
        SELECT sequence(CAST(1 AS BIGINT), CAST(5 AS BIGINT), CAST(2 AS BIGINT)) AS result
        """
      Then query result
        | result      |
        | [1, 3, 5] |

    Scenario: SMALLINT sequence
      When query
        """
        SELECT sequence(CAST(1 AS SMALLINT), CAST(5 AS SMALLINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: Mixed TINYINT start BIGINT end INT step
      When query
        """
        SELECT sequence(CAST(1 AS TINYINT), CAST(10 AS BIGINT), CAST(2 AS INT)) AS result
        """
      Then query result
        | result              |
        | [1, 3, 5, 7, 9] |

    Scenario: INT start with BIGINT end coerces to BIGINT
      When query
        """
        SELECT sequence(1, CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: BIGINT start with INT end coerces to BIGINT
      When query
        """
        SELECT sequence(CAST(1 AS BIGINT), 5) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

  Rule: NULL handling

    Scenario: NULL start returns NULL
      When query
        """
        SELECT sequence(NULL, 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL end returns NULL
      When query
        """
        SELECT sequence(1, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL step returns NULL
      When query
        """
        SELECT sequence(1, 5, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Typed NULL start and end returns NULL
      When query
        """
        SELECT sequence(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL date start returns NULL
      When query
        """
        SELECT sequence(CAST(NULL AS DATE), DATE'2024-01-05') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Date sequences

    Scenario: Date sequence with default step of 1 day
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-01-05') AS result
        """
      Then query result
        | result                                                          |
        | [2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05] |

    Scenario: Date sequence with explicit day interval
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-01-10', interval 2 day) AS result
        """
      Then query result
        | result                                                          |
        | [2024-01-01, 2024-01-03, 2024-01-05, 2024-01-07, 2024-01-09] |

    Scenario: Date sequence with month interval
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-06-01', interval 1 month) AS result
        """
      Then query result
        | result                                                                                  |
        | [2024-01-01, 2024-02-01, 2024-03-01, 2024-04-01, 2024-05-01, 2024-06-01] |

    Scenario: Date sequence with 2-month interval
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-12-01', interval '2' month) AS result
        """
      Then query result
        | result                                                                                                      |
        | [2024-01-01, 2024-03-01, 2024-05-01, 2024-07-01, 2024-09-01, 2024-11-01] |

    Scenario: Date sequence descending with default step
      When query
        """
        SELECT sequence(DATE'2024-01-05', DATE'2024-01-01') AS result
        """
      Then query result
        | result                                                          |
        | [2024-01-05, 2024-01-04, 2024-01-03, 2024-01-02, 2024-01-01] |

    Scenario: Date sequence descending with negative day interval
      When query
        """
        SELECT sequence(DATE'2024-01-05', DATE'2024-01-01', interval -1 day) AS result
        """
      Then query result
        | result                                                          |
        | [2024-01-05, 2024-01-04, 2024-01-03, 2024-01-02, 2024-01-01] |

    Scenario: Date sequence descending with negative month interval
      When query
        """
        SELECT sequence(DATE'2024-06-01', DATE'2024-01-01', interval '-1' month) AS result
        """
      Then query result
        | result                                                                                  |
        | [2024-06-01, 2024-05-01, 2024-04-01, 2024-03-01, 2024-02-01, 2024-01-01] |

    Scenario: Date sequence where step overshoots end
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-01-03', interval 1 month) AS result
        """
      Then query result
        | result       |
        | [2024-01-01] |

    Scenario: Date sequence with year interval
      When query
        """
        SELECT sequence(DATE'2020-01-01', DATE'2024-01-01', interval 1 year) AS result
        """
      Then query result
        | result                                                          |
        | [2020-01-01, 2021-01-01, 2022-01-01, 2023-01-01, 2024-01-01] |

    Scenario: Date sequence with year interval descending
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2020-01-01', interval -1 year) AS result
        """
      Then query result
        | result                                                          |
        | [2024-01-01, 2023-01-01, 2022-01-01, 2021-01-01, 2020-01-01] |

    Scenario: Date month interval with leap year end-of-month clamping
      When query
        """
        SELECT sequence(DATE'2024-01-31', DATE'2024-04-30', interval 1 month) AS result
        """
      Then query result
        | result                                              |
        | [2024-01-31, 2024-02-29, 2024-03-31, 2024-04-30] |

    Scenario: Date month interval with non-leap year end-of-month clamping
      When query
        """
        SELECT sequence(DATE'2023-01-31', DATE'2023-04-30', interval 1 month) AS result
        """
      Then query result
        | result                                              |
        | [2023-01-31, 2023-02-28, 2023-03-31, 2023-04-30] |

    Scenario: Date sequence with 3-month interval
      When query
        """
        SELECT sequence(DATE'2024-01-15', DATE'2024-07-15', interval 3 month) AS result
        """
      Then query result
        | result                                    |
        | [2024-01-15, 2024-04-15, 2024-07-15] |

    Scenario: Date same start and end returns single element
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-01-01') AS result
        """
      Then query result
        | result       |
        | [2024-01-01] |

    Scenario: Date same start and end with explicit step
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-01-01', interval 1 day) AS result
        """
      Then query result
        | result       |
        | [2024-01-01] |

    Scenario: Date spanning multiple months with default step
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-03-01') AS result
        """
      Then query result
        | result                                                                                                                                                                                                                                                                                                                                                                                                                |
        | [2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05, 2024-01-06, 2024-01-07, 2024-01-08, 2024-01-09, 2024-01-10, 2024-01-11, 2024-01-12, 2024-01-13, 2024-01-14, 2024-01-15, 2024-01-16, 2024-01-17, 2024-01-18, 2024-01-19, 2024-01-20, 2024-01-21, 2024-01-22, 2024-01-23, 2024-01-24, 2024-01-25, 2024-01-26, 2024-01-27, 2024-01-28, 2024-01-29, 2024-01-30, 2024-01-31, 2024-02-01, 2024-02-02, 2024-02-03, 2024-02-04, 2024-02-05, 2024-02-06, 2024-02-07, 2024-02-08, 2024-02-09, 2024-02-10, 2024-02-11, 2024-02-12, 2024-02-13, 2024-02-14, 2024-02-15, 2024-02-16, 2024-02-17, 2024-02-18, 2024-02-19, 2024-02-20, 2024-02-21, 2024-02-22, 2024-02-23, 2024-02-24, 2024-02-25, 2024-02-26, 2024-02-27, 2024-02-28, 2024-02-29, 2024-03-01] |

    Scenario: Date sequence with year-month interval across years
      When query
        """
        SELECT sequence(DATE'2024-01-01', DATE'2024-12-01', interval '1' month) AS result
        """
      Then query result
        | result                                                                                                                                                    |
        | [2024-01-01, 2024-02-01, 2024-03-01, 2024-04-01, 2024-05-01, 2024-06-01, 2024-07-01, 2024-08-01, 2024-09-01, 2024-10-01, 2024-11-01, 2024-12-01] |

  Rule: Timestamp sequences

    Scenario: Timestamp sequence with hourly interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 05:00:00', interval 1 hour) AS result
        """
      Then query result
        | result                                                                                                                                        |
        | [2024-01-01 00:00:00, 2024-01-01 01:00:00, 2024-01-01 02:00:00, 2024-01-01 03:00:00, 2024-01-01 04:00:00, 2024-01-01 05:00:00] |

    Scenario: Timestamp sequence with minute interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 00:05:00', interval 1 minute) AS result
        """
      Then query result
        | result                                                                                                                                        |
        | [2024-01-01 00:00:00, 2024-01-01 00:01:00, 2024-01-01 00:02:00, 2024-01-01 00:03:00, 2024-01-01 00:04:00, 2024-01-01 00:05:00] |

    Scenario: Timestamp sequence with 30-minute interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 02:00:00', interval 30 minute) AS result
        """
      Then query result
        | result                                                                                                                            |
        | [2024-01-01 00:00:00, 2024-01-01 00:30:00, 2024-01-01 01:00:00, 2024-01-01 01:30:00, 2024-01-01 02:00:00] |

    Scenario: Timestamp sequence with second interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 00:00:10', interval 3 second) AS result
        """
      Then query result
        | result                                                                                                        |
        | [2024-01-01 00:00:00, 2024-01-01 00:00:03, 2024-01-01 00:00:06, 2024-01-01 00:00:09] |

    Scenario: Timestamp sequence with default step spans days
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-03 00:00:00') AS result
        """
      Then query result
        | result                                                                        |
        | [2024-01-01 00:00:00, 2024-01-02 00:00:00, 2024-01-03 00:00:00] |

    Scenario: Timestamp sequence descending with default step
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-03 00:00:00', TIMESTAMP'2024-01-01 00:00:00') AS result
        """
      Then query result
        | result                                                                        |
        | [2024-01-03 00:00:00, 2024-01-02 00:00:00, 2024-01-01 00:00:00] |

    Scenario: Timestamp sequence with year-month interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-06-01', interval 1 month) AS result
        """
      Then query result
        | result                                                                                                                            |
        | [2024-01-01 00:00:00, 2024-02-01 00:00:00, 2024-03-01 00:00:00, 2024-04-01 00:00:00, 2024-05-01 00:00:00, 2024-06-01 00:00:00] |

    Scenario: Timestamp sequence with day interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-03', interval 1 day) AS result
        """
      Then query result
        | result                                                                        |
        | [2024-01-01 00:00:00, 2024-01-02 00:00:00, 2024-01-03 00:00:00] |

    Scenario: Timestamp descending with negative hour interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 05:00:00', TIMESTAMP'2024-01-01 00:00:00', interval -1 hour) AS result
        """
      Then query result
        | result                                                                                                                                        |
        | [2024-01-01 05:00:00, 2024-01-01 04:00:00, 2024-01-01 03:00:00, 2024-01-01 02:00:00, 2024-01-01 01:00:00, 2024-01-01 00:00:00] |

    Scenario: Timestamp descending with negative month interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-06-01', TIMESTAMP'2024-01-01', interval -1 month) AS result
        """
      Then query result
        | result                                                                                                                            |
        | [2024-06-01 00:00:00, 2024-05-01 00:00:00, 2024-04-01 00:00:00, 2024-03-01 00:00:00, 2024-02-01 00:00:00, 2024-01-01 00:00:00] |

    Scenario: Timestamp default step within same day returns only start
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 05:00:00') AS result
        """
      Then query result
        | result                  |
        | [2024-01-01 00:00:00] |

    Scenario: Timestamp descending default step within same day returns only start
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01 05:00:00', TIMESTAMP'2024-01-01 00:00:00') AS result
        """
      Then query result
        | result                  |
        | [2024-01-01 05:00:00] |

    Scenario: Timestamp same start and end returns single element
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-01') AS result
        """
      Then query result
        | result                  |
        | [2024-01-01 00:00:00] |

    Scenario: Timestamp same start and end with explicit step
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-01', interval 1 hour) AS result
        """
      Then query result
        | result                  |
        | [2024-01-01 00:00:00] |

    Scenario: sequence timestamp with day interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-05', INTERVAL 1 DAY) AS result
        """
      Then query result
        | result                                                                                            |
        | [2024-01-01 00:00:00, 2024-01-02 00:00:00, 2024-01-03 00:00:00, 2024-01-04 00:00:00, 2024-01-05 00:00:00] |

  Rule: Argument count validation

    Scenario: sequence zero arguments errors
      When query
        """
        SELECT sequence() AS result
        """
      Then query error .*

    Scenario: sequence one argument errors
      When query
        """
        SELECT sequence(1) AS result
        """
      Then query error .*

  Rule: NULL combinatorial

    Scenario: sequence NULL start
      When query
        """
        SELECT sequence(CAST(NULL AS INT), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence NULL stop
      When query
        """
        SELECT sequence(1, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence NULL step
      When query
        """
        SELECT sequence(1, 5, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence all NULL
      When query
        """
        SELECT sequence(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic integer sequences

    Scenario: sequence ascending
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence descending
      When query
        """
        SELECT sequence(5, 1) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: sequence equal start and stop
      When query
        """
        SELECT sequence(3, 3) AS result
        """
      Then query result
        | result |
        | [3]    |

    Scenario: sequence zero to zero
      When query
        """
        SELECT sequence(0, 0) AS result
        """
      Then query result
        | result |
        | [0]    |

  Rule: With explicit step

    Scenario: sequence with positive step
      When query
        """
        SELECT sequence(1, 10, 2) AS result
        """
      Then query result
        | result          |
        | [1, 3, 5, 7, 9] |

    Scenario: sequence with negative step
      When query
        """
        SELECT sequence(10, 1, -2) AS result
        """
      Then query result
        | result           |
        | [10, 8, 6, 4, 2] |

    Scenario: sequence step of one
      When query
        """
        SELECT sequence(1, 5, 1) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence step larger than range
      When query
        """
        SELECT sequence(1, 5, 10) AS result
        """
      Then query result
        | result |
        | [1]    |

    Scenario: sequence negative step larger than range
      When query
        """
        SELECT sequence(5, 1, -10) AS result
        """
      Then query result
        | result |
        | [5]    |

  Rule: Negative ranges

    Scenario: sequence negative ascending
      When query
        """
        SELECT sequence(-5, -1) AS result
        """
      Then query result
        | result               |
        | [-5, -4, -3, -2, -1] |

    Scenario: sequence negative descending
      When query
        """
        SELECT sequence(-1, -5) AS result
        """
      Then query result
        | result               |
        | [-1, -2, -3, -4, -5] |

  Rule: Type coercion

    Scenario: sequence BIGINT
      When query
        """
        SELECT sequence(CAST(1 AS BIGINT), CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence BIGINT descending
      When query
        """
        SELECT sequence(CAST(5 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: sequence TINYINT
      When query
        """
        SELECT sequence(CAST(1 AS TINYINT), CAST(5 AS TINYINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

  Rule: Multi-row

    Scenario: sequence multi-row
      When query
        """
        SELECT sequence(a, b) AS result FROM VALUES (1, 3), (5, 5), (3, 1) AS t(a, b)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | [5]       |
        | [3, 2, 1] |

  Rule: Error conditions

    Scenario: sequence step zero errors
      When query
        """
        SELECT sequence(1, 5, 0) AS result
        """
      Then query error .*

    Scenario: sequence step wrong direction errors
      When query
        """
        SELECT sequence(1, 5, -1) AS result
        """
      Then query error .*

    Scenario: sequence string input errors
      When query
        """
        SELECT sequence('a', 'z') AS result
        """
      Then query error .*

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    Scenario Outline: sequence loses non-nullability through Spark's implicit cast: <case>
      When query
        """
        SELECT sequence(<input>, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = <nullable>)
         |    |-- element: <element> (containsNull = false)
        """

      Examples:
        | case             | input | nullable | element |
        | no cast          | 1     | false    | integer |
        | STRING -> BIGINT | '1'   | true     | long    |
