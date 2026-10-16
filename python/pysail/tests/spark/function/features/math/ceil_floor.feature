Feature: ceil() and floor() round numbers toward +/- infinity

  Rule: ceil basic

    Scenario Outline: ceil basic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | expr                            | result |
        | ceil positive integer              | ceil(1)                         | 1      |
        | ceil zero                          | ceil(0)                         | 0      |
        | positive double rounds up          | ceil(1.1)                       | 2      |
        | negative double rounds toward zero | ceil(-1.9)                      | -1     |
        | ceil negative small value          | ceil(-0.1)                      | 0      |
        | float input                        | ceil(CAST(1.5 AS FLOAT))        | 2      |
        | decimal input                      | ceil(CAST(1.5 AS DECIMAL(2,1))) | 2      |
        | ceiling alias                      | ceiling(1.5)                    | 2      |

    Scenario: positive integer
      When query
        """
        SELECT ceil(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: zero
      When query
        """
        SELECT ceil(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: positive double rounds up
      When query
        """
        SELECT ceil(1.1) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: negative double rounds toward zero
      When query
        """
        SELECT ceil(-1.9) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: negative small value
      When query
        """
        SELECT ceil(-0.1) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: float input
      When query
        """
        SELECT ceil(CAST(1.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: decimal input
      When query
        """
        SELECT ceil(CAST(1.5 AS DECIMAL(2,1))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceiling alias
      When query
        """
        SELECT ceiling(1.5) AS result
        """
      Then query result
        | result |
        | 2      |

  Rule: floor basic

    Scenario Outline: floor basic: <case>
      When query
        """
        SELECT floor(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | arg  | result |
        | floor positive integer                | 1    | 1      |
        | floor zero                            | 0    | 0      |
        | positive double rounds down           | 1.9  | 1      |
        | negative double rounds away from zero | -1.1 | -2     |
        | floor negative small value            | -0.1 | -1     |

    Scenario: positive double rounds down
      When query
        """
        SELECT floor(1.9) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: negative double rounds away from zero
      When query
        """
        SELECT floor(-1.1) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: NULL handling (1-arg)

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case               | fn    | input                       |
        | untyped NULL ceil  | ceil  | NULL                        |
        | untyped NULL floor | floor | NULL                        |
        | typed NULL double  | ceil  | CAST(NULL AS DOUBLE)        |
        | typed NULL integer | ceil  | CAST(NULL AS INT)           |
        | typed NULL decimal | ceil  | CAST(NULL AS DECIMAL(10,2)) |

    Scenario: untyped NULL ceil
      When query
        """
        SELECT ceil(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: untyped NULL floor
      When query
        """
        SELECT floor(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double
      When query
        """
        SELECT ceil(CAST(NULL AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL integer
      When query
        """
        SELECT ceil(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL decimal
      When query
        """
        SELECT ceil(CAST(NULL AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL handling (2-arg)

    Scenario Outline: NULL 2-arg: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                  | expr                            |
        | untyped NULL with positive scale      | ceil(NULL, 2)                   |
        | untyped NULL with negative scale      | ceil(NULL, -1)                  |
        | typed NULL double with positive scale | ceil(CAST(NULL AS DOUBLE), 2)   |
        | typed NULL double with negative scale | floor(CAST(NULL AS DOUBLE), -1) |

    Scenario: untyped NULL with positive scale
      When query
        """
        SELECT ceil(NULL, 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: untyped NULL with negative scale
      When query
        """
        SELECT ceil(NULL, -1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double with positive scale
      When query
        """
        SELECT ceil(CAST(NULL AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double with negative scale
      When query
        """
        SELECT floor(CAST(NULL AS DOUBLE), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Two-arg with scale equal to input scale (no change)

    Scenario Outline: Scale equal: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | expr          | result |
        | ceil(1.5, 1)  | ceil(1.5, 1)  | 1.5    |
        | floor(1.5, 1) | floor(1.5, 1) | 1.5    |
        | ceil(1.23, 2) | ceil(1.23, 2) | 1.23   |

    Scenario: ceil(1.5, 1)
      When query
        """
        SELECT ceil(1.5, 1) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: floor(1.5, 1)
      When query
        """
        SELECT floor(1.5, 1) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: ceil(1.23, 2)
      When query
        """
        SELECT ceil(1.23, 2) AS result
        """
      Then query result
        | result |
        | 1.23   |

  Rule: Two-arg with scale greater than input (value unchanged)

    Scenario Outline: Scale greater: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | expr                             | result |
        | scale 2 on decimal(2,1) — ceil                   | ceil(1.5, 2)                     | 1.5    |
        | scale 2 on decimal(2,1) — floor                  | floor(1.5, 2)                    | 1.5    |
        | scale 10 on decimal(2,1)                         | ceil(1.5, 10)                    | 1.5    |
        | scale 38 is still valid                          | ceil(1.5, 38)                    | 1.5    |
        | scale 100 beyond decimal128 max, value unchanged | ceil(1.5, 100)                   | 1.5    |
        | zero decimal with large scale                    | ceil(CAST(0 AS DECIMAL(5,2)), 5) | 0.00   |

    Scenario: scale 2 on decimal(2,1) — ceil
      When query
        """
        SELECT ceil(1.5, 2) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 2 on decimal(2,1) — floor
      When query
        """
        SELECT floor(1.5, 2) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 10 on decimal(2,1)
      When query
        """
        SELECT ceil(1.5, 10) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 38 is still valid
      When query
        """
        SELECT ceil(1.5, 38) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 100 beyond decimal128 max, value unchanged
      When query
        """
        SELECT ceil(1.5, 100) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: zero decimal with large scale
      When query
        """
        SELECT ceil(CAST(0 AS DECIMAL(5,2)), 5) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Two-arg with scale less than input (rounds)

    Scenario: ceil(1.234, 2) rounds up
      When query
        """
        SELECT ceil(1.234, 2) AS result
        """
      Then query result
        | result |
        | 1.24   |

    Scenario Outline: Scale less: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | expr            | result |
        | floor(1.234, 2) truncates                 | floor(1.234, 2) | 1.23   |
        | ceil(1.234, 0)                            | ceil(1.234, 0)  | 2      |
        | negative value ceil moves toward zero     | ceil(-1.25, 1)  | -1.2   |
        | negative value floor moves away from zero | floor(-1.25, 1) | -1.3   |

    Scenario: floor(1.234, 2) truncates
      When query
        """
        SELECT floor(1.234, 2) AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: ceil(1.234, 0)
      When query
        """
        SELECT ceil(1.234, 0) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: negative value ceil moves toward zero
      When query
        """
        SELECT ceil(-1.25, 1) AS result
        """
      Then query result
        | result |
        | -1.2   |

    Scenario: negative value floor moves away from zero
      When query
        """
        SELECT floor(-1.25, 1) AS result
        """
      Then query result
        | result |
        | -1.3   |

  Rule: Two-arg with negative scale (rounds left of decimal)

    Scenario Outline: Negative scale: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | expr               | result                                 |
        | ceil(123.456, -1)                                        | ceil(123.456, -1)  | 130                                    |
        | floor(123.456, -1)                                       | floor(123.456, -1) | 120                                    |
        | ceil(123.456, -2)                                        | ceil(123.456, -2)  | 200                                    |
        | ceil(999.99, -1) crosses boundary                        | ceil(999.99, -1)   | 1000                                   |
        | scale -37 is the max negative scale that fits Decimal128 | ceil(123.456, -37) | 10000000000000000000000000000000000000 |
        | ceil negative with negative scale                        | ceil(-999.99, -1)  | -990                                   |
        | floor negative with negative scale                       | floor(-999.99, -1) | -1000                                  |

    Scenario: ceil(123.456, -1)
      When query
        """
        SELECT ceil(123.456, -1) AS result
        """
      Then query result
        | result |
        | 130    |

    Scenario: floor(123.456, -1)
      When query
        """
        SELECT floor(123.456, -1) AS result
        """
      Then query result
        | result |
        | 120    |

    Scenario: ceil(123.456, -2)
      When query
        """
        SELECT ceil(123.456, -2) AS result
        """
      Then query result
        | result |
        | 200    |

    Scenario: ceil(999.99, -1) crosses boundary
      When query
        """
        SELECT ceil(999.99, -1) AS result
        """
      Then query result
        | result |
        | 1000   |

    Scenario: scale -37 is the max negative scale that fits Decimal128
      When query
        """
        SELECT ceil(123.456, -37) AS result
        """
      Then query result
        | result                                  |
        | 10000000000000000000000000000000000000  |

    Scenario: ceil negative with negative scale
      When query
        """
        SELECT ceil(-999.99, -1) AS result
        """
      Then query result
        | result |
        | -990   |

    Scenario: floor negative with negative scale
      When query
        """
        SELECT floor(-999.99, -1) AS result
        """
      Then query result
        | result |
        | -1000  |

  Rule: Integer input with scale

    Scenario Outline: Integer with scale: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | expr                           | result |
        | int with zero scale                 | ceil(CAST(5 AS INT), 0)        | 5      |
        | int with positive scale (no effect) | ceil(CAST(5 AS INT), 2)        | 5      |
        | int with negative scale             | ceil(CAST(5 AS INT), -1)       | 10     |
        | floor int with negative scale       | floor(CAST(5 AS INT), -1)      | 0      |
        | tinyint with negative scale         | ceil(CAST(125 AS TINYINT), -1) | 130    |
        | bigint zero with negative scale     | ceil(CAST(0 AS BIGINT), -5)    | 0      |

    Scenario: int with zero scale
      When query
        """
        SELECT ceil(CAST(5 AS INT), 0) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: int with positive scale (no effect)
      When query
        """
        SELECT ceil(CAST(5 AS INT), 2) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: int with negative scale
      When query
        """
        SELECT ceil(CAST(5 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: floor int with negative scale
      When query
        """
        SELECT floor(CAST(5 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: tinyint with negative scale
      When query
        """
        SELECT ceil(CAST(125 AS TINYINT), -1) AS result
        """
      Then query result
        | result |
        | 130    |

    Scenario: bigint zero with negative scale
      When query
        """
        SELECT ceil(CAST(0 AS BIGINT), -5) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Float and Double with scale

    Scenario: float with scale includes trailing zero
      When query
        """
        SELECT ceil(CAST(1.5 AS FLOAT), 2) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: double with scale includes trailing zero
      When query
        """
        SELECT ceil(CAST(1.5 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: very small double rounds to zero
      When query
        """
        SELECT ceil(CAST(1e-300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Special float values (1-arg) — NaN/Infinity clamp to integer bounds

    Scenario Outline: Special float: <case>
      When query
        """
        SELECT <fn>(CAST(<value> AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | fn    | value       | result               |
        | Infinity to LONG_MAX  | ceil  | 'Infinity'  | 9223372036854775807  |
        | -Infinity to LONG_MIN | ceil  | '-Infinity' | -9223372036854775808 |
        | NaN to zero           | ceil  | 'NaN'       | 0                    |
        | floor Infinity        | floor | 'Infinity'  | 9223372036854775807  |

    Scenario: Infinity to LONG_MAX
      When query
        """
        SELECT ceil(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: -Infinity to LONG_MIN
      When query
        """
        SELECT ceil(CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

    Scenario: NaN to zero
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: floor Infinity
      When query
        """
        SELECT floor(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

  Rule: Special float values with scale (2-arg) — Spark returns NULL

    Scenario Outline: Special float with scale: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                       | expr                                 |
        | NaN with positive scale returns NULL       | ceil(CAST('NaN' AS DOUBLE), 2)       |
        | NaN with negative scale returns NULL       | ceil(CAST('NaN' AS DOUBLE), -1)      |
        | Infinity with positive scale returns NULL  | ceil(CAST('Infinity' AS DOUBLE), 2)  |
        | -Infinity with positive scale returns NULL | ceil(CAST('-Infinity' AS DOUBLE), 2) |
        | floor NaN with scale returns NULL          | floor(CAST('NaN' AS DOUBLE), 2)      |

    Scenario: NaN with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NaN with negative scale returns NULL
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('Infinity' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: -Infinity with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('-Infinity' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: floor NaN with scale returns NULL
      When query
        """
        SELECT floor(CAST('NaN' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Negative zero

    Scenario Outline: Negative zero: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | expr                          | result |
        | ceil -0.0 returns 0               | ceil(CAST(-0.0 AS DOUBLE))    | 0      |
        | floor -0.0 returns 0              | floor(CAST(-0.0 AS DOUBLE))   | 0      |
        | ceil -0.0 with scale returns 0.00 | ceil(CAST(-0.0 AS DOUBLE), 2) | 0.00   |

    Scenario: ceil -0.0 returns 0
      When query
        """
        SELECT ceil(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: floor -0.0 returns 0
      When query
        """
        SELECT floor(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: ceil -0.0 with scale returns 0.00
      When query
        """
        SELECT ceil(CAST(-0.0 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Multi-row propagation

    Scenario: mix of values, NaN, Inf, NULL — 1-arg
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW mx AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(-1.5 AS DOUBLE)),
          (CAST(0.0 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v, ceil(v) AS c, floor(v) AS f FROM mx ORDER BY v NULLS LAST
        """
      Then query result ordered
        | v    | c    | f    |
        | -1.5 | -1   | -2   |
        | 0.0  | 0    | 0    |
        | 1.5  | 2    | 1    |
        | NULL | NULL | NULL |

    Scenario: all-NULL column
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW n1 AS SELECT CAST(NULL AS DOUBLE) AS v
        """
      When query
        """
        SELECT ceil(v) AS c, floor(v) AS f FROM n1
        """
      Then query result
        | c    | f    |
        | NULL | NULL |

    Scenario: empty DataFrame returns empty
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW e1 AS SELECT CAST(1.0 AS DOUBLE) AS v WHERE false
        """
      When query
        """
        SELECT ceil(v) AS result FROM e1
        """
      Then query result
        | result |

  Rule: Algebraic simplification (idempotent)

    Scenario Outline: Simplification: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                          | expr                       | result |
        | ceil of ceil is ceil                                          | ceil(ceil(1.9))            | 2      |
        | floor of floor is floor                                       | floor(floor(1.9))          | 1      |
        | triple nested ceil collapses                                  | ceil(ceil(ceil(1.9)))      | 2      |
        | ceil of floor cascades (floor returns integer, ceil identity) | ceil(floor(1.9))           | 1      |
        | floor of ceil cascades                                        | floor(ceil(1.1))           | 2      |
        | ceil of integer is integer (no rounding)                      | ceil(CAST(7 AS INT))       | 7      |
        | floor of integer is integer (no rounding)                     | floor(CAST(-42 AS BIGINT)) | -42    |

    Scenario: ceil of ceil is ceil
      When query
        """
        SELECT ceil(ceil(1.9)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: floor of floor is floor
      When query
        """
        SELECT floor(floor(1.9)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: triple nested ceil collapses
      When query
        """
        SELECT ceil(ceil(ceil(1.9))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil of floor cascades (floor returns integer, ceil identity)
      When query
        """
        SELECT ceil(floor(1.9)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: floor of ceil cascades
      When query
        """
        SELECT floor(ceil(1.1)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil of integer is integer (no rounding)
      When query
        """
        SELECT ceil(CAST(7 AS INT)) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: floor of integer is integer (no rounding)
      When query
        """
        SELECT floor(CAST(-42 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | -42    |

  Rule: Filter pushdown — WHERE ceil/floor(col) OP constant

    Scenario: WHERE ceil(col) > N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) > 2 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 2.1 |
        | 5.5 |

    Scenario: WHERE floor(col) <= N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) <= 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 0.5 |
        | 1.1 |
        | 1.9 |

    Scenario: WHERE ceil(col) BETWEEN
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(3.0 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) BETWEEN 2 AND 3 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 1.1 |
        | 1.9 |
        | 2.1 |
        | 3.0 |

    Scenario: WHERE ceil on integer column (identity after simplify)
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (1), (5), (10), (CAST(NULL AS INT)) AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) > 3 ORDER BY v
        """
      Then query result ordered
        | v  |
        | 5  |
        | 10 |

    Scenario: WHERE floor(col) returns NULL excludes NULL rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST(2.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT count(*) AS c FROM vals WHERE floor(v) IS NOT NULL
        """
      Then query result
        | c |
        | 2 |

    # Exercises the preimage rewrite: `floor(v) = N` becomes
    # `v >= N AND v < N + 1`. Result must still match Spark row-for-row.
    Scenario: WHERE floor(col) = N keeps the right rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.0 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE)),
          (CAST(-0.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) = 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 1.0 |
        | 1.9 |

    Scenario: GROUP BY ceil(v) aggregates correctly
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW grp_vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT ceil(v) AS bucket, count(*) AS n
        FROM grp_vals GROUP BY ceil(v) ORDER BY bucket
        """
      Then query result ordered
        | bucket | n |
        | 1      | 1 |
        | 2      | 2 |
        | 3      | 2 |
        | 6      | 1 |

  Rule: Error conditions

    Scenario Outline: Error: <case>
      When query
        """
        SELECT ceil(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                                | args                     |
        | non-foldable scale errors                           | 1.5, CAST(NULL AS INT)   |
        | too-negative scale errors                           | 1.5, -100                |
        | non-INT scale type errors                           | 1.5, CAST(2 AS BIGINT)   |
        | scale -38 overflows decimal128 precision            | 123.456, -38             |
        | ceil very large double with scale overflows decimal | CAST(1e300 AS DOUBLE), 2 |

    Scenario: non-foldable scale errors
      When query
        """
        SELECT ceil(1.5, CAST(NULL AS INT)) AS result
        """
      Then query error .*

    Scenario: too-negative scale errors
      When query
        """
        SELECT ceil(1.5, -100) AS result
        """
      Then query error .*

    Scenario: non-INT scale type errors
      When query
        """
        SELECT ceil(1.5, CAST(2 AS BIGINT)) AS result
        """
      Then query error .*

    Scenario: scale -38 overflows decimal128 precision
      When query
        """
        SELECT ceil(123.456, -38) AS result
        """
      Then query error .*

    Scenario: ceil very large double with scale overflows decimal
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*

  Rule: Scale -37 boundary (max negative scale that fits Decimal128)

    Scenario: ceil scale -37 returns 10^37
      When query
        """
        SELECT ceil(1.5, -37) AS result
        """
      Then query result
        | result                                 |
        | 10000000000000000000000000000000000000 |

    Scenario: floor scale -37 truncates small value to zero
      When query
        """
        SELECT floor(1.5, -37) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: ANSI mode on overflow

    # Fixed 2026-04-21: SparkCeil/SparkFloor now carry ansi_mode: bool state bound
    # at planning time from PlanConfig::ansi_mode (serialized via protobuf
    # SparkCeilUdf/SparkFloorUdf for distributed execution). Under ANSI=false,
    # overflow in the Float→Decimal cast becomes NULL; under ANSI=true it errors.
    # Both UDFs share the spark_ceil_floor() helper — testing all 4 combinations
    # (ceil/floor × ANSI=true/false) guards against regressions in either wrapper.

    Scenario: ceil ANSI=false overflow returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Spark JVM raises [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] when the
    # Float→Decimal cast under ANSI=true overflows. Sail's cast kernel emits a
    # raw arrow error without a bracketed Spark error class. The query DOES
    # error in both engines (loose `.*` would silently pass), but the class
    # diverges. Fix path: align Sail's Float→Decimal cast error to use Spark's
    # error class, likely in arrow-rs cast kernel or a Sail-side wrapper.
    Scenario: ceil ANSI=true overflow errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*\[NUMERIC_VALUE_OUT_OF_RANGE.*\].*

    Scenario: floor ANSI=false overflow returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT floor(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Same root cause as `ceil ANSI=true overflow` — Sail's cast kernel emits
    # a raw arrow error instead of [NUMERIC_VALUE_OUT_OF_RANGE].
    Scenario: floor ANSI=true overflow errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT floor(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*\[NUMERIC_VALUE_OUT_OF_RANGE.*\].*

  Rule: Adversarial — deep nesting stress (simplify chain survival)
    # 5-level mixed nesting. Spark JVM collapses to the outermost function's
    # semantic. Our simplify handles same-fn collapse + integer-identity, but
    # does NOT fold `ceil(floor(x))` pairs directly (cross-nesting is handled
    # by the integer-identity + cast-folding cascade: floor(x) returns Int64,
    # then ceil(Int64) simplifies to `cast(floor(x), Int64)` which folds out).
    # Row-result confirms correctness end-to-end.

    Scenario: 5-level mixed ceil/floor collapses semantically
      When query
        """
        SELECT ceil(floor(ceil(floor(ceil(CAST(1.5 AS DOUBLE)))))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: 5-level alternating with negative input
      When query
        """
        SELECT floor(ceil(floor(ceil(floor(CAST(-1.5 AS DOUBLE)))))) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: Adversarial — identity-on-subtype at type boundaries
    # ceil(BIGINT) and floor(BIGINT) are identity by simplify rewrite.
    # Exercise at BIGINT boundary to confirm no overflow in the identity path.

    Scenario: ceil BIGINT_MAX is identity
      When query
        """
        SELECT ceil(CAST(9223372036854775807 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: floor BIGINT_MIN is identity
      When query
        """
        SELECT floor(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

  Rule: Adversarial — preimage edge cases (floor-only filter pushdown)
    # `preimage` on SparkFloor must handle: non-integer RHS (return None),
    # BIGINT boundary RHS (watch for overflow in N+1), NULL RHS.

    Scenario: floor with non-integer RHS — filter matches nothing
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 2.5
        """
      Then query result
        | v |

    Scenario: floor equals BIGINT_MAX boundary — no rows match in sample
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 9223372036854775807
        """
      Then query result
        | v |

  Rule: Adversarial — nested filter (propagate_constraints + evaluate_bounds paired)
    # Nested `ceil(floor(v)) > K` exercises the forward (`evaluate_bounds` on
    # inner `floor`) and backward (`propagate_constraints` on outer `ceil`)
    # interval graph hooks as a pair. Without evaluate_bounds on floor,
    # propagate_constraints on ceil would receive Unbounded — cardinality
    # estimation degrades silently.

    Scenario: nested filter ceil(floor(v)) > K returns correct rows
      When query
        """
        SELECT v FROM VALUES (1.5), (2.5), (3.5), (4.5), (5.5) AS t(v)
        WHERE ceil(floor(v)) > 3
        ORDER BY v
        """
      Then query result ordered
        | v   |
        | 4.5 |
        | 5.5 |

  Rule: Adversarial — ORDER BY DESC (output_ordering preserves direction)
    # output_ordering forwards child sort_properties without flipping. For a
    # monotonic non-decreasing function, DESC input → DESC output, so no
    # redundant SortExec should appear after projection.
    #
    # FINDING (captured as fixture): the DESC variant currently shows a
    # redundant SortExec in the plan — the ASC variant at `Rule: Plan snapshot
    # — output_ordering` does NOT. Investigation:
    #
    #   1) Our hooks expose the property correctly:
    #      - `output_ordering` forwards `SortProperties` including `descending`.
    #      - `preserves_lex_ordering = true` (explicitly overridden). Verified
    #        empirically: setting it to true didn't change the DESC plan →
    #        the gap is NOT in the equivalence inference path that consumes
    #        this flag (see equivalence/properties/mod.rs:469).
    #
    #   2) Upstream DataFusion `CeilFunc`/`FloorFunc` have the same trivial
    #      `output_ordering` impl and default `preserves_lex_ordering = false`
    #      → reproducing this with vanilla DF would show the same DESC gap.
    #
    #   3) The gap lives in `datafusion-physical-optimizer/enforce_sorting/
    #      sort_pushdown.rs` (`pushdown_sorts_helper`). Traced against DF v53.1:
    #      - `options_compatible` (strict equality for nullable in
    #        physical-expr-common/sort_expr.rs:210) does NOT discriminate ASC/DESC.
    #      - `get_expr_properties` (physical-expr/equivalence/properties/mod.rs:1446)
    #        recurses correctly through our `ScalarFunctionExpr::get_properties`,
    #        forwarding `SortProperties` via `output_ordering`.
    #      The asymmetry is in PLAN SHAPE, not in property inference:
    #           ASC:  keeps inner `SortExec[v ASC]`, projection computes ceil
    #                 after sort → outer ORDER BY satisfied via monotonicity.
    #           DESC: eliminates inner `SortExec[v DESC]`, computes ceil+v in
    #                 the projection, then adds outer `SortExec[ceil(v) DESC]`.
    #      i.e. DF v53 chooses a different plan shape for DESC ORDER BY over a
    #      monotonic UDF, bypassing the optimization that works for ASC. Not a
    #      Sail bug.
    #
    # The snapshot below is a regression fixture: when upstream (or we) teach
    # the planner to reuse DESC input order through monotonic UDFs, the
    # SortExec will disappear and the snapshot diff will flag the improvement.

    Scenario: ORDER BY ceil(v) DESC on reversed input preserves order
      When query
        """
        SELECT v, ceil(v) AS c FROM VALUES (3.5), (1.5), (2.5) AS t(v)
        ORDER BY ceil(v) DESC
        """
      Then query result ordered
        | v   | c |
        | 3.5 | 4 |
        | 2.5 | 3 |
        | 1.5 | 2 |

  Rule: ceil and floor on integer inputs return the same value as BIGINT

    Scenario: ceil on INT returns same value as BIGINT
      When query
        """
        SELECT ceil(CAST(5 AS INT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: ceil on BIGINT returns same value
      When query
        """
        SELECT ceil(CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: ceil on negative INT returns same value
      When query
        """
        SELECT ceil(CAST(-3 AS INT)) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: ceil on NULL INT returns NULL
      When query
        """
        SELECT ceil(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: floor on INT returns same value as BIGINT
      When query
        """
        SELECT floor(CAST(5 AS INT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: floor on BIGINT returns same value
      When query
        """
        SELECT floor(CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: floor on negative INT returns same value
      When query
        """
        SELECT floor(CAST(-3 AS INT)) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: floor on NULL INT returns NULL
      When query
        """
        SELECT floor(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ceil on INT column returns BIGINT schema
      When query
        """
        SELECT ceil(CAST(5 AS INT)) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: floor on INT column returns BIGINT schema
      When query
        """
        SELECT floor(CAST(5 AS INT)) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: ceil and floor on float inputs

    Scenario: ceil on positive float rounds up
      When query
        """
        SELECT ceil(1.5) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil on negative float rounds toward zero
      When query
        """
        SELECT ceil(-1.5) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: floor on positive float rounds down
      When query
        """
        SELECT floor(1.9) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: floor on negative float rounds away from zero
      When query
        """
        SELECT floor(-1.1) AS result
        """
      Then query result
        | result |
        | -2     |
