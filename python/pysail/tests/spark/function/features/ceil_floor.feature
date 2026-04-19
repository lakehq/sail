@ceil @floor
Feature: ceil() and floor() round numbers toward +/- infinity

  Rule: ceil basic

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

    Scenario: positive integer
      When query
        """
        SELECT floor(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: zero
      When query
        """
        SELECT floor(0) AS result
        """
      Then query result
        | result |
        | 0      |

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

    Scenario: negative small value
      When query
        """
        SELECT floor(-0.1) AS result
        """
      Then query result
        | result |
        | -1     |

  Rule: NULL handling (1-arg)

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

  Rule: Special float values (1-arg returns zero/LONG_MAX/LONG_MIN)

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

    Scenario: floor NaN to zero
      When query
        """
        SELECT floor(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Special float values with scale (2-arg) — Spark returns NULL

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

    Scenario: ceil very large double with scale overflows decimal
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*

  Rule: Negative zero

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

    Scenario: ceil of ceil is ceil
      When query
        """
        SELECT ceil(ceil(1.9)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil of ceil negative
      When query
        """
        SELECT ceil(ceil(-1.9)) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: floor of floor is floor
      When query
        """
        SELECT floor(floor(1.9)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: floor of floor negative
      When query
        """
        SELECT floor(floor(-1.1)) AS result
        """
      Then query result
        | result |
        | -2     |

    Scenario: triple nested ceil collapses
      When query
        """
        SELECT ceil(ceil(ceil(1.9))) AS result
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

  Rule: Plan snapshot — filter pushdown on Parquet (propagate_constraints)

    @sail-only
    Scenario: EXPLAIN SELECT from Parquet with ceil filter shows pushdown
      Given variable location for temporary directory explain_ceil_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_ceil_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_ceil_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_ceil_parquet WHERE ceil(v) > 2
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN SELECT from Parquet with floor filter shows pushdown
      Given variable location for temporary directory explain_floor_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_floor_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_floor_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_floor_parquet WHERE floor(v) <= 1
        """
      Then query plan matches snapshot

  Rule: Plan snapshot — filter pushdown

    @sail-only
    Scenario: EXPLAIN WHERE ceil(col) > N
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE ceil(v) > 2
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE floor(col) <= N
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) <= 1
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE ceil(col) BETWEEN
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE ceil(v) BETWEEN 2 AND 3
        """
      Then query plan matches snapshot

  Rule: Error conditions

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
