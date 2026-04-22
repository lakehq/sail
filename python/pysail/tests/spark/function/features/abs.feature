@abs
Feature: abs comprehensive tests

  Rule: Argument count validation

    Scenario: abs zero args errors
      When query
        """
        SELECT abs() AS result
        """
      Then query error .*

    Scenario: abs two args errors
      When query
        """
        SELECT abs(1, 2) AS result
        """
      Then query error .*

  Rule: NULL propagation

    Scenario: abs untyped NULL
      When query
        """
        SELECT abs(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed INT
      When query
        """
        SELECT abs(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed TINYINT
      When query
        """
        SELECT abs(CAST(NULL AS TINYINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed SMALLINT
      When query
        """
        SELECT abs(CAST(NULL AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed BIGINT
      When query
        """
        SELECT abs(CAST(NULL AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed FLOAT
      When query
        """
        SELECT abs(CAST(NULL AS FLOAT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed DOUBLE
      When query
        """
        SELECT abs(CAST(NULL AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed DECIMAL
      When query
        """
        SELECT abs(CAST(NULL AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: abs NULL typed INTERVAL DAY TO SECOND
      When query
        """
        SELECT abs(CAST(NULL AS INTERVAL DAY TO SECOND)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic integer types happy path

    Scenario: abs negative INT
      When query
        """
        SELECT abs(-5) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: abs positive INT
      When query
        """
        SELECT abs(5) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: abs zero INT
      When query
        """
        SELECT abs(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: abs negative TINYINT safe
      When query
        """
        SELECT abs(CAST(-127 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: abs negative SMALLINT safe
      When query
        """
        SELECT abs(CAST(-32767 AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | 32767  |

    Scenario: abs negative BIGINT safe
      When query
        """
        SELECT abs(CAST(-9223372036854775807 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

  Rule: Float and double values

    Scenario: abs negative DOUBLE
      When query
        """
        SELECT abs(CAST(-1.5 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: abs positive FLOAT
      When query
        """
        SELECT abs(CAST(1.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: abs DOUBLE negative zero
      When query
        """
        SELECT abs(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: abs FLOAT negative zero
      When query
        """
        SELECT abs(CAST(-0.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: abs DOUBLE NaN
      When query
        """
        SELECT abs(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: abs DOUBLE Infinity
      When query
        """
        SELECT abs(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: abs DOUBLE negative Infinity
      When query
        """
        SELECT abs(CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: Decimal values

    Scenario: abs negative DECIMAL
      When query
        """
        SELECT abs(CAST(-1.5 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: abs DECIMAL zero
      When query
        """
        SELECT abs(CAST(0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 0.00   |

    Scenario: abs DECIMAL very small
      When query
        """
        SELECT abs(CAST(-0.001 AS DECIMAL(10,3))) AS result
        """
      Then query result
        | result |
        | 0.001  |

    @sail-bug
    # JVM promotes precision during cast rounding → 10^37; Sail keeps 37 nines (mathematically correct)
    Scenario: abs DECIMAL 38,0 near max
      When query
        """
        SELECT abs(CAST(-9999999999999999999999999999999999999 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result                                  |
        | 10000000000000000000000000000000000000  |

    @sail-bug
    # JVM errors on overflow; Sail may silently succeed or diverge
    Scenario: abs DECIMAL 38,0 exceeds range errors
      When query
        """
        SELECT abs(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0))) AS result
        """
      Then query error .*

  Rule: Integer overflow under ANSI=false wraps to MIN
    # Two's-complement quirk: signed integer range is asymmetric (e.g. TINYINT
    # is [-128, 127]), so -MIN cannot be represented in the same width. Spark
    # under ANSI=false matches Java's Math.abs(int) and returns MIN itself
    # (wrap-around) instead of erroring. ANSI=true raises ARITHMETIC_OVERFLOW.

    @sail-bug
    Scenario: abs TINYINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-128 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | -128   |

    @sail-bug
    Scenario: abs SMALLINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-32768 AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | -32768 |

    @sail-bug
    Scenario: abs INT MIN via CAST wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-2147483648 AS INT)) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    @sail-bug
    Scenario: abs BIGINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |

    @sail-bug
    # Sail promotes the literal to BIGINT; JVM keeps INT and wraps to MIN
    Scenario: abs INT literal MIN preserves INT type and wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(-2147483648) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

  Rule: Integer overflow under ANSI=true errors

    Scenario: abs TINYINT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-128 AS TINYINT)) AS result
        """
      Then query error .*

    Scenario: abs INT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-2147483648 AS INT)) AS result
        """
      Then query error .*

    Scenario: abs BIGINT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query error .*

  Rule: String coercion under ANSI=false
    # JVM coerces STRING → DOUBLE before calling abs. Sail does not perform
    # this implicit coercion, so every scenario in this rule currently fails.

    @sail-bug
    Scenario: abs negative numeric string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('-5') AS result
        """
      Then query result
        | result |
        | 5.0    |

    @sail-bug
    Scenario: abs numeric string with decimal
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('5.5') AS result
        """
      Then query result
        | result |
        | 5.5    |

    @sail-bug
    Scenario: abs whitespace-padded numeric string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('  -5  ') AS result
        """
      Then query result
        | result |
        | 5.0    |

    @sail-bug
    Scenario: abs non-numeric string returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('hello') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: abs empty string returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: abs NaN string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('NaN') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: abs Infinity string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('Infinity') AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: Interval values
    # Sail does not support INTERVAL types in abs (rendering diverges from JVM)

    @sail-bug
    Scenario: abs negative INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '-5' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '5' DAY |

    @sail-bug
    Scenario: abs positive INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '5' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '5' DAY |

    @sail-bug
    Scenario: abs zero INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '0' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '0' DAY |

    @sail-bug
    Scenario: abs negative INTERVAL DAY TO SECOND
      When query
        """
        SELECT abs(INTERVAL '-1 02:03:04' DAY TO SECOND) AS result
        """
      Then query result
        | result                              |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

    @sail-bug
    Scenario: abs negative INTERVAL HOUR TO MINUTE
      When query
        """
        SELECT abs(INTERVAL '-1:30' HOUR TO MINUTE) AS result
        """
      Then query result
        | result                         |
        | INTERVAL '01:30' HOUR TO MINUTE |

  Rule: Multi-row vectorized path

    @sail-bug
    # Includes a row with INT MIN under ANSI=false — tests both vectorized path and wrap
    Scenario: abs INT column with NULL mix
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES (-5), (0), (5), (CAST(NULL AS INT)), (-2147483648) AS t(v)
        """
      Then query result
        | result      |
        | 5           |
        | 0           |
        | 5           |
        | NULL        |
        | -2147483648 |

    @sail-bug
    Scenario: abs INTERVAL DAY column with NULL mix
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES
          (INTERVAL '-5' DAY),
          (INTERVAL '0' DAY),
          (INTERVAL '10' DAY),
          (CAST(NULL AS INTERVAL DAY))
        AS t(v)
        """
      Then query result
        | result             |
        | INTERVAL '5' DAY   |
        | INTERVAL '0' DAY   |
        | INTERVAL '10' DAY  |
        | NULL               |

  Rule: Type rejection

    Scenario: abs on BOOLEAN errors
      When query
        """
        SELECT abs(true) AS result
        """
      Then query error .*

    Scenario: abs on DATE errors
      When query
        """
        SELECT abs(DATE '2024-01-15') AS result
        """
      Then query error .*

    Scenario: abs on TIMESTAMP errors
      When query
        """
        SELECT abs(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query error .*

    Scenario: abs on BINARY errors
      When query
        """
        SELECT abs(X'48656C6C6F') AS result
        """
      Then query error .*

    Scenario: abs on ARRAY errors
      When query
        """
        SELECT abs(array(1,2,3)) AS result
        """
      Then query error .*

    Scenario: abs on MAP errors
      When query
        """
        SELECT abs(map('a',1)) AS result
        """
      Then query error .*

    Scenario: abs on STRUCT errors
      When query
        """
        SELECT abs(named_struct('a',1)) AS result
        """
      Then query error .*
