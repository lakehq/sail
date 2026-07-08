@modulo
Feature: Spark-compatible modulo (% / mod / try_mod)

  # `%`/`mod` route through SparkModulo, honoring spark.sql.ansi.enabled:
  # remainder by zero raises REMAINDER_BY_ZERO under ANSI on and returns NULL
  # under ANSI off. `try_mod` is the ANSI-invariant `safe` face (always NULL on
  # zero). Integer/float keep their type (float % float stays float, never
  # double), and decimal follows Spark's remainder precision/scale rule. All
  # values validated against Spark 4.1.1. Paired ANSI on/off per scenario.

  Rule: Integer modulo
    Scenario: integer modulo
      When query
        """
        SELECT 10 % 3 AS result
        """
      Then query result
        | result |
        | 1      |

    # INT_MIN % -1 is defined as 0 (no overflow), unlike INT_MIN / -1.
    Scenario: INT_MIN modulo negative one is zero
      When query
        """
        SELECT CAST(-2147483648 AS INT) % CAST(-1 AS INT) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Integer modulo by zero raises under ANSI on, NULL under ANSI off
    Scenario: integer modulo by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query error (?i)remainder.*zero

    Scenario: integer modulo by zero returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Float and double modulo keep their type
    Scenario: double modulo double stays double
      When query
        """
        SELECT CAST(10.0 AS DOUBLE) % CAST(3.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 1.0    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: float modulo float stays float
      When query
        """
        SELECT CAST(10.0 AS FLOAT) % CAST(3.0 AS FLOAT) AS result
        """
      Then query result
        | result |
        | 1.0    |
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Double modulo by zero raises under ANSI on, NULL under ANSI off
    # Spark treats double remainder-by-zero like the integer case (governed by
    # the ANSI flag), not as NaN.
    Scenario: double modulo by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(10.0 AS DOUBLE) % CAST(0.0 AS DOUBLE) AS result
        """
      Then query error (?i)remainder.*zero

    Scenario: double modulo by zero returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(10.0 AS DOUBLE) % CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Decimal modulo follows Spark's remainder precision and scale
    Scenario: decimal modulo decimal keeps decimal type
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) % CAST(3.2 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | 0.90   |
      Then query schema
        """
        root
         |-- result: decimal(10,2) (nullable = true)
        """

    Scenario: integer modulo decimal narrows the literal
      When query
        """
        SELECT 7 % CAST(2.5 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | 2.00   |
      Then query schema
        """
        root
         |-- result: decimal(3,2) (nullable = true)
        """

    Scenario: double modulo decimal promotes to double
      When query
        """
        SELECT CAST(10.0 AS DOUBLE) % CAST(3 AS DECIMAL(10,2)) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Decimal modulo by zero raises under ANSI on, NULL under ANSI off
    Scenario: decimal modulo by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) % CAST(0.0 AS DECIMAL(10,2)) AS result
        """
      Then query error (?i)remainder.*zero

    Scenario: decimal modulo by zero returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) % CAST(0.0 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: try_mod tolerates remainder by zero as NULL in both ANSI modes
    Scenario: try_mod integer by zero is NULL under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_mod(10, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_mod integer by zero is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_mod(10, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_mod double by zero is NULL
      When query
        """
        SELECT try_mod(CAST(10.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_mod decimal by zero is NULL
      When query
        """
        SELECT try_mod(CAST(10.5 AS DECIMAL(10,2)), CAST(0.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: try_mod computes the remainder for non-zero divisors
    Scenario: try_mod double stays double
      When query
        """
        SELECT try_mod(CAST(10.0 AS DOUBLE), CAST(3.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 1.0    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Remainder sign follows the dividend (truncated division)
    Scenario: negative dividend gives a negative remainder
      When query
        """
        SELECT -10 % 3 AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: negative divisor is ignored for the sign
      When query
        """
        SELECT 10 % -3 AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Long modulo
    Scenario: LONG_MIN modulo negative one is zero
      When query
        """
        SELECT CAST(-9223372036854775808 AS BIGINT) % CAST(-1 AS BIGINT) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bigint modulo int widens to bigint
      When query
        """
        SELECT CAST(10 AS BIGINT) % CAST(3 AS INT) AS result
        """
      Then query result
        | result |
        | 1      |
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: Non-finite float operands return NaN, never an error (ANSI-invariant)
    # Only the integer/decimal/float divide-by-zero path is ANSI-gated; Inf/NaN
    # operands are ordinary IEEE 754 results in both ANSI modes.
    Scenario: infinity modulo two is NaN under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) % CAST(2.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity modulo two is NaN under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) % CAST(2.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: NaN modulo two is NaN
      When query
        """
        SELECT CAST('NaN' AS DOUBLE) % CAST(2.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: value modulo infinity is the value
      When query
        """
        SELECT CAST(5.0 AS DOUBLE) % CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 5.0    |

    Scenario: negative zero dividend normalizes to positive zero
      When query
        """
        SELECT CAST(-0.0 AS DOUBLE) % CAST(3.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 0.0    |

  Rule: NULL propagates
    Scenario: NULL dividend yields NULL
      When query
        """
        SELECT CAST(NULL AS INT) % 3 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL divisor yields NULL
      When query
        """
        SELECT 10 % CAST(NULL AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Per-row modulo by zero in a multi-row batch
    Scenario: mixed divisors null only the zero row under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a % b AS result FROM VALUES (10, 3), (7, 0), (8, 4) AS t(a, b) ORDER BY a
        """
      Then query result ordered
        | result |
        | NULL   |
        | 0      |
        | 1      |

    Scenario: mixed divisors raise for the whole batch under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a % b AS result FROM VALUES (10, 3), (7, 0), (8, 4) AS t(a, b) ORDER BY a
        """
      Then query error (?i)remainder.*zero

    Scenario: try_mod nulls only the zero row in both ANSI modes
      When query
        """
        SELECT try_mod(a, b) AS result FROM VALUES (10, 3), (7, 0), (8, 4) AS t(a, b) ORDER BY a
        """
      Then query result ordered
        | result |
        | NULL   |
        | 0      |
        | 1      |
