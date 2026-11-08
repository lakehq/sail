Feature: pmod of DECIMAL values

  # Spark's `Pmod.resultDecimalType` follows the `Remainder` rule on the declared argument
  # types: scale = max(s1, s2) and precision = min(p1 - s1, p2 - s2) + scale.
  Rule: The result type of DECIMAL arguments follows Spark's remainder rule

    Scenario: pmod of DECIMAL literals with different precision and scale
      When query
        """
        SELECT
          pmod(CAST(7.5 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(3,1))) AS a,
          pmod(CAST(-7.5 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(3,1))) AS b,
          pmod(CAST(-7 AS DECIMAL(10,0)), CAST(3.25 AS DECIMAL(4,2))) AS c
        """
      Then query schema
        """
        root
         |-- a: decimal(4,2) (nullable = true)
         |-- b: decimal(4,2) (nullable = true)
         |-- c: decimal(4,2) (nullable = true)
        """
      Then query result
        | a    | b    | c    |
        | 1.50 | 0.50 | 2.75 |

    Scenario: pmod of DECIMAL literals with a large scale
      When query
        """
        SELECT
          pmod(CAST(123.456 AS DECIMAL(6,3)), CAST(10 AS DECIMAL(2,0))) AS a,
          pmod(CAST(-1 AS DECIMAL(38,18)), CAST(0.3 AS DECIMAL(2,1))) AS b
        """
      Then query schema
        """
        root
         |-- a: decimal(5,3) (nullable = true)
         |-- b: decimal(19,18) (nullable = true)
        """
      Then query result
        | a     | b                    |
        | 3.456 | 0.200000000000000000 |

    Scenario: pmod of DECIMAL columns
      When query
        """
        SELECT pmod(x, y) AS a
        FROM VALUES
          (CAST(-9.99 AS DECIMAL(4,2)), CAST(4.5 AS DECIMAL(2,1))),
          (CAST(9.99 AS DECIMAL(4,2)), CAST(-4.5 AS DECIMAL(2,1))),
          (CAST(NULL AS DECIMAL(4,2)), CAST(1 AS DECIMAL(2,1)))
        AS t(x, y)
        """
      Then query schema
        """
        root
         |-- a: decimal(3,2) (nullable = true)
        """
      Then query result ordered
        | a    |
        | 3.51 |
        | 0.99 |
        | NULL |

  Rule: A DECIMAL zero divisor

    Scenario: ANSI on: pmod by a DECIMAL zero is an error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(CAST(5 AS DECIMAL(3,0)), CAST(0 AS DECIMAL(3,1))) AS a
        """
      Then query error (?i)by zero

    Scenario: ANSI off: pmod by a DECIMAL zero is NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(CAST(5 AS DECIMAL(3,0)), CAST(0 AS DECIMAL(3,1))) AS a
        """
      Then query result
        | a    |
        | NULL |

  Rule: A NULL dividend with a zero divisor

    # Spark's remainder is null-intolerant, so a NULL dividend gives NULL without raising, even
    # with a zero divisor under ANSI mode.
    Scenario: ANSI on: pmod of NULL by zero is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          pmod(CAST(NULL AS INT), 0) AS a,
          pmod(CAST(NULL AS DOUBLE), 0.0D) AS b,
          pmod(CAST(NULL AS DECIMAL(3,1)), CAST(0 AS DECIMAL(2,0))) AS c
        """
      Then query result
        | a    | b    | c    |
        | NULL | NULL | NULL |

    Scenario: ANSI on: pmod of NULL columns by zero columns is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(x, 0) AS a, pmod(x, z) AS b FROM VALUES (CAST(NULL AS INT), 0), (CAST(NULL AS INT), 0) AS t(x, z)
        """
      Then query result
        | a    | b    |
        | NULL | NULL |
        | NULL | NULL |
