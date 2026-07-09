@arithmetic_coercion
Feature: Spark type coercion for the +, -, * operators

  # DataFusion's BinaryTypeCoercer does not perform these Spark coercions; Sail
  # applies them in the arithmetic plan builders (math.rs) so the *result type*
  # matches Spark 4.1.1. Values and precision/scale are asserted below via typeof.
  #
  # Known remaining gap (see the @sail-bug scenario): Spark marks decimal
  # arithmetic nullable=true (overflow may yield NULL) even for non-null operands,
  # while a native BinaryExpr inherits its operands' nullability (=> false).
  # Matching Spark's nullability needs the custom PhysicalExpr follow-up.

  Rule: Decimal with an integer literal narrows the literal to minimal precision
    Scenario: decimal times a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * 3 AS r
        """
      Then query result
        | t             | r    |
        | decimal(12,2) | 7.50 |

    Scenario: decimal plus a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) + 3) AS t,
               CAST(2.5 AS DECIMAL(10,2)) + 3 AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 5.50 |

    Scenario: decimal minus a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(5.5 AS DECIMAL(10,2)) - 2) AS t,
               CAST(5.5 AS DECIMAL(10,2)) - 2 AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 3.50 |

    Scenario: decimal times a three-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 100) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * 100 AS r
        """
      Then query result
        | t             | r      |
        | decimal(14,2) | 250.00 |

  Rule: Float or double combined with a decimal promotes to double
    Scenario: float times decimal returns double
      When query
        """
        SELECT typeof(CAST(1.5 AS FLOAT) * CAST(2.0 AS DECIMAL(10,2))) AS t,
               CAST(1.5 AS FLOAT) * CAST(2.0 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r   |
        | double | 3.0 |

  Rule: Decimal arithmetic is nullable in Spark (known gap — needs custom PhysicalExpr)
    # Spark marks decimal +, -, * as nullable=true even for non-null operands,
    # because the operation can overflow to NULL. A native BinaryExpr built in the
    # plan builder inherits nullability from its operands, so Sail reports false.
    @sail-bug
    Scenario: decimal arithmetic reports nullable=true like Spark
      When query
        """
        SELECT CAST(2.5 AS DECIMAL(10,2)) * 3 AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(12,2) (nullable = true)
        """

  Rule: Further Spark coercion divergences beyond + - * (bug-hunt, not yet implemented)
    # Validated against Spark 4.1.1. Division/modulo/precision-cap cases the current
    # plan-builder coercion does not cover yet.

    @sail-bug
    Scenario: decimal divided by decimal uses Spark's division scale rule
      # Spark: scale = max(6, s1 + p2 + 1), precision = p1 - s1 + s2 + scale => decimal(23,13).
      # Sail currently produces decimal(16,6).
      When query
        """
        SELECT typeof(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2))) AS t
        """
      Then query result
        | t              |
        | decimal(23,13) |

    @sail-bug
    Scenario: decimal modulo an integer literal uses Spark's remainder rule
      # Spark: scale = max(s1,s2), precision = min(p1-s1, p2-s2) + scale => decimal(3,2).
      # Sail currently produces decimal(10,2).
      When query
        """
        SELECT typeof(CAST(10.5 AS DECIMAL(10,2)) % 3) AS t
        """
      Then query result
        | t            |
        | decimal(3,2) |

    @sail-bug
    Scenario: decimal multiply capped at precision 38 uses adjustPrecisionScale
      # Spark caps precision at 38: adjustedScale = max(38 - intDigits, min(scale, 6)) => decimal(38,6).
      # Sail currently produces decimal(38,15).
      When query
        """
        SELECT typeof(CAST(1.0 AS DECIMAL(38,10)) * CAST(2.0 AS DECIMAL(10,5))) AS t
        """
      Then query result
        | t             |
        | decimal(38,6) |

    @sail-bug
    Scenario: ANSI string plus integer widens to bigint like Spark
      Given config spark.sql.ansi.enabled = true
      # Spark casts the string operand so the result is BIGINT; Sail casts to INT.
      When query
        """
        SELECT typeof('5' + 3) AS t
        """
      Then query result
        | t      |
        | bigint |
