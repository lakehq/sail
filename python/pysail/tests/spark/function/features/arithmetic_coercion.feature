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
