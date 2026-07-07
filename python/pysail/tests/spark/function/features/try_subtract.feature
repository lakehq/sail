@try_subtract
Feature: try_subtract with float and decimal inputs

  # try_subtract is ANSI-invariant: integer/decimal overflow returns NULL per
  # element regardless of spark.sql.ansi.enabled. Float subtract follows IEEE 754
  # (no overflow-to-NULL). Decimal keeps a DECIMAL result type, never double.

  Rule: Float and double subtract keep their float type
    Scenario: double minus double
      When query
        """
        SELECT try_subtract(CAST(5.5 AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 3.5    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: float minus float stays float
      When query
        """
        SELECT try_subtract(CAST(5.5 AS FLOAT), CAST(2.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 3.5    |
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Decimal subtract keeps a DECIMAL result type
    Scenario: decimal minus decimal stays decimal
      When query
        """
        SELECT try_subtract(CAST(5.50 AS DECIMAL(10,2)), CAST(2.00 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 3.50   |
      Then query schema
        """
        root
         |-- result: decimal(11,2) (nullable = true)
        """

    Scenario: integer minus decimal stays decimal
      When query
        """
        SELECT try_subtract(5, CAST(2.5 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 2.50   |

  Rule: Overflow returns NULL and is ANSI-invariant
    Scenario: integer subtract overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_subtract(CAST(-2147483648 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: integer subtract overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_subtract(CAST(-2147483648 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: decimal subtract precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_subtract(CAST(-9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: decimal subtract precision overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_subtract(CAST(-9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite doubles pass through, never NULL
    Scenario: infinity minus infinity is NaN
      When query
        """
        SELECT try_subtract(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity minus one is infinity
      When query
        """
        SELECT try_subtract(CAST('Infinity' AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: Per-element overflow in an array nulls only the offending rows
    Scenario: mixed integer subtract array nulls only overflow rows
      When query
        """
        SELECT try_subtract(CAST(a AS INT), CAST(b AS INT)) AS result
        FROM VALUES (-2147483648, 1), (10, 3), (2147483647, -1) AS t(a, b)
        """
      Then query result ordered
        | result |
        | NULL   |
        | 7      |
        | NULL   |

  Rule: NULL propagates
    Scenario: NULL operand yields NULL
      When query
        """
        SELECT try_subtract(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |
