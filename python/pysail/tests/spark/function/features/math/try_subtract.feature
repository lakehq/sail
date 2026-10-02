Feature: try_subtract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_subtract yields the schema Spark declares
      When query
        """
        SELECT try_subtract(2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_subtract yields the schema Spark declares
      When query
        """
        SELECT try_subtract(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_subtract stays nullable
      When query
        """
        SELECT try_subtract(c, 1) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Basic integer subtraction

    Scenario: Integer subtraction
      When query
        """
        SELECT try_subtract(10, 3) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: Integer subtraction with negative result
      When query
        """
        SELECT try_subtract(1, 100) AS result
        """
      Then query result
        | result |
        | -99    |

    Scenario: Integer minus zero
      When query
        """
        SELECT try_subtract(42, 0) AS result
        """
      Then query result
        | result |
        | 42     |

  Rule: Integer overflow returns NULL

    Scenario: INT overflow returns NULL
      When query
        """
        SELECT try_subtract(CAST(-2147483648 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: BIGINT overflow returns NULL
      When query
        """
        SELECT try_subtract(CAST(-9223372036854775808 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Date subtraction

    Scenario: Date minus integer days
      When query
        """
        SELECT try_subtract(DATE '2024-01-10', 5) AS result
        """
      Then query result
        | result     |
        | 2024-01-05 |

    Scenario: Date minus zero days
      When query
        """
        SELECT try_subtract(DATE '2024-06-15', 0) AS result
        """
      Then query result
        | result     |
        | 2024-06-15 |

    Scenario: Date minus year-month interval
      When query
        """
        SELECT try_subtract(DATE '2024-03-31', INTERVAL 1 MONTH) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: Date minus negative interval adds time
      When query
        """
        SELECT try_subtract(DATE '2024-01-15', INTERVAL -1 MONTH) AS result
        """
      Then query result
        | result     |
        | 2024-02-15 |

  Rule: String inputs return NULL (Spark behavior)

    @sail-bug
    Scenario: Valid date string minus integer returns NULL
      When query
        """
        SELECT try_subtract('2024-01-10', 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Invalid date string minus integer returns NULL
      When query
        """
        SELECT try_subtract('not-a-date', 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Empty string minus integer returns NULL
      When query
        """
        SELECT try_subtract('', 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL handling

    Scenario: NULL left operand returns NULL
      When query
        """
        SELECT try_subtract(CAST(NULL AS INT), 3) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL right operand returns NULL
      When query
        """
        SELECT try_subtract(10, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Both operands NULL returns NULL
      When query
        """
        SELECT try_subtract(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL date minus integer returns NULL
      When query
        """
        SELECT try_subtract(CAST(NULL AS DATE), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Float and double subtract keep their float type
    @sail-bug
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

    @sail-bug
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
    @sail-bug
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

    @sail-bug
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

    @sail-bug
    Scenario: decimal subtract precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_subtract(CAST(-9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
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
    @sail-bug
    Scenario: infinity minus infinity is NaN
      When query
        """
        SELECT try_subtract(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
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
    @sail-bug
    Scenario: NULL operand yields NULL
      When query
        """
        SELECT try_subtract(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |
