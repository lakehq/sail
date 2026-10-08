Feature: round with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: round — the argument must be foldable

    @function(columnargs)
    Scenario: round with the argument as a literal
      When query
        """
        SELECT round(2.5, 0) AS result
        """
      Then query result ordered
        | result |
        | 3      |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', 'NULL'].
    @function(columnargs) @sail-bug
    Scenario: round takes argument 2 from a column containing NULL
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', '3.0'].
    @function(columnargs) @sail-bug
    Scenario: round takes argument 2 from a column
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, 0) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null numeric literal is nullable (inherently nullable in Spark)
      When query
        """
        SELECT round(2.567, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(4,2) (nullable = true)
        """

    @sail-bug
    Scenario: a non-null numeric column is nullable (inherently nullable in Spark)
      When query
        """
        SELECT round(id, 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable numeric column stays nullable
      When query
        """
        SELECT round(c, 1) AS result FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  # Spark's `Round` implicitly casts a string value to DOUBLE (`NumericType.defaultConcreteType`),
  # so the result is DOUBLE. The cast follows ANSI mode. All expected values were captured on
  # Spark JVM 4.1.1.
  Rule: A string value is implicitly cast to double

    Scenario Outline: round string coercion: <case>
      When query
        """
        SELECT round(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | args                    | result    |
        | round a numeric string                   | '0.123456', 5           | 0.12346   |
        | round a numeric string with no scale     | '2.5'                   | 3.0       |
        | round a negative tie away from zero      | '-2.5', 0               | -3.0      |
        | round a numeric string to negative scale | '1234.5678', -2         | 1200.0    |
        | round an exponent string                 | '1e2', 0                | 100.0     |
        | round an integral string returns double  | '7', 2                  | 7.0       |
        | round a string with surrounding spaces   | ' 1.5 ', 0              | 2.0       |
        | round a NaN string                       | 'NaN', 2                | NaN       |
        | round a negative infinity string         | '-Infinity', 2          | -Infinity |
        | round a NULL string                      | CAST(NULL AS STRING), 2 | NULL      |

    Scenario: round a string column
      When query
        """
        SELECT i, round(v, 5) AS result FROM VALUES (1, '0.123456'), (2, '0.987654'), (3, NULL) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | i | result  |
        | 1 | 0.12346 |
        | 2 | 0.98765 |
        | 3 | NULL    |

    Scenario: round of a string column returns double
      When query
        """
        SELECT round(v, 1) AS result FROM VALUES ('1.25'), (NULL) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: round a malformed string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, round(v, 1) AS result FROM VALUES (1, '1.25'), (2, 'x') AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | 1.3    |
        | 2 | NULL   |

    Scenario: round a malformed string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round('abc', 2) AS result
        """
      Then query error (?i)cannot (be )?cast

    # Spark rounds a DOUBLE via `BigDecimal(d).setScale(scale, HALF_UP)`, i.e. on the shortest
    # decimal representation of the double, so the tie in '1.005' rounds up. Sail uses DataFusion's
    # `(x * 10^scale).round() / 10^scale`, which sees 100.49999999999999: Sail returns 1.0.
    @sail-bug
    Scenario: round a string whose decimal tie is not exact in binary
      When query
        """
        SELECT round('1.005', 2) AS result
        """
      Then query result
        | result |
        | 1.01   |

    Scenario: round of a non-null string has a nullable double schema under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(v) AS default_scale, round(v, 1) AS explicit_scale
        FROM VALUES ('1.25') AS t(v)
        """
      Then query schema
        """
        root
         |-- default_scale: double (nullable = true)
         |-- explicit_scale: double (nullable = true)
        """

    Scenario Outline: round skips a malformed string with a NULL scale: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(<value>, <scale>) AS result FROM VALUES ('abc') AS t(v)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                  | value | scale                     |
        | literal value         | 'abc' | CAST(NULL AS INT)         |
        | column value          | v     | CAST(NULL AS INT)         |
        | folded NULL scale     | 'abc' | CAST(NULL AS INT) + 1     |

    @sail-bug
    Scenario: round skips a string expression with a literal zero divisor when scale is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(CAST(1 / 0 AS STRING), CAST(NULL AS INT)) AS result
        FROM range(2)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |

    Scenario: round of a malformed string column still errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(v, 1) AS result FROM VALUES ('abc') AS t(v)
        """
      Then query error (?i)cannot (be )?cast

  # CASE branch coercion and final expression typing are shared analyzer limitations.
  Rule: Deferred common-type coercion of mixed CASE arguments

    @sail-bug
    Scenario: round of a mixed CASE uses the final string type under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT round(CASE WHEN id = 0 THEN 2 ELSE '1.25' END, 1) AS result
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | result |
        | 2.0    |
        | 1.3    |

    @sail-bug
    Scenario: round respects ANSI coercion inside a mixed CASE
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(CASE WHEN id = 0 THEN '1.25' ELSE 2 END, 1) AS result
        FROM range(2)
        """
      Then query error (?i)cannot (be )?cast
