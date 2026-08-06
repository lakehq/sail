Feature: pmod (positive modulo) honors ANSI mode and Spark semantics

  # Spark `pmod(a, b)` returns the positive remainder of `a / b` (always in
  # range `[0, |b|)`), unlike `mod` which preserves the sign of `a`.
  # Under `spark.sql.ansi.enabled = true`, a zero divisor raises
  # REMAINDER_BY_ZERO; under ANSI=false it returns NULL.
  #
  # Sail delegates to `datafusion_spark::SparkPmod` which reads
  # `config_options.execution.enable_ansi_mode` at runtime. If the Sail
  # session never propagates the ANSI flag to that config, the ANSI=true
  # error path is dead — `pmod(x, 0)` silently returns NULL.

  Rule: Basic behavior — positive remainder regardless of dividend sign

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT pmod(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | args   | result |
        | positive dividend                            | 10, 3  | 1      |
        | negative dividend returns positive remainder | -7, 3  | 2      |
        | exact multiple gives zero                    | -15, 5 | 0      |

    Scenario: negative divisor — pmod still uses |b| domain
      When query
        """
        SELECT pmod(10, -3) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: NULL operands propagate

    Scenario Outline: NULL operand: <case>
      When query
        """
        SELECT pmod(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                       | args                  |
        | NULL dividend returns NULL | CAST(NULL AS INT), 3  |
        | NULL divisor returns NULL  | 10, CAST(NULL AS INT) |

  Rule: Divide by zero under ANSI on errors

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(<args>) AS result
        """
      Then query error (?i)by zero

      Examples:
        | case                                             | args  |
        | pmod by 0 errors under ANSI on                   | 10, 0 |
        | pmod negative dividend by 0 errors under ANSI on | -7, 0 |

  Rule: Divide by zero under ANSI off returns NULL

    Scenario: pmod by 0 returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(10, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: pmod per-row zero divisor nulls only that row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(a, b) AS result FROM VALUES
          (0, 10, 3),
          (1, -7, 0),
          (2, 15, 4)
        AS t(id, a, b) ORDER BY id
        """
      Then query result
        | result |
        | 1      |
        | NULL   |
        | 3      |

  Rule: FLOAT/DOUBLE mixed with DECIMAL coerces the result to DOUBLE

    # Spark widens `float`/`double` + `decimal` to DOUBLE, so the result is a
    # double (e.g. `1.5`), not a decimal. `pmod` now takes the same operand coercion
    # as the `%` operator, which promotes the pair to DOUBLE before the UDF sees it.

    Scenario Outline: Double with decimal: <case>
      When query
        """
        SELECT pmod(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | args                            | result |
        | double pmod decimal returns a double                  | CAST(5.5 AS DOUBLE), 2.0        | 1.5    |
        | infinity double pmod decimal returns NaN not an error | CAST('Infinity' AS DOUBLE), 2.0 | NaN    |

  Rule: operand typing follows Spark's remainder rule, not DataFusion's coercion
    # `SparkPmod` inherits DataFusion's `Signature::numeric`, which unifies both operands
    # before the remainder rule can see them. The plan builder now applies Spark's own
    # coercion first — the integer column takes its type-based decimal, and a string pair
    # promotes BOTH operands to DOUBLE (leaving the peer alone let the UDF pick its own
    # common type, which is what produced decimal(30,15) below).

    Scenario Outline: Literal operands: <case>
      When query
        """
        SELECT typeof(pmod(<left>, <right>)) AS t,
               pmod(<left>, <right>) AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                     | left       | right                      | t      | r   |
        | a string and a decimal promote to double | '5.5'      | CAST(2.0 AS DECIMAL(10,2)) | double | 1.5 |
        | Infinity and a decimal is NaN            | 'Infinity' | CAST(2.0 AS DECIMAL(10,2)) | double | NaN |

    Scenario: pmod of a decimal and an integer column keeps the remainder type
      When query
        """
        SELECT typeof(pmod(a, b)) AS t, pmod(a, b) AS r
        FROM VALUES (CAST(1.5 AS DECIMAL(3,2)), CAST(2 AS INT)) AS t(a, b)
        """
      Then query result
        | t            | r    |
        | decimal(3,2) | 1.50 |

    Scenario: pmod of NULL and a string is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(NULL, '3') AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: pmod of a decimal column and an integer column over rows
      # The literal scenarios fold at plan time; this one drives the coercion through the
      # runtime kernel.
      When query
        """
        SELECT pmod(a, b) AS r
        FROM VALUES (CAST(1.5 AS DECIMAL(3,2)), CAST(2 AS INT)),
                    (CAST(-1.5 AS DECIMAL(3,2)), CAST(2 AS INT)) AS t(a, b)
        """
      Then query result
        | r    |
        | 1.50 |
        | 0.50 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null integer literal is nullable (inherently nullable in Spark)
      When query
        """
        SELECT pmod(10, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null integer column is nullable (inherently nullable in Spark)
      When query
        """
        SELECT pmod(id, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable integer column stays nullable
      When query
        """
        SELECT pmod(c, 3) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
