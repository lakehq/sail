Feature: try_to_binary with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: try_to_binary — the argument is resolved per row, not taken from the first row

    @function(columnargs)
    Scenario: try_to_binary with the argument as a literal
      When query
        """
        SELECT hex(try_to_binary('abc', 'utf-8')) AS result
        """
      Then query result ordered
        | result |
        | 616263 |

    @function(columnargs)
    Scenario: try_to_binary takes argument 1 from a column holding two different values
      When query
        """
        select hex(try_to_binary(c, 'base64')) AS result FROM VALUES (1, 'a!'), (2, 'abc') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | NULL   |
        | 69B7   |

  # `TryEval` turns every error raised while the call is evaluated into NULL (a malformed value, an
  # unknown format NAME, and an error of the argument itself). A value of any atomic type is cast
  # to STRING first, so `try_to_binary(true, 'utf-8')` is the text of `true`, and a format of a type
  # Spark rejects (ARRAY, MAP, STRUCT) is still an analysis error: `TryEval` only wraps evaluation.
  Rule: try_to_binary — a non-string value is cast to STRING, a non-string format is cast too

    Scenario Outline: try_to_binary utf-8 of <case>
      When query template
        """
        SELECT hex(try_to_binary(<input>, 'utf-8')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case      | input                          | result                                 |
        | BOOLEAN   | true                           | 74727565                               |
        | DOUBLE    | 1.5D                           | 312E35                                 |
        | DECIMAL   | 1.50BD                         | 312E3530                               |
        | DATE      | DATE'2024-01-02'               | 323032342D30312D3032                   |
        | TIMESTAMP | TIMESTAMP'2024-03-05 06:07:08' | 323032342D30332D30352030363A30373A3038 |

    Scenario: try_to_binary utf-8 over a BOOLEAN column keeps NULL and is per row
      When query
        """
        SELECT hex(try_to_binary(v, 'utf-8')) AS result
        FROM VALUES (1, true), (2, CAST(NULL AS BOOLEAN)), (3, false) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 74727565   |
        | NULL       |
        | 66616C7365 |

    Scenario: try_to_binary of a DOUBLE with the hex format is NULL per row
      When query
        """
        SELECT hex(try_to_binary(v)) AS result FROM VALUES (1, 1.5D), (2, CAST(NULL AS DOUBLE)) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

    Scenario: try_to_binary with a BINARY format is cast to its name
      When query
        """
        SELECT hex(try_to_binary('41', X'686578')) AS result
        """
      Then query result collected
        | result |
        | 41     |

    Scenario: try_to_binary with a BINARY format that is not a name is NULL
      When query
        """
        SELECT try_to_binary('41', X'626164') AS result
        """
      Then query result collected
        | result |
        | NULL   |

    Scenario Outline: try_to_binary with a <case> format is NULL
      When query template
        """
        SELECT try_to_binary('41', <fmt>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case    | fmt              |
        | INT     | 1                |
        | BOOLEAN | true             |
        | DATE    | DATE'2024-01-02' |
        | NULL    | NULL             |

    Scenario Outline: try_to_binary with a <case> format is still a type error
      When query template
        """
        SELECT try_to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got <shown>

      Examples:
        | case   | fmt                 | shown        |
        | ARRAY  | array('hex')        | ARRAY        |
        | MAP    | map('a', 'b')       | MAP          |
        | STRUCT | named_struct('a',1) | NAMED_STRUCT |

  # `TryEval` nulls only the row that is malformed, and `UnBase64.isValidBase64` decides it.
  Rule: try_to_binary base64 is decided per row

    Scenario: a column with a valid, a malformed and a valid value
      When query
        """
        SELECT hex(try_to_binary(c, 'base64')) AS result
        FROM VALUES (1, 'YQ=='), (2, 'a!'), (3, 'YWJj') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 61     |
        | NULL   |
        | 616263 |

    Scenario: a malformed literal is NULL
      When query
        """
        SELECT try_to_binary('abc?', 'base64') AS result
        """
      Then query result collected
        | result |
        | NULL   |

  # `TryToBinary` is `TryEval(ToBinary(..))`: any error raised while the argument is evaluated is
  # NULL too, not only a malformed value.
  Rule: try_to_binary nulls an error of its own argument

    # Known gap, not specific to this function. Spark's `TryEval` wraps the whole call, so an error
    # raised while the ARGUMENT is evaluated is NULL too. In Sail the argument is evaluated before
    # the function is called (DataFusion evaluates arguments eagerly), so the function never sees the
    # error. A fix needs a generic TryEval: a marker in the logical plan plus a physical expression
    # that catches the error of its child, and DataFusion has no expression-level planner hook (an
    # `ExtensionPlanner` only handles plan nodes), so it means rewriting the expressions of every
    # execution node. Rewriting the argument with `try_cast` / `try_divide` instead is not
    # equivalent: `try_to_binary(coalesce(CAST(1/0 AS STRING), '41'))` is NULL in Spark.
    # The same call without `try_` raises, as Spark does (see `to_binary.feature`).
    @sail-bug
    Scenario: a division by zero in the argument
      When query
        """
        SELECT try_to_binary(CAST(1/0 AS STRING)) AS result
        """
      Then query result collected
        | result |
        | NULL   |

    @sail-bug
    Scenario: an overflowing cast in the argument
      When query
        """
        SELECT try_to_binary(CAST(1.0E30D AS BIGINT), 'utf-8') AS result
        """
      Then query result collected
        | result |
        | NULL   |

  @function(nullability)
  Rule: try_to_binary base64 is always nullable

    Scenario: a non-null literal
      When query
        """
        SELECT try_to_binary('YWJj', 'base64') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

  Rule: try_to_binary — the argument must be foldable

    # Spark requires a foldable argument here, for try_to_binary as much as for to_binary.
    @function(columnargs)
    Scenario: try_to_binary takes argument 2 from a column holding two different values
      When query
        """
        select try_to_binary('a!', c) AS result FROM VALUES (1, 'base64'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here, for try_to_binary as much as for to_binary.
    @function(columnargs)
    Scenario: try_to_binary takes argument 2 from a column containing NULL
      When query
        """
        SELECT try_to_binary('abc', c) AS result FROM VALUES (1, 'utf-8'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here, for try_to_binary as much as for to_binary.
    @function(columnargs)
    Scenario: try_to_binary takes argument 2 from a column
      When query
        """
        SELECT try_to_binary('abc', c) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_to_binary yields the schema Spark declares
      When query
        """
        SELECT try_to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to try_to_binary yields the schema Spark declares
      When query
        """
        SELECT try_to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to try_to_binary stays nullable
      When query
        """
        SELECT try_to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
