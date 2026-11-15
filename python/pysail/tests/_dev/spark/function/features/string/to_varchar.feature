Feature: to_varchar with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_varchar — a numeric format must be foldable

    @function(columnargs)
    Scenario: to_varchar with the argument as a literal
      When query
        """
        SELECT to_varchar(454, '999') AS result
        """
      Then query result ordered
        | result |
        | 454    |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @function(columnargs) @sail-bug
    Scenario: to_varchar takes argument 2 from a column holding two different values
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, '000D00') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @function(columnargs) @sail-bug
    Scenario: to_varchar takes argument 2 from a column containing NULL
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @function(columnargs) @sail-bug
    Scenario: to_varchar takes argument 2 from a column
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, '999') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  # With a DATE or TIMESTAMP input, Spark resolves `to_varchar` to `DateFormatClass`, which accepts a
  # non-foldable format and applies it row by row. So the foldable rule above is specific to the
  # numeric format; here the column is legal and each row must use its own format.

  Rule: to_varchar — a date format is resolved per row

    @function(columnargs)
    Scenario: to_varchar takes the date format from a column holding two different values
      When query
        """
        SELECT to_varchar(DATE '2026-02-02', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null numeric literal yields a non-nullable string
      When query
        """
        SELECT to_varchar(78.12, '99D99') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable numeric column stays nullable
      When query
        """
        SELECT to_varchar(c, '99D99') AS result FROM VALUES (CAST(78.12 AS DECIMAL(4,2))), (CAST(NULL AS DECIMAL(4,2))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null numeric column yields a non-nullable string
      When query
        """
        SELECT to_varchar(CAST(id AS DECIMAL(3,0)), '999') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_to_varchar.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT to_varchar(<args>) AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                           | args                  | result    |
        | to_varchar doctest #1 (result) | 454, '999'            | 454       |
        | to_varchar doctest #2 (result) | 78.12, '$99.99'       | $78.12    |
        | to_varchar doctest #3 (result) | -12454.8, '99G999D9S' | 12,454.8- |

    @sail-only
    Scenario: to_varchar doctest #5 (result)
      When query
        """
        SELECT to_varchar(encode('Spark SQL', 'utf-8'), 'hex') AS r
        """
      Then query result
        | r                  |
        | 537061726B2053514C |

    Scenario: to_varchar doctest #4 (error)
      When query
        """
        SELECT to_varchar(454, 'PR999') AS r
        """
      Then query error (?i).*
