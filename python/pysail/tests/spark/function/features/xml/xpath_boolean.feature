Feature: xpath_boolean with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: xpath_boolean — the argument must be foldable

    @function(columnargs)
    Scenario: xpath_boolean with the argument as a literal
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>','a/b') AS result
        """
      Then query result ordered
        | result |
        | true   |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['True', 'NULL'].
    @function(columnargs) @sail-bug
    Scenario: xpath_boolean takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', c) AS result FROM VALUES (1, 'a/b'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['True', 'True'].
    @function(columnargs) @sail-bug
    Scenario: xpath_boolean takes argument 2 from a column
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', c) AS result FROM VALUES (1, 'a/b'), (2, 'a/b') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null xml literal yields a boolean
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', 'a/b=1') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a non-null xml column yields a boolean
      When query
        """
        SELECT xpath_boolean(CONCAT('<a><b>', CAST(id AS STRING), '</b></a>'), 'a/b=1') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a nullable xml column stays nullable
      When query
        """
        SELECT xpath_boolean(c, 'a/b=1') AS result FROM VALUES ('<a><b>1</b></a>'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
