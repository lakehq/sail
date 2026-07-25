@xpath_int
Feature: xpath_int with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: xpath_int — the argument must be foldable

    @column_args
    Scenario: xpath_int with the argument as a literal
      When query
        """
        SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result ordered
        | result |
        | 3      |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3', 'NULL'].
    @column_args @sail-bug
    Scenario: xpath_int takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath_int('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3', '3'].
    @column_args @sail-bug
    Scenario: xpath_int takes argument 2 from a column
      When query
        """
        SELECT xpath_int('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, 'sum(a/b)') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @spark_null
  Rule: Output schema

    Scenario: a non-null xml literal yields an integer
      When query
        """
        SELECT xpath_int('<a><b>5</b></a>', 'a/b') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null xml column yields an integer
      When query
        """
        SELECT xpath_int(CONCAT('<a><b>', CAST(id AS STRING), '</b></a>'), 'a/b') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable xml column stays nullable
      When query
        """
        SELECT xpath_int(c, 'a/b') AS result FROM VALUES ('<a><b>5</b></a>'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
