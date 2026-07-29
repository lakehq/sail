@to_unix_timestamp
Feature: to_unix_timestamp with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_unix_timestamp — the argument may come from a column

    @column_args
    Scenario: to_unix_timestamp with the argument as a literal
      When query
        """
        SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 1460073600 |

    @column_args
    Scenario: to_unix_timestamp takes argument 2 from a column
      When query
        """
        SELECT to_unix_timestamp('2016-04-08', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1460073600 |
        | 1460073600 |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to to_unix_timestamp yields the schema Spark declares
      When query
        """
        SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to to_unix_timestamp yields the schema Spark declares
      When query
        """
        SELECT to_unix_timestamp(CAST(id AS STRING), 'yyyy-MM-dd') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to to_unix_timestamp stays nullable
      When query
        """
        SELECT to_unix_timestamp(c, 'yyyy-MM-dd') AS result FROM VALUES ('2016-04-08'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
