@unix_timestamp
Feature: unix_timestamp with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: unix_timestamp — the argument may come from a column

    @column_args
    Scenario: unix_timestamp with the argument as a literal
      When query
        """
        SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 1460073600 |

    @column_args
    Scenario: unix_timestamp takes argument 2 from a column
      When query
        """
        SELECT unix_timestamp('2016-04-08', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1460073600 |
        | 1460073600 |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a bigint
      When query
        """
        SELECT unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a bigint
      When query
        """
        SELECT unix_timestamp(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd'), 'yyyy-MM-dd') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT unix_timestamp(c, 'yyyy-MM-dd') AS result FROM VALUES ('2024-01-15'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
