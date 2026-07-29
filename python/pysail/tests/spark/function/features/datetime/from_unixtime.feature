@from_unixtime
Feature: from_unixtime with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: from_unixtime — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: from_unixtime with the argument as a literal
      When query
        """
        SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |

    @column_args
    Scenario: from_unixtime takes argument 2 from a column containing NULL
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy-MM-dd HH:mm:ss'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |
        | NULL                |

    @column_args
    Scenario: from_unixtime takes argument 2 from a column holding two different values
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 1970   |
        | 01     |

  @spark_null
  Rule: Output schema

    Scenario: a non-null bigint literal yields a string
      When query
        """
        SELECT from_unixtime(0) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null bigint column yields a string
      When query
        """
        SELECT from_unixtime(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable bigint column stays nullable
      When query
        """
        SELECT from_unixtime(c) AS result FROM VALUES (CAST(0 AS BIGINT)), (CAST(NULL AS BIGINT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
