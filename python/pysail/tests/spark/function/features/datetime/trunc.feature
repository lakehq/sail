@trunc
Feature: trunc with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: trunc — the argument may come from a column

    @column_args
    Scenario: trunc with the argument as a literal
      When query
        """
        SELECT trunc('2009-02-12', 'MM') AS result
        """
      Then query result ordered
        | result     |
        | 2009-02-01 |

    # Sail rejects the column: Sail errors: Granularity of `date_trunc` must be non-null scalar Utf8
    @column_args @sail-bug
    Scenario Outline: Trunc: <case>
      When query
        """
        SELECT trunc(<date>, c) AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                                              | date         | v1     | v2     | r1         | r2         |
        | trunc takes argument 2 from a column holding two different values | '2009-02-12' | 'MM'   | 'week' | 2009-02-01 | 2009-02-09 |
        | trunc takes argument 2 from a column                              | '2019-08-04' | 'week' | 'week' | 2019-07-29 | 2019-07-29 |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null date literal yields a date
      When query
        """
        SELECT trunc(DATE '2024-01-15', 'YEAR') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    @sail-bug
    Scenario: a non-null date column yields a date
      When query
        """
        SELECT trunc(CAST(CAST(id AS TIMESTAMP) AS DATE), 'YEAR') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable date column stays nullable
      When query
        """
        SELECT trunc(c, 'YEAR') AS result FROM VALUES (DATE '2024-01-15'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
