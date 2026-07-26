@to_date
Feature: to_date with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_date — the argument may come from a column

    @column_args
    Scenario: to_date with the argument as a literal
      When query
        """
        SELECT to_date('2016-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |

    @column_args
    Scenario: to_date takes argument 2 from a column
      When query
        """
        SELECT to_date('2016-12-31', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |
        | 2016-12-31 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null string literal yields a date
      When query
        """
        SELECT to_date('2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null string column yields a date
      When query
        """
        SELECT to_date(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd')) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT to_date(c) AS result FROM VALUES ('2024-01-15'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  Rule: Result values (migrated from test_to_date.txt doctests)

    # `name` is a separate slot because Spark rewrites the derived column name:
    # TIMESTAMP_LTZ becomes TIMESTAMP and the value is shown already converted.
    Scenario Outline: Result values: <case>
      When query
        """
        SELECT to_date(<args>)
        """
      Then query result
        | to_date(<name>) |
        | <result>        |

      Examples:
        | case                        | args                                                        | name                                                   | result     |
        | to_date doctest #1 (result) | TIMESTAMP_NTZ '2025-11-02 23:30:45.123456'                  | TIMESTAMP_NTZ '2025-11-02 23:30:45.123456'             | 2025-11-02 |
        | to_date doctest #2 (result) | TIMESTAMP_LTZ '2025-11-02 23:30:45.123456'                  | TIMESTAMP '2025-11-02 23:30:45.123456'                 | 2025-11-02 |
        | to_date doctest #3 (result) | TIMESTAMP_LTZ '2025-11-02 23:30:45.123456 America/New_York' | TIMESTAMP '2025-11-03 04:30:45.123456'                 | 2025-11-03 |
        | to_date doctest #4 (result) | TIMESTAMP '2025-11-03 23:30:45.123456', 'invalid_format'    | TIMESTAMP '2025-11-03 23:30:45.123456', invalid_format | 2025-11-03 |

    Scenario: to_date doctest #5 (result)
      When query
        """
        SELECT ts, CAST(ts AS TIMESTAMP_NTZ) AS ts_ntz, CAST(ts AS TIMESTAMP_LTZ) AS ts_ltz, to_date(CAST(ts AS TIMESTAMP_NTZ)) AS date_ntz, to_date(CAST(ts AS TIMESTAMP_LTZ)) AS date_ltz FROM VALUES ('2025-11-02 23:30:45.123456'), ('2025-11-02 23:30:45.123456-08:00'), ('2025-11-02 23:30:45.123456+01:00') AS t(ts)
        """
      Then query result
        | ts                               | ts_ntz                     | ts_ltz                     | date_ntz   | date_ltz   |
        | 2025-11-02 23:30:45.123456       | 2025-11-02 23:30:45.123456 | 2025-11-02 23:30:45.123456 | 2025-11-02 | 2025-11-02 |
        | 2025-11-02 23:30:45.123456-08:00 | 2025-11-02 23:30:45.123456 | 2025-11-03 07:30:45.123456 | 2025-11-02 | 2025-11-03 |
        | 2025-11-02 23:30:45.123456+01:00 | 2025-11-02 23:30:45.123456 | 2025-11-02 22:30:45.123456 | 2025-11-02 | 2025-11-02 |
