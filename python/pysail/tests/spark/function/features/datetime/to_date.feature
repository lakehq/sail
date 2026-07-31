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

  Rule: Explicit NULL format semantics

    Scenario: To date NULL format semantics distinguishes omitted and explicit format
      When query
        """
        SELECT
          to_date('2024-01-15') AS omitted_format,
          to_date('2024-01-15', CAST(NULL AS STRING)) AS explicit_null_format
        """
      Then query result
        | omitted_format | explicit_null_format |
        | 2024-01-15     | NULL                 |

    Scenario: To date NULL format semantics propagates a column format for a scalar value
      When query
        """
        SELECT id, format, to_date('2024-01-15', format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING))
          AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | id | format     | result     |
        | 1  | yyyy-MM-dd | 2024-01-15 |
        | 2  | NULL       | NULL       |

    Scenario: To date NULL format semantics propagates a scalar NULL format for column values
      When query
        """
        SELECT id, value, to_date(value, CAST(NULL AS STRING)) AS result
        FROM VALUES
          (1, '2024-01-15'),
          (2, '2024-01-16'),
          (3, CAST(NULL AS STRING))
          AS t(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | value      | result |
        | 1  | 2024-01-15 | NULL   |
        | 2  | 2024-01-16 | NULL   |
        | 3  | NULL       | NULL   |

    Scenario: To date NULL format semantics propagates paired value and format columns
      When query
        """
        SELECT id, to_date(value, format) AS result
        FROM VALUES
          (1, '2024-01-15', 'yyyy-MM-dd'),
          (2, '15/01/2024', 'dd/MM/yyyy'),
          (3, '2024-01-15', CAST(NULL AS STRING)),
          (4, CAST(NULL AS STRING), 'yyyy-MM-dd')
          AS t(id, value, format)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 1  | 2024-01-15 |
        | 2  | 2024-01-15 |
        | 3  | NULL       |
        | 4  | NULL       |

    Scenario Outline: To date NULL format semantics ignores format for a <type> input
      When query
        """
        SELECT id, format, to_date(<value>, format) AS result
        FROM VALUES
          (1, 'yyyy-MM-dd'),
          (2, CAST(NULL AS STRING)),
          (3, 'invalid_format')
          AS t(id, format)
        ORDER BY id
        """
      Then query result ordered
        | id | format         | result     |
        | 1  | yyyy-MM-dd     | 2024-01-15 |
        | 2  | NULL           | 2024-01-15 |
        | 3  | invalid_format | 2024-01-15 |

      Examples:
        | type      | value                                |
        | DATE      | DATE '2024-01-15'                    |
        | TIMESTAMP | TIMESTAMP '2024-01-15 23:45:00'      |

    Scenario: To date typed DATE rejects a complex format
      When query
        """
        SELECT to_date(
          DATE '2024-01-15',
          array('yyyy-MM-dd')
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|format.*string.*(array|list)|expects.*STRING.*(array|list)|requires.*STRING.*(array|list))

    Scenario: To date typed DATE accepts an atomic format through string coercion
      When query
        """
        SELECT to_date(DATE '2024-01-15', 123) AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario Outline: To date NULL format semantics short-circuits a bad value with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_date('not-a-date', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

  @spark_null
  Rule: Output schema

    Scenario Outline: a typed DATE with a literal format respects ANSI <ansi> nullability
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_date(DATE '2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = <nullable>)
        """

      Examples:
        | ansi  | nullable |
        | false | true     |
        | true  | false    |

    Scenario: a typed DATE with a nullable format stays nullable in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_date(DATE '2024-01-15', format) AS result
        FROM VALUES
          ('yyyy-MM-dd'),
          (CAST(NULL AS STRING))
          AS t(format)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | 2024-01-15 |
      And query schema
        """
        root
         |-- result: date (nullable = true)
        """

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
