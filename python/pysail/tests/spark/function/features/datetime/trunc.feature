Feature: trunc with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: trunc — the argument may come from a column

    @function(columnargs)
    Scenario: trunc with the argument as a literal
      When query
        """
        SELECT trunc('2009-02-12', 'MM') AS result
        """
      Then query result ordered
        | result     |
        | 2009-02-01 |

    @function(columnargs)
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
        | trunc handles NULL and invalid column units                      | '2009-02-12' | NULL   | 'bad'  | NULL       | NULL       |
        | trunc accepts mixed-case column units                            | '2009-02-12' | 'mOn'  | 'yY'   | 2009-02-01 | 2009-01-01 |

    Scenario: paired dates and units are truncated independently
      When query
        """
        SELECT trunc(value, unit) AS result FROM VALUES
          (1, DATE '2024-02-29', 'MONTH'),
          (2, DATE '1969-12-31', 'YEAR'),
          (3, CAST(NULL AS DATE), 'WEEK') AS t(id, value, unit)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 2024-02-01 |
        | 1969-01-01 |
        | NULL       |

    @sail-only
    Scenario: Sail preserves trunc support for units below a week
      When query
        """
        SELECT trunc(DATE '2024-02-29', unit) AS result
        FROM VALUES ('DAY'), ('HOUR'), ('SECOND') AS t(unit)
        """
      Then query result
        | result     |
        | 2024-02-29 |
        | 2024-02-29 |
        | 2024-02-29 |

  @function(nullability)
  Rule: Output schema

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

  Rule: Date truncation does not require nanosecond timestamps

    Scenario Outline: trunc accepts an ordinary date outside nanosecond range: <input>
      When query
        """
        SELECT trunc(<input>, 'YEAR') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input              | result     |
        | DATE '1000-06-15' | 1000-01-01 |
        | '1000-06-15'      | 1000-01-01 |
        | DATE '9999-06-15' | 9999-01-01 |

    Scenario: date columns preserve wide years, pre-epoch weeks, and NULL
      When query
        """
        SELECT trunc(value, unit) AS result
        FROM VALUES (1, DATE '1000-06-15', 'QUARTER'),
                    (2, DATE '9999-06-15', 'YEAR'),
                    (3, DATE '1969-12-31', 'WEEK'),
                    (4, CAST(NULL AS DATE), 'YEAR') AS t(id, value, unit)
        ORDER BY id
        """
      Then query result ordered
        | result     |
        | 1000-04-01 |
        | 9999-01-01 |
        | 1969-12-29 |
        | NULL       |

    Scenario Outline: invalid scalar date truncation units return NULL: <unit>
      When query
        """
        SELECT trunc(DATE '2024-06-15', <unit>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | unit                 |
        | 'bad'                |
        | CAST(NULL AS STRING) |
