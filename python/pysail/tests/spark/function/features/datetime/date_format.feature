Feature: date_format with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: date_format — the argument is resolved per row, not taken from the first row

    @function(columnargs)
    Scenario: date_format with the argument as a literal
      When query
        """
        SELECT date_format('2016-04-08', 'y') AS result
        """
      Then query result ordered
        | result |
        | 2016   |

    @function(columnargs)
    Scenario: date_format takes argument 2 from a column containing NULL
      When query
        """
        SELECT date_format('2016-04-08', c) AS result FROM VALUES (1, 'y'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2016   |
        | NULL   |

    @function(columnargs)
    Scenario: date_format takes argument 2 from a column holding two different values
      When query
        """
        SELECT date_format(TIMESTAMP '2026-02-02 10:20:30', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |

    Scenario: a column format displays a scalar LTZ value in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT date_format(
          TIMESTAMP '1970-01-01 00:00:00Z',
          c
        ) AS result
        FROM VALUES (1, 'yyyy-MM-dd HH:mm:ss'), (2, 'HH:mm:ss') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 1970-01-01 01:02:03 |
        | 01:02:03            |

  Rule: Spark timestamp zone IDs

    Scenario: date_format accepts a second-precision offset in its timestamp string
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT date_format(
          '1970-01-01 00:00:00+01:02:03',
          'yyyy-MM-dd HH:mm:ss'
        ) AS result
        """
      Then query result
        | result              |
        | 1969-12-31 22:57:57 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null timestamp literal yields a string
      When query
        """
        SELECT date_format(TIMESTAMP '2024-01-15 10:00:00', 'yyyy-MM') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null timestamp column yields a string
      When query
        """
        SELECT date_format(CAST(id AS TIMESTAMP), 'yyyy') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT date_format(c, 'yyyy') AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
