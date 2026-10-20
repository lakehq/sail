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

  Rule: date_format converts dates and timestamp_ntz values in the session timezone

    # Fixed offsets must remain usable after conversion to a session timestamp.
    # The IANA control retains the conversion that resolves a nonexistent local time.
    Scenario Outline: date_format converts DATE and TIMESTAMP_NTZ literals in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_format(DATE '2024-03-10', 'yyyy-MM-dd') AS date_result,
               date_format(TIMESTAMP_NTZ '2024-03-10 02:30:00.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS ntz_result
        """
      Then query result
        | date_result | ntz_result   |
        | 2024-03-10  | <ntz_result> |

      Examples:
        | zone                | ntz_result                 |
        | +05:30              | 2024-03-10 02:30:00.123456 |
        | -08:00              | 2024-03-10 02:30:00.123456 |
        | America/Los_Angeles | 2024-03-10 03:30:00.123456 |

    Scenario: date_format converts DATE and TIMESTAMP_NTZ columns in a fixed-offset session
      Given config spark.sql.session.timeZone = +05:30
      When query
        """
        SELECT date_format(d, 'yyyy-MM-dd') AS date_result,
               date_format(ts, fmt) AS ntz_result
        FROM VALUES
          (1, DATE '2024-01-01', TIMESTAMP_NTZ '2024-01-01 00:30:00.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
          (2, DATE '2024-01-02', TIMESTAMP_NTZ '2024-01-02 12:34:56', 'HH:mm:ss'),
          (3, CAST(NULL AS DATE), CAST(NULL AS TIMESTAMP_NTZ), 'yyyy-MM-dd')
          AS t(i, d, ts, fmt)
        ORDER BY i
        """
      Then query result ordered
        | date_result | ntz_result                 |
        | 2024-01-01  | 2024-01-01 00:30:00.123456 |
        | 2024-01-02  | 12:34:56                   |
        | NULL        | NULL                       |

  @function(nullability)
  Rule: Output schema

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
