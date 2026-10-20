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

  Rule: Formatting retains valid local calendars at the timestamp boundaries

    Scenario Outline: date_format retains extreme local literals in <zone> at <date>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_format(DATE '<date>', 'HH:mm:ss') AS date_result,
               date_format(TIMESTAMP_NTZ '<date> <time>', 'HH:mm:ss.SSSSSS') AS ntz_result
        """
      Then query result
        | date_result | ntz_result |
        | 00:00:00    | <time>     |

      Examples:
        | zone       | date          | time            |
        | Asia/Tokyo | -262143-01-01 | 00:00:00.000000 |
        | UTC        | -262143-01-01 | 00:00:00.000000 |
        | +09:00     | -262143-01-01 | 00:00:00.000000 |
        | Asia/Tokyo | -262143-01-02 | 00:00:00.000000 |
        | Asia/Tokyo | -262143-01-01 | 12:00:00.000000 |
        | -01:00     | +262142-12-31 | 23:59:59.999999 |

    Scenario Outline: date_format retains extreme local columns and dynamic formats in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT id, date_format(d, 'HH:mm:ss') AS date_result,
               date_format(ts, fmt) AS ntz_result
        FROM (
          SELECT id,
                 CASE WHEN id = 0 OR id = 3 THEN DATE '-262143-01-01'
                      WHEN id = 1 THEN DATE '-262143-01-02' END AS d,
                 CAST(CASE WHEN id = 0 OR id = 3 THEN '-262143-01-01 00:00:00.123456'
                           WHEN id = 1 THEN '-262143-01-02 12:00:00' END AS TIMESTAMP_NTZ) AS ts,
                 CASE WHEN id = 0 THEN 'HH:mm:ss.SSSSSS'
                      WHEN id = 1 OR id = 2 THEN 'HH:mm:ss' END AS fmt
          FROM range(4)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | date_result | ntz_result     |
        | 0  | 00:00:00    | 00:00:00.123456 |
        | 1  | 00:00:00    | 12:00:00       |
        | 2  | NULL        | NULL           |
        | 3  | 00:00:00    | NULL           |

      Examples:
        | zone       |
        | Asia/Tokyo |
        | +09:00     |

    Scenario Outline: date_format preserves gap resolution and zone tokens in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_format(DATE '<date>', 'yyyy-MM-dd HH:mm:ssXXX') AS date_result,
               date_format(TIMESTAMP_NTZ '<date> <time>', 'yyyy-MM-dd HH:mm:ssXXX') AS ntz_result
        """
      Then query result
        | date_result   | ntz_result   |
        | <date_result> | <ntz_result> |

      Examples:
        | zone                | date       | time     | date_result               | ntz_result                |
        | America/Los_Angeles | 2024-03-10 | 02:30:00 | 2024-03-10 00:00:00-08:00 | 2024-03-10 03:30:00-07:00 |
        | Asia/Kathmandu      | 1986-01-01 | 00:00:00 | 1986-01-01 00:15:00+05:45 | 1986-01-01 00:15:00+05:45 |
        | Pacific/Apia        | 2011-12-30 | 12:34:56 | 2011-12-31 00:00:00+14:00 | 2011-12-31 12:34:56+14:00 |

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
