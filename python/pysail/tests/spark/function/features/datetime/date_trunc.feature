Feature: DATE_TRUNC output type

  Rule: date_trunc always returns TIMESTAMP

    Scenario: date_trunc on a timestamp column returns timestamp
      When query
        """
        WITH t(ts) AS (VALUES (TIMESTAMP '2026-02-02 00:00:00 UTC'))
        SELECT date_trunc('YEAR', ts) AS result FROM t
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # `date_trunc` is declared to return TIMESTAMP regardless of the input type, so a
    # TIMESTAMP_NTZ argument is converted rather than preserved.
    @function(nullability)
    Scenario Outline: date_trunc on timestamp_ntz returns timestamp: <case>
      When query
        """
        <query>
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case    | query                                                                                                  |
        | column  | WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00')) SELECT date_trunc('YEAR', ts) AS result FROM t |
        | literal | SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result                                |

    Scenario: date_trunc YEAR on timestamp values
      When query
        """
        SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
        """
        SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-01 00:00:00 |

    Scenario: date_trunc DAY on timestamp with America/Los_Angeles timezone
      When query
        """
        SELECT date_trunc('DAY', TIMESTAMP '2026-03-15 02:30:00 America/Los_Angeles') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-15 00:00:00 |

    Scenario: date_trunc HOUR on timestamp with America/New_York timezone
      When query
        """
        SELECT date_trunc('HOUR', TIMESTAMP '2026-03-15 14:45:30 America/New_York') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
      Then query result
        | result              |
        | 2026-03-15 18:00:00 |

  Rule: date_trunc — the argument may come from a column

    @function(columnargs)
    Scenario: date_trunc with the argument as a literal
      When query
        """
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359') AS result
        """
      Then query result ordered
        | result              |
        | 2015-03-01 00:00:00 |

    @function(columnargs)
    Scenario Outline: Date_trunc: <case>
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                                                   | v1     | v2     | r1                  | r2                  |
        | date_trunc takes argument 1 from a column holding two different values | 'YEAR' | 'MM'   | 2015-01-01 00:00:00 | 2015-03-01 00:00:00 |
        | date_trunc takes argument 1 from a column                              | 'YEAR' | 'YEAR' | 2015-01-01 00:00:00 | 2015-01-01 00:00:00 |

    Scenario: invalid and null formats return null
      When query
        """
        SELECT
          date_trunc('not-a-unit', TIMESTAMP '2024-01-15 10:00:00') AS invalid_format,
          date_trunc(CAST(NULL AS STRING), TIMESTAMP '2024-01-15 10:00:00') AS null_format
        """
      Then query result
        | invalid_format | null_format |
        | NULL           | NULL        |

  Rule: date_trunc resolves calendar boundaries in the session time zone

    Scenario: truncating an instant in the repeated hour preserves its later offset
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_micros(date_trunc(
          'HOUR',
          TIMESTAMP '2019-11-03 01:30:45.987654-08:00'
        )) AS result
        """
      Then query result
        | result           |
        | 1572771600000000 |

    Scenario: truncating to a nonexistent midnight shifts across the DST gap
      Given config spark.sql.session.timeZone = America/Sao_Paulo
      When query
        """
        SELECT
          CAST(date_trunc('DAY', TIMESTAMP '2018-11-04 01:30:00') AS STRING) AS rendered,
          unix_micros(date_trunc('DAY', TIMESTAMP '2018-11-04 01:30:00')) AS micros
        """
      Then query result
        | rendered            | micros           |
        | 2018-11-04 01:00:00 | 1541300400000000 |

    Scenario: truncation supports a historical second-precision session offset
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT CAST(date_trunc(
          'MINUTE',
          TIMESTAMP '1769-10-17 17:10:02.123456'
        ) AS STRING) AS result
        """
      Then query result
        | result              |
        | 1769-10-17 17:10:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null timestamp literal yields a timestamp
      When query
        """
        SELECT date_trunc('year', TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null timestamp column yields a timestamp
      When query
        """
        SELECT date_trunc('year', CAST(id AS TIMESTAMP)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT date_trunc('year', c) AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
