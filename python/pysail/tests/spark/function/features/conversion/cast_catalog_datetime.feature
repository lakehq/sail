# CAST scenarios imported from test/bug_catalog (0886a9e7f): datetime/datetime.feature
Feature: Additional CAST coverage from datetime

  Rule: timestamp_seconds, timestamp_millis and timestamp_micros render in the session time zone

    Scenario Outline: datetime catalog: timestamp_seconds casts to DATE and STRING in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(timestamp_seconds(c) AS DATE) AS d, CAST(timestamp_seconds(c) AS STRING) AS s
        FROM VALUES (1, 0), (2, 1720000000), (3, 1704067200) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | d    | s    |
        | <d1> | <s1> |
        | <d2> | <s2> |
        | <d3> | <s3> |

      Examples:
        | zone                | d1         | s1                  | d2         | s2                  | d3         | s3                  |
        | America/Los_Angeles | 1969-12-31 | 1969-12-31 16:00:00 | 2024-07-03 | 2024-07-03 02:46:40 | 2023-12-31 | 2023-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 | 1970-01-01 05:30:00 | 2024-07-03 | 2024-07-03 15:16:40 | 2024-01-01 | 2024-01-01 05:30:00 |
        | Pacific/Chatham     | 1970-01-01 | 1970-01-01 12:45:00 | 2024-07-03 | 2024-07-03 22:31:40 | 2024-01-01 | 2024-01-01 13:45:00 |
        | Pacific/Pago_Pago   | 1969-12-31 | 1969-12-31 13:00:00 | 2024-07-02 | 2024-07-02 22:46:40 | 2023-12-31 | 2023-12-31 13:00:00 |

    Scenario Outline: datetime catalog: unix_date of a timestamp_seconds cast to DATE in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_date(CAST(timestamp_seconds(0) AS DATE)) AS result
        """
      Then query result
        | result |
        | -1     |

      Examples:
        | zone                |
        | America/Los_Angeles |
        | Pacific/Pago_Pago   |

  Rule: casts between TIMESTAMP, TIMESTAMP_NTZ, DATE and STRING use the session time zone

    Scenario Outline: datetime catalog: casts to TIMESTAMP interpret the wall clock in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          unix_seconds(CAST(DATE '2024-01-15' AS TIMESTAMP)) AS from_date,
          unix_seconds(CAST('2024-07-15 01:02:03' AS TIMESTAMP)) AS from_string,
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-01-15 01:02:03' AS TIMESTAMP)) AS from_ntz
        """
      Then query result
        | from_date   | from_string   | from_ntz   |
        | <from_date> | <from_string> | <from_ntz> |

      Examples:
        | zone                | from_date  | from_string | from_ntz   |
        | America/Los_Angeles | 1705305600 | 1721030523  | 1705309323 |
        | Asia/Kolkata        | 1705257000 | 1720985523  | 1705260723 |
        | Pacific/Chatham     | 1705227300 | 1720959423  | 1705231023 |
        | Pacific/Pago_Pago   | 1705316400 | 1721044923  | 1705320123 |

    Scenario Outline: datetime catalog: DATE rows cast to TIMESTAMP in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(CAST(c AS TIMESTAMP)) AS result
        FROM VALUES (1, DATE '2024-01-15'), (2, DATE '2024-07-15') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | zone                | r1         | r2         |
        | America/Los_Angeles | 1705305600 | 1721026800 |
        | Asia/Kolkata        | 1705257000 | 1720981800 |
        | Pacific/Chatham     | 1705227300 | 1720955700 |
        | Pacific/Pago_Pago   | 1705316400 | 1721041200 |

    Scenario: datetime catalog: TIMESTAMP_NTZ in a Los Angeles DST gap or overlap casts to TIMESTAMP
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-03-10 02:30:00' AS TIMESTAMP)) AS gap,
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-11-03 01:30:00' AS TIMESTAMP)) AS overlap
        """
      Then query result
        | gap        | overlap    |
        | 1710066600 | 1730622600 |

    Scenario Outline: datetime catalog: TIMESTAMP casts to TIMESTAMP_NTZ keeping the session-zone wall clock in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          CAST(CAST(TIMESTAMP '2024-01-15 01:02:03' AS TIMESTAMP_NTZ) AS STRING) AS from_literal,
          CAST(CAST(timestamp_seconds(0) AS TIMESTAMP_NTZ) AS STRING) AS from_epoch
        """
      Then query result
        | from_literal        | from_epoch   |
        | 2024-01-15 01:02:03 | <from_epoch> |

      Examples:
        | zone                | from_epoch          |
        | America/Los_Angeles | 1969-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 |
        | Pacific/Chatham     | 1970-01-01 12:45:00 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 |
