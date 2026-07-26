@window
Feature: window() time-based windowing function

  Rule: Tumbling window

    Scenario: 5-minute tumbling windows group rows by their bucket
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:03:00'),
                    (TIMESTAMP '2021-01-01 00:07:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:05:00 | 2   |
        | 2021-01-01 00:05:00 | 2021-01-01 00:10:00 | 1   |

  Rule: Sliding window

    Scenario: exact sliding window assigns each row to multiple overlapping buckets
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30'),
                    (TIMESTAMP '2021-01-01 00:06:00'),
                    (TIMESTAMP '2021-01-01 00:01:00') AS t(b)
        GROUP BY window(b, '10 minutes', '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2020-12-31 23:55:00 | 2021-01-01 00:05:00 | 3   |
        | 2021-01-01 00:00:00 | 2021-01-01 00:10:00 | 4   |
        | 2021-01-01 00:05:00 | 2021-01-01 00:15:00 | 1   |

    Scenario: inexact sliding window drops over-estimated candidates
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:02:00') AS t(b)
        GROUP BY window(b, '10 minutes', '4 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2020-12-31 23:56:00 | 2021-01-01 00:06:00 | 1   |
        | 2021-01-01 00:00:00 | 2021-01-01 00:10:00 | 1   |

  Rule: Start-time offset

    Scenario: window with a 2-minute startTime offset
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:03:00') AS t(b)
        GROUP BY window(b, '5 minutes', '5 minutes', '2 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2020-12-31 23:57:00 | 2021-01-01 00:02:00 | 1   |
        | 2021-01-01 00:02:00 | 2021-01-01 00:07:00 | 1   |

  Rule: Null time values

    Scenario: rows with a null time value are dropped
      When query
        """
        SELECT window.start, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (CAST(NULL AS TIMESTAMP)) AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | cnt |
        | 2021-01-01 00:00:00 | 1   |

  Rule: Projection (no GROUP BY)

    Scenario: window can be projected per-row and its field accessed
      When query
        """
        SELECT window(b, '5 minutes').start AS s
        FROM VALUES (TIMESTAMP '2021-01-01 00:03:00'),
                    (TIMESTAMP '2021-01-01 00:07:00') AS t(b)
        ORDER BY s
        """
      Then query result
        | s                   |
        | 2021-01-01 00:00:00 |
        | 2021-01-01 00:05:00 |

  Rule: Re-invoking the window in SELECT

    Scenario: window re-used in SELECT resolves to the grouping column
      When query
        """
        SELECT window(b, '5 minutes').start AS s, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:07:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY s
        """
      Then query result
        | s                   | cnt |
        | 2021-01-01 00:00:00 | 1   |
        | 2021-01-01 00:05:00 | 1   |

  Rule: Pre-epoch timestamps

    Scenario: window correctly buckets a timestamp more than one slide before the start time
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '1969-12-31 23:50:01') AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 1969-12-31 23:50:00 | 1969-12-31 23:55:00 | 1   |

  Rule: Multi-unit duration strings

    Scenario: a "1 hour 30 minutes" duration is parsed and used as a tumbling window
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:10:00'),
                    (TIMESTAMP '2021-01-01 01:45:00') AS t(b)
        GROUP BY window(b, '1 hour 30 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 01:30:00 | 1   |
        | 2021-01-01 01:30:00 | 2021-01-01 03:00:00 | 1   |

  Rule: Integer duration literals

    # TODO: The aliases should be `start` and `end` (valid in Spark), but the SQL
    #   parser rejects `end` as a column alias even after an explicit `AS`.
    #   See the TODO on the `NamedExpr` alias parser in `sail-sql-parser`.
    Scenario: an integer duration is interpreted as microseconds
      When query
        """
        SELECT
            window(ts, 300000000).start AS ws,
            window(ts, 300000000).end AS we
        FROM VALUES (TIMESTAMP '2021-01-01 00:03:00') AS t(ts)
        """
      Then query result
        | ws                  | we                  |
        | 2021-01-01 00:00:00 | 2021-01-01 00:05:00 |

    Scenario: integer window and slide durations form a sliding window
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:03:00') AS t(ts)
        GROUP BY window(ts, 600000000, 300000000)
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2020-12-31 23:55:00 | 2021-01-01 00:05:00 | 1   |
        | 2021-01-01 00:00:00 | 2021-01-01 00:10:00 | 1   |

  Rule: Date time column

  Background:
      Given config spark.sql.session.timeZone = America/Los_Angeles

    Scenario: a date is cast to a session time zone timestamp, not timestamp_ntz
      When query
        """
        SELECT window(b, '1 day') AS w
        FROM VALUES (DATE '2021-01-01') AS t(b)
        """
      Then query schema
        """
        root
         |-- w: struct (nullable = true)
         |    |-- start: timestamp (nullable = true)
         |    |-- end: timestamp (nullable = true)
        """

    Scenario: date buckets are computed from the session time zone instant
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (DATE '2021-01-01') AS t(b)
        GROUP BY window(b, '1 day')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2020-12-31 16:00:00 | 2021-01-01 16:00:00 | 1   |

  Rule: Interval literal durations

    Scenario: a day-time interval literal is used as the window duration
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:03:00') AS t(b)
        GROUP BY window(b, INTERVAL '5' MINUTE)
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:05:00 | 1   |

    Scenario: a calendar interval literal is used as the window duration
      When query
        """
        SELECT window.start, window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:03:00') AS t(b)
        GROUP BY window(b, INTERVAL '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:05:00 | 1   |

  Rule: Argument validation

    Scenario Outline: Argument validation: <case>
      When query
        """
        SELECT count(*) FROM VALUES (<values>) AS t(<cols>)
        GROUP BY window(<args>)
        """
      Then query error <error>

      Examples:
        | case                                                             | values                                       | cols | args                         | error                                                                |
        | window rejects a single argument                                 | TIMESTAMP '2021-01-01 00:00:00'              | b    | b                            | .*window requires 2 to 4 arguments.*                                 |
        | window rejects a non-literal duration                            | TIMESTAMP '2021-01-01 00:00:00', '5 minutes' | b, d | b, d                         | .*must be literal strings, intervals, or integers.*                  |
        | window rejects a zero window duration                            | TIMESTAMP '2021-01-01 00:00:00'              | b    | b, '0 seconds'               | .*the window duration must be greater than 0.*                       |
        | window rejects a zero slide duration                             | TIMESTAMP '2021-01-01 00:00:00'              | b    | b, '10 minutes', '0 seconds' | .*the slide duration must be greater than 0.*                        |
        | window rejects a slide duration greater than the window duration | TIMESTAMP '2021-01-01 00:00:00'              | b    | b, '5 minutes', '10 minutes' | .*slide duration must be less than or equal to the window duration.* |
        | window rejects a non-timestamp time column                       | true                                         | b    | b, '5 minutes'               | .*window requires a timestamp time column.*                          |
        | window rejects a month-based duration (non-constant length)      | TIMESTAMP '2021-01-01 00:00:00'              | b    | b, '1 month'                 | .*must not contain months or years.*                                 |

    # The expected error regex contains `\(` escape sequences, and pytest-bdd doubles a
    # backslash inside an Examples cell, so this scenario cannot be parameterised.
    Scenario: window rejects a start time whose absolute value is not less than the slide duration
      When query
        """
        SELECT count(*) FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '10 minutes', '5 minutes', '5 minutes')
        """
      Then query error .*abs\(start_time\).*must be < the .*slide_duration.*

  Rule: Bounded plan size

    Scenario: window rejects pathological window/slide ratios
      When query
        """
        SELECT count(*) FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '1 day', '1 microsecond')
        """
      Then query error .*exceeds the limit.*

  Rule: ORDER BY a grouping output

    Scenario: ORDER BY window.start references the grouping output by name
      When query
        """
        SELECT window.start, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:07:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY window.start
        """
      Then query result
        | start               | cnt |
        | 2021-01-01 00:00:00 | 1   |
        | 2021-01-01 00:05:00 | 1   |
