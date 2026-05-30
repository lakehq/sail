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

  Rule: Argument validation

    Scenario: window rejects a start time whose absolute value is not less than the slide duration
      When query
        """
        SELECT count(*) FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '10 minutes', '5 minutes', '5 minutes')
        """
      Then query error .*abs\(start_time\).*must be < the .*slide_duration.*

    Scenario: window rejects a month-based duration (non-constant length)
      When query
        """
        SELECT count(*) FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '1 month')
        """
      Then query error .*must not contain months or years.*

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
