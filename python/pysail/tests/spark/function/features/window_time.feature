Feature: window_time() event-time extraction function

  Rule: window_time returns the window end minus one microsecond

    Scenario: window_time over a tumbling window
      When query
        """
        SELECT window.start AS start, window.end AS end, window_time(window) AS wt, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30'),
                    (TIMESTAMP '2021-01-01 00:07:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | wt                         | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:05:00 | 2021-01-01 00:04:59.999999 | 2   |
        | 2021-01-01 00:05:00 | 2021-01-01 00:10:00 | 2021-01-01 00:09:59.999999 | 1   |

    Scenario: window_time over a sliding window
      When query
        """
        SELECT window_time(window) AS wt, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:06:00') AS t(b)
        GROUP BY window(b, '10 minutes', '5 minutes')
        ORDER BY wt
        """
      Then query result
        | wt                         | cnt |
        | 2021-01-01 00:04:59.999999 | 1   |
        | 2021-01-01 00:09:59.999999 | 2   |
        | 2021-01-01 00:14:59.999999 | 1   |

    Scenario: window_time output column is named after the call
      When query
        """
        SELECT window_time(window)
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        """
      Then query result
        | window_time(window)        |
        | 2021-01-01 00:04:59.999999 |

  Rule: window_time argument validation

    Scenario: window_time rejects a non-window column
      When query
        """
        SELECT window_time(b)
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        """
      Then query error .*window_time requires a window column.*
