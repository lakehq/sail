Feature: session_window() gap-based sessionization

  # Sessions merge consecutive rows (per group, ordered by time) while the next
  # row starts at or before the current session's end (max of merged time + gap).

  Rule: Static gap

    Scenario: a 5-minute gap merges rows per key into sessions
      When query
        """
        SELECT a, session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES ('A1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A1', TIMESTAMP '2021-01-01 00:04:30'),
                    ('A1', TIMESTAMP '2021-01-01 00:10:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:01:00') AS tab(a, b)
        GROUP BY a, session_window(b, '5 minutes')
        ORDER BY a, start
        """
      Then query result
        | a  | start               | end                 | cnt |
        | A1 | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | A1 | 2021-01-01 00:10:00 | 2021-01-01 00:15:00 | 1   |
        | A2 | 2021-01-01 00:01:00 | 2021-01-01 00:06:00 | 1   |

    Scenario: a row landing exactly on the session end merges (not a new session)
      When query
        """
        SELECT session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:05:00'),
                    (TIMESTAMP '2021-01-01 00:10:00') AS t(b)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      # Rows exactly on the running session end merge (start <= end), not split.
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:15:00 | 3   |

  Rule: Dynamic gap

    Scenario: a per-row CASE gap is applied independently per key
      When query
        """
        SELECT a, session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES ('A1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A1', TIMESTAMP '2021-01-01 00:04:30'),
                    ('A1', TIMESTAMP '2021-01-01 00:10:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:01:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:04:30') AS tab(a, b)
        GROUP BY a, session_window(b, CASE WHEN a = 'A1' THEN '5 minutes'
                                           WHEN a = 'A2' THEN '1 minute'
                                           ELSE '10 minutes' END)
        ORDER BY a, start
        """
      Then query result
        | a  | start               | end                 | cnt |
        | A1 | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | A1 | 2021-01-01 00:10:00 | 2021-01-01 00:15:00 | 1   |
        | A2 | 2021-01-01 00:01:00 | 2021-01-01 00:02:00 | 1   |
        | A2 | 2021-01-01 00:04:30 | 2021-01-01 00:05:30 | 1   |

  Rule: Month-bearing gap

    # A month-bearing string gap is valid and uses per-row calendar arithmetic:
    # Jan 31 + 1 month = Feb 28 (day clamped to month end).
    Scenario: a month string gap uses calendar arithmetic and merges across months
      When query
        """
        SELECT session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-31 00:00:00'),
                    (TIMESTAMP '2021-02-27 00:00:00') AS t(b)
        GROUP BY session_window(b, '1 month')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-31 00:00:00 | 2021-03-27 00:00:00 | 2   |

    # Spark rejects a year-month *typed* interval gap; only strings and calendar
    # intervals pass.
    Scenario: a year-month typed interval gap is rejected
      When query
        """
        SELECT count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY session_window(b, INTERVAL '1' MONTH)
        """
      Then query error .*ap duration expression used in session window must be.*

  Rule: Interval literal gap

    # Spark 4.2 rejects a day-time *typed* interval gap; Sail deliberately
    # accepts it as unambiguous.
    @sail-only
    Scenario: a day-time interval literal is used as the gap (no other group keys)
      When query
        """
        SELECT session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30'),
                    (TIMESTAMP '2021-01-01 00:10:00') AS t(b)
        GROUP BY session_window(b, INTERVAL '5' MINUTE)
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | 2021-01-01 00:10:00 | 2021-01-01 00:15:00 | 1   |

  Rule: Null time values

    Scenario: rows with a null time value are dropped
      When query
        """
        SELECT a, session_window.start, count(*) AS cnt
        FROM VALUES ('A', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A', CAST(NULL AS TIMESTAMP)) AS tab(a, b)
        GROUP BY a, session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | a | start               | cnt |
        | A | 2021-01-01 00:00:00 | 1   |

  Rule: Non-positive gap

    # A non-positive gap is not an analysis error: the `end > start` filter
    # drops every row.
    Scenario: a static gap of zero yields an empty result, not an error
      When query
        """
        SELECT session_window.start, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY session_window(b, '0 seconds')
        """
      Then query result
        | start | cnt |

    Scenario: a static negative gap yields an empty result, not an error
      When query
        """
        SELECT session_window.start, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY session_window(b, '-5 seconds')
        """
      Then query result
        | start | cnt |

    Scenario: a dynamic gap of zero drops only that key's rows
      When query
        """
        SELECT a, session_window.start, count(*) AS cnt
        FROM VALUES ('A1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:01:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:02:00') AS tab(a, b)
        GROUP BY a, session_window(b, CASE WHEN a = 'A1' THEN '5 minutes'
                                           ELSE '0 seconds' END)
        ORDER BY start
        """
      Then query result
        | a  | start               | cnt |
        | A1 | 2021-01-01 00:00:00 | 1   |

  Rule: Unsorted input

    Scenario: rows out of time order are sessionized after sorting
      When query
        """
        SELECT session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:10:00'),
                    (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30') AS t(b)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | end                 | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | 2021-01-01 00:10:00 | 2021-01-01 00:15:00 | 1   |

  Rule: Whole struct selection

    Scenario: the session_window column is a struct of start/end timestamps
      When query
        """
        SELECT session_window(b, '5 minutes') AS w
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY session_window(b, '5 minutes')
        """
      Then query schema
        """
        root
         |-- w: struct (nullable = false)
         |    |-- start: timestamp (nullable = true)
         |    |-- end: timestamp (nullable = true)
        """

  Rule: Re-using the session window in SELECT and HAVING

    Scenario: session_window re-used in SELECT resolves to the grouping column, and HAVING filters sessions
      When query
        """
        SELECT session_window(b, '5 minutes').start AS s, count(*) AS cnt
        FROM VALUES ('A1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A1', TIMESTAMP '2021-01-01 00:04:30'),
                    ('A1', TIMESTAMP '2021-01-01 00:10:00'),
                    ('A2', TIMESTAMP '2021-01-01 00:01:00') AS tab(a, b)
        GROUP BY a, session_window(b, '5 minutes')
        HAVING count(*) > 1
        ORDER BY s
        """
      Then query result
        | s                   | cnt |
        | 2021-01-01 00:00:00 | 2   |

  Rule: Grouping forms

    Scenario: session_window referenced by GROUP BY ordinal
      When query
        """
        SELECT a, session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES ('A1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('A1', TIMESTAMP '2021-01-01 00:04:30'),
                    ('A2', TIMESTAMP '2021-01-01 00:01:00') AS tab(a, b)
        GROUP BY session_window(b, '5 minutes'), 1
        ORDER BY a, start
        """
      Then query result
        | a  | start               | end                 | cnt |
        | A1 | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | A2 | 2021-01-01 00:01:00 | 2021-01-01 00:06:00 | 1   |

    Scenario: session_window in the SELECT list referenced only by ordinal
      When query
        """
        SELECT session_window(b, '5 minutes'), count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30') AS t(b)
        GROUP BY 1
        """
      Then query result
        | session_window                             | cnt |
        | {2021-01-01 00:00:00, 2021-01-01 00:09:30} | 2   |

    Scenario: session_window grouped by its SELECT-list alias
      When query
        """
        SELECT session_window(b, '5 minutes') AS sw, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30') AS t(b)
        GROUP BY sw
        """
      Then query result
        | sw                                         | cnt |
        | {2021-01-01 00:00:00, 2021-01-01 00:09:30} | 2   |

  Rule: Expression grouping keys

    Scenario: a non-column grouping key (upper) partitions sessions and resolves in SELECT
      When query
        """
        SELECT upper(a) AS k, session_window.start, session_window.end, count(*) AS cnt
        FROM VALUES ('a1', TIMESTAMP '2021-01-01 00:00:00'),
                    ('a1', TIMESTAMP '2021-01-01 00:04:30'),
                    ('A1', TIMESTAMP '2021-01-01 00:10:00') AS tab(a, b)
        GROUP BY session_window(b, '5 minutes'), upper(a)
        ORDER BY start
        """
      Then query result
        | k  | start               | end                 | cnt |
        | A1 | 2021-01-01 00:00:00 | 2021-01-01 00:09:30 | 2   |
        | A1 | 2021-01-01 00:10:00 | 2021-01-01 00:15:00 | 1   |

    Scenario: an arithmetic grouping key partitions sessions
      When query
        """
        SELECT id + 1 AS k, session_window.start, count(*) AS cnt
        FROM VALUES (10, TIMESTAMP '2021-01-01 00:00:00'),
                    (10, TIMESTAMP '2021-01-01 00:01:00'),
                    (20, TIMESTAMP '2021-01-01 00:00:00') AS tab(id, b)
        GROUP BY session_window(b, '5 minutes'), id + 1
        ORDER BY k, start
        """
      Then query result
        | k  | start               | cnt |
        | 11 | 2021-01-01 00:00:00 | 2   |
        | 21 | 2021-01-01 00:00:00 | 1   |

    Scenario: a cast grouping key partitions sessions
      When query
        """
        SELECT CAST(id AS STRING) AS k, session_window.start, count(*) AS cnt
        FROM VALUES (1, TIMESTAMP '2021-01-01 00:00:00'),
                    (1, TIMESTAMP '2021-01-01 00:01:00'),
                    (2, TIMESTAMP '2021-01-01 00:00:00') AS tab(id, b)
        GROUP BY session_window(b, '5 minutes'), CAST(id AS STRING)
        ORDER BY k, start
        """
      Then query result
        | k | start               | cnt |
        | 1 | 2021-01-01 00:00:00 | 2   |
        | 2 | 2021-01-01 00:00:00 | 1   |

    Scenario: a date_trunc grouping key over a second timestamp partitions sessions
      When query
        """
        SELECT date_trunc('day', c) AS day, session_window.start AS s, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 23:00:00', TIMESTAMP '2021-01-01 00:01:00'),
                    (TIMESTAMP '2021-01-02 05:00:00', TIMESTAMP '2021-01-01 00:00:00') AS tab(c, b)
        GROUP BY session_window(b, '5 minutes'), date_trunc('day', c)
        ORDER BY day, s
        """
      Then query result
        | day                 | s                   | cnt |
        | 2021-01-01 00:00:00 | 2021-01-01 00:00:00 | 2   |
        | 2021-01-02 00:00:00 | 2021-01-01 00:00:00 | 1   |

  Rule: Aggregates

    # Spark evaluates the marker inside an aggregate argument with per-row
    # (pre-merge) semantics; Sail rejects that path instead of silently
    # aggregating the merged struct.
    @sail-only
    Scenario: an aggregate over the session struct is rejected (per-row semantics)
      When query
        """
        SELECT max(session_window(b, '5 minutes').start) AS m, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:04:30'),
                    (TIMESTAMP '2021-01-01 00:10:00') AS t(b)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY m
        """
      Then query error .*session_window inside an aggregate function.*

    Scenario: multiple aggregates are computed per session (fused path)
      When query
        """
        SELECT session_window.start, count(*) AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00', 10),
                    (TIMESTAMP '2021-01-01 00:04:30', 20),
                    (TIMESTAMP '2021-01-01 00:10:00', 5) AS t(b, v)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | cnt | s  | mn | mx |
        | 2021-01-01 00:00:00 | 2   | 30 | 10 | 20 |
        | 2021-01-01 00:10:00 | 1   | 5  | 5  | 5  |

    Scenario: a FILTER (WHERE) aggregate is computed per session (fused path)
      When query
        """
        SELECT session_window.start,
               count(*) FILTER (WHERE v > 10) AS cnt_big,
               sum(v) AS total
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00', 5),
                    (TIMESTAMP '2021-01-01 00:04:30', 20),
                    (TIMESTAMP '2021-01-01 00:10:00', 30) AS t(b, v)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | cnt_big | total |
        | 2021-01-01 00:00:00 | 1       | 25    |
        | 2021-01-01 00:10:00 | 1       | 30    |

    Scenario: a FILTER that excludes every row of a session yields zero, not a dropped session
      When query
        """
        SELECT session_window.start,
               count(*) FILTER (WHERE v > 100) AS cnt_big,
               count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00', 5),
                    (TIMESTAMP '2021-01-01 00:04:30', 20) AS t(b, v)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | cnt_big | cnt |
        | 2021-01-01 00:00:00 | 0       | 2   |

    Scenario: a DISTINCT aggregate over a session (fallback path)
      When query
        """
        SELECT session_window.start, count(DISTINCT v) AS dc
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00', 10),
                    (TIMESTAMP '2021-01-01 00:01:00', 10),
                    (TIMESTAMP '2021-01-01 00:02:00', 20) AS t(b, v)
        GROUP BY session_window(b, '5 minutes')
        ORDER BY start
        """
      Then query result
        | start               | dc |
        | 2021-01-01 00:00:00 | 2  |

  Rule: Argument validation

    # Spark gives a nested session_window per-row semantics (no merge); Sail
    # rejects it at analysis time instead of implementing that path.
    @sail-only
    Scenario: session_window nested in a grouping expression is rejected
      When query
        """
        SELECT count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY struct(session_window(b, '5 minutes'))
        """
      Then query error .*session_window is only supported as a top-level grouping expression.*

    Scenario: session_window rejects a single argument
      When query
        """
        SELECT count(*) FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY session_window(b)
        """
      Then query error .*session_window.*requires (exactly 2 arguments|2 parameters).*
