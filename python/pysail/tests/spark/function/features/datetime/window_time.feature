Feature: window_time() event-time extraction function

  Rule: window_time returns the window end minus one microsecond

    # Spark treats `window_time(window)` in the SELECT list as a non-aggregating expression over
    # columns absent from GROUP BY and rejects it with MISSING_AGGREGATION. Sail resolves it
    # against the grouping output and computes the value.
    @sail-bug
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
      Then query error \[MISSING_AGGREGATION\].*window_time

    @sail-bug
    Scenario: window_time over a sliding window
      When query
        """
        SELECT window_time(window) AS wt, count(*) AS cnt
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00'),
                    (TIMESTAMP '2021-01-01 00:06:00') AS t(b)
        GROUP BY window(b, '10 minutes', '5 minutes')
        ORDER BY wt
        """
      Then query error \[MISSING_AGGREGATION\].*window_time

    @sail-bug
    Scenario: window_time output column is named after the call
      When query
        """
        SELECT window_time(window)
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        GROUP BY window(b, '5 minutes')
        """
      Then query error \[MISSING_AGGREGATION\].*window_time

  Rule: window_time argument validation

    @sail-bug
    Scenario: window_time rejects a non-window column
      When query
        """
        SELECT window_time(b)
        FROM VALUES (TIMESTAMP '2021-01-01 00:00:00') AS t(b)
        """
      Then query error \[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "STRUCT" type

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to window_time yields the schema Spark declares
      When query
        """
        SELECT a, window.start as start, window.end as end, window_time(window), cnt FROM (SELECT a, window, count(*) as cnt FROM VALUES ('A1', '2021-01-01 00:00:00'), ('A1', '2021-01-01 00:04:30'), ('A1', '2021-01-01 00:06:00'), ('A2', '2021-01-01 00:01:00') AS tab(a, b) GROUP by a, window(b, '5 minutes') ORDER BY a, window.start) AS result
        """
      Then query schema
        """
        root
         |-- a: string (nullable = false)
         |-- start: timestamp (nullable = true)
         |-- end: timestamp (nullable = true)
         |-- window_time(window): timestamp (nullable = true)
         |-- cnt: long (nullable = false)
        """
