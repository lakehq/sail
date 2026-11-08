Feature: Aggregate functions with FILTER over a window

  # Spark supports FILTER (WHERE ...) for aggregate functions over a window. Rows that do not
  # match the filter are not aggregated but still count toward the window frame.
  Rule: Rows that do not match the filter are not aggregated

    Scenario: FILTER over partitions and the whole input
      When query
        """
        SELECT id,
          count(s) FILTER (WHERE id > 1) OVER (PARTITION BY id % 2) AS a,
          sum(v) FILTER (WHERE v > 15) OVER () AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a | b   |
        | 1  | 2 | 140 |
        | 2  | 2 | 140 |
        | 3  | 2 | 140 |
        | 4  | 2 | 140 |
        | 5  | 2 | 140 |
        | 6  | 2 | 140 |

    Scenario: FILTER over a growing window
      When query
        """
        SELECT id,
          sum(v) FILTER (WHERE id % 2 = 1) OVER (ORDER BY id) AS a,
          count(*) FILTER (WHERE v IS NULL) OVER (ORDER BY id) AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a  | b |
        | 1  | 10 | 0 |
        | 2  | 10 | 0 |
        | 3  | 40 | 0 |
        | 4  | 40 | 0 |
        | 5  | 90 | 0 |
        | 6  | 90 | 1 |

    Scenario: FILTER over sliding row frames
      When query
        """
        SELECT id,
          sum(v) FILTER (WHERE id <> 3) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a,
          avg(v) FILTER (WHERE id <> 3) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a  | b    |
        | 1  | 30 | 10.0 |
        | 2  | 30 | 15.0 |
        | 3  | 60 | 20.0 |
        | 4  | 90 | 40.0 |
        | 5  | 90 | 45.0 |
        | 6  | 50 | 50.0 |

    Scenario: FILTER for min and max over sliding frames
      When query
        """
        SELECT id,
          min(v) FILTER (WHERE id > 2) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a,
          max(s) FILTER (WHERE s <> 'e') OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a    | b    |
        | 1  | NULL | d    |
        | 2  | NULL | d    |
        | 3  | 30   | d    |
        | 4  | 30   | d    |
        | 5  | 30   | NULL |
        | 6  | 40   | NULL |

    Scenario: FILTER over a named window and a RANGE frame
      When query
        """
        SELECT id,
          sum(v) FILTER (WHERE id > 2) OVER w AS a,
          sum(v) FILTER (WHERE v > 15) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        WINDOW w AS (PARTITION BY id % 2 ORDER BY id)
        ORDER BY id
        """
      Then query result ordered
        | id | a    | b    |
        | 1  | NULL | NULL |
        | 2  | NULL | 20   |
        | 3  | 30   | 50   |
        | 4  | 40   | 70   |
        | 5  | 80   | 90   |
        | 6  | 40   | 50   |

    Scenario: FILTER for collect_list, max_by and listagg over a window
      When query
        """
        SELECT id,
          collect_list(s) FILTER (WHERE id % 2 = 0) OVER (ORDER BY id) AS a,
          max_by(s, v) FILTER (WHERE id <> 5) OVER () AS b,
          listagg(s, '-') FILTER (WHERE id > 2) OVER (ORDER BY id) AS c
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a      | b | c     |
        | 1  | []     | d | NULL  |
        | 2  | [b]    | d | NULL  |
        | 3  | [b]    | d | c     |
        | 4  | [b, d] | d | c-d   |
        | 5  | [b, d] | d | c-d-e |
        | 6  | [b, d] | d | c-d-e |

    Scenario: FILTER for percentile_cont WITHIN GROUP over a window
      When query
        """
        SELECT id, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FILTER (WHERE id > 2) OVER (PARTITION BY id % 2) AS a
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a    |
        | 1  | 40.0 |
        | 2  | 40.0 |
        | 3  | 40.0 |
        | 4  | 40.0 |
        | 5  | 40.0 |
        | 6  | 40.0 |

    Scenario: FILTER for percentile_cont and percentile_disc WITHIN GROUP DESC over a window
      When query
        """
        SELECT id,
          percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE id > 1) OVER () AS a,
          percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC) FILTER (WHERE id < 5) OVER (PARTITION BY id % 2) AS b
        FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (5, 'e', 50), (6, CAST(NULL AS STRING), CAST(NULL AS INT)) AS t(id, s, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a    | b    |
        | 1  | 42.5 | 30.0 |
        | 2  | 42.5 | 40.0 |
        | 3  | 42.5 | 30.0 |
        | 4  | 42.5 | 40.0 |
        | 5  | 42.5 | 30.0 |
        | 6  | 42.5 | 40.0 |

    Scenario: FILTER that matches no rows over a window
      When query
        """
        SELECT id, sum(v) FILTER (WHERE id > 5) OVER () AS a, count(v) FILTER (WHERE v > 100) OVER () AS b
        FROM VALUES (1, 10), (2, 20) AS t(id, v)
        ORDER BY id
        """
      Then query result ordered
        | id | a    | b |
        | 1  | NULL | 0 |
        | 2  | NULL | 0 |

  Rule: FILTER is not supported for every window function

    Scenario Outline: <case> is an error
      When query
        """
        SELECT id, <expr> AS a
        FROM VALUES (1, 'a', 10), (2, 'b', 20) AS t(id, s, v)
        """
      Then query error <error>

      Examples:
        | case                       | expr                                                  | error                                               |
        | DISTINCT with FILTER       | count(DISTINCT s) FILTER (WHERE id > 1) OVER ()       | DISTINCT_WINDOW_FUNCTION_UNSUPPORTED                |
        | row_number with FILTER     | row_number() FILTER (WHERE id > 1) OVER (ORDER BY id) | INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX |
        | a NULL filter              | count(v) FILTER (WHERE NULL) OVER ()                  | INVALID_AGGREGATE_FILTER.NOT_BOOLEAN                |
        | a non-deterministic filter | sum(v) FILTER (WHERE rand() > 0.5) OVER ()            | INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC          |
