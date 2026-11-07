Feature: collect_set over window functions

  Rule: collect_set over expanding window

    Scenario: collect_set returns distinct values without NULLs over expanding window
      When query
        """
        SELECT
          id,
          value,
          sort_array(collect_set(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), true) AS cset
        FROM VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, NULL), (5, 'c') AS t(id, value)
        """
      Then query result ordered
        | id | value | cset      |
        | 1  | a     | [a]       |
        | 2  | b     | [a, b]    |
        | 3  | a     | [a, b]    |
        | 4  | NULL  | [a, b]    |
        | 5  | c     | [a, b, c] |

  Rule: collect_set over partitioned window

    Scenario: collect_set partitioned by group collects distinct values per partition
      When query
        """
        SELECT
          grp,
          id,
          value,
          sort_array(collect_set(value) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), true) AS cset
        FROM VALUES (1, 1, 10), (1, 2, 20), (1, 3, 10), (2, 4, 30), (2, 5, 30) AS t(grp, id, value)
        """
      Then query result ordered
        | grp | id | value | cset     |
        | 1   | 1  | 10    | [10]     |
        | 1   | 2  | 20    | [10, 20] |
        | 1   | 3  | 10    | [10, 20] |
        | 2   | 4  | 30    | [30]     |
        | 2   | 5  | 30    | [30]     |

  @function(nullability)
  Rule: Output schema

    # Spark's collect_set ignores NULL values unless it respects them, so the result elements
    # are never NULL (containsNull = false). With RESPECT NULLS, the result can hold one NULL.
    Scenario: collect_set over a window has elements that are never NULL
      When query
        """
        SELECT collect_set(x) OVER () AS a, collect_set(x) RESPECT NULLS OVER () AS b
        FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    Scenario: collect_set over a sliding window frame
      When query
        """
        SELECT id, sort_array(collect_set(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) AS a
        FROM VALUES (1, 1), (2, NULL), (3, 2) AS t(id, x)
        ORDER BY id
        """
      Then query result ordered
        | id | a   |
        | 1  | [1] |
        | 2  | [1] |
        | 3  | [2] |
