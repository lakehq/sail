Feature: collect_set output schema

  # Spark's collect_set ignores NULL values unless it respects them, so the result elements
  # are never NULL (containsNull = false). With RESPECT NULLS, the result can hold one NULL.
  @function(nullability)
  Rule: Output schema

    Scenario: collect_set elements are never NULL
      When query
        """
        SELECT collect_set(x) AS a FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: collect_set with IGNORE NULLS, DISTINCT or FILTER elements are never NULL
      When query
        """
        SELECT
          collect_set(x) IGNORE NULLS AS a,
          collect_set(DISTINCT x) AS b,
          collect_set(x) FILTER (WHERE x > 1) AS c
        FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- c: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: collect_set with RESPECT NULLS elements can be NULL
      When query
        """
        SELECT collect_set(x) RESPECT NULLS AS a FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    Scenario: collect_set of structs with NULL fields
      When query
        """
        SELECT collect_set(s) AS a FROM VALUES (named_struct('f', 1)), (named_struct('f', CAST(NULL AS INT))), (NULL) AS t(s)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- f: integer (nullable = true)
        """

    Scenario: collect_set over no rows is an empty array
      When query
        """
        SELECT collect_set(x) AS a FROM VALUES (1), (2) AS t(x) WHERE x > 5
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
      Then query result
        | a  |
        | [] |

  Rule: Values

    Scenario: collect_set collects distinct values and ignores NULLs
      When query
        """
        SELECT sort_array(collect_set(x)) AS a, sort_array(collect_set(x) RESPECT NULLS) AS b
        FROM VALUES (1), (NULL), (2), (1) AS t(x)
        """
      Then query result
        | a      | b            |
        | [1, 2] | [NULL, 1, 2] |

    Scenario: collect_set per group with only NULL values is an empty array
      When query
        """
        SELECT g, sort_array(collect_set(x)) AS a
        FROM VALUES (1, 1), (1, NULL), (2, NULL), (1, 1) AS t(g, x)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | a   |
        | 1 | [1] |
        | 2 | []  |

  # The elements of collect_set are never NULL, while other arrays can hold NULL elements.
  # Functions of several arrays accept arrays that differ in the nullability of their elements.
  Rule: collect_set results with arrays that can hold NULL elements

    Scenario: array_except with collect_set
      When query
        """
        SELECT
          sort_array(array_except(collect_set(x), array(1, NULL))) AS a,
          sort_array(array_except(array(1, NULL, 3), collect_set(x))) AS b
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a   | b         |
        | [2] | [NULL, 3] |

    Scenario: array_union with collect_set
      When query
        """
        SELECT
          sort_array(array_union(collect_set(x), array(3, NULL))) AS a,
          sort_array(array_union(array(3, NULL), collect_set(x))) AS b
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a               | b               |
        | [NULL, 1, 2, 3] | [NULL, 1, 2, 3] |

    Scenario: array_intersect and arrays_overlap with collect_set
      When query
        """
        SELECT
          sort_array(array_intersect(collect_set(x), array(2, NULL))) AS a,
          arrays_overlap(collect_set(x), array(5, NULL)) AS b
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a   | b    |
        | [2] | NULL |

    Scenario: map_concat with collect_set values
      When query
        """
        SELECT
          map_concat(map(1, sort_array(collect_set(x))), map(2, array(3, NULL))) AS a,
          map_concat(map(1, sort_array(collect_set(x))), map(2, sort_array(collect_set(x)))) AS b
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a                             | b                          |
        | {1 -> [1, 2], 2 -> [3, NULL]} | {1 -> [1, 2], 2 -> [1, 2]} |

    Scenario: concat of collect_set with an array of NULL values
      When query
        """
        SELECT
          sort_array(concat(collect_set(x), array(NULL))) AS a,
          sort_array(array_repeat(NULL, 1) || collect_set(x)) AS b
        FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query result
        | a            | b            |
        | [NULL, 1, 2] | [NULL, 1, 2] |

    Scenario: functions of collect_set and an array of NULL values
      When query
        """
        SELECT
          sort_array(array_union(collect_set(x), array(NULL))) AS a,
          sort_array(array_union(array(NULL), collect_set(x))) AS b,
          sort_array(array_except(collect_set(x), array(NULL))) AS c,
          sort_array(array_except(array(NULL), collect_set(x))) AS d,
          sort_array(array_intersect(collect_set(x), array(NULL))) AS e,
          arrays_overlap(collect_set(x), array(NULL)) AS f,
          element_at(map(sort_array(collect_set(x)), 'k'), array(NULL)) AS g
        FROM VALUES (1), (NULL), (2) AS t(x)
        """
      Then query result
        | a            | b            | c      | d      | e  | f    | g    |
        | [NULL, 1, 2] | [NULL, 1, 2] | [1, 2] | [NULL] | [] | NULL | NULL |

    Scenario: element_at of a map with collect_set keys
      When query
        """
        SELECT
          element_at(map(sort_array(collect_set(x)), 'found'), array(1, 2)) AS a,
          element_at(map(sort_array(collect_set(x)), 'found'), array(1, NULL)) AS b
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a     | b    |
        | found | NULL |

    Scenario: functions of nested arrays with collect_set
      When query
        """
        SELECT
          size(array_except(array(collect_set(x)), array(array(1, NULL)))) AS a,
          arrays_overlap(array(sort_array(collect_set(x))), array(array(1, NULL))) AS b,
          size(array_intersect(array(collect_set(x)), array(array(1, NULL)))) AS c,
          array_intersect(array(sort_array(collect_set(x))), array(array(1, 2))) AS d
        FROM VALUES (1), (2), (NULL), (2) AS t(x)
        """
      Then query result
        | a | b     | c | d        |
        | 1 | false | 0 | [[1, 2]] |

  Rule: collect_set of nested values

    Scenario: collect_set of structs is an array of structs that are never NULL
      When query
        """
        SELECT collect_set(named_struct('a', CAST(NULL AS INT))) AS a FROM range(2)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- a: integer (nullable = true)
        """
      Then query result
        | a        |
        | [{NULL}] |

    Scenario: collect_set of structs over a sliding window
      When query
        """
        SELECT id, collect_set(named_struct('a', id % 1)) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a
        FROM range(2)
        ORDER BY id
        """
      Then query result ordered
        | id | a     |
        | 0  | [{0}] |
        | 1  | [{0}] |

    Scenario: collect_set of arrays of structs
      When query
        """
        SELECT collect_set(array(named_struct('a', 1))) AS a FROM range(2)
        """
      Then query result
        | a       |
        | [[{1}]] |
