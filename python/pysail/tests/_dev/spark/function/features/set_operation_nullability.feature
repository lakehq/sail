Feature: Nested nullability of set operations and array functions

  # Spark's common type of inputs that differ in the nullability of nested fields is nullable
  # wherever an input is nullable, e.g. an array of non-null values with an array that holds
  # NULL values is an array that can hold NULL values.
  Rule: UNION merges the nullability of nested fields

    Scenario Outline: UNION ALL of <first> and <second>
      When query
        """
        SELECT a FROM (SELECT <first> AS a UNION ALL SELECT <second>) ORDER BY size(a)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
      Then query result ordered
        | a         |
        | [1]       |
        | [1, NULL] |

      Examples:
        | first          | second         |
        | array(1)       | array(1, NULL) |
        | array(1, NULL) | array(1)       |

    Scenario Outline: collect_list over UNION ALL of <first> and <second>
      When query
        """
        SELECT sort_array(collect_list(a)) AS c FROM (SELECT <first> AS a UNION ALL SELECT <second>)
        """
      Then query result
        | c             |
        | [[NULL], [1]] |

      Examples:
        | first       | second      |
        | array(1)    | array(NULL) |
        | array(NULL) | array(1)    |

    Scenario: collect_list over UNION ALL of collect_set and arrays with NULL values
      When query
        """
        SELECT sort_array(collect_list(a)) AS c
        FROM (
          SELECT sort_array(collect_set(x)) AS a FROM VALUES (1), (2) AS t(x)
          UNION ALL SELECT array(2, NULL)
          UNION ALL SELECT array(3)
        )
        """
      Then query result
        | c                        |
        | [[1, 2], [2, NULL], [3]] |

    Scenario: collect_list over UNION of collect_set and an array of NULL values
      When query
        """
        SELECT sort_array(collect_list(a)) AS c
        FROM (SELECT sort_array(collect_set(x)) AS a FROM VALUES (1), (2) AS t(x) UNION SELECT array(NULL))
        """
      Then query result
        | c                |
        | [[NULL], [1, 2]] |

    Scenario Outline: UNION ALL of nested arrays <first> and <second>
      When query
        """
        SELECT a FROM (SELECT <first> AS a UNION ALL SELECT <second>)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a        |
        | [[NULL]] |
        | [[1]]    |

      Examples:
        | first              | second             |
        | array(array(1))    | array(array(NULL)) |
        | array(array(NULL)) | array(array(1))    |

    Scenario Outline: collect_list over UNION ALL of nested arrays <first> and <second>
      When query
        """
        SELECT sort_array(collect_list(a)) AS c FROM (SELECT <first> AS a UNION ALL SELECT <second>)
        """
      Then query result
        | c                 |
        | [[[NULL]], [[1]]] |

      Examples:
        | first              | second             |
        | array(array(1))    | array(array(NULL)) |
        | array(array(NULL)) | array(array(1))    |

    Scenario: collect_list over UNION ALL of collect_set of arrays and a nested array of NULL values
      When query
        """
        SELECT size(collect_list(a)) AS n, sort_array(flatten(collect_list(a))) AS c
        FROM (
          SELECT collect_set(array(x)) AS a FROM VALUES (1) AS t(x)
          UNION ALL SELECT array(array(NULL))
        )
        """
      Then query result
        | n | c             |
        | 2 | [[NULL], [1]] |

    Scenario: UNION ALL of structs with an array field of NULL values
      When query
        """
        SELECT to_json(s) AS j
        FROM (SELECT named_struct('a', array(1)) AS s UNION ALL SELECT named_struct('a', array(NULL)))
        """
      Then query schema
        """
        root
         |-- j: string (nullable = true)
        """
      Then query result
        | j            |
        | {"a":[null]} |
        | {"a":[1]}    |

  Rule: INTERSECT and EXCEPT compare arrays that differ in nullability

    Scenario: INTERSECT and EXCEPT of arrays with and without NULL values
      When query
        """
        SELECT
          (SELECT count(*) FROM (SELECT array(1) AS a INTERSECT SELECT array(1, NULL))) AS i,
          (SELECT count(*) FROM (SELECT array(1) AS a EXCEPT SELECT array(2, NULL))) AS e
        """
      Then query result
        | i | e |
        | 0 | 1 |

  # Spark widens the inputs of INTERSECT and EXCEPT if some column types differ in more than
  # nullability, and the common type is nullable wherever an input is nullable. (If the inputs
  # differ only in nullability, Spark keeps the types of the left input, e.g. `array(1) INTERSECT
  # array(1, NULL)` has containsNull = false, where Sail reports containsNull = true.)
  Rule: INTERSECT and EXCEPT widen the nullability of nested fields

    Scenario Outline: <left> <op> <right>
      When query
        """
        SELECT * FROM (SELECT <left> AS a <op> SELECT <right>)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a |

      Examples:
        | left           | op        | right       |
        | array(1, NULL) | INTERSECT | array(1)    |
        | array(1)       | INTERSECT | array(NULL) |

    Scenario Outline: <left> <op> <right> with a result
      When query
        """
        SELECT * FROM (SELECT <left> AS a <op> SELECT <right>)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a        |
        | <result> |

      Examples:
        | left           | op     | right       | result    |
        | array(1, NULL) | EXCEPT | array(1)    | [1, NULL] |
        | array(1)       | EXCEPT | array(NULL) | [1]       |
        | array(NULL)    | EXCEPT | array(1)    | [NULL]    |

    Scenario: INTERSECT widens all columns if a column type differs
      When query
        """
        SELECT * FROM (SELECT array(1) AS a, 1 AS b INTERSECT SELECT array(1, NULL), 1L)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
         |-- b: long (nullable = false)
        """
      Then query result
        | a | b |

  Rule: Array functions merge the nullability of their array arguments

    Scenario: concat of arrays with and without NULL values
      When query
        """
        SELECT concat(array(1), array(2, NULL)) AS a, concat(array(2, NULL), array(1)) AS b
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a            | b            |
        | [1, 2, NULL] | [2, NULL, 1] |

    Scenario: concat of nested arrays with a nested array of NULL values
      When query
        """
        SELECT
          concat(collect_set(array(x)), array(array(NULL))) AS a,
          concat(array(array(NULL)), collect_set(array(x))) AS b,
          collect_set(array(x)) || array(array(NULL)) AS c,
          array(array(NULL)) || collect_set(array(x)) AS d
        FROM VALUES (1) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- b: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- c: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- d: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a             | b             | c             | d             |
        | [[1], [NULL]] | [[NULL], [1]] | [[1], [NULL]] | [[NULL], [1]] |

    Scenario: concat of nested array literals with a nested array of NULL values
      When query
        """
        SELECT
          concat(array(array(1)), array(array(NULL))) AS a,
          concat(array(array(NULL)), array(array(1))) AS b,
          array(array(1)) || array(array(NULL)) AS c
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- b: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- c: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a             | b             | c             |
        | [[1], [NULL]] | [[NULL], [1]] | [[1], [NULL]] |

    Scenario: array set functions with an array of NULL values
      When query
        """
        SELECT
          array_union(array(1), array(NULL)) AS a,
          array_except(array(1, 2), array(NULL)) AS b,
          array_except(array(NULL), array(1)) AS c,
          arrays_overlap(array(1), array(NULL)) AS d,
          array_intersect(array(1), array(NULL)) AS e
        """
      Then query result
        | a         | b      | c      | d    | e  |
        | [1, NULL] | [1, 2] | [NULL] | NULL | [] |

    # Spark's array_intersect result contains NULL only if both arrays can contain NULL, and the
    # array_except result has the element nullability of the first array.
    Scenario: element nullability of array_intersect
      When query
        """
        SELECT
          array_intersect(array(1), array(2, NULL)) AS a,
          array_intersect(array(2, NULL), array(1)) AS b,
          array_intersect(array(1, NULL), array(NULL)) AS c,
          array_intersect(array(1), array(NULL)) AS d
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- c: array (nullable = false)
         |    |-- element: integer (containsNull = true)
         |-- d: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
      Then query result
        | a  | b  | c      | d  |
        | [] | [] | [NULL] | [] |

    Scenario: element nullability of array_intersect with collect_set
      When query
        """
        SELECT
          array_intersect(collect_set(x), array(NULL)) AS a,
          array_intersect(array(NULL), collect_set(x)) AS b
        FROM VALUES (1) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
      Then query result
        | a  | b  |
        | [] | [] |

    # TODO: Remove @sail-bug after array_except declares a non-nullable result like Spark (pre-existing).
    @sail-bug
    Scenario: element nullability of array_except
      When query
        """
        SELECT
          array_except(array(1), array(2, NULL)) AS a,
          array_except(array(2, NULL), array(1)) AS b,
          array_except(array(1), array(NULL)) AS c,
          array_except(collect_set(x), array(NULL)) AS d
        FROM VALUES (1) AS t(x)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- b: array (nullable = false)
         |    |-- element: integer (containsNull = true)
         |-- c: array (nullable = false)
         |    |-- element: integer (containsNull = false)
         |-- d: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
      Then query result
        | a   | b         | c   | d   |
        | [1] | [2, NULL] | [1] | [1] |

    Scenario: element nullability of array_intersect of nested arrays
      When query
        """
        SELECT
          array_intersect(array(array(1)), array(array(1, NULL))) AS a,
          array_intersect(array(array(1)), array(array(NULL))) AS b
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
         |-- b: array (nullable = false)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: integer (containsNull = true)
        """
      Then query result
        | a  | b  |
        | [] | [] |

    Scenario: array set functions with a nested array of NULL values
      When query
        """
        SELECT
          sort_array(array_union(array(array(1)), array(array(NULL)))) AS a,
          array_except(array(array(1), array(2)), array(array(NULL))) AS b,
          arrays_overlap(array(array(1)), array(array(NULL))) AS c,
          element_at(map(array(array(1)), 'x'), array(array(NULL))) AS d
        """
      Then query result
        | a             | b          | c     | d    |
        | [[NULL], [1]] | [[1], [2]] | false | NULL |
