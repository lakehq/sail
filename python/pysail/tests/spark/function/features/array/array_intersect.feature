@array_intersect
Feature: array_intersect() returns common array elements without duplicates

  Rule: Source-of-truth examples

    Scenario Outline: array_intersect <case>
      When query
        """
        SELECT sort_array(array_intersect(<left>, <right>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | left                  | right                     | result    |
        | basic usage              | array('b', 'a', 'c')  | array('c', 'd', 'a', 'f') | [a, c]    |
        | with all common elements | array('a', 'b', 'c')  | array('a', 'b', 'c')      | [a, b, c] |
        | with null values         | array('a', 'b', NULL) | array('a', NULL, 'c')     | [NULL, a] |

    # These two keep the raw (unsorted) result because it is already empty.
    Scenario Outline: array_intersect <case> returns an empty array
      When query
        """
        SELECT array_intersect(<left>, <right>) AS result
        """
      Then query result
        | result |
        | []     |

      Examples:
        | case                    | left                           | right                |
        | with no common elements | array('b', 'a', 'c')           | array('d', 'e', 'f') |
        | with empty arrays       | CAST(array() AS ARRAY<STRING>) | array('a', 'b', 'c') |

    Scenario Outline: array_intersect on array columns <case>
      When query
        """
        SELECT array_intersect(c1, c2) AS result
        FROM VALUES
          (<pair>)
        AS t(c1, c2)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | pair                                            | result |
        | preserves first-array order               | array('b', 'a', 'c'), array('c', 'd', 'a', 'f') | [a, c] |
        | preserves left order when left is shorter | array(1, 2), array(2, 1, 3)                     | [1, 2] |

  Rule: Duplicates and multiple rows

    Scenario: array_intersect removes duplicates from the result
      When query
        """
        SELECT sort_array(array_intersect(array(1, 2, 2, 3, 3), array(2, 2, 3, 4))) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: array_intersect on identical arrays deduplicates in left-array order
      When query
        """
        SELECT array_intersect(c1, c2) AS result
        FROM VALUES
          (array('b', 'a', 'c', 'c'), array('b', 'a', 'c', 'c'))
        AS t(c1, c2)
        """
      Then query result
        | result    |
        | [b, a, c] |

    Scenario: array_intersect across multiple rows
      When query
        """
        SELECT id, sort_array(array_intersect(left_arr, right_arr)) AS result
        FROM VALUES
          (1, array(1, 2, 3), array(2, 3, 4)),
          (2, array(1, 1, 1), array(1, 2)),
          (3, array(1, 2), array(3, 4)),
          (4, CAST(NULL AS ARRAY<INT>), array(1, 2))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | [2, 3] |
        | 2  | [1]    |
        | 3  | []     |
        | 4  | NULL   |

  Rule: Type-specific behavior

    Scenario Outline: array_intersect with <case> arrays
      When query
        """
        SELECT sort_array(array_intersect(<left>, <right>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | left                     | right                   | result     |
        | boolean | array(true, false, true) | array(false, false)     | [false]    |
        | double  | array(1.5D, 2.5D, 3.5D)  | array(3.5D, 4.5D, 1.5D) | [1.5, 3.5] |

    Scenario: array_intersect with arrays of structs
      When query
        """
        SELECT array_intersect(
          array(named_struct('id', 1, 'name', 'a'), named_struct('id', 2, 'name', 'b')),
          array(named_struct('id', 2, 'name', 'b'), named_struct('id', 3, 'name', 'c'))
        ) AS result
        """
      Then query result
        | result   |
        | [{2, b}] |

    Scenario: array_intersect with array<null> element type
      When query
        """
        SELECT id, array_intersect(left_arr, right_arr) AS result
        FROM VALUES
          (1, array(NULL), array(NULL, NULL)),
          (2, array(NULL), array()),
          (3, array(NULL, NULL), array(NULL))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | [NULL] |
        | 2  | []     |
        | 3  | [NULL] |

    Scenario: array<null> elements do not match typed non-null elements
      When query
        """
        SELECT id, array_intersect(left_arr, right_arr) AS result
        FROM VALUES
          (1, array(NULL, NULL), array('d', 'e', 'f')),
          (2, array(NULL),       CAST(array() AS ARRAY<STRING>)),
          (3, array(),           CAST(array() AS ARRAY<STRING>))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | []     |
        | 2  | []     |
        | 3  | []     |

  Rule: Invalid inputs

    Scenario Outline: array_intersect rejects <case>
      When query
        """
        SELECT array_intersect(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                       | args                 |
        | non-array inputs           | 1, array(1)          |
        | incompatible element types | array(1), array('1') |

  @spark_null
  Rule: Output schema

    Scenario: non-null array literals yield a non-nullable array
      When query
        """
        SELECT array_intersect(array(1, 2), array(2, 3)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a non-null array column yields a non-nullable array
      When query
        """
        SELECT array_intersect(array(id), array(id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: long (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable array column stays nullable
      When query
        """
        SELECT array_intersect(c, array(2)) AS result FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: nullable input elements propagate to the element nullability
      When query
        """
        SELECT array_intersect(c, array(1)) AS result FROM VALUES (array(1, CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
