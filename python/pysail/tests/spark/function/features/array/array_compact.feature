Feature: array_compact() removes null values from an array

  Rule: Basic usage

    Scenario Outline: array_compact <case>
      When query
        """
        SELECT array_compact(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | arr                                         | result    |
        | removes null values from integer array       | array(1, NULL, 2, NULL, 3)                  | [1, 2, 3] |
        | removes null values from string array        | array('a', NULL, 'b', NULL, 'c')            | [a, b, c] |
        | with no null values returns same array       | array(1, 2, 3)                              | [1, 2, 3] |
        | with all null values returns empty array     | array(CAST(NULL AS INT), CAST(NULL AS INT)) | []        |
        | with untyped null values returns empty array | array(NULL, NULL)                           | []        |
        | with null at beginning                       | array(NULL, 1, 2, 3)                        | [1, 2, 3] |
        | with null at end                             | array(1, 2, 3, NULL)                        | [1, 2, 3] |

  Rule: Empty array handling

    Scenario: array_compact with empty array returns empty array
      When query
        """
        SELECT array_compact(array()) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Null input propagation

    Scenario Outline: array_compact with <case> null input returns null
      When query
        """
        SELECT array_compact(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case  | arg                      |
        | an    | NULL                     |
        | typed | CAST(NULL AS ARRAY<INT>) |

  Rule: Multiple rows

    Scenario: array_compact across multiple rows
      When query
        """
        SELECT id, array_compact(arr) AS result
        FROM VALUES
          (1, array(1, NULL, 2)),
          (2, array(NULL, NULL)),
          (3, array(3, 4, 5)),
          (4, CAST(NULL AS ARRAY<INT>))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | [1, 2]    |
        | 2  | []        |
        | 3  | [3, 4, 5] |
        | 4  | NULL      |

  Rule: Various data types

    Scenario Outline: array_compact with <case> values
      When query
        """
        SELECT array_compact(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | arr                         | result        |
        | double  | array(1.1, NULL, 2.2, NULL) | [1.1, 2.2]    |
        | boolean | array(true, NULL, false)    | [true, false] |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null array literal yields a non-nullable array
      When query
        """
        SELECT array_compact(array(1, NULL, 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null array column yields a non-nullable array
      When query
        """
        SELECT array_compact(array(id)) AS result FROM range(3)
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
        SELECT array_compact(c) AS result FROM VALUES (array(1)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: removing NULLs makes the element non-nullable regardless of input
      When query
        """
        SELECT array_compact(c) AS result FROM VALUES (array(1, CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """
