@flatten
Feature: flatten() flattens nested arrays

  Rule: Basic flattening

    Scenario Outline: flatten nested array of <case>
      When query
        """
        SELECT flatten(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | arr                                | result       |
        | integers | array(array(1, 2), array(3, 4))    | [1, 2, 3, 4] |
        | strings  | array(array('a', 'b'), array('c')) | [a, b, c]    |

  Rule: Empty array edge cases

    Scenario Outline: Empty array: <case>
      When query
        """
        SELECT flatten(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | arr                                   | result    |
        | flatten with empty inner arrays          | array(array(), array())               | []        |
        | flatten single empty inner array         | array(array())                        | []        |
        | flatten mixed empty and non-empty arrays | array(array(1), array(), array(2, 3)) | [1, 2, 3] |

  Rule: Null handling

    Scenario: flatten with null element in outer array
      When query
        """
        SELECT flatten(array(array(1, 2), NULL, array(3))) AS result
        """
      Then query result
        | result |
        | NULL   |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null nested array literal yields a non-nullable array
      When query
        """
        SELECT flatten(array(array(1, 2), array(3))) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null nested array column yields a non-nullable array
      When query
        """
        SELECT flatten(array(array(id))) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: long (containsNull = false)
        """

    Scenario: a nullable array column stays nullable
      When query
        """
        SELECT flatten(c) AS result FROM VALUES (array(array(1))), (CAST(NULL AS ARRAY<ARRAY<INT>>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """

    @sail-bug
    Scenario: nullable inner elements propagate to the element nullability
      When query
        """
        SELECT flatten(c) AS result FROM VALUES (array(array(1, CAST(NULL AS INT)))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
