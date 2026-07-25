@flatten
Feature: flatten() flattens nested arrays

  Rule: Basic flattening

    Scenario: flatten nested array of integers
      When query
        """
        SELECT flatten(array(array(1, 2), array(3, 4))) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: flatten nested array of strings
      When query
        """
        SELECT flatten(array(array('a', 'b'), array('c'))) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

  Rule: Empty array edge cases

    Scenario: flatten with empty inner arrays
      When query
        """
        SELECT flatten(array(array(), array())) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: flatten single empty inner array
      When query
        """
        SELECT flatten(array(array())) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: flatten mixed empty and non-empty arrays
      When query
        """
        SELECT flatten(array(array(1), array(), array(2, 3))) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

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
