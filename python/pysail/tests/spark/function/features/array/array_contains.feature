Feature: array_contains

  Rule: Null elements

    Scenario Outline: array_contains(<array>, <value>)
      When query
        """
        SELECT array_contains(<array>, <value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | array                       | value                | result |
        | array('a', 'ab')            | 'ab'                 | true   |
        | array('x', NULL)            | 'ab'                 | NULL   |
        | array('x', NULL)            | 'x'                  | true   |
        | array('x', 'y')             | 'ab'                 | false  |
        | array('x', 'y')             | CAST(NULL AS STRING) | NULL   |
        | CAST(NULL AS ARRAY<INT>)    | 1                    | NULL   |
        | CAST(array() AS ARRAY<INT>) | 1                    | false  |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_contains yields the schema Spark declares
      When query
        """
        SELECT array_contains(array(1, 2, 3), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to array_contains stays nullable
      When query
        """
        SELECT array_contains(c, 2) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
