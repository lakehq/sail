@sail-only
Feature: array_concat() concatenates arrays (Sail extension)

  Note: array_concat is a Sail extension not available in standard Spark.
  Use concat() for Spark-compatible array concatenation.

  Rule: Basic concatenation

    @sail-only
    Scenario Outline: array_concat two <case> arrays
      When query
        """
        SELECT array_concat(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | left            | right       | result          |
        | integer | array(1, 2, 3)  | array(4, 5) | [1, 2, 3, 4, 5] |
        | string  | array('a', 'b') | array('c')  | [a, b, c]       |

  Rule: Empty array handling

    @sail-only
    Scenario Outline: array_concat <case>
      When query
        """
        SELECT array_concat(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | left        | right          | result    |
        | empty array with typed array | array()     | array(1, 2, 3) | [1, 2, 3] |
        | typed array with empty array | array(1, 2) | array()        | [1, 2]    |

  Rule: Null propagation

    @sail-only
    Scenario Outline: array_concat <case> returns null
      When query
        """
        SELECT array_concat(<left>, <right>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case            | left                     | right                    |
        | array with null | array(1, 2)              | CAST(NULL AS ARRAY<INT>) |
        | null with array | CAST(NULL AS ARRAY<INT>) | array(1, 2)              |
