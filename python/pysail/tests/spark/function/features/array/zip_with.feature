@function(lambda)
Feature: zip_with applies a lambda to padded pairs

  Background:
    Given config spark.sql.ansi.enabled = true

  Rule: Element pairs preserve their independent types and nullable padding

    Scenario Outline: Zip array pair with <case>
      When query
        """
        SELECT zip_with(<left>, <right>, (x, y) -> <body>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | left                           | right                         | body                         | result                 |
        | equal lengths               | array(1, 2)                    | array(3, 4)                   | x + y                        | [4, 6]                 |
        | shorter right               | array(1, 2, 3)                 | array(4)                      | x + y                        | [5, NULL, NULL]        |
        | shorter left                | array(1)                       | array(4, 5, 6)                | coalesce(x, 0) + y           | [5, 5, 6]              |
        | empty left                  | array()                        | array(2, 3)                   | y                            | [2, 3]                 |
        | both empty                  | array()                        | array()                       | 1                            | []                     |
        | null elements               | array(1, NULL)                 | array(NULL, 2)                | coalesce(x, y)               | [1, 2]                 |
        | unused left parameter       | array(1, 2)                    | array('a', 'b', 'c')           | y                            | [a, b, c]              |
        | unused right parameter      | array(1, 2)                    | array('a')                    | x                            | [1, 2]                 |
        | unused parameters           | array(1)                       | array('a', 'b')               | 9                            | [9, 9]                 |
        | null result                 | array(1)                       | array('a', 'b')               | NULL                         | [NULL, NULL]           |
        | independent element types   | array('foo', 'bar')             | array(1, 2, 3)                | concat_ws('_', x, y)          | [foo_1, bar_2, 3]      |
        | null left container         | CAST(NULL AS ARRAY<INT>)       | array(1, 2)                   | coalesce(x, y)               | NULL                   |
        | null right container        | array(1, 2)                    | CAST(NULL AS ARRAY<INT>)       | coalesce(x, y)               | NULL                   |

    Scenario: Lambda builds nested values with correctly padded parameters
      When query
        """
        SELECT zip_with(array(1, 2), array('a'), (x, y) -> named_struct('left', x, 'right', y)) AS result
        """
      Then query result
        | result                |
        | [{1, a}, {2, NULL}]    |

  Rule: Captures and nested lambdas retain their row and scope

    Scenario: Capture original rows across null and empty collections
      When query
        """
        SELECT id, zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0) + id) AS result
        FROM VALUES
          (1, array(1, 2), array(10)),
          (2, CAST(NULL AS ARRAY<INT>), array(10)),
          (3, array(), array()),
          (4, array(3), array(20, 30)) AS t(id, a, b)
        """
      Then query result
        | id | result   |
        | 1  | [12, 3]  |
        | 2  | NULL     |
        | 3  | []       |
        | 4  | [27, 34] |

    Scenario: Nested zip lambda captures outer parameters
      When query
        """
        SELECT zip_with(array(1, 2), array(10, 20), (x, y) ->
                 zip_with(array(3), array(4), (a, b) -> a + b + x + y)) AS result
        """
      Then query result
        | result       |
        | [[18], [29]] |

    Scenario: An enclosing array lambda is visible in a zip input and body
      When query
        """
        SELECT transform(array(1, 2), x -> zip_with(array(x), array(10),
                 (a, b) -> a + b + x)) AS result
        """
      Then query result
        | result       |
        | [[12], [14]] |

    Scenario: Inner lambda shadows an outer name
      When query
        """
        SELECT zip_with(array(1), array(10), (x, y) ->
                 zip_with(array(3), array(4), (x, z) -> x + z + y)) AS result
        """
      Then query result
        | result |
        | [[17]] |

  Rule: Unreachable expressions are not evaluated

    Scenario: Null left input skips right input evaluation
      When query
        """
        SELECT zip_with(CAST(NULL AS ARRAY<INT>), array(CAST(raise_error('boom') AS INT)),
                        (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty inputs skip an erroring lambda
      When query
        """
        SELECT zip_with(array(), array(), (x, y) -> raise_error('boom')) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Plain expressions bind as constant lambdas
      When query
        """
        SELECT zip_with(array(1), array(2, 3), 7) AS result
        """
      Then query result
        | result |
        | [7, 7] |

  Rule: Lambda arity and collection types are validated

    Scenario Outline: Invalid zip argument <case>
      When query
        """
        SELECT zip_with(<arguments>)
        """
      Then query error (?i)<error>

      Examples:
        | case               | arguments                                  | error               |
        | too few parameters | array(1), array(2), x -> x                  | lambda              |
        | too many parameters| array(1), array(2), (x, y, z) -> x          | lambda              |
        | duplicate names    | array(1), array(2), (x, x) -> x             | lambda              |
        | non-array input    | 1, array(2), (x, y) -> y                    | array\|list          |
        | untyped null input | NULL, array(2), (x, y) -> y                 | array\|list          |

    Scenario: Plain zip body permits an untyped null collection
      When query
        """
        SELECT zip_with(NULL, array(1), 7) AS result
        """
      Then query result
        | result |
        | NULL   |
