@map_zip_with
Feature: map_zip_with higher-order function

  Rule: Zip maps with a 3-parameter lambda

    Scenario: Zip maps with integer values and missing keys
      When query
        """
        SELECT map_zip_with(
          map('a', 1, 'b', 2),
          map('b', 3, 'c', 4),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        ) AS result
        """
      Then query result
        | result                  |
        | {a -> 1, b -> 5, c -> 4} |

    Scenario: Zip maps with string values
      When query
        """
        SELECT map_zip_with(
          map(1, 'a', 2, 'b'),
          map(1, 'x', 2, 'y'),
          (k, v1, v2) -> concat(v1, v2)
        ) AS result
        """
      Then query result
        | result             |
        | {1 -> ax, 2 -> by} |

    Scenario: Zip maps and use the key in the lambda
      When query
        """
        SELECT map_zip_with(
          map(1, 10, 2, 20),
          map(2, 200, 3, 300),
          (k, v1, v2) -> k + coalesce(v1, 0) + coalesce(v2, 0)
        ) AS result
        """
      Then query result
        | result                    |
        | {1 -> 11, 2 -> 222, 3 -> 303} |

    Scenario: Zip maps with a missing string value on one side
      When query
        """
        SELECT map_zip_with(
          map('x', 'L'),
          map('y', 'R'),
          (k, v1, v2) -> concat(coalesce(v1, '-'), coalesce(v2, '-'))
        ) AS result
        """
      Then query result
        | result              |
        | {x -> L-, y -> -R}  |
