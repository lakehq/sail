@function(lambda)
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
        | result                 |
        | {a -> 1, b -> 5, c -> 4} |

    Scenario: Coerce integral map key types to a common type
      When query
        """
        SELECT map_zip_with(
          map(CAST(1 AS INT), 'left'),
          map(CAST(1 AS BIGINT), 'right'),
          (k, v1, v2) -> concat(v1, v2)
        ) AS result
        """
      Then query result
        | result          |
        | {1 -> leftright} |

    Scenario: Coerce an untyped empty map to the other map's key type
      When query
        """
        SELECT map_zip_with(
          map(),
          map('a', 1),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        ) AS result
        """
      Then query result
        | result   |
        | {a -> 1} |

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

    Scenario: Use only the second map value in the lambda
      When query
        """
        SELECT map_zip_with(map('a', 1), map('a', 2), (k, v1, v2) -> v2) AS result
        """
      Then query result
        | result   |
        | {a -> 2} |

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
        | result               |
        | {x -> L-, y -> -R}   |

  Rule: Empty and NULL maps

    Scenario: Zip two empty maps
      When query
        """
        SELECT map_zip_with(
          CAST(map() AS MAP<STRING, INT>),
          CAST(map() AS MAP<STRING, INT>),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        ) AS result
        """
      Then query result
        | result |
        | {}     |

    Scenario: A NULL map input returns NULL
      When query
        """
        SELECT map_zip_with(
          CAST(NULL AS MAP<STRING, INT>),
          map('a', 1),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty and NULL maps skip lambda evaluation when there are no keys
      When query
        """
        SELECT map_zip_with(map_value, CAST(map() AS MAP<STRING, INT>), (k, v1, v2) -> CAST(raise_error('boom') AS INT)) AS result
        FROM VALUES (CAST(NULL AS MAP<STRING, INT>)), (CAST(map() AS MAP<STRING, INT>)) AS t(map_value)
        """
      Then query result
        | result |
        | NULL   |
        | {}     |

  Rule: Lambda arity errors

    Scenario Outline: Reject a lambda with incorrect arity
      When query
        """
        SELECT map_zip_with(map('a', 1), map('a', 2), <lambda>)
        """
      Then query error .*<expected>.*

      Examples:
        | lambda                  | expected                               |
        | (k, v) -> v             | map_zip_with lambda function expects 3 arguments |
        | (k, v1, v2, v3) -> v2   | only 3 are supported                   |
