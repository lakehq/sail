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

    Scenario: Preserve left key order and append right-only keys
      When query
        """
        SELECT map_keys(map_zip_with(
          map(1, 'left-1', 3, 'left-3'),
          map(2, 'right-2', 3, 'right-3'),
          (k, v1, v2) -> coalesce(v1, v2)
        )) AS result
        """
      Then query result
        | result    |
        | [1, 3, 2] |

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

  Rule: Ordinary expressions are bound as lambdas that ignore their parameters

    Scenario: Use a constant expression as the body
      When query
        """
        SELECT map_zip_with(map('a', 1), map('b', 2), 5) AS result
        """
      Then query result
        | result           |
        | {a -> 5, b -> 5} |

    Scenario: Preserve an enclosing lambda capture
      When query
        """
        SELECT transform(array(1, 2), x -> map_zip_with(map('a', 10), map('b', 20), x)) AS result
        """
      Then query result collected
        | result                                 |
        | [{'a': 1, 'b': 1}, {'a': 2, 'b': 2}] |

    Scenario: An untyped NULL input with an ordinary body returns NULL
      When query
        """
        SELECT map_zip_with(NULL, map('a', 1), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Map key widening follows ANSI mode

    Scenario: Non-ANSI mode preserves distinct string and integer keys
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map_keys(map_zip_with(
          map('01', 10), map(1, 20), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        )) AS result
        """
      Then query result
        | result  |
        | [01, 1] |

    Scenario: Non-ANSI mode formats timestamp keys with Spark semantics
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT size(map_zip_with(
          map(TIMESTAMP '2024-01-01 00:00:00', 1),
          map('2024-01-01 00:00:00', 2),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        )) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: ANSI mode promotes string timestamp keys in the session time zone
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT size(map_zip_with(
          map('2020-06-14 12:00:00', 10),
          map(TIMESTAMP '2020-06-14 12:00:00', 20),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        )) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Align LTZ and NTZ timestamp keys in the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT size(map_zip_with(
          map(TIMESTAMP_NTZ '2020-06-14 12:00:00', 10),
          map(TIMESTAMP '2020-06-14 12:00:00', 20),
          (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        )) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: ANSI mode coerces string and integer keys to a common integer type
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map_keys(map_zip_with(
          map('01', 10), map(1, 20), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)
        )) AS result
        """
      Then query result
        | result |
        | [1]    |

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

    Scenario: A NULL left map skips right argument evaluation in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map_zip_with(map_value, map(1, 1 / divisor), (k, x, y) -> y) AS result
        FROM VALUES
          (CAST(NULL AS MAP<INT, INT>), 0),
          (map(1, 10), 1)
        AS t(map_value, divisor)
        """
      Then query result
        | result |
        | NULL   |
        | {1 -> 1.0} |

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
