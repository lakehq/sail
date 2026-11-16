@function(lambda)
Feature: zip_with higher-order function

  Rule: Zip arrays with a 2-parameter lambda

    Scenario: Zip equal-length string arrays
      When query
        """
        SELECT zip_with(
          array('a', 'b', 'c'),
          array('d', 'e', 'f'),
          (x, y) -> concat(x, y)
        ) AS result
        """
      Then query result
        | result     |
        | [ad, be, cf] |

    Scenario: Pad the shorter left array with NULL
      When query
        """
        SELECT zip_with(
          array(1, 2),
          array(10, 20, 30),
          (x, y) -> coalesce(x, 0) + y
        ) AS result
        """
      Then query result
        | result       |
        | [11, 22, 30] |

    Scenario: Pad the shorter right array with NULL
      When query
        """
        SELECT zip_with(
          array(1, 2, 3),
          array(10, 20),
          (x, y) -> x + coalesce(y, 0)
        ) AS result
        """
      Then query result
        | result      |
        | [11, 22, 3] |

    Scenario: Preserve NULL input elements
      When query
        """
        SELECT zip_with(
          array(1, NULL, 3),
          array(10, 20, NULL),
          (x, y) -> coalesce(x, 0) + coalesce(y, 0)
        ) AS result
        """
      Then query result
        | result      |
        | [11, 20, 3] |

    Scenario: Return structs from the lambda
      When query
        """
        SELECT zip_with(
          array(1, 2),
          array('a', 'b'),
          (x, y) -> named_struct('number', x, 'letter', y)
        ) AS result
        """
      Then query result
        | result             |
        | [{1, a}, {2, b}]   |

    Scenario: Use only the right array element
      When query
        """
        SELECT zip_with(
          array(1, 2),
          array(10, 20),
          (x, y) -> y
        ) AS result
        """
      Then query result
        | result     |
        | [10, 20]   |

  Rule: Ordinary expressions are bound as lambdas that ignore their parameters

    Scenario: Use a constant expression as the body
      When query
        """
        SELECT zip_with(array(1, 2), array(3), 5) AS result
        """
      Then query result
        | result |
        | [5, 5] |

    Scenario: Preserve an enclosing lambda capture
      When query
        """
        SELECT transform(array(1, 2), x -> zip_with(array(10), array(20, 30), x)) AS result
        """
      Then query result collected
        | result             |
        | [[1, 1], [2, 2]]   |

    Scenario: Preserve a captured column for each zipped element
      When query
        """
        SELECT zip_with(left_array, right_array, captured) AS result
        FROM VALUES (array(10), array(20, 30), 1), (array(11), array(40, 50), 2)
          AS t(left_array, right_array, captured)
        """
      Then query result
        | result  |
        | [1, 1] |
        | [2, 2] |

    Scenario: Preserve zipped length with a constant body in an enclosing lambda
      When query
        """
        SELECT transform(array(1, 2), x -> zip_with(array(10), array(20, 30), 5)) AS result
        """
      Then query result
        | result               |
        | [[5, 5], [5, 5]]     |

    Scenario: Preserve an enclosing lambda capture with an explicit lambda
      When query
        """
        SELECT transform(array(1, 2), x -> zip_with(array(10), array(20, 30), (a, b) -> x)) AS result
        """
      Then query result
        | result     |
        | [[1, 1], [2, 2]] |

    Scenario: A NULL body remains NULL for each zipped element
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), NULL) AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

    Scenario: An untyped NULL input with an ordinary body returns NULL
      When query
        """
        SELECT zip_with(NULL, array(1, 2), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Empty and NULL arrays

    Scenario: Zip two typed empty arrays
      When query
        """
        SELECT zip_with(
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>),
          (x, y) -> x + y
        ) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: A NULL array input returns NULL
      When query
        """
        SELECT zip_with(
          CAST(NULL AS ARRAY<INT>),
          array(1, 2),
          (x, y) -> coalesce(x, 0) + y
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: A NULL left array skips right argument evaluation in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT zip_with(array_value, array(1 / divisor), (x, y) -> y) AS result
        FROM VALUES
          (CAST(NULL AS ARRAY<INT>), 0),
          (array(10), 1)
        AS t(array_value, divisor)
        """
      Then query result
        | result |
        | NULL   |
        | [1.0]  |

    Scenario: Empty and NULL arrays skip lambda evaluation when there are no elements
      When query
        """
        SELECT zip_with(array_value, CAST(array() AS ARRAY<INT>), (x, y) -> CAST(raise_error('boom') AS INT)) AS result
        FROM VALUES (CAST(NULL AS ARRAY<INT>)), (CAST(array() AS ARRAY<INT>)) AS t(array_value)
        """
      Then query result
        | result |
        | NULL   |
        | []     |

  Rule: Lambda arity errors

    Scenario Outline: Reject a lambda with incorrect arity
      When query
        """
        SELECT zip_with(array(1), array(2), <lambda>)
        """
      Then query error .*<expected>.*

      Examples:
        | lambda         | expected                           |
        | x -> x         | zip_with lambda function expects 2 arguments |
        | (x, y, z) -> x | only 2 are supported               |
