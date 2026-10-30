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
