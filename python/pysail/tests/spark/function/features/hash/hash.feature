Feature: hash() returns murmur3 hash

  Rule: Basic usage

    Scenario Outline: Basic usage: <case>
      When query
        """
        SELECT hash(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | args      | result      |
        | hash integer       | 42        | 29417773    |
        | hash string        | 'hello'   | -1008564952 |
        | hash multiple args | 1, 'a', 2 | -355304976  |

  Rule: Null handling

    Scenario: hash null input
      When query
        """
        SELECT hash(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | 42     |

  Rule: Interval values

    # `hash.scala:556-557`: a day-time interval hashes its MICROSECONDS as a long and a year-month
    # interval its MONTHS as an int, so the Spark field range never enters the hash and two equal
    # durations with different ranges hash the same. Values measured on the Spark 4.2 JVM.
    Scenario Outline: hash of an interval: <case>
      When query
        """
        SELECT hash(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case            | input                              | result      |
        | DAY             | INTERVAL '2' DAY                   | -879985862  |
        | HOUR            | INTERVAL '25' HOUR                 | 1276317283  |
        | DAY TO SECOND   | INTERVAL '1 02:03:04' DAY TO SECOND | 1431373363  |
        | YEAR            | INTERVAL '2' YEAR                  | 69695535    |
        | MONTH           | INTERVAL '14' MONTH                | 335551368   |
        | date difference | DATE'2024-01-15' - DATE'2024-01-01' | -337802204  |

    # The range is not hashed: the same duration written two ways hashes alike.
    Scenario: two ranges of the same duration hash alike
      When query
        """
        SELECT hash(INTERVAL '1' DAY) = hash(INTERVAL '24' HOUR) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: hash of a NULL interval is the seed
      When query
        """
        SELECT hash(CAST(NULL AS INTERVAL DAY)) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: hash of an interval column
      When query
        """
        SELECT hash(i) AS result FROM VALUES (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | result     |
        | -879985862 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal yields a non-nullable integer
      When query
        """
        SELECT hash('a') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column yields a non-nullable integer
      When query
        """
        SELECT hash(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column: hash is still non-nullable (hash of NULL is defined)
      When query
        """
        SELECT hash(c) AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
