@position
Feature: position() finds the position of a substring in a string

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT position(<substr>, 'bbba') AS v
        """
      Then query result
        | v   |
        | <v> |

      Examples:
        | case                              | substr | v |
        | position with substring found     | 'a'    | 4 |
        | position with substring not found | 'x'    | 0 |

  Rule: Zero or negative start position returns 0

    Scenario Outline: Non-positive start: <case>
      When query
        """
        SELECT position('a', 'bbba', <start>) AS v
        """
      Then query result
        | v |
        | 0 |

      Examples:
        | case                                      | start |
        | position with start position 0 returns 0  | 0     |
        | position with start position -1 returns 0 | -1    |
        | position with start position -2 returns 0 | -2    |

  Rule: Positive start position

    Scenario Outline: Positive start: <case>
      When query
        """
        SELECT position('a', 'bbba', <start>) AS v
        """
      Then query result
        | v   |
        | <v> |

      Examples:
        | case                                                  | start | v |
        | position with start position 1 finds substring        | 1     | 4 |
        | position with start position past the match returns 0 | 5     | 0 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to position yields the schema Spark declares
      When query
        """
        SELECT position('bar', 'foobarbar') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to position yields the schema Spark declares
      When query
        """
        SELECT position(CAST(id AS STRING), 'foobarbar') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to position stays nullable
      When query
        """
        SELECT position(c, 'foobarbar') AS result FROM VALUES ('bar'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
