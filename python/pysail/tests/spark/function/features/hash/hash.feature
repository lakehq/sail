@hash
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

  @spark_null
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
