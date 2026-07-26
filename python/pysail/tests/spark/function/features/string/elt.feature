@elt
Feature: elt output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(1, 'scala', 'java') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(CAST(id AS INT), 'scala', 'java') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to elt stays nullable
      When query
        """
        SELECT elt(c, 'scala', 'java') AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_elt.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT elt(<args>)
        """
      Then query result
        | <name>   |
        | <result> |

      Examples:
        | case                    | args                                             | name                                      | result |
        | elt doctest #2 (result) | 1, 10, 20                                        | elt(1, 10, 20)                            | 10     |
        | elt doctest #3 (result) | 1, 'scala', 'java'                               | elt(1, scala, java)                       | scala  |
        | elt doctest #4 (result) | 11, 10, 20                                       | elt(11, 10, 20)                           | NULL   |
        | elt doctest #5 (result) | 6, 'scala', 'java', 'c', 'c++', 'python', 'rust' | elt(6, scala, java, c, c++, python, rust) | rust   |

  Rule: Output schema (migrated from test_elt.txt printSchema doctests)

    Scenario: elt doctest #1 (schema)
      When query
        """
        SELECT elt(1, 10, 20)
        """
      Then query schema
        """
        root
         |-- elt(1, 10, 20): string (nullable = true)
        """
