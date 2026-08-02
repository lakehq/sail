@sentences
Feature: sentences output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to sentences yields the schema Spark declares
      When query
        """
        SELECT sentences('Hi there! Good morning.') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null column input to sentences yields the schema Spark declares
      When query
        """
        SELECT sentences(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to sentences stays nullable
      When query
        """
        SELECT sentences(c) AS result FROM VALUES ('Hi there! Good morning.'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """

  Rule: Result values (migrated from test_sentences.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT sentences(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | args                      | result                         |
        | sentences doctest #1 (result) | 'Hi there! Good morning.' | [[Hi, there], [Good, morning]] |
        | sentences doctest #2 (result) | 'Hello world'             | [[Hello, world]]               |
        | sentences doctest #3 (result) | 'What? Yes! OK.'          | [[What], [Yes], [OK]]          |
        | sentences doctest #4 (result) | NULL                      | NULL                           |
        | sentences doctest #5 (result) | ''                        | []                             |
        | sentences doctest #6 (result) | 'Hi there!', 'en'         | [[Hi, there]]                  |
        | sentences doctest #7 (result) | 'Hi there!', 'en', 'US'   | [[Hi, there]]                  |
