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

    Scenario: sentences doctest #1 (result)
      When query
        """
        SELECT sentences('Hi there! Good morning.') AS result
        """
      Then query result
        | result                         |
        | [[Hi, there], [Good, morning]] |

    Scenario: sentences doctest #2 (result)
      When query
        """
        SELECT sentences('Hello world') AS result
        """
      Then query result
        | result           |
        | [[Hello, world]] |

    Scenario: sentences doctest #3 (result)
      When query
        """
        SELECT sentences('What? Yes! OK.') AS result
        """
      Then query result
        | result                |
        | [[What], [Yes], [OK]] |

    Scenario: sentences doctest #4 (result)
      When query
        """
        SELECT sentences(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sentences doctest #5 (result)
      When query
        """
        SELECT sentences('') AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: sentences doctest #6 (result)
      When query
        """
        SELECT sentences('Hi there!', 'en') AS result
        """
      Then query result
        | result        |
        | [[Hi, there]] |

    Scenario: sentences doctest #7 (result)
      When query
        """
        SELECT sentences('Hi there!', 'en', 'US') AS result
        """
      Then query result
        | result        |
        | [[Hi, there]] |
