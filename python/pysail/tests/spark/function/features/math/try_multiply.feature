@try_multiply
Feature: try_multiply output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_multiply stays nullable
      When query
        """
        SELECT try_multiply(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_multiply.txt doctests)

    Scenario: try_multiply doctest #1 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result |
        | 2 days |

    Scenario: try_multiply doctest #2 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 0, 10, 0, 0), 4) AS result
        """
      Then query result
        | result   |
        | 40 hours |

    Scenario: try_multiply doctest #3 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 3, 12, 0, 0), 3) AS result
        """
      Then query result
        | result          |
        | 9 days 36 hours |

    Scenario: try_multiply doctest #4 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 0, 0, 0, 10.5), 2) AS result
        """
      Then query result
        | result     |
        | 21 seconds |

    Scenario: try_multiply doctest #5 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 1, 0, 0, 0, 0), 2) AS result
        """
      Then query result
        | result  |
        | 14 days |

    Scenario: try_multiply doctest #6 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_multiply doctest #7 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), 0) AS result
        """
      Then query result
        | result    |
        | 0 seconds |

    Scenario: try_multiply doctest #8 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, -1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result  |
        | -2 days |

    Scenario: try_multiply doctest #9 (result)
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 90, 0), 2) AS result
        """
      Then query result
        | result         |
        | 2 days 3 hours |

    Scenario: try_multiply doctest #10 (result)
      When query
        """
        SELECT try_multiply(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '3-0' YEAR TO MONTH |

    Scenario: try_multiply doctest #11 (result)
      When query
        """
        SELECT try_multiply(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '3-0' YEAR TO MONTH |
