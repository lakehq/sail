@try_divide
Feature: try_divide output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(3, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to try_divide stays nullable
      When query
        """
        SELECT try_divide(c, 2) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Result values (migrated from test_try_divide.txt doctests)

    Scenario: try_divide doctest #1 (result)
      When query
        """
        SELECT try_divide(a, b) AS r FROM VALUES (6000, 15), (1990, 2) AS t(a, b)
        """
      Then query result
        | r     |
        | 400.0 |
        | 995.0 |

    Scenario: try_divide doctest #2 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result   |
        | 12 hours |

    Scenario: try_divide doctest #3 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 0, 10, 0, 0), 4) AS result
        """
      Then query result
        | result             |
        | 2 hours 30 minutes |

    Scenario: try_divide doctest #4 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 3, 12, 0, 0), 3) AS result
        """
      Then query result
        | result         |
        | 1 days 4 hours |

    Scenario: try_divide doctest #5 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 0, 0, 0, 10.5), 2) AS result
        """
      Then query result
        | result       |
        | 5.25 seconds |

    Scenario: try_divide doctest #6 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 1, 0, 0, 0, 0), 2) AS result
        """
      Then query result
        | result          |
        | 3 days 12 hours |

    Scenario: try_divide doctest #7 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_divide doctest #8 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_divide doctest #9 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result   |
        | 12 hours |

    Scenario: try_divide doctest #10 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, -1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result    |
        | -12 hours |

    Scenario: try_divide doctest #11 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 90, 0), 2) AS result
        """
      Then query result
        | result              |
        | 12 hours 45 minutes |

    Scenario: try_divide doctest #12 (result)
      When query
        """
        SELECT try_divide(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-9' YEAR TO MONTH |

    Scenario: try_divide doctest #13 (result)
      When query
        """
        SELECT try_divide(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-9' YEAR TO MONTH |

    Scenario: try_divide doctest #14 (result)
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 0, 0, 0, 1), 2) AS result
        """
      Then query result
        | result      |
        | 0.5 seconds |
