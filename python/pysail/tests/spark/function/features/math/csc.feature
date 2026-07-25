@csc
Feature: csc output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(1) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to csc stays nullable
      When query
        """
        SELECT csc(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Result values (migrated from test_csc.txt doctests)

    Scenario: csc doctest #1 (result)
      When query
        """
        SELECT csc(radians(90)) AS csc
        """
      Then query result
        | csc |
        | 1.0 |

    Scenario: csc doctest #2 (result)
      When query
        """
        SELECT csc(1.5707963267948966) AS csc
        """
      Then query result
        | csc |
        | 1.0 |

    Scenario: csc doctest #3 (result)
      When query
        """
        SELECT csc(90) AS csc
        """
      Then query result
        | csc |
        | 1.1185724071637084 |

    Scenario: csc doctest #4 (result)
      When query
        """
        SELECT csc(90.0) AS csc
        """
      Then query result
        | csc |
        | 1.1185724071637084 |

    Scenario: csc doctest #5 (result)
      When query
        """
        SELECT csc(-90.0) AS csc
        """
      Then query result
        | csc |
        | -1.1185724071637084 |

    Scenario: csc doctest #6 (result)
      When query
        """
        SELECT csc(0) AS csc
        """
      Then query result
        | csc |
        | Infinity |

