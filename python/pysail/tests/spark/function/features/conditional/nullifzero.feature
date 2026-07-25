@nullifzero
Feature: nullifzero output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(0) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullifzero stays nullable
      When query
        """
        SELECT nullifzero(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_nullifzero.txt doctests)

    Scenario: nullifzero doctest #1 (result)
      When query
        """
        SELECT nullifzero(0), typeof(nullifzero(0))
        """
      Then query result
        | nullifzero(0) | typeof(nullifzero(0)) |
        | NULL          | int                   |

    Scenario: nullifzero doctest #2 (result)
      When query
        """
        SELECT nullifzero(2), typeof(nullifzero(2))
        """
      Then query result
        | nullifzero(2) | typeof(nullifzero(2)) |
        | 2             | int                   |

    Scenario: nullifzero doctest #3 (result)
      When query
        """
        SELECT nullifzero(CAST(0 AS TINYINT)), typeof(nullifzero(CAST(0 AS TINYINT)))
        """
      Then query result
        | nullifzero(CAST(0 AS TINYINT)) | typeof(nullifzero(CAST(0 AS TINYINT))) |
        | NULL                           | tinyint                                |

    Scenario: nullifzero doctest #4 (result)
      When query
        """
        SELECT nullifzero(CAST(2 AS TINYINT)), typeof(nullifzero(CAST(2 AS TINYINT)))
        """
      Then query result
        | nullifzero(CAST(2 AS TINYINT)) | typeof(nullifzero(CAST(2 AS TINYINT))) |
        | 2                              | tinyint                                |

    Scenario: nullifzero doctest #5 (result)
      When query
        """
        SELECT nullifzero(CAST(0 AS SMALLINT)), typeof(nullifzero(CAST(0 AS SMALLINT)))
        """
      Then query result
        | nullifzero(CAST(0 AS SMALLINT)) | typeof(nullifzero(CAST(0 AS SMALLINT))) |
        | NULL                            | smallint                                |

    Scenario: nullifzero doctest #6 (result)
      When query
        """
        SELECT nullifzero(CAST(0 AS BIGINT)), typeof(nullifzero(CAST(0 AS BIGINT)))
        """
      Then query result
        | nullifzero(CAST(0 AS BIGINT)) | typeof(nullifzero(CAST(0 AS BIGINT))) |
        | NULL                          | bigint                                |

    Scenario: nullifzero doctest #7 (result)
      When query
        """
        SELECT nullifzero(CAST(0.0 AS FLOAT)), typeof(nullifzero(CAST(0.0 AS FLOAT)))
        """
      Then query result
        | nullifzero(CAST(0.0 AS FLOAT)) | typeof(nullifzero(CAST(0.0 AS FLOAT))) |
        | NULL                           | float                                  |

    Scenario: nullifzero doctest #8 (result)
      When query
        """
        SELECT nullifzero(CAST(1.5 AS FLOAT)), typeof(nullifzero(CAST(1.5 AS FLOAT)))
        """
      Then query result
        | nullifzero(CAST(1.5 AS FLOAT)) | typeof(nullifzero(CAST(1.5 AS FLOAT))) |
        | 1.5                            | float                                  |

    Scenario: nullifzero doctest #9 (result)
      When query
        """
        SELECT nullifzero(CAST(0.0 AS DOUBLE)), typeof(nullifzero(CAST(0.0 AS DOUBLE)))
        """
      Then query result
        | nullifzero(CAST(0.0 AS DOUBLE)) | typeof(nullifzero(CAST(0.0 AS DOUBLE))) |
        | NULL                            | double                                  |

    Scenario: nullifzero doctest #10 (result)
      When query
        """
        SELECT nullifzero(CAST(2.5 AS DOUBLE)), typeof(nullifzero(CAST(2.5 AS DOUBLE)))
        """
      Then query result
        | nullifzero(CAST(2.5 AS DOUBLE)) | typeof(nullifzero(CAST(2.5 AS DOUBLE))) |
        | 2.5                             | double                                  |

    Scenario: nullifzero doctest #11 (result)
      When query
        """
        SELECT nullifzero(CAST(0.00 AS DECIMAL(10,2))), typeof(nullifzero(CAST(0.00 AS DECIMAL(10,2))))
        """
      Then query result
        | nullifzero(CAST(0.00 AS DECIMAL(10,2))) | typeof(nullifzero(CAST(0.00 AS DECIMAL(10,2)))) |
        | NULL                                    | decimal(10,2)                                   |

    Scenario: nullifzero doctest #12 (result)
      When query
        """
        SELECT nullifzero(CAST(5.25 AS DECIMAL(10,2))), typeof(nullifzero(CAST(5.25 AS DECIMAL(10,2))))
        """
      Then query result
        | nullifzero(CAST(5.25 AS DECIMAL(10,2))) | typeof(nullifzero(CAST(5.25 AS DECIMAL(10,2)))) |
        | 5.25                                    | decimal(10,2)                                   |

    Scenario: nullifzero doctest #13 (result)
      When query
        """
        SELECT nullifzero(-1), typeof(nullifzero(-1))
        """
      Then query result
        | nullifzero((- 1)) | typeof(nullifzero((- 1))) |
        | -1                | int                       |

    Scenario: nullifzero doctest #14 (result)
      When query
        """
        SELECT nullifzero(NULL), typeof(nullifzero(NULL))
        """
      Then query result
        | nullifzero(NULL) | typeof(nullifzero(NULL)) |
        | NULL             | int                      |
