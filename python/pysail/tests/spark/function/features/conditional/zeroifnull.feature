@zeroifnull
Feature: zeroifnull output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to zeroifnull yields the schema Spark declares
      When query
        """
        SELECT zeroifnull(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Result values (migrated from test_zeroifnull.txt doctests)

    Scenario: zeroifnull doctest #1 (result)
      When query
        """
        SELECT zeroifnull(NULL), typeof(zeroifnull(NULL))
        """
      Then query result
        | zeroifnull(NULL) | typeof(zeroifnull(NULL)) |
        | 0                | int                      |

    Scenario: zeroifnull doctest #2 (result)
      When query
        """
        SELECT zeroifnull(2), typeof(zeroifnull(2))
        """
      Then query result
        | zeroifnull(2) | typeof(zeroifnull(2)) |
        | 2             | int                   |

    Scenario: zeroifnull doctest #3 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS TINYINT)), typeof(zeroifnull(CAST(NULL AS TINYINT)))
        """
      Then query result
        | zeroifnull(CAST(NULL AS TINYINT)) | typeof(zeroifnull(CAST(NULL AS TINYINT))) |
        | 0                                 | tinyint                                   |

    Scenario: zeroifnull doctest #4 (result)
      When query
        """
        SELECT zeroifnull(CAST(5 AS TINYINT)), typeof(zeroifnull(CAST(5 AS TINYINT)))
        """
      Then query result
        | zeroifnull(CAST(5 AS TINYINT)) | typeof(zeroifnull(CAST(5 AS TINYINT))) |
        | 5                              | tinyint                                |

    Scenario: zeroifnull doctest #5 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS SMALLINT)), typeof(zeroifnull(CAST(NULL AS SMALLINT)))
        """
      Then query result
        | zeroifnull(CAST(NULL AS SMALLINT)) | typeof(zeroifnull(CAST(NULL AS SMALLINT))) |
        | 0                                  | smallint                                   |

    Scenario: zeroifnull doctest #6 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS BIGINT)), typeof(zeroifnull(CAST(NULL AS BIGINT)))
        """
      Then query result
        | zeroifnull(CAST(NULL AS BIGINT)) | typeof(zeroifnull(CAST(NULL AS BIGINT))) |
        | 0                                | bigint                                   |

    Scenario: zeroifnull doctest #7 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS FLOAT)), typeof(zeroifnull(CAST(NULL AS FLOAT)))
        """
      Then query result
        | zeroifnull(CAST(NULL AS FLOAT)) | typeof(zeroifnull(CAST(NULL AS FLOAT))) |
        | 0.0                             | float                                   |

    Scenario: zeroifnull doctest #8 (result)
      When query
        """
        SELECT zeroifnull(CAST(3.14 AS FLOAT)), typeof(zeroifnull(CAST(3.14 AS FLOAT)))
        """
      Then query result
        | zeroifnull(CAST(3.14 AS FLOAT)) | typeof(zeroifnull(CAST(3.14 AS FLOAT))) |
        | 3.14                            | float                                   |

    Scenario: zeroifnull doctest #9 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS DOUBLE)), typeof(zeroifnull(CAST(NULL AS DOUBLE)))
        """
      Then query result
        | zeroifnull(CAST(NULL AS DOUBLE)) | typeof(zeroifnull(CAST(NULL AS DOUBLE))) |
        | 0.0                              | double                                   |

    Scenario: zeroifnull doctest #10 (result)
      When query
        """
        SELECT zeroifnull(CAST(2.718 AS DOUBLE)), typeof(zeroifnull(CAST(2.718 AS DOUBLE)))
        """
      Then query result
        | zeroifnull(CAST(2.718 AS DOUBLE)) | typeof(zeroifnull(CAST(2.718 AS DOUBLE))) |
        | 2.718                             | double                                    |

    Scenario: zeroifnull doctest #11 (result)
      When query
        """
        SELECT zeroifnull(CAST(NULL AS DECIMAL(10,2))), typeof(zeroifnull(CAST(NULL AS DECIMAL(10,2))))
        """
      Then query result
        | zeroifnull(CAST(NULL AS DECIMAL(10,2))) | typeof(zeroifnull(CAST(NULL AS DECIMAL(10,2)))) |
        | 0.0                                     | double                                          |

    Scenario: zeroifnull doctest #12 (result)
      When query
        """
        SELECT zeroifnull(CAST(42.99 AS DECIMAL(10,2))), typeof(zeroifnull(CAST(42.99 AS DECIMAL(10,2))))
        """
      Then query result
        | zeroifnull(CAST(42.99 AS DECIMAL(10,2))) | typeof(zeroifnull(CAST(42.99 AS DECIMAL(10,2)))) |
        | 42.99                                    | double                                           |

    Scenario: zeroifnull doctest #13 (result)
      When query
        """
        SELECT zeroifnull(0), typeof(zeroifnull(0))
        """
      Then query result
        | zeroifnull(0) | typeof(zeroifnull(0)) |
        | 0             | int                   |

    Scenario: zeroifnull doctest #14 (result)
      When query
        """
        SELECT zeroifnull(-5), typeof(zeroifnull(-5))
        """
      Then query result
        | zeroifnull((- 5)) | typeof(zeroifnull((- 5))) |
        | -5                | int                       |
