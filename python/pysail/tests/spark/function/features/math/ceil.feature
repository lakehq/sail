@ceil
Feature: ceil output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to ceil yields the schema Spark declares
      When query
        """
        SELECT ceil(-0.1) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """

    Scenario: a nullable column input to ceil stays nullable
      When query
        """
        SELECT ceil(c) AS result FROM VALUES (-0.1), (CAST(NULL AS DECIMAL(1,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """


  Rule: Result values (migrated from test_ceil.txt doctests)

    Scenario: ceil doctest #1 — SELECT ceil(-0.1), typeof(ceil(-0.1)), typeof(-0.1)
      When query
        """
        SELECT ceil(-0.1), typeof(ceil(-0.1)), typeof(-0.1)
        """
      Then query result
        | CEIL(-0.1) | typeof(CEIL(-0.1)) | typeof(-0.1) |
        | 0          | decimal(1,0)       | decimal(1,1) |

    Scenario: ceil doctest #2 — SELECT ceil(5), typeof(ceil(5)), typeof(5)
      When query
        """
        SELECT ceil(5), typeof(ceil(5)), typeof(5)
        """
      Then query result
        | CEIL(5) | typeof(CEIL(5)) | typeof(5) |
        | 5       | bigint          | int       |

    Scenario: ceil doctest #3 — SELECT ceil(5.4), typeof(ceil(5.4)), typeof(5.4)
      When query
        """
        SELECT ceil(5.4), typeof(ceil(5.4)), typeof(5.4)
        """
      Then query result
        | CEIL(5.4) | typeof(CEIL(5.4)) | typeof(5.4)  |
        | 6         | decimal(2,0)      | decimal(2,1) |

    Scenario: ceil doctest #4 — SELECT ceil(3.1411, -3), typeof(ceil(3.1411, -3)), typeof(3.
      When query
        """
        SELECT ceil(3.1411, -3), typeof(ceil(3.1411, -3)), typeof(3.1411)
        """
      Then query result
        | ceil(3.1411, -3) | typeof(ceil(3.1411, -3)) | typeof(3.1411) |
        | 1000             | decimal(4,0)             | decimal(5,4)   |

    Scenario: ceil doctest #5 — SELECT ceil(3.1411, 3), typeof(ceil(3.1411, 3)), typeof(3.14
      When query
        """
        SELECT ceil(3.1411, 3), typeof(ceil(3.1411, 3)), typeof(3.1411)
        """
      Then query result
        | ceil(3.1411, 3) | typeof(ceil(3.1411, 3)) | typeof(3.1411) |
        | 3.142           | decimal(5,3)            | decimal(5,4)   |

    Scenario: ceil doctest #6 — SELECT ceil(3345.1, -2), typeof(ceil(3345.1, -2)), typeof(33
      When query
        """
        SELECT ceil(3345.1, -2), typeof(ceil(3345.1, -2)), typeof(3345.1)
        """
      Then query result
        | ceil(3345.1, -2) | typeof(ceil(3345.1, -2)) | typeof(3345.1) |
        | 3400             | decimal(5,0)             | decimal(5,1)   |

    Scenario: ceil doctest #7 — SELECT ceil(-12.345, 1), typeof(ceil(-12.345, 1)), typeof(33
      When query
        """
        SELECT ceil(-12.345, 1), typeof(ceil(-12.345, 1)), typeof(3345.1)
        """
      Then query result
        | ceil(-12.345, 1) | typeof(ceil(-12.345, 1)) | typeof(3345.1) |
        | -12.3            | decimal(4,1)             | decimal(5,1)   |

    Scenario: ceil doctest #8 — SELECT ceil(CAST(5 as TINYINT), 1), typeof(ceil(CAST(5 as TI
      When query
        """
        SELECT ceil(CAST(5 as TINYINT), 1), typeof(ceil(CAST(5 as TINYINT), 1))
        """
      Then query result
        | ceil(CAST(5 AS TINYINT), 1) | typeof(ceil(CAST(5 AS TINYINT), 1)) |
        | 5                           | decimal(4,0)                        |

    Scenario: ceil doctest #9 — SELECT ceil(CAST(5 as TINYINT), -4), typeof(ceil(CAST(5 as T
      When query
        """
        SELECT ceil(CAST(5 as TINYINT), -4), typeof(ceil(CAST(5 as TINYINT), -4))
        """
      Then query result
        | ceil(CAST(5 AS TINYINT), -4) | typeof(ceil(CAST(5 AS TINYINT), -4)) |
        | 10000                        | decimal(5,0)                         |

    Scenario: ceil doctest #10 — SELECT ceil(CAST(5 as SMALLINT), 1), typeof(ceil(CAST(5 as S
      When query
        """
        SELECT ceil(CAST(5 as SMALLINT), 1), typeof(ceil(CAST(5 as SMALLINT), 1))
        """
      Then query result
        | ceil(CAST(5 AS SMALLINT), 1) | typeof(ceil(CAST(5 AS SMALLINT), 1)) |
        | 5                            | decimal(6,0)                         |

    Scenario: ceil doctest #11 — SELECT ceil(CAST(5 as SMALLINT), -6), typeof(ceil(CAST(5 as
      When query
        """
        SELECT ceil(CAST(5 as SMALLINT), -6), typeof(ceil(CAST(5 as SMALLINT), -6))
        """
      Then query result
        | ceil(CAST(5 AS SMALLINT), -6) | typeof(ceil(CAST(5 AS SMALLINT), -6)) |
        | 1000000                       | decimal(7,0)                          |

    Scenario: ceil doctest #12 — SELECT ceil(CAST(5 as INT), 1), typeof(ceil(CAST(5 as INT),
      When query
        """
        SELECT ceil(CAST(5 as INT), 1), typeof(ceil(CAST(5 as INT), 1))
        """
      Then query result
        | ceil(CAST(5 AS INT), 1) | typeof(ceil(CAST(5 AS INT), 1)) |
        | 5                       | decimal(11,0)                   |

    Scenario: ceil doctest #13 — SELECT ceil(CAST(5 as INT), -11), typeof(ceil(CAST(5 as INT)
      When query
        """
        SELECT ceil(CAST(5 as INT), -11), typeof(ceil(CAST(5 as INT), -11))
        """
      Then query result
        | ceil(CAST(5 AS INT), -11) | typeof(ceil(CAST(5 AS INT), -11)) |
        | 100000000000              | decimal(12,0)                     |

    Scenario: ceil doctest #14 — SELECT ceil(CAST(5 as BIGINT), 1), typeof(ceil(CAST(5 as BIG
      When query
        """
        SELECT ceil(CAST(5 as BIGINT), 1), typeof(ceil(CAST(5 as BIGINT), 1))
        """
      Then query result
        | ceil(CAST(5 AS BIGINT), 1) | typeof(ceil(CAST(5 AS BIGINT), 1)) |
        | 5                          | decimal(21,0)                      |

    Scenario: ceil doctest #15 — SELECT ceil(CAST(5 as BIGINT), -21), typeof(ceil(CAST(5 as B
      When query
        """
        SELECT ceil(CAST(5 as BIGINT), -21), typeof(ceil(CAST(5 as BIGINT), -21))
        """
      Then query result
        | ceil(CAST(5 AS BIGINT), -21) | typeof(ceil(CAST(5 AS BIGINT), -21)) |
        | 1000000000000000000000       | decimal(22,0)                        |

    Scenario: ceil doctest #16 — SELECT ceil(CAST(5 as FLOAT), 1), typeof(ceil(CAST(5 as FLOA
      When query
        """
        SELECT ceil(CAST(5 as FLOAT), 1), typeof(ceil(CAST(5 as FLOAT), 1))
        """
      Then query result
        | ceil(CAST(5 AS FLOAT), 1) | typeof(ceil(CAST(5 AS FLOAT), 1)) |
        | 5.0                       | decimal(9,1)                      |

    Scenario: ceil doctest #17 — SELECT ceil(CAST(5 as FLOAT), -15), typeof(ceil(CAST(5 as FL
      When query
        """
        SELECT ceil(CAST(5 as FLOAT), -15), typeof(ceil(CAST(5 as FLOAT), -15))
        """
      Then query result
        | ceil(CAST(5 AS FLOAT), -15) | typeof(ceil(CAST(5 AS FLOAT), -15)) |
        | 1000000000000000            | decimal(16,0)                       |

    Scenario: ceil doctest #18 — SELECT ceil(CAST(5 as DOUBLE), 1), typeof(ceil(CAST(5 as DOU
      When query
        """
        SELECT ceil(CAST(5 as DOUBLE), 1), typeof(ceil(CAST(5 as DOUBLE), 1))
        """
      Then query result
        | ceil(CAST(5 AS DOUBLE), 1) | typeof(ceil(CAST(5 AS DOUBLE), 1)) |
        | 5.0                        | decimal(17,1)                      |

    Scenario: ceil doctest #19 — SELECT ceil(CAST(5 as DOUBLE), -31), typeof(ceil(CAST(5 as D
      When query
        """
        SELECT ceil(CAST(5 as DOUBLE), -31), typeof(ceil(CAST(5 as DOUBLE), -31))
        """
      Then query result
        | ceil(CAST(5 AS DOUBLE), -31)     | typeof(ceil(CAST(5 AS DOUBLE), -31)) |
        | 10000000000000000000000000000000 | decimal(32,0)                        |

    Scenario: ceil doctest #20 — SELECT ceil(CAST(5 as DECIMAL(5, 2)), 1), typeof(ceil(CAST(5
      When query
        """
        SELECT ceil(CAST(5 as DECIMAL(5, 2)), 1), typeof(ceil(CAST(5 as DECIMAL(5, 2)), 1))
        """
      Then query result
        | ceil(CAST(5 AS DECIMAL(5,2)), 1) | typeof(ceil(CAST(5 AS DECIMAL(5,2)), 1)) |
        | 5.0                              | decimal(5,1)                             |

    Scenario: ceil doctest #21 — SELECT ceil(CAST(5 as DECIMAL(5, 2))), typeof(ceil(CAST(5 as
      When query
        """
        SELECT ceil(CAST(5 as DECIMAL(5, 2))), typeof(ceil(CAST(5 as DECIMAL(5, 2))))
        """
      Then query result
        | CEIL(CAST(5 AS DECIMAL(5,2))) | typeof(CEIL(CAST(5 AS DECIMAL(5,2)))) |
        | 5                             | decimal(4,0)                          |

    Scenario: ceil doctest #22 — SELECT ceil(5.4, -1), typeof(ceil(5.4, -1)), typeof(5.4)
      When query
        """
        SELECT ceil(5.4, -1), typeof(ceil(5.4, -1)), typeof(5.4)
        """
      Then query result
        | ceil(5.4, -1) | typeof(ceil(5.4, -1)) | typeof(5.4)  |
        | 10            | decimal(2,0)          | decimal(2,1) |

    Scenario: ceil doctest #23 — SELECT ceil(5, -1), typeof(ceil(5, -1)), typeof(5)
      When query
        """
        SELECT ceil(5, -1), typeof(ceil(5, -1)), typeof(5)
        """
      Then query result
        | ceil(5, -1) | typeof(ceil(5, -1)) | typeof(5) |
        | 10          | decimal(11,0)       | int       |

    Scenario: ceil doctest #24 — SELECT ceil(5, 0), typeof(ceil(5, 0)), typeof(5)
      When query
        """
        SELECT ceil(5, 0), typeof(ceil(5, 0)), typeof(5)
        """
      Then query result
        | ceil(5, 0) | typeof(ceil(5, 0)) | typeof(5) |
        | 5          | decimal(11,0)      | int       |
