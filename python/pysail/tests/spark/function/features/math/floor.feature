@floor
Feature: floor output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to floor yields the schema Spark declares
      When query
        """
        SELECT floor(-0.1) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """

    Scenario: a nullable column input to floor stays nullable
      When query
        """
        SELECT floor(c) AS result FROM VALUES (-0.1), (CAST(NULL AS DECIMAL(1,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """

  Rule: Result values (migrated from test_floor.txt doctests)

    Scenario: floor doctest #1 — SELECT floor(-0.1), typeof(floor(-0.1)), typeof(-0.1)
      When query
        """
        SELECT floor(-0.1), typeof(floor(-0.1)), typeof(-0.1)
        """
      Then query result
        | FLOOR(-0.1) | typeof(FLOOR(-0.1)) | typeof(-0.1) |
        | -1          | decimal(1,0)        | decimal(1,1) |

    Scenario: floor doctest #2 — SELECT floor(5), typeof(floor(5)), typeof(5)
      When query
        """
        SELECT floor(5), typeof(floor(5)), typeof(5)
        """
      Then query result
        | FLOOR(5) | typeof(FLOOR(5)) | typeof(5) |
        | 5        | bigint           | int       |

    Scenario: floor doctest #3 — SELECT floor(5.4), typeof(floor(5.4)), typeof(5.4)
      When query
        """
        SELECT floor(5.4), typeof(floor(5.4)), typeof(5.4)
        """
      Then query result
        | FLOOR(5.4) | typeof(FLOOR(5.4)) | typeof(5.4)  |
        | 5          | decimal(2,0)       | decimal(2,1) |

    Scenario: floor doctest #4 — SELECT floor(3.1411, -3), typeof(floor(3.1411, -3)), typeof(
      When query
        """
        SELECT floor(3.1411, -3), typeof(floor(3.1411, -3)), typeof(3.1411)
        """
      Then query result
        | floor(3.1411, -3) | typeof(floor(3.1411, -3)) | typeof(3.1411) |
        | 0                 | decimal(4,0)              | decimal(5,4)   |

    Scenario: floor doctest #5 — SELECT floor(3.1411, 3), typeof(floor(3.1411, 3)), typeof(3.
      When query
        """
        SELECT floor(3.1411, 3), typeof(floor(3.1411, 3)), typeof(3.1411)
        """
      Then query result
        | floor(3.1411, 3) | typeof(floor(3.1411, 3)) | typeof(3.1411) |
        | 3.141            | decimal(5,3)             | decimal(5,4)   |

    Scenario: floor doctest #6 — SELECT floor(3345.1, -2), typeof(floor(3345.1, -2)), typeof(
      When query
        """
        SELECT floor(3345.1, -2), typeof(floor(3345.1, -2)), typeof(3345.1)
        """
      Then query result
        | floor(3345.1, -2) | typeof(floor(3345.1, -2)) | typeof(3345.1) |
        | 3300              | decimal(5,0)              | decimal(5,1)   |

    Scenario: floor doctest #7 — SELECT floor(-12.345, 1), typeof(floor(-12.345, 1)), typeof(
      When query
        """
        SELECT floor(-12.345, 1), typeof(floor(-12.345, 1)), typeof(3345.1)
        """
      Then query result
        | floor(-12.345, 1) | typeof(floor(-12.345, 1)) | typeof(3345.1) |
        | -12.4             | decimal(4,1)              | decimal(5,1)   |

    Scenario: floor doctest #8 — SELECT floor(CAST(5 as TINYINT), 1), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as TINYINT), 1), typeof(floor(CAST(5 as TINYINT), 1))
        """
      Then query result
        | floor(CAST(5 AS TINYINT), 1) | typeof(floor(CAST(5 AS TINYINT), 1)) |
        | 5                            | decimal(4,0)                         |

    Scenario: floor doctest #9 — SELECT floor(CAST(5 as TINYINT), -4), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as TINYINT), -4), typeof(floor(CAST(5 as TINYINT), -4))
        """
      Then query result
        | floor(CAST(5 AS TINYINT), -4) | typeof(floor(CAST(5 AS TINYINT), -4)) |
        | 0                             | decimal(5,0)                          |

    Scenario: floor doctest #10 — SELECT floor(CAST(5 as SMALLINT), 1), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as SMALLINT), 1), typeof(floor(CAST(5 as SMALLINT), 1))
        """
      Then query result
        | floor(CAST(5 AS SMALLINT), 1) | typeof(floor(CAST(5 AS SMALLINT), 1)) |
        | 5                             | decimal(6,0)                          |

    Scenario: floor doctest #11 — SELECT floor(CAST(5 as SMALLINT), -6), typeof(floor(CAST(5 a
      When query
        """
        SELECT floor(CAST(5 as SMALLINT), -6), typeof(floor(CAST(5 as SMALLINT), -6))
        """
      Then query result
        | floor(CAST(5 AS SMALLINT), -6) | typeof(floor(CAST(5 AS SMALLINT), -6)) |
        | 0                              | decimal(7,0)                           |

    Scenario: floor doctest #12 — SELECT floor(CAST(5 as INT), 1), typeof(floor(CAST(5 as INT)
      When query
        """
        SELECT floor(CAST(5 as INT), 1), typeof(floor(CAST(5 as INT), 1))
        """
      Then query result
        | floor(CAST(5 AS INT), 1) | typeof(floor(CAST(5 AS INT), 1)) |
        | 5                        | decimal(11,0)                    |

    Scenario: floor doctest #13 — SELECT floor(CAST(5 as INT), -11), typeof(floor(CAST(5 as IN
      When query
        """
        SELECT floor(CAST(5 as INT), -11), typeof(floor(CAST(5 as INT), -11))
        """
      Then query result
        | floor(CAST(5 AS INT), -11) | typeof(floor(CAST(5 AS INT), -11)) |
        | 0                          | decimal(12,0)                      |

    Scenario: floor doctest #14 — SELECT floor(CAST(5 as BIGINT), 1), typeof(floor(CAST(5 as B
      When query
        """
        SELECT floor(CAST(5 as BIGINT), 1), typeof(floor(CAST(5 as BIGINT), 1))
        """
      Then query result
        | floor(CAST(5 AS BIGINT), 1) | typeof(floor(CAST(5 AS BIGINT), 1)) |
        | 5                           | decimal(21,0)                       |

    Scenario: floor doctest #15 — SELECT floor(CAST(5 as BIGINT), -21), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as BIGINT), -21), typeof(floor(CAST(5 as BIGINT), -21))
        """
      Then query result
        | floor(CAST(5 AS BIGINT), -21) | typeof(floor(CAST(5 AS BIGINT), -21)) |
        | 0                             | decimal(22,0)                         |

    Scenario: floor doctest #16 — SELECT floor(CAST(5 as FLOAT), 1), typeof(floor(CAST(5 as FL
      When query
        """
        SELECT floor(CAST(5 as FLOAT), 1), typeof(floor(CAST(5 as FLOAT), 1))
        """
      Then query result
        | floor(CAST(5 AS FLOAT), 1) | typeof(floor(CAST(5 AS FLOAT), 1)) |
        | 5.0                        | decimal(9,1)                       |

    Scenario: floor doctest #17 — SELECT floor(CAST(5 as FLOAT), -15), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as FLOAT), -15), typeof(floor(CAST(5 as FLOAT), -15))
        """
      Then query result
        | floor(CAST(5 AS FLOAT), -15) | typeof(floor(CAST(5 AS FLOAT), -15)) |
        | 0                            | decimal(16,0)                        |

    Scenario: floor doctest #18 — SELECT floor(CAST(5 as DOUBLE), 1), typeof(floor(CAST(5 as D
      When query
        """
        SELECT floor(CAST(5 as DOUBLE), 1), typeof(floor(CAST(5 as DOUBLE), 1))
        """
      Then query result
        | floor(CAST(5 AS DOUBLE), 1) | typeof(floor(CAST(5 AS DOUBLE), 1)) |
        | 5.0                         | decimal(17,1)                       |

    Scenario: floor doctest #19 — SELECT floor(CAST(5 as DOUBLE), -31), typeof(floor(CAST(5 as
      When query
        """
        SELECT floor(CAST(5 as DOUBLE), -31), typeof(floor(CAST(5 as DOUBLE), -31))
        """
      Then query result
        | floor(CAST(5 AS DOUBLE), -31) | typeof(floor(CAST(5 AS DOUBLE), -31)) |
        | 0                             | decimal(32,0)                         |

    Scenario: floor doctest #20 — SELECT floor(CAST(5 as DECIMAL(5, 2)), 1), typeof(floor(CAST
      When query
        """
        SELECT floor(CAST(5 as DECIMAL(5, 2)), 1), typeof(floor(CAST(5 as DECIMAL(5, 2)), 1))
        """
      Then query result
        | floor(CAST(5 AS DECIMAL(5,2)), 1) | typeof(floor(CAST(5 AS DECIMAL(5,2)), 1)) |
        | 5.0                               | decimal(5,1)                              |

    Scenario: floor doctest #21 — SELECT floor(CAST(5 as DECIMAL(5, 2))), typeof(floor(CAST(5
      When query
        """
        SELECT floor(CAST(5 as DECIMAL(5, 2))), typeof(floor(CAST(5 as DECIMAL(5, 2))))
        """
      Then query result
        | FLOOR(CAST(5 AS DECIMAL(5,2))) | typeof(FLOOR(CAST(5 AS DECIMAL(5,2)))) |
        | 5                              | decimal(4,0)                           |

    Scenario: floor doctest #22 — SELECT floor(5.4, -1), typeof(floor(5.4, -1)), typeof(5.4)
      When query
        """
        SELECT floor(5.4, -1), typeof(floor(5.4, -1)), typeof(5.4)
        """
      Then query result
        | floor(5.4, -1) | typeof(floor(5.4, -1)) | typeof(5.4)  |
        | 0              | decimal(2,0)           | decimal(2,1) |

    Scenario: floor doctest #23 — SELECT floor(5, -1), typeof(floor(5, -1)), typeof(5)
      When query
        """
        SELECT floor(5, -1), typeof(floor(5, -1)), typeof(5)
        """
      Then query result
        | floor(5, -1) | typeof(floor(5, -1)) | typeof(5) |
        | 0            | decimal(11,0)        | int       |

    Scenario: floor doctest #24 — SELECT floor(5, 0), typeof(floor(5, 0)), typeof(5)
      When query
        """
        SELECT floor(5, 0), typeof(floor(5, 0)), typeof(5)
        """
      Then query result
        | floor(5, 0) | typeof(floor(5, 0)) | typeof(5) |
        | 5           | decimal(11,0)       | int       |
