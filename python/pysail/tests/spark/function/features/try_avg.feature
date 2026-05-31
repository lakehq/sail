@try_avg
Feature: try_avg returns the average, or NULL on overflow

  Rule: Basic usage

    Scenario: average of integers
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (1), (2), (3) AS t(col)
        """
      Then query result
        | result |
        | 2.0    |

    # Spark returns 2.00000 (scale=5), Sail returns 2.0 (truncates trailing zeros)
    # CAST forces consistent display across both engines
    Scenario: average of doubles
      When query
        """
        SELECT CAST(try_avg(col) AS DECIMAL(10,1)) AS result FROM VALUES (1.0), (2.0), (3.0) AS t(col)
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: average of bigints
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT)) AS t(col)
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: single value
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (42) AS t(col)
        """
      Then query result
        | result |
        | 42.0   |

    Scenario: all zeros
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (0), (0), (0) AS t(col)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: mixed positive and negative cancel out
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (-100), (50), (50) AS t(col)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: negative values
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (-10), (10) AS t(col)
        """
      Then query result
        | result |
        | 0.0    |

  Rule: Decimal types

    # Spark returns 2.000000 (scale=6), Sail returns 2.00 (truncates trailing zeros)
    Scenario: basic decimal average
      When query
        """
        SELECT CAST(try_avg(col) AS DECIMAL(10,2)) AS result FROM VALUES (CAST(1.5 AS DECIMAL(10,2))), (CAST(2.5 AS DECIMAL(10,2))) AS t(col)
        """
      Then query result
        | result |
        | 2.00   |

    # Spark returns 150.0000 (scale=4), Sail returns 150 (truncates trailing zeros)
    Scenario: decimal no overflow
      When query
        """
        SELECT CAST(try_avg(col) AS DECIMAL(10,0)) AS result FROM VALUES (CAST(100 AS DECIMAL(38,0))), (CAST(200 AS DECIMAL(38,0))) AS t(col)
        """
      Then query result
        | result |
        | 150    |

  Rule: Overflow returns NULL

    Scenario: decimal overflow returns NULL
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (CAST(11111111111111111111111111111111111111 AS DECIMAL(38,0))), (CAST(0 AS DECIMAL(38,0))) AS t(col)
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL handling

    Scenario: nulls are ignored
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (1), (NULL), (3) AS t(col)
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: all nulls returns NULL
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(col)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: empty dataset returns NULL
      When query
        """
        SELECT try_avg(col) AS result FROM VALUES (CAST(NULL AS INT)) AS t(col) WHERE col IS NOT NULL
        """
      Then query result
        | result |
        | NULL   |

  Rule: Group by

    Scenario: try_avg with group by
      When query
        """
        SELECT grp, try_avg(val) AS result FROM VALUES (1, 10), (1, 20), (2, 100), (2, NULL) AS t(grp, val) GROUP BY grp ORDER BY grp
        """
      Then query result ordered
        | grp | result |
        | 1   | 15.0   |
        | 2   | 100.0  |
