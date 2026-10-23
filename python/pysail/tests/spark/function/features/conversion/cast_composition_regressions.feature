Feature: CAST composition preserves Spark schemas and VALUES widening

  Scenario: Cast regression 00 values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT * FROM VALUES (CAST('NaN' AS FLOAT)), (2.5) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |NaN   |
      |2.5   |

  Scenario: Cast regression 01 values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT * FROM VALUES (2.5), (CAST('NaN' AS FLOAT)) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |2.5   |
      |NaN   |

  Scenario: Cast regression 02 values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT * FROM VALUES (CAST('NaN' AS DOUBLE)), (2.5) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |NaN   |
      |2.5   |

  Scenario: Cast regression 03 values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT * FROM VALUES (2.5), (CAST('NaN' AS DOUBLE)) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |2.5   |
      |NaN   |

  Scenario: Cast regression 04 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(CAST(1 AS DECIMAL(5,2)), 10F, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: float (nullable = false)
      """
    Then query result ordered
      |result   |
      |7.8444586|

  Scenario: Cast regression 05 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(CAST(1 AS DECIMAL(5,2)), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result           |
      |7.844458382457324|

  Scenario: Cast regression 06 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(NULL, 10, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 07 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(CAST(NULL AS DOUBLE), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 08 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(CAST(NULL AS DECIMAL(5,2)), 10F, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: float (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 09 uniform
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT uniform(TRY_CAST('bad' AS DOUBLE), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 10 values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT * FROM VALUES (CAST('NaN' AS FLOAT)), (2.5) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |NaN   |
      |2.5   |

  Scenario: Cast regression 11 values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT * FROM VALUES (2.5), (CAST('NaN' AS FLOAT)) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |2.5   |
      |NaN   |

  Scenario: Cast regression 12 values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT * FROM VALUES (CAST('NaN' AS DOUBLE)), (2.5) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |NaN   |
      |2.5   |

  Scenario: Cast regression 13 values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT * FROM VALUES (2.5), (CAST('NaN' AS DOUBLE)) AS t(result)
      """
    Then query schema
      """
      root
       |-- result: double (nullable = true)
      """
    Then query result ordered
      |result|
      |2.5   |
      |NaN   |

  Scenario: Cast regression 14 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(CAST(1 AS DECIMAL(5,2)), 10F, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: float (nullable = false)
      """
    Then query result ordered
      |result   |
      |7.8444586|

  Scenario: Cast regression 15 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(CAST(1 AS DECIMAL(5,2)), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result           |
      |7.844458382457324|

  Scenario: Cast regression 16 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(NULL, 10, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 17 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(CAST(NULL AS DOUBLE), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 18 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(CAST(NULL AS DECIMAL(5,2)), 10F, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: float (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario: Cast regression 19 uniform
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT uniform(TRY_CAST('bad' AS DOUBLE), 10D, 0) AS result
      """
    Then query schema
      """
      root
       |-- result: double (nullable = false)
      """
    Then query result ordered
      |result|
      |NULL  |

  Scenario Outline: Cast regression uniform NULL composition <expression>
    When query
      """
      SELECT <expression> AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | expression                                              | result |
      | uniform(NULL, 10, 0) IS NULL                             | true   |
      | uniform(TRY_CAST('bad' AS DOUBLE), 10D, 0) IS NULL         | true   |
      | coalesce(uniform(CAST(NULL AS DOUBLE), 10D, 0), 99D)      | 99.0   |
