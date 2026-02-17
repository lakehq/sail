Feature: ANSI mode behaviors

  Rule: Division by zero

    Scenario: Integer division by zero returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Division by zero throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query error (?i)divide.*zero

  Rule: Float/Double division by zero

    Scenario: Double division by zero throws error when ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE) AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Double division by zero returns NULL when ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(1.0 AS DOUBLE) / CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Float division by zero returns NULL when ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(1.0 AS FLOAT) / CAST(0.0 AS FLOAT) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Modulo by zero

    Scenario: Integer modulo by zero returns NULL regardless of ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Integer modulo by zero throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query error (?i)(remainder.*zero|divide.*zero)

    Scenario: Double modulo by zero returns NULL when ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(10.5 AS DOUBLE) % CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Normal modulo works correctly
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 % 3 AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Array out of bounds

    Scenario: Array out of bounds returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(1, 2, 3)[5] AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Array out of bounds throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(1, 2, 3)[5] AS result
        """
      Then query error (?i)index.*out.*bounds

    Scenario: Array valid index works correctly
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(10, 20, 30)[1] AS result
        """
      Then query result
        | result |
        | 20     |

  Rule: CAST invalid values

    Scenario: CAST invalid string to double returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('abc' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: CAST invalid string to double throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('abc' AS DOUBLE) AS result
        """
      Then query error (?i)(invalid|cast|cannot)

    Scenario: CAST invalid string to int returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('not_a_number' AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: CAST invalid string to int throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('not_a_number' AS INT) AS result
        """
      Then query error (?i)(invalid|cast|cannot)

    Scenario: CAST valid string to double works correctly
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('123.45' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 123.45 |

    Scenario: CAST invalid string to boolean returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('maybe' AS BOOLEAN) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: CAST invalid string to boolean throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('maybe' AS BOOLEAN) AS result
        """
      Then query error (?i)(invalid|cast|cannot)

  Rule: unix_timestamp invalid format

    Scenario: unix_timestamp with invalid date string returns NULL when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unix_timestamp('invalid_date', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: unix_timestamp with invalid date string throws error when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp('invalid_date', 'yyyy-MM-dd') AS result
        """
      Then query error (?i)(parse|timestamp|invalid|error)

    Scenario: unix_timestamp with valid date works correctly
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unix_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 1705276800 |

  Rule: Edge cases

    Scenario: Array with null elements
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(1, NULL, 3)[1] AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty array access returns NULL when ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array()[0] AS result
        """
      Then query result
        | result |
        | NULL   |
