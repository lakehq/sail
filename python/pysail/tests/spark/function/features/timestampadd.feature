@timestampadd
Feature: timestampadd function

  Scenario: add years
    When query
    """
    SELECT timestampadd(YEAR, 2, timestamp'2016-03-11 09:00:07') AS result
    """
    Then query result
    | result              |
    | 2018-03-11 09:00:07 |

  Scenario: add weeks
    When query
    """
    SELECT timestampadd(WEEK, 5, timestamp'2016-03-11 09:00:07') AS result
    """
    Then query result
    | result              |
    | 2016-04-15 09:00:07 |

  Scenario: subtract days
    When query
    """
    SELECT timestampadd(day, -5, timestamp'2016-03-11 09:00:07') AS result
    """
    Then query result
    | result              |
    | 2016-03-06 09:00:07 |

  Scenario: add microseconds
    When query
    """
    SELECT timestampadd(MICROSECOND, 2, timestamp'2016-03-11 09:00:07.000001') AS result
    """
    Then query result
    | result                     |
    | 2016-03-11 09:00:07.000003 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null timestamp literal yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a non-null timestamp column yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, CAST(id AS TIMESTAMP)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT timestampadd(HOUR, 1, c) AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
