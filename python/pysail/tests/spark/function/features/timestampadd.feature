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
