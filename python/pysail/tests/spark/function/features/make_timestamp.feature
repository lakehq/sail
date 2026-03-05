Feature: make_timestamp_ntz and try_make_timestamp_ntz functions

  Rule: Basic timestamp creation with 6 arguments

    Scenario: create timestamp from date and time components
      When query
      """
      SELECT make_timestamp_ntz(2014, 12, 28, 6, 30, 45.887) AS result
      """
      Then query result
      | result                  |
      | 2014-12-28 06:30:45.887 |

    Scenario: create timestamp at midnight
      When query
      """
      SELECT make_timestamp_ntz(2023, 12, 31, 0, 0, 0.0) AS result
      """
      Then query result
      | result              |
      | 2023-12-31 00:00:00 |

    Scenario: create timestamp near end of valid range
      When query
      """
      SELECT make_timestamp_ntz(9999, 12, 31, 23, 58, 59.999999) AS result
      """
      Then query result
      | result                  |
      | 9999-12-31 23:58:59.999999 |

    Scenario: sec=60 adds one minute
      When query
      """
      SELECT make_timestamp_ntz(2024, 6, 15, 14, 30, 60.0) AS result
      """
      Then query result
      | result              |
      | 2024-06-15 14:31:00 |

  Rule: Timestamp creation with date and time arguments

    Scenario: combine date and time
      When query
      """
      SELECT make_timestamp_ntz(DATE '2024-03-15', TIME '14:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-03-15 14:30:00 |

    Scenario: combine date and time with microsecond precision
      When query
      """
      SELECT make_timestamp_ntz(DATE '2024-01-01', TIME '12:34:56.123456') AS result
      """
      Then query result
      | result                  |
      | 2024-01-01 12:34:56.123456 |

  Rule: try_make_timestamp_ntz with valid inputs

    Scenario: valid 6-argument call
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 2, 14, 15, 45, 30.5) AS result
      """
      Then query result
      | result              |
      | 2024-02-14 15:45:30.5 |

    Scenario: valid date and time combination
      When query
      """
      SELECT try_make_timestamp_ntz(DATE '2024-07-04', TIME '18:00:00') AS result
      """
      Then query result
      | result              |
      | 2024-07-04 18:00:00 |

  Rule: try_make_timestamp_ntz with invalid inputs returns NULL

    Scenario: invalid month
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 13, 1, 0, 0, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: invalid day
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 2, 30, 0, 0, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: invalid hour
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 1, 1, 24, 0, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: invalid minute
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 1, 1, 0, 60, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: invalid second
      When query
      """
      SELECT try_make_timestamp_ntz(2024, 1, 1, 0, 0, 61.0) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: NULL handling

    Scenario: make_timestamp_ntz with null date
      When query
      """
      SELECT make_timestamp_ntz(CAST(NULL AS DATE), TIME '10:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_make_timestamp_ntz with null year
      When query
      """
      SELECT try_make_timestamp_ntz(CAST(NULL AS INT), 1, 1, 0, 0, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Multiple rows with mixed valid and invalid inputs

    Scenario: try_make_timestamp_ntz on array of inputs
      When query
      """
      SELECT 
        year,
        try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
      FROM VALUES 
        (2024, 1, 1, 0, 0, 0.0),
        (2024, 13, 1, 0, 0, 0.0),
        (2024, 6, 15, 12, 30, 45.5)
      AS t(year, month, day, hour, min, sec)
      ORDER BY year, month, day
      """
      Then query result ordered
      | year | result              |
      | 2024 | 2024-01-01 00:00:00 |
      | 2024 | 2024-06-15 12:30:45.5 |
      | 2024 | NULL                |
