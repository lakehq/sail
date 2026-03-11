Feature: TIME data type support

  Rule: TIME literal syntax

    Scenario: basic time literal
      When query
      """
      SELECT TIME '10:30:45' AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    Scenario: time with microseconds
      When query
      """
      SELECT TIME '14:25:36.123456' AS result
      """
      Then query result
      | result          |
      | 14:25:36.123456 |

    Scenario: midnight
      When query
      """
      SELECT TIME '00:00:00' AS result
      """
      Then query result
      | result   |
      | 00:00:00 |

    Scenario: one microsecond before midnight
      When query
      """
      SELECT TIME '23:59:59.999999' AS result
      """
      Then query result
      | result          |
      | 23:59:59.999999 |

  Rule: TIME in table operations

    Scenario: select from table with TIME column
      When query
      """
      SELECT * FROM VALUES 
        (TIME '09:00:00'),
        (TIME '12:30:00'),
        (TIME '18:45:00')
      AS t(time_col)
      ORDER BY time_col
      """
      Then query result ordered
      | time_col |
      | 09:00:00 |
      | 12:30:00 |
      | 18:45:00 |

    Scenario: filter by TIME value
      When query
      """
      SELECT time_col FROM VALUES 
        (TIME '08:00:00'),
        (TIME '12:00:00'),
        (TIME '16:00:00')
      AS t(time_col)
      WHERE time_col > TIME '10:00:00'
      ORDER BY time_col
      """
      Then query result ordered
      | time_col |
      | 12:00:00 |
      | 16:00:00 |

  Rule: NULL handling

    Scenario: TIME column with NULLs
      When query
      """
      SELECT time_col FROM VALUES 
        (TIME '10:00:00'),
        (NULL),
        (TIME '14:00:00')
      AS t(time_col)
      WHERE time_col IS NOT NULL
      ORDER BY time_col
      """
      Then query result ordered
      | time_col |
      | 10:00:00 |
      | 14:00:00 |

  Rule: Precision levels

    Scenario: second precision
      When query
      """
      SELECT TIME '12:34:56' AS result
      """
      Then query result
      | result   |
      | 12:34:56 |

    Scenario: millisecond precision
      When query
      """
      SELECT TIME '12:34:56.123' AS result
      """
      Then query result
      | result       |
      | 12:34:56.123 |

    Scenario: microsecond precision
      When query
      """
      SELECT TIME '12:34:56.123456' AS result
      """
      Then query result
      | result          |
      | 12:34:56.123456 |
