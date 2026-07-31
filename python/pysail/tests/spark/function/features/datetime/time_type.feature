@time_type
Feature: TIME data type support

  Rule: TIME literal syntax

    Scenario Outline: Literal: <case>
      When query
        """
        SELECT TIME <lit> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | lit               | result          |
        | basic time literal              | '10:30:45'        | 10:30:45        |
        | time with microseconds          | '14:25:36.123456' | 14:25:36.123456 |
        | midnight                        | '00:00:00'        | 00:00:00        |
        | one microsecond before midnight | '23:59:59.999999' | 23:59:59.999999 |

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

    Scenario Outline: Precision: <case>
      When query
        """
        SELECT TIME <lit> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | lit               | result          |
        | second precision      | '12:34:56'        | 12:34:56        |
        | millisecond precision | '12:34:56.123'    | 12:34:56.123    |
        | microsecond precision | '12:34:56.123456' | 12:34:56.123456 |
