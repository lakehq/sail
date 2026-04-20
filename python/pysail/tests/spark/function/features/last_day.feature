Feature: last_day function

  Rule: last_day with timestamp input

    Scenario: last_day on timestamp with UTC timezone
      When query
        """
        SELECT last_day(TIMESTAMP '2024-01-15 12:30:00 UTC') AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day on timestamp column
      When query
        """
        WITH t(ts) AS (VALUES (TIMESTAMP '2024-02-10 00:00:00 UTC'))
        SELECT last_day(ts) AS result FROM t
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day on date input
      When query
        """
        SELECT last_day(DATE '2024-03-05') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day on timestamp_ntz input
      When query
        """
        SELECT last_day(TIMESTAMP_NTZ '2024-12-01 08:00:00') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

    Scenario: last_day NULL propagation with timestamp
      When query
        """
        SELECT last_day(CAST(NULL AS TIMESTAMP)) AS result
        """
      Then query result
        | result |
        | NULL   |
