Feature: to_utc_timestamp

  Rule: Type coercion

    Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `to_utc_timestamp` with coercible input
      When query
        """
        SELECT to_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 21:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 21:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 21:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 21:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 21:30:00 |
        | '2024-06-15 13:30:00+07:00'           | 2024-06-15 21:30:00 |
        | DATE '2024-06-15'                     | 2024-06-15 07:00:00 |

  Rule: Daylight saving time handling

    Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `to_utc_timestamp` around daylight saving time transition
      When query
        """
        SELECT to_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | result              |
        | '2025-03-09 09:30:00' | 2025-03-09 17:30:00 |
        | '2025-03-09 10:30:00' | 2025-03-09 18:30:00 |
        | '2025-03-09 11:30:00' | 2025-03-09 18:30:00 |
