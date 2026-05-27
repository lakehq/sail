Feature: convert_timezone

  Rule: Type coercion

    Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` with coercible input
      When query
        """
        SELECT convert_timezone('America/Los_Angeles', 'Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 23:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 23:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 23:30:00 |
        | DATE '2024-06-15'                     | 2024-06-15 09:00:00 |

    Scenario Outline: `convert_timezone` with coercible input and implicit source timezone
      When query
        """
        SELECT convert_timezone('Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 08:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 08:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 08:30:00 |
        | DATE '2024-06-15'                     | 2024-06-14 18:00:00 |

  Rule: Daylight saving time handling

    Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` around daylight saving time transition
      When query
        """
        SELECT convert_timezone(<from>, <to>, <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | from                  | to                    | result              |
        | '2025-03-09 01:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 10:30:00 |
        | '2025-03-09 02:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 03:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 10:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 01:30:00 |
        | '2025-03-09 11:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 03:30:00 |
