@interval_day_to_second
Feature: INTERVAL DAY TO SECOND literal parsing and operations

  Rule: Basic literals

    Scenario Outline: Literal: <case>
      When query
        """
        SELECT INTERVAL <lit> DAY TO SECOND AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | lit           | result                               |
        | negative interval      | '-3 04:05:06' | INTERVAL '-3 04:05:06' DAY TO SECOND |
        | negative zero interval | '-0 00:00:00' | INTERVAL '0 00:00:00' DAY TO SECOND  |

    Scenario: negative hours in interval is invalid
      When query
        """
        SELECT INTERVAL '3 -04:00:00' DAY TO SECOND AS result
        """
      Then query error (?i)invalid.*interval

  Rule: Overflow and large values

    Scenario: overflow hours into days
      When query
        """
        SELECT INTERVAL '0 25:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result                              |
        | INTERVAL '1 01:00:00' DAY TO SECOND |

  Rule: Cast operations

    Scenario Outline: Cast: <case>
      When query
        """
        SELECT CAST(<expr> AS INTERVAL DAY TO SECOND) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | expr                                                | result                              |
        | roundtrip cast to string and back    | CAST(INTERVAL '2 10:20:30' DAY TO SECOND AS STRING) | INTERVAL '2 10:20:30' DAY TO SECOND |
        | cast HOUR TO SECOND to DAY TO SECOND | INTERVAL '12:30:45' HOUR TO SECOND                  | INTERVAL '0 12:30:45' DAY TO SECOND |

  Rule: Arithmetic and comparison

    Scenario Outline: Arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | expr                                                                      | result                              |
        | equality with normalized form | INTERVAL '1 24:00:00' DAY TO SECOND = INTERVAL '2 00:00:00' DAY TO SECOND | true                                |
        | addition of intervals         | INTERVAL '0 23:00:00' DAY TO SECOND + INTERVAL '0 02:00:00' DAY TO SECOND | INTERVAL '1 01:00:00' DAY TO SECOND |

    Scenario: comparison in WHERE clause
      When query
        """
        SELECT * FROM (VALUES (1)) t(a)
        WHERE INTERVAL '2 03:04:05' DAY TO SECOND > INTERVAL '1 23:59:59' DAY TO SECOND
        """
      Then query result
        | a |
        | 1 |

  Rule: Subquery and projection

    Scenario: interval in subquery
      When query
        """
        SELECT x FROM (SELECT INTERVAL '3 10:00:00' DAY TO SECOND AS x) t
        """
      Then query result
        | x                                   |
        | INTERVAL '3 10:00:00' DAY TO SECOND |
