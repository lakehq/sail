Feature: INTERVAL DAY TO SECOND literal parsing and operations

  Rule: Basic literals

    Scenario: negative interval
      When query
        """
        SELECT INTERVAL '-3 04:05:06' DAY TO SECOND AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '-3 04:05:06' DAY TO SECOND         |

    Scenario: negative hours in interval is invalid
      When query
        """
        SELECT INTERVAL '3 -04:00:00' DAY TO SECOND AS result
        """
      Then query error (?i)invalid.*interval

    Scenario: negative zero interval
      When query
        """
        SELECT INTERVAL '-0 00:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '0 00:00:00' DAY TO SECOND          |

  Rule: Overflow and large values

    Scenario: overflow hours into days
      When query
        """
        SELECT INTERVAL '0 25:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '1 01:00:00' DAY TO SECOND          |

  Rule: Cast operations

    Scenario: roundtrip cast to string and back
      When query
        """
        SELECT CAST(CAST(INTERVAL '2 10:20:30' DAY TO SECOND AS STRING) AS INTERVAL DAY TO SECOND) AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '2 10:20:30' DAY TO SECOND          |

    Scenario: cast HOUR TO SECOND to DAY TO SECOND
      When query
        """
        SELECT CAST(INTERVAL '12:30:45' HOUR TO SECOND AS INTERVAL DAY TO SECOND) AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '0 12:30:45' DAY TO SECOND          |

  Rule: Arithmetic and comparison

    Scenario: equality with normalized form
      When query
        """
        SELECT INTERVAL '1 24:00:00' DAY TO SECOND = INTERVAL '2 00:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: addition of intervals
      When query
        """
        SELECT INTERVAL '0 23:00:00' DAY TO SECOND + INTERVAL '0 02:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result                                       |
        | INTERVAL '1 01:00:00' DAY TO SECOND          |

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
        | x                                            |
        | INTERVAL '3 10:00:00' DAY TO SECOND          |
