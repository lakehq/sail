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

    # Spark validates each field of a DAY TO SECOND literal and rejects an out-of-range hour
    # instead of carrying it into days: "requirement failed: hour 25 outside range [0, 23]".
    # Sail normalizes it to `INTERVAL '1 01:00:00' DAY TO SECOND`.
    @sail-bug
    Scenario: overflow hours into days
      When query
        """
        SELECT INTERVAL '0 25:00:00' DAY TO SECOND AS result
        """
      Then query error hour 25 outside range \[0, 23\]

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
        | case                  | expr                                                                      | result                              |
        | addition of intervals | INTERVAL '0 23:00:00' DAY TO SECOND + INTERVAL '0 02:00:00' DAY TO SECOND | INTERVAL '1 01:00:00' DAY TO SECOND |

    Scenario Outline: string and day-time interval addition with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          ('2026-03-15 12:30:00'),
          ('not-a-timestamp'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | 2026-03-15 12:30:05 |
        | NULL                |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | operand order  | expr                                  |
        | string first   | ts_str + INTERVAL 1 SECOND * 5         |
        | interval first | INTERVAL 1 SECOND * 5 + ts_str         |

    Scenario: string and day-time interval addition under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: invalid string and day-time interval addition errors under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES ('not-a-timestamp') AS t(ts_str)
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|found n at 0:1)

    Scenario Outline: string and whole-day interval addition keeps local time across DST with <operand order>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2019-03-09 12:00:00'),
          ('2019-11-02 12:00:00')
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2019-03-10 12:00:00 |
        | 2019-11-03 12:00:00 |

      Examples:
        | operand order  | expr                     |
        | string first   | ts_str + INTERVAL 1 DAY  |
        | interval first | INTERVAL 1 DAY + ts_str  |

    Scenario Outline: leap-second string and interval addition returns NULL in legacy mode with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    Scenario Outline: leap-second string and interval addition errors in ANSI mode with <operand order>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|leap seconds)

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    # Same field validation as above: Spark will not even parse `INTERVAL '1 24:00:00'`, so the
    # normalized-equality comparison never runs. Sail normalizes and answers `true`.
    @sail-bug
    Scenario: equality with normalized form
      When query
        """
        SELECT INTERVAL '1 24:00:00' DAY TO SECOND = INTERVAL '2 00:00:00' DAY TO SECOND AS result
        """
      Then query error hour 24 outside range \[0, 23\]

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
