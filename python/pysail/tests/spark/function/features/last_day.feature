@last_day
Feature: last_day comprehensive tests

  Rule: Argument count validation

    Scenario: last_day zero arguments errors
      When query
        """
        SELECT last_day() AS result
        """
      Then query error .*

    Scenario: last_day two arguments errors
      When query
        """
        SELECT last_day(DATE'2024-01-01', 'extra') AS result
        """
      Then query error .*

  Rule: NULL handling

    Scenario: last_day NULL date
      When query
        """
        SELECT last_day(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: last_day NULL string
      When query
        """
        SELECT last_day(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic usage per month

    Scenario: last_day January
      When query
        """
        SELECT last_day(DATE'2024-01-15') AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day February leap year
      When query
        """
        SELECT last_day(DATE'2024-02-01') AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day February non-leap year
      When query
        """
        SELECT last_day(DATE'2023-02-01') AS result
        """
      Then query result
        | result     |
        | 2023-02-28 |

    Scenario: last_day March
      When query
        """
        SELECT last_day(DATE'2024-03-01') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day April
      When query
        """
        SELECT last_day(DATE'2024-04-15') AS result
        """
      Then query result
        | result     |
        | 2024-04-30 |

    Scenario: last_day December
      When query
        """
        SELECT last_day(DATE'2024-12-05') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

  Rule: String input coercion

    Scenario: last_day with string input
      When query
        """
        SELECT last_day('2024-03-15') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day with invalid string errors
      When query
        """
        SELECT last_day('not-a-date') AS result
        """
      Then query error .*

  Rule: Boundary dates

    Scenario: last_day epoch
      When query
        """
        SELECT last_day(DATE'1970-01-01') AS result
        """
      Then query result
        | result     |
        | 1970-01-31 |

    Scenario: last_day minimum date
      When query
        """
        SELECT last_day(DATE'0001-01-01') AS result
        """
      Then query result
        | result     |
        | 0001-01-31 |

    Scenario: last_day maximum date
      When query
        """
        SELECT last_day(DATE'9999-12-01') AS result
        """
      Then query result
        | result     |
        | 9999-12-31 |

  Rule: Multi-row

    Scenario: last_day multi-row
      When query
        """
        SELECT last_day(d) AS result FROM VALUES (DATE'2024-01-15'), (CAST(NULL AS DATE)), (DATE'2024-02-29') AS t(d)
        """
      Then query result
        | result     |
        | 2024-01-31 |
        | NULL       |
        | 2024-02-29 |

  Rule: Timestamp implicit coercion to Date

    # Spark implicitly casts Timestamp / Timestamp_NTZ to Date before applying
    # last_day. Regression test for issue #1735 — Sail previously rejected
    # these types at plan time.

    Scenario: last_day accepts TIMESTAMP input (Spark casts to Date)
      When query
        """
        SELECT last_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP)) AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day accepts TIMESTAMP_NTZ input (Spark casts to Date)
      When query
        """
        SELECT last_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)) AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day on TIMESTAMP with non-midnight time still truncates
      When query
        """
        SELECT last_day(CAST('2024-02-29 23:59:59' AS TIMESTAMP)) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day on TIMESTAMP column from VALUES
      When query
        """
        SELECT last_day(ts) AS result FROM VALUES
          (CAST('2024-01-15 10:30:00' AS TIMESTAMP)),
          (CAST('2024-12-01 00:00:00' AS TIMESTAMP))
          AS t(ts)
        """
      Then query result
        | result     |
        | 2024-01-31 |
        | 2024-12-31 |

  Rule: Gregorian calendar edge cases (century leap rules + min date)

    # Century rule: divisible by 100 is NOT leap unless also divisible by 400.
    # 1900 → non-leap (Feb=28 days). 2000 → leap (Feb=29 days).
    # Exercises the leap-year algorithm correctness, not just the generic path.

    Scenario: last_day February 1900 (century non-leap, rule of 100)
      When query
        """
        SELECT last_day(DATE '1900-02-15') AS result
        """
      Then query result
        | result     |
        | 1900-02-28 |

    Scenario: last_day February 2000 (century leap, rule of 400)
      When query
        """
        SELECT last_day(DATE '2000-02-15') AS result
        """
      Then query result
        | result     |
        | 2000-02-29 |

    Scenario: last_day year 1 AD (min date boundary)
      When query
        """
        SELECT last_day(DATE '0001-01-15') AS result
        """
      Then query result
        | result     |
        | 0001-01-31 |
