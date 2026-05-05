@next_day
Feature: next_day comprehensive tests

  Rule: Argument count validation

    Scenario: next_day zero arguments errors
      When query
        """
        SELECT next_day() AS result
        """
      Then query error .*

    Scenario: next_day one argument errors
      When query
        """
        SELECT next_day(DATE'2024-01-10') AS result
        """
      Then query error .*

    Scenario: next_day three arguments errors
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Mon', 'extra') AS result
        """
      Then query error .*

  Rule: NULL combinatorial

    Scenario: next_day NULL date
      When query
        """
        SELECT next_day(CAST(NULL AS DATE), 'Monday') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: next_day NULL day name
      When query
        """
        SELECT next_day(DATE'2024-01-10', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: next_day both NULL
      When query
        """
        SELECT next_day(CAST(NULL AS DATE), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: All days of week (from Wednesday 2024-01-10)

    Scenario: next_day Monday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Monday') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: next_day Tuesday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Tuesday') AS result
        """
      Then query result
        | result     |
        | 2024-01-16 |

    Scenario: next_day Wednesday (same day skips to next week)
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Wednesday') AS result
        """
      Then query result
        | result     |
        | 2024-01-17 |

    Scenario: next_day Thursday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Thursday') AS result
        """
      Then query result
        | result     |
        | 2024-01-11 |

    Scenario: next_day Friday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Friday') AS result
        """
      Then query result
        | result     |
        | 2024-01-12 |

    Scenario: next_day Saturday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Saturday') AS result
        """
      Then query result
        | result     |
        | 2024-01-13 |

    Scenario: next_day Sunday
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Sunday') AS result
        """
      Then query result
        | result     |
        | 2024-01-14 |

  Rule: Abbreviated day names

    Scenario: next_day Mo
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Mo') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: next_day Tu
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Tu') AS result
        """
      Then query result
        | result     |
        | 2024-01-16 |

    Scenario: next_day We
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'We') AS result
        """
      Then query result
        | result     |
        | 2024-01-17 |

  Rule: Case insensitive

    Scenario: next_day lowercase
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'monday') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: next_day uppercase
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'MONDAY') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

  Rule: String date input coercion

    Scenario: next_day with string date
      When query
        """
        SELECT next_day('2024-01-10', 'Monday') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

  Rule: Multi-row

    Scenario: next_day multi-row
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES (DATE'2024-01-10', 'Monday'), (DATE'2024-01-10', 'Friday'), (CAST(NULL AS DATE), 'Monday') AS t(d, day)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | 2024-01-12 |
        | NULL       |

  Rule: Error conditions

    Scenario: next_day multi-row with invalid day name errors
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES (DATE'2024-01-10', 'Monday'), (DATE'2024-01-10', 'InvalidDay') AS t(d, day)
        """
      Then query error .*Illegal input for day of week.*

    Scenario: next_day invalid day name errors
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query error .*Illegal input for day of week.*

  Rule: ANSI mode on invalid day name (Spark JVM parity)

    # ANSI=true → error with ILLEGAL_DAY_OF_WEEK (matches Spark strict mode).
    # ANSI=false → returns NULL (matches Spark lenient mode).
    # Bound at planning time via PlanConfig::ansi_mode; serialized in
    # SparkNextDayUdf for distributed execution.

    Scenario: next_day ANSI=true errors on invalid day name
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query error .*Illegal input for day of week.*

    Scenario: next_day ANSI=false returns NULL on invalid day name
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: next_day ANSI=false multi-row with mixed valid and invalid day names
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES
          (DATE'2024-01-10', 'Monday'),
          (DATE'2024-01-10', 'InvalidDay')
          AS t(d, day)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | NULL       |

  Rule: Timestamp implicit coercion to Date

    # Spark implicitly casts Timestamp / Timestamp_NTZ to Date before applying
    # next_day. Regression test for the same pattern as issue #1735 (last_day).

    Scenario: next_day accepts TIMESTAMP input (Spark casts to Date)
      When query
        """
        SELECT next_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP), 'Mon') AS result
        """
      Then query result
        | result     |
        | 2024-01-22 |

    Scenario: next_day accepts TIMESTAMP_NTZ input (Spark casts to Date)
      When query
        """
        SELECT next_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ), 'Fri') AS result
        """
      Then query result
        | result     |
        | 2024-01-19 |

    Scenario: next_day on TIMESTAMP at year boundary
      When query
        """
        SELECT next_day(CAST('2024-12-31 23:59:59' AS TIMESTAMP), 'Wed') AS result
        """
      Then query result
        | result     |
        | 2025-01-01 |
