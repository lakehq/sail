@spark-4
Feature: make_timestamp_ntz

  # `MakeTimestamp` carries `failOnError = SQLConf.get.ansiEnabled`
  # (`datetimeExpressions.scala:2886`), and that single flag drives TWO branches at once:
  #   * the VALUE -- a field out of range raises under ANSI and becomes NULL without it;
  #   * the SCHEMA -- `nullable = if (failOnError) children.exists(_.nullable) else true` (:2921),
  #     so with ANSI on and non-null arguments the column is declared NOT nullable, and with ANSI
  #     off it is always nullable even though nothing can be null.
  # The pair below is what discriminates: asserting only one ANSI mode cannot tell the rule apart
  # from "always nullable".
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: the value it builds

    Scenario Outline: make_timestamp_ntz of <case>
      Given config spark.sql.session.timeZone = UTC
      When query template
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case               | args                      | result                     |
        | a plain timestamp  | 2024, 3, 5, 6, 7, 8       | 2024-03-05 06:07:08        |
        | a fractional second| 2024, 3, 5, 6, 7, 8.123456| 2024-03-05 06:07:08.123456 |
        | a leap day         | 2024, 2, 29, 0, 0, 0      | 2024-02-29 00:00:00        |

    # It is the NTZ variant, so the session zone must not move it. A zone with a half-hour offset
    # is used on purpose: an engine that applied a whole-hour zone by mistake would still look
    # right against UTC.
    Scenario: the session zone does not move it
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT make_timestamp_ntz(2024, 3, 5, 6, 7, 8) AS result
        """
      Then query result collected
        | result              |
        | 2024-03-05 06:07:08 |

    Scenario Outline: a NULL argument gives NULL: <case>
      When query template
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case   | args                                  |
        | year   | CAST(NULL AS INT), 3, 5, 6, 7, 8      |
        | month  | 2024, CAST(NULL AS INT), 5, 6, 7, 8   |
        | day    | 2024, 3, CAST(NULL AS INT), 6, 7, 8   |
        | second | 2024, 3, 5, 6, 7, CAST(NULL AS DECIMAL(16,6)) |

  Rule: a field out of range follows ANSI

    # Each field names itself in the message, so the case is pinned to the field it exercises
    # rather than to "something out of bounds". The valid-range part is left out on purpose: a
    # regex escape inside an `Examples` cell gets doubled and then matches nothing.
    @sail-bug
    Scenario Outline: <case> raises when ANSI is on
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query error \[DATETIME_FIELD_OUT_OF_BOUNDS\.WITH_SUGGESTION\] Invalid <detail>

      Examples:
        | case                         | args                 | detail                                          |
        | month 13                     | 2024, 13, 5, 6, 7, 8 | value for MonthOfYear     |
        | day 32                       | 2024, 3, 32, 6, 7, 8 | value for DayOfMonth      |
        | hour 25                      | 2024, 3, 5, 25, 7, 8 | value for HourOfDay       |
        | second 61                    | 2024, 3, 5, 6, 7, 61 | value for SecondOfMinute  |
        | 29 february of a common year | 2023, 2, 29, 0, 0, 0 | date 'February 29' as '2023' is not a leap year  |

    @sail-bug
    Scenario Outline: <case> is NULL when ANSI is off
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case       | args                  |
        | month 13   | 2024, 13, 5, 6, 7, 8  |
        | day 32     | 2024, 3, 32, 6, 7, 8  |
        | hour 25    | 2024, 3, 5, 25, 7, 8  |
        | second 61  | 2024, 3, 5, 6, 7, 61  |
        | 29 february of a common year | 2023, 2, 29, 0, 0, 0 |

  @function(nullability)
  Rule: ANSI decides the nullability of the result

    @sail-bug
    Scenario: with ANSI on and non-null arguments the result is not nullable
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp_ntz(2024, 3, 5, 6, 7, 8) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    Scenario: with ANSI off the result is nullable even from non-null arguments
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp_ntz(2024, 3, 5, 6, 7, 8) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """
