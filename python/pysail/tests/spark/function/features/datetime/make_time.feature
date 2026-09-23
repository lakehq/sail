@spark-4.2
Feature: make_time

  # `MakeTime` (`timeExpressions.scala:553`) is the odd one of the `make_*` family: it carries NO
  # `failOnError`, so its validation is NOT ANSI-gated -- the usage doc says "For invalid inputs it
  # will throw an error" full stop, and it does so with ANSI on and with ANSI off alike. Its
  # siblings `make_date` and `make_timestamp` return NULL when ANSI is off. Running the same case
  # under both modes is what tells the two rules apart.
  # Its third argument is declared `DecimalType(16, 6)` on purpose, "to avoid loosing precision of
  # microseconds", and the result is always `TimeType(MICROS_PRECISION)` -- time(6) -- however few
  # digits the seconds carry.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: the time it builds

    Scenario Outline: make_time of <case>
      Given config spark.sql.timeType.enabled = true
      When query template
        """
        SELECT make_time(<args>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                        | args             | result          |
        | a plain time                | 6, 7, 8          | 06:07:08        |
        | the documented example      | 6, 30, 45.887    | 06:30:45.887000 |
        | midnight                    | 0, 0, 0          | 00:00:00        |
        | the last microsecond of day | 23, 59, 59.999999| 23:59:59.999999 |
        | a whole second              | 6, 7, 8.000000   | 06:07:08        |

    # The declared type does not follow the argument: a whole second still gives time(6).
    Scenario: the result is always time(6)
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_time(6, 7, 8) AS result
        """
      Then query schema
        """
        root
         |-- result: time(6) (nullable = true)
        """

    Scenario Outline: a NULL argument gives NULL: <case>
      Given config spark.sql.timeType.enabled = true
      When query template
        """
        SELECT make_time(<args>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case   | args                                    |
        | hour   | CAST(NULL AS INT), 30, 0                |
        | minute | 6, CAST(NULL AS INT), 0                 |
        | second | 6, 30, CAST(NULL AS DECIMAL(16,6))      |

    # Through a column, so the row path runs and not only constant folding.
    Scenario: make_time over a column
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_time(h, 0, 0) AS result FROM VALUES (6), (23), (CAST(NULL AS INT)) AS t(h)
        """
      Then query result collected ordered
        | result   |
        | 06:00:00 |
        | 23:00:00 |
        | NULL     |

  # This is the branch that separates make_time from make_date and make_timestamp: they turn an
  # out-of-range field into NULL when ANSI is off, and make_time still raises.
  # The assertion is the BODY of the message, not the error class: Spark prefixes it with
  # `[DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION]` and Sail with `make_time:`, but both spell
  # out the same field and range, so this pins the failure to the right field without pinning the
  # wording of either engine.
  Rule: an out-of-range field raises whatever ANSI says

    Scenario Outline: <case> with ANSI on
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT make_time(<args>) AS result
        """
      Then query error Invalid <detail>

      Examples:
        | case       | args      | detail                   |
        | hour 24    | 24, 0, 0  | value for HourOfDay      |
        | minute 60  | 0, 60, 0  | value for MinuteOfHour   |
        | second 60  | 0, 0, 60  | value for SecondOfMinute |
        | a negative hour | -1, 0, 0 | value for HourOfDay      |

    Scenario Outline: <case> with ANSI off
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT make_time(<args>) AS result
        """
      Then query error Invalid <detail>

      Examples:
        | case       | args      | detail                   |
        | hour 24    | 24, 0, 0  | value for HourOfDay      |
        | minute 60  | 0, 60, 0  | value for MinuteOfHour   |
        | second 60  | 0, 0, 60  | value for SecondOfMinute |
        | a negative hour | -1, 0, 0 | value for HourOfDay      |
