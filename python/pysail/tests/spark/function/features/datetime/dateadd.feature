Feature: dateadd function

  Scenario Outline: three-argument dateadd: <case>
    When query
      """
      SELECT dateadd(<unit>, <quantity>, <value>) AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case                     | unit | quantity | value                           | result              |
      | lowercase unit           | week | 2        | DATE '2024-01-01'               | 2024-01-15 00:00:00 |
      | uppercase negative unit  | WEEK | -1       | DATE '2024-01-15'               | 2024-01-08 00:00:00 |
      | timestamp sub-day unit   | HOUR | 2        | TIMESTAMP '2024-01-15 10:00:00' | 2024-01-15 12:00:00 |

  Scenario: three-argument dateadd supports column input
    When query
      """
      SELECT dateadd(DAY, id, DATE '2024-01-15') AS result FROM range(3)
      """
    Then query result
      | result              |
      | 2024-01-15 00:00:00 |
      | 2024-01-16 00:00:00 |
      | 2024-01-17 00:00:00 |

  Scenario: two-argument dateadd remains date arithmetic
    When query
      """
      SELECT dateadd(DATE '2024-01-15', 2) AS result
      """
    Then query result
      | result     |
      | 2024-01-17 |

  @function(nullability)
  Rule: Output schema

    Scenario Outline: three-argument <function> with a date returns a timestamp
      When query
        """
        SELECT <function>(WEEK, 1, DATE '2024-01-15')
        """
      Then query schema
        """
        root
         |-- timestampadd(WEEK, 1, DATE '2024-01-15'): timestamp (nullable = false)
        """

      Examples:
        | function |
        | dateadd  |
        | date_add |

    Scenario: two-argument dateadd returns a date
      When query
        """
        SELECT dateadd(DATE '2024-01-15', 1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

  # Spark 4.2.0 datetimeExpressions.scala: DateAdd/DateSub take (DATE, INT|SMALLINT|TINYINT) and
  # evaluate `start + days` on the raw Int day count, with no Math.addExact and no ANSI gate.
  Rule: Two-argument date_add and date_sub over the day count

    Scenario Outline: date_add edge: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | expr                                                | result       |
        | past the maximum literal date          | date_add(DATE '9999-12-31', 1)                      | +10000-01-01 |
        | before the minimum literal date        | date_sub(DATE '0001-01-01', 1)                      | 0000-12-31   |
        | TINYINT delta onto a leap day          | date_add(DATE '2024-02-28', CAST(1 AS TINYINT))     | 2024-02-29   |
        | negative SMALLINT delta                | date_add(DATE '2024-02-28', CAST(-1000 AS SMALLINT)) | 2021-06-03   |
        | SMALLINT literal suffix on date_sub    | date_sub(DATE '2024-03-01', 1S)                     | 2024-02-29   |
        | NULL day count                         | date_add(DATE '2024-02-28', CAST(NULL AS INT))      | NULL         |
        | NULL start date                        | date_add(CAST(NULL AS DATE), 1)                     | NULL         |

    Scenario: date_add and date_sub read each row's own date and day count
      When query
        """
        SELECT date_add(d, n) AS added, date_sub(d, n) AS subtracted
        FROM VALUES (DATE '2024-02-28', 1), (DATE '2023-02-28', 1), (DATE '2024-12-31', -366),
          (CAST(NULL AS DATE), 5), (DATE '2000-01-01', CAST(NULL AS INT)) AS t(d, n)
        """
      Then query result
        | added      | subtracted |
        | 2024-02-29 | 2024-02-27 |
        | 2023-03-01 | 2023-02-27 |
        | 2023-12-31 | 2026-01-01 |
        | NULL       | NULL       |
        | NULL       | NULL       |

    # The Int addition in DateAdd.nullSafeEval wraps: Spark returns a date near -5877587, it never raises.
    @sail-bug
    Scenario Outline: date arithmetic wraps the Int day count under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | ansi  | expr                                           | result         |
        | date_add of Int.MaxValue       | true  | date_add(DATE '2024-01-01', 2147483647)        | -5877587-06-21 |
        | date_add of Int.MaxValue       | false | date_add(DATE '2024-01-01', 2147483647)        | -5877587-06-21 |
        | date_sub of Int.MinValue       | true  | date_sub(DATE '2024-01-01', -2147483648)       | -5877587-06-22 |
        | two-argument dateadd           | true  | dateadd(DATE '2024-01-01', 2147483647)         | -5877587-06-21 |
        | date plus integer operator     | true  | DATE '2024-02-28' + 2147483647                 | -5877587-08-18 |

    @sail-bug
    Scenario: date_add wraps only the overflowing row of a column
      When query
        """
        SELECT date_add(d, n) AS result FROM VALUES (DATE '2024-01-01', 2147483647), (DATE '2024-01-01', 1) AS t(d, n)
        """
      Then query result ordered
        | result         |
        | -5877587-06-21 |
        | 2024-01-02     |

    # Spark's date range reaches +5881580-07-11; Sail rejects results past a narrower bound.
    @sail-bug
    Scenario: date_add reaches years beyond 9999 without overflowing
      When query
        """
        SELECT date_add(DATE '2024-01-01', 1000000000) AS result
        """
      Then query result
        | result         |
        | +2739931-01-04 |

    @sail-bug
    Scenario Outline: date_add rejects a non-integral day count: <case>
      When query
        """
        SELECT date_add(DATE '2024-02-28', <days>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | case    | days                     |
        | BIGINT  | 1L                       |
        | DECIMAL | 1.5                      |
        | DECIMAL | CAST(1 AS DECIMAL(3, 0)) |

    @sail-bug
    Scenario Outline: date_add with a malformed string day count under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT date_add(DATE '2024-02-28', 'x') AS result
        """
      Then query error <error>

      Examples:
        | ansi  | error                                    |
        | true  | CAST_INVALID_INPUT                   |
        | false | SECOND_FUNCTION_ARGUMENT_NOT_INTEGER |

  # Spark 4.2.0 datetimeExpressions.scala: AddMonths (DATE, INT) -> DateTimeUtils.dateAddMonths =
  # LocalDate.plusMonths, which clamps the day to the target month's length (not a last-day rule);
  # localDateToDays uses MathUtils.toIntExact, so an out-of-range result raises ARITHMETIC_OVERFLOW.
  Rule: add_months clamps the day of month

    Scenario Outline: add_months: <case>
      When query
        """
        SELECT add_months(<date>, <months>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | date                            | months | result       |
        | end of a 30-day month stays on day 30          | DATE '2016-04-30'               | 1      | 2016-05-30   |
        | Jan 31 clamps to Feb 29 in a leap year         | DATE '2024-01-31'               | 1      | 2024-02-29   |
        | Jan 31 clamps to Feb 28 in a common year       | DATE '2023-01-31'               | 1      | 2023-02-28   |
        | leap day minus a year clamps to Feb 28         | DATE '2024-02-29'               | -12    | 2023-02-28   |
        | Mar 31 minus a month clamps to Feb 29          | DATE '2024-03-31'               | -1     | 2024-02-29   |
        | leap day plus a month keeps day 29             | DATE '2024-02-29'               | 1      | 2024-03-29   |
        | past the maximum literal date                  | DATE '9999-12-31'               | 1      | +10000-01-31 |
        | before the minimum literal date                | DATE '0001-01-31'               | -1     | 0000-12-31   |
        | timestamp input is truncated to its date       | TIMESTAMP '2024-01-31 23:00:00' | 1      | 2024-02-29   |
        | BIGINT month count within INT range            | DATE '2024-01-15'               | 1L     | 2024-02-15   |
        | NULL month count                               | DATE '2024-01-15'               | NULL   | NULL         |
        | NULL date                                      | CAST(NULL AS DATE)              | 1      | NULL         |

    Scenario: add_months reads each row's own date and month count
      When query
        """
        SELECT add_months(d, n) AS result
        FROM VALUES (1, DATE '2016-04-30', 1), (2, DATE '2024-01-31', 1), (3, DATE '2023-01-31', 1),
          (4, DATE '2024-02-29', -12), (5, CAST(NULL AS DATE), 1), (6, DATE '2024-05-31', CAST(NULL AS INT)) AS t(i, d, n)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2016-05-30 |
        | 2024-02-29 |
        | 2023-02-28 |
        | 2023-02-28 |
        | NULL       |
        | NULL       |

    @sail-bug
    Scenario Outline: add_months overflow raises Spark's error class under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT add_months(DATE '2024-01-15', <months>) AS result
        """
      Then query error <error>

      Examples:
        | case                                  | ansi  | months      | error                   |
        | Int.MaxValue months                   | true  | 2147483647  | ARITHMETIC_OVERFLOW |
        | Int.MaxValue months                   | false | 2147483647  | ARITHMETIC_OVERFLOW |
        | BIGINT month count beyond INT (cast)  | true  | 2147483648L | CAST_OVERFLOW       |
        | BIGINT month count beyond INT (wraps) | false | 2147483648L | ARITHMETIC_OVERFLOW |

    @sail-bug
    Scenario Outline: add_months reaches years beyond 9999 without overflowing: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | expr                                         | result         |
        | add_months                  | add_months(DATE '2024-01-31', 12000000)      | +1002024-01-31 |
        | date plus year interval     | DATE '2024-01-31' + INTERVAL '1000000' YEAR  | +1002024-01-31 |

    @sail-bug
    Scenario: add_months truncates a decimal month count
      When query
        """
        SELECT add_months(DATE '2024-01-31', 1.5) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    @sail-bug
    Scenario: add_months returns NULL per row for a malformed string month count under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT add_months(d, n) AS result
        FROM VALUES (1, DATE '2024-01-31', '1'), (2, DATE '2024-03-31', 'x'), (3, DATE '2023-01-31', '-1') AS t(i, d, n)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2024-02-29 |
        | NULL       |
        | 2022-12-31 |

  # Spark 4.2.0: a DATE plus a day-time interval is resolved to a TIMESTAMP (the date is cast first),
  # while a DAY-only interval or a year-month interval keeps a DATE (DateAddInterval / DateAddYMInterval).
  Rule: DATE plus an interval

    Scenario Outline: date plus a date-granular interval stays a date: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | expr                                                | result       |
        | month interval clamps the day        | DATE '2024-01-31' + INTERVAL 1 MONTH                | 2024-02-29   |
        | month interval subtracted            | DATE '2024-03-31' - INTERVAL 1 MONTH                | 2024-02-29   |
        | year interval from a leap day        | DATE '2024-02-29' + INTERVAL '1' YEAR               | 2025-02-28   |
        | year-month interval                  | DATE '2024-01-31' + INTERVAL '1-1' YEAR TO MONTH    | 2025-02-28   |
        | day interval over a leap day         | DATE '2024-02-28' + INTERVAL '2' DAY                | 2024-03-01   |
        | day interval past the maximum date   | DATE '9999-12-31' + INTERVAL 1 DAY                  | +10000-01-01 |
        | day interval before the minimum date | DATE '0001-01-01' - INTERVAL 1 DAY                  | 0000-12-31   |

    Scenario: date plus a year-month interval column clamps each row
      When query
        """
        SELECT d + i AS result
        FROM VALUES (1, DATE '2024-01-31', INTERVAL '1' MONTH), (2, DATE '2023-01-31', INTERVAL '1' MONTH),
          (3, DATE '2024-02-29', INTERVAL '-12' MONTH) AS t(n, d, i)
        ORDER BY n
        """
      Then query result ordered
        | result     |
        | 2024-02-29 |
        | 2023-02-28 |
        | 2023-02-28 |

    @sail-bug
    Scenario Outline: date plus a day-time interval becomes a timestamp: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | expr                                                    | result              |
        | day to second interval       | DATE '2024-01-31' + INTERVAL '1 12:00:00' DAY TO SECOND | 2024-02-01 12:00:00 |
        | hour interval subtracted     | DATE '2024-01-31' - INTERVAL '12' HOUR                  | 2024-01-30 12:00:00 |
        | whole-day hour interval      | DATE '2024-01-31' + INTERVAL '24' HOUR                  | 2024-02-01 00:00:00 |

    @sail-bug
    Scenario: date plus a day-time interval column becomes a timestamp per row
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT d + INTERVAL '36' HOUR AS result FROM VALUES (1, DATE '2024-01-31'), (2, DATE '2024-02-28') AS t(i, d) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2024-02-01 12:00:00 |
        | 2024-02-29 12:00:00 |

    @sail-bug
    Scenario: date plus a day-time interval has a timestamp schema
      When query
        """
        SELECT DATE '2024-01-31' + INTERVAL '1 12:00:00' DAY TO SECOND AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    # DateAddInterval: under ANSI a calendar interval with a time part raises; under ANSI false the
    # date goes through a timestamp in the session zone and back, so the time part moves the date.
    @sail-bug
    Scenario Outline: date plus a calendar interval with a time part under ANSI true: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT DATE '2024-01-31' <op> <interval> AS result
        """
      Then query error \[INVALID_INTERVAL_WITH_MICROSECONDS_ADDITION\]

      Examples:
        | case                 | op | interval                               |
        | months, days, hours  | +  | make_interval(0, 1, 0, 1, 12, 0, 0)    |
        | one microsecond      | +  | make_interval(0, 0, 0, 0, 0, 0, 0.000001) |
        | hours subtracted     | -  | make_interval(0, 0, 0, 0, 12, 0, 0)    |

    Scenario: date plus a calendar interval with a whole-day time part under ANSI false
      Given config spark.sql.ansi.enabled = false
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT DATE '2024-01-31' + make_interval(0, 1, 0, 1, 36, 0, 0) AS result
        """
      Then query result
        | result     |
        | 2024-03-02 |

    @sail-bug
    Scenario: date minus a calendar interval with a time part moves the date under ANSI false
      Given config spark.sql.ansi.enabled = false
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT DATE '2024-01-31' - make_interval(0, 0, 0, 0, 12, 0, 0) AS result
        """
      Then query result
        | result     |
        | 2024-01-30 |

  @function(nullability)
  Rule: Output schema through implicit casts

    # A string argument is cast (to DATE or INT) and the cast can fail, so Spark marks the result nullable.
    @sail-bug
    Scenario Outline: a string argument makes the result nullable: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case                         | expr                                |
        | date_add string date         | date_add('2024-02-28', 1)           |
        | date_add string day count    | date_add(DATE '2024-02-28', '1')    |
        | add_months string date       | add_months('2024-01-31', 1)         |
        | add_months string month      | add_months(DATE '2024-01-31', '1')  |

  # Spark 4.2.0: DATE + day-time interval casts the date to a TIMESTAMP at local midnight of the
  # session zone; DateAddInterval under ANSI false goes through daysToMicros/microsToDays in that zone.
  Rule: DATE plus a sub-day interval in the session time zone

    @sail-bug
    Scenario Outline: date plus a day-time interval starts at local midnight: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | zone                | expr                                                    | result              |
        | LA 12 hours on the gap day        | America/Los_Angeles | DATE '2024-03-10' + INTERVAL '12' HOUR                  | 2024-03-10 13:00:00 |
        | LA 36 hours over the gap          | America/Los_Angeles | DATE '2024-03-09' + INTERVAL '36' HOUR                  | 2024-03-10 13:00:00 |
        | Kolkata day and twelve hours      | Asia/Kolkata        | DATE '2024-01-31' + INTERVAL '1 12:00:00' DAY TO SECOND | 2024-02-01 12:00:00 |

    @sail-bug
    Scenario Outline: date plus a calendar interval with hours under ANSI false follows the zone's day length: <case>
      Given config spark.sql.ansi.enabled = false
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT DATE '<date>' <op> make_interval(0, 0, 0, 0, <hours>, <minutes>, 0) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | zone                | date       | op | hours | minutes | result     |
        | LA 23.5 hours fill the short day  | America/Los_Angeles | 2024-03-10 | +  | 23    | 30      | 2024-03-11 |
        | LA 24.5 hours within the long day | America/Los_Angeles | 2024-11-03 | +  | 24    | 30      | 2024-11-03 |
        | Kolkata 12 hours back             | Asia/Kolkata        | 2024-01-31 | -  | 12    | 0       | 2024-01-30 |

    Scenario Outline: date plus a date-granular interval ignores the session zone: <case>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT DATE '2024-03-10' + <interval> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | interval           | result     |
        | a day over the gap | INTERVAL '1' DAY   | 2024-03-11 |
        | a month            | INTERVAL '1' MONTH | 2024-04-10 |
