# Moved from features/timestampdiff.feature by the datetime/ layout reorganisation.
Feature: timestampdiff calendar units

  Rule: timestampdiff uses calendar-aware month, quarter, and year units

    Scenario Outline: Calendar unit: <case>
      When query
        """
        SELECT timestampdiff(<unit>, TIMESTAMP '<start>', TIMESTAMP '<end>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                          | unit    | start               | end                 | result |
        | timestampdiff MONTH counts a leap February calendar month                     | MONTH   | 2024-02-01 00:00:00 | 2024-03-01 00:00:00 | 1      |
        | timestampdiff MONTH truncates incomplete trailing months                      | MONTH   | 2024-01-31 10:00:00 | 2024-02-29 09:59:59 | 0      |
        | timestampdiff MONTH includes complete trailing month at matching day and time | MONTH   | 2024-02-29 10:00:00 | 2024-03-29 10:00:00 | 1      |
        | timestampdiff QUARTER counts calendar quarters                                | QUARTER | 2024-01-01 00:00:00 | 2024-04-01 00:00:00 | 1      |
        | timestampdiff YEAR counts completed calendar years                            | YEAR    | 2020-02-29 12:00:00 | 2021-02-28 11:59:59 | 0      |
        | timestampdiff MONTH truncates negative intervals toward zero                  | MONTH   | 2024-03-01 00:00:00 | 2024-02-01 00:00:01 | 0      |

    Scenario: date_diff and datediff use the same calendar month behavior
      When query
        """
        SELECT
          date_diff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS date_diff_result,
          datediff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS datediff_result
        """
      Then query result
        | date_diff_result | datediff_result |
        | 1                | 1               |

    Scenario: timestampdiff MONTH counts a leap February calendar month
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff MONTH truncates incomplete trailing months
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-01-31 10:00:00', TIMESTAMP '2024-02-29 09:59:59') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: timestampdiff MONTH includes complete trailing month at matching day and time
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-02-29 10:00:00', TIMESTAMP '2024-03-29 10:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff QUARTER counts calendar quarters
      When query
      """
      SELECT timestampdiff(QUARTER, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-04-01 00:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff YEAR counts completed calendar years
      When query
      """
      SELECT timestampdiff(YEAR, TIMESTAMP '2020-02-29 12:00:00', TIMESTAMP '2021-02-28 11:59:59') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: timestampdiff MONTH truncates negative intervals toward zero
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-02-01 00:00:01') AS result
      """
      Then query result
      | result |
      | 0      |

  # Spark 4.2.0 DateTimeUtils.timestampDiff: ChronoUnit.<unit>.between on the two LocalDateTimes,
  # which counts only COMPLETE units and truncates toward zero; QUARTER is MONTHS.between / 3.
  Rule: timestampdiff counts complete units for every unit

    Scenario Outline: timestampdiff truncation: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampdiff(<unit>, <start>, <end>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | unit    | start                              | end                                | result       |
        | incomplete trailing second              | SECOND  | TIMESTAMP '2024-01-01 12:30:30.5'  | TIMESTAMP '2024-01-01 12:30:32.4'  | 1            |
        | negative incomplete second              | SECOND  | TIMESTAMP '2024-01-01 00:00:01.5'  | TIMESTAMP '2024-01-01 00:00:00'    | -1           |
        | incomplete trailing minute              | MINUTE  | TIMESTAMP '2024-01-01 12:30:30'    | TIMESTAMP '2024-01-01 12:32:29'    | 1            |
        | incomplete trailing hour                | HOUR    | TIMESTAMP '2024-01-01 12:30:00'    | TIMESTAMP '2024-01-01 14:29:59'    | 1            |
        | negative hours across a leap day        | HOUR    | TIMESTAMP '2024-03-01 00:00:00'    | TIMESTAMP '2024-02-28 01:00:00'    | -47          |
        | complete days across a leap day         | DAY     | TIMESTAMP '2024-02-28 12:00:00'    | TIMESTAMP '2024-03-01 12:00:00'    | 2            |
        | incomplete trailing week                | WEEK    | TIMESTAMP '2024-01-01 12:00:00'    | TIMESTAMP '2024-01-15 11:59:59'    | 1            |
        | negative incomplete week                | WEEK    | TIMESTAMP '2024-01-15 00:00:00'    | TIMESTAMP '2024-01-01 00:00:01'    | -1           |
        | month short by one second               | MONTH   | TIMESTAMP '2024-01-15 12:00:00'    | TIMESTAMP '2024-02-15 11:59:59'    | 0            |
        | backwards month ending on an earlier day | MONTH  | TIMESTAMP '2024-03-31 10:00:00'    | TIMESTAMP '2024-02-29 09:00:00'    | -1           |
        | quarter from month end to month end     | QUARTER | TIMESTAMP '2024-01-31 12:00:00'    | TIMESTAMP '2024-04-30 12:00:00'    | 0            |
        | quarter short by an hour                | QUARTER | TIMESTAMP '2024-01-15 12:00:00'    | TIMESTAMP '2025-01-15 11:00:00'    | 3            |
        | negative quarter                        | QUARTER | TIMESTAMP '2024-05-15 00:00:00'    | TIMESTAMP '2024-01-16 00:00:00'    | -1           |
        | leap day to the next Feb 28             | YEAR    | TIMESTAMP '2024-02-29 12:00:00'    | TIMESTAMP '2025-02-28 12:00:00'    | 0            |
        | negative years one second short         | YEAR    | TIMESTAMP '2024-02-29 12:00:00'    | TIMESTAMP '2020-02-29 12:00:01'    | -3           |
        | years over the whole literal range      | YEAR    | TIMESTAMP '0001-01-01 00:00:00'    | TIMESTAMP '9999-12-31 00:00:00'    | 9998         |
        | seconds over the whole literal range    | SECOND  | TIMESTAMP '0001-01-01 00:00:00'    | TIMESTAMP '9999-12-31 00:00:00'    | 315537811200 |
        | hours past the maximum literal date     | HOUR    | TIMESTAMP '0001-01-01 00:00:00'    | TIMESTAMP '+10000-01-01 00:00:00'  | 87649416     |
        | DATE arguments are promoted             | DAY     | DATE '2024-01-01'                  | DATE '2024-03-01'                  | 60           |
        | TIMESTAMP_NTZ arguments                 | HOUR    | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP_NTZ '2024-01-02 01:00:00' | 25          |
        | NULL start                              | DAY     | CAST(NULL AS TIMESTAMP)            | TIMESTAMP '2024-01-01 00:00:00'    | NULL         |

    # ChronoUnit.DAYS.between on LocalDateTime needs a full 24 hours; it is not a calendar-date difference.
    @sail-bug
    Scenario Outline: timestampdiff DAY needs a complete 24 hours: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <function>(DAY, TIMESTAMP '<start>', TIMESTAMP '<end>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | function      | start               | end                 | result |
        | one second short of a day          | timestampdiff | 2024-01-01 12:00:00 | 2024-01-02 11:59:59 | 0      |
        | one second short of two days       | timestampdiff | 2024-01-01 12:00:00 | 2024-01-03 11:59:59 | 1      |
        | short of two days over a leap day  | timestampdiff | 2024-02-28 12:00:00 | 2024-03-01 11:59:59 | 1      |
        | negative, short of two days        | timestampdiff | 2024-03-01 11:59:59 | 2024-02-28 12:00:00 | -1     |
        | three-argument datediff alias      | datediff      | 2024-01-01 12:00:00 | 2024-01-03 11:59:59 | 1      |
        | three-argument date_diff alias     | date_diff     | 2024-01-01 12:00:00 | 2024-01-03 11:59:59 | 1      |

    @sail-bug
    Scenario: timestampdiff DAY needs a complete 24 hours in every row
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampdiff(DAY, s, e) AS result
        FROM VALUES (1, TIMESTAMP '2024-01-01 12:00:00', TIMESTAMP '2024-01-03 11:59:59'),
          (2, TIMESTAMP '2024-01-01 12:00:00', TIMESTAMP '2024-01-03 12:00:00'),
          (3, TIMESTAMP '2024-01-03 11:59:59', TIMESTAMP '2024-01-01 12:00:00') AS t(i, s, e)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | -1     |

    # LocalDateTime.until(MONTHS) moves the end date one day forward when going backwards and the
    # end time-of-day is later than the start's, so Mar 30 10:00 -> Feb 29 12:00 is not a full month.
    @sail-bug
    Scenario Outline: timestampdiff MONTH backwards honours the time of day: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampdiff(MONTH, TIMESTAMP '<start>', TIMESTAMP '<end>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | start               | end                 | result |
        | from Mar 30 to a later hour  | 2024-03-30 10:00:00 | 2024-02-29 12:00:00 | 0      |
        | from Mar 31 to a later hour  | 2024-03-31 10:00:00 | 2024-02-29 12:00:00 | 0      |

    @sail-bug
    Scenario Outline: timestampdiff supports sub-second units: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <function>(<unit>, TIMESTAMP '<start>', TIMESTAMP '<end>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | function      | unit        | start                      | end                        | result             |
        | microseconds                 | timestampdiff | MICROSECOND | 2024-01-01 00:00:00        | 2024-01-01 00:00:01.000001 | 1000001            |
        | milliseconds truncate        | timestampdiff | MILLISECOND | 2024-01-01 00:00:00        | 2024-01-01 00:00:01.0019   | 1001               |
        | negative milliseconds        | timestampdiff | MILLISECOND | 2024-01-01 00:00:00.0019   | 2024-01-01 00:00:00        | -1                 |
        | microseconds over the range  | timestampdiff | MICROSECOND | 0001-01-01 00:00:00        | 9999-12-31 23:59:59.999999 | 315537897599999999 |
        | three-argument datediff      | datediff      | MICROSECOND | 2024-01-01 00:00:00        | 2024-01-01 00:00:01.000001 | 1000001            |

    @sail-bug
    Scenario: timestampdiff MICROSECOND reads each row
      When query
        """
        SELECT timestampdiff(MICROSECOND, s, e) AS result
        FROM VALUES (1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00.000007'),
          (2, TIMESTAMP '2024-01-01 00:00:01', TIMESTAMP '2024-01-01 00:00:00') AS t(i, s, e)
        ORDER BY i
        """
      Then query result ordered
        | result   |
        | 7        |
        | -1000000 |

    @sail-bug
    Scenario: timestampdiff rejects DAYOFYEAR with Spark's error class
      When query
        """
        SELECT timestampdiff(DAYOFYEAR, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-03 00:00:00') AS result
        """
      Then query error \[INVALID_PARAMETER_VALUE\.DATETIME_UNIT\]

  # Spark 4.2.0 DateTimeUtils.timestampDiff converts both instants to LocalDateTime in the session
  # zone first, so every unit (even HOUR, MINUTE, SECOND) measures local wall-clock distance, not
  # elapsed time; TIMESTAMP_NTZ is read in UTC.
  Rule: timestampdiff in the session time zone

    @sail-bug
    Scenario Outline: timestampdiff measures local wall-clock distance: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT timestampdiff(<unit>, <start>, <end>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | zone                | unit   | start                           | end                                                  | result |
        | LA hours over the gap                  | America/Los_Angeles | HOUR   | TIMESTAMP '2024-03-09 12:00:00' | TIMESTAMP '2024-03-10 12:00:00'                      | 24     |
        | LA hours over the overlap              | America/Los_Angeles | HOUR   | TIMESTAMP '2024-11-02 12:00:00' | TIMESTAMP '2024-11-03 12:00:00'                      | 24     |
        | LA minutes over the gap                | America/Los_Angeles | MINUTE | TIMESTAMP '2024-03-10 01:00:00' | TIMESTAMP '2024-03-10 03:00:00'                      | 120    |
        | LA seconds over the overlap            | America/Los_Angeles | SECOND | TIMESTAMP '2024-11-03 00:00:00' | TIMESTAMP '2024-11-03 03:00:00'                      | 10800  |
        | LA elapsed hour inside the overlap     | America/Los_Angeles | HOUR   | TIMESTAMP '2024-11-03 01:30:00' | TIMESTAMP '2024-11-03 01:30:00' + INTERVAL '1' HOUR  | 0      |
        | LA week over the gap                   | America/Los_Angeles | WEEK   | TIMESTAMP '2024-03-03 12:00:00' | TIMESTAMP '2024-03-10 12:00:00'                      | 1      |
        | LA month over the gap                  | America/Los_Angeles | MONTH  | TIMESTAMP '2024-02-10 12:00:00' | TIMESTAMP '2024-03-10 12:00:00'                      | 1      |
        | LA day short by half an hour           | America/Los_Angeles | DAY    | TIMESTAMP '2024-03-09 12:30:00' | TIMESTAMP '2024-03-10 12:00:00'                      | 0      |
        | Chatham hours over its fall-back       | Pacific/Chatham     | HOUR   | TIMESTAMP '2024-04-06 12:00:00' | TIMESTAMP '2024-04-07 12:00:00'                      | 24     |
        | Kolkata year short by a local hour     | Asia/Kolkata        | YEAR   | TIMESTAMP '2023-03-01 02:00:00' | TIMESTAMP '2024-03-01 01:00:00'                      | 0      |
        | Pago Pago month from local Jan 31      | Pacific/Pago_Pago   | MONTH  | TIMESTAMP '2024-01-31 20:00:00' | TIMESTAMP '2024-02-29 20:00:00'                      | 0      |
        | Pago Pago day short by a second        | Pacific/Pago_Pago   | DAY    | TIMESTAMP '2024-02-28 20:00:00' | TIMESTAMP '2024-03-01 19:59:59'                      | 1      |

    Scenario Outline: timestampdiff outside UTC without a divergence: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT timestampdiff(<unit>, <start>, <end>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | zone                | unit    | start                               | end                                 | result |
        | LA whole day over the gap           | America/Los_Angeles | DAY     | TIMESTAMP '2024-03-09 12:00:00'     | TIMESTAMP '2024-03-10 12:00:00'     | 1      |
        | LA whole day over the overlap       | America/Los_Angeles | DAY     | TIMESTAMP '2024-11-02 12:00:00'     | TIMESTAMP '2024-11-03 12:00:00'     | 1      |
        | LA NTZ hours over the gap           | America/Los_Angeles | HOUR    | TIMESTAMP_NTZ '2024-03-09 12:00:00' | TIMESTAMP_NTZ '2024-03-10 12:00:00' | 24     |
        | LA NTZ day over the gap             | America/Los_Angeles | DAY     | TIMESTAMP_NTZ '2024-03-09 12:00:00' | TIMESTAMP_NTZ '2024-03-10 12:00:00' | 1      |
        | Kolkata whole day                   | Asia/Kolkata        | DAY     | TIMESTAMP '2024-01-01 00:00:00'     | TIMESTAMP '2024-01-02 00:00:00'     | 1      |
        | Kolkata month from a late Jan 31    | Asia/Kolkata        | MONTH   | TIMESTAMP '2024-01-31 23:00:00'     | TIMESTAMP '2024-02-29 23:00:00'     | 0      |
        | Chatham whole day over its fall-back | Pacific/Chatham    | DAY     | TIMESTAMP '2024-04-06 12:00:00'     | TIMESTAMP '2024-04-07 12:00:00'     | 1      |
        | Chatham month end to month end      | Pacific/Chatham     | MONTH   | TIMESTAMP '2024-03-31 00:30:00'     | TIMESTAMP '2024-04-30 00:30:00'     | 0      |
        | Pago Pago quarter short by an hour  | Pacific/Pago_Pago   | QUARTER | TIMESTAMP '2024-01-01 20:00:00'     | TIMESTAMP '2024-04-01 19:00:00'     | 0      |
        | Pago Pago DATE days                 | Pacific/Pago_Pago   | DAY     | DATE '2024-02-28'                   | DATE '2024-03-01'                   | 2      |
        | Pago Pago DATE months               | Pacific/Pago_Pago   | MONTH   | DATE '2024-01-31'                   | DATE '2024-02-29'                   | 0      |

    @sail-bug
    Scenario: timestampdiff HOUR measures each row's local wall clock in Los Angeles
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT timestampdiff(HOUR, s, e) AS hours, timestampdiff(DAY, s, e) AS days
        FROM VALUES (1, TIMESTAMP '2024-03-09 12:00:00', TIMESTAMP '2024-03-10 12:00:00'),
          (2, TIMESTAMP '2024-11-02 12:00:00', TIMESTAMP '2024-11-03 12:00:00'),
          (3, TIMESTAMP '2024-06-01 12:00:00', TIMESTAMP '2024-06-02 12:00:00') AS t(i, s, e)
        ORDER BY i
        """
      Then query result ordered
        | hours | days |
        | 24    | 1    |
        | 24    | 1    |
        | 24    | 1    |
