Feature: months_between output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between('1997-02-28 10:30:00', '1996-10-30') AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to months_between yields the schema Spark declares
      When query
        """
        SELECT months_between(CAST(id AS STRING), '1996-10-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to months_between stays nullable
      When query
        """
        SELECT months_between(c, '1996-10-30') AS result FROM VALUES ('1997-02-28 10:30:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  # Spark 4.2.0 DateTimeUtils.monthsBetween: whole months when both days of month are equal or both
  # are the last day of their month (time of day ignored); otherwise the day and second difference
  # is divided by 31 days, and rounded to 8 digits unless roundOff is false.
  Rule: months_between end-of-month rule and 31-day fraction

    Scenario Outline: months_between: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT months_between(<a>, <b>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | a                                      | b                                      | result              |
        | both last days, leap February                  | DATE '2024-03-31'                      | DATE '2024-02-29'                      | 1.0                 |
        | both last days, from January                   | DATE '2024-02-29'                      | DATE '2024-01-31'                      | 1.0                 |
        | both last days, common February                | DATE '2023-02-28'                      | DATE '2023-01-31'                      | 1.0                 |
        | both last days, negative                       | DATE '2024-01-31'                      | DATE '2024-02-29'                      | -1.0                |
        | only the second is a last day                  | DATE '2024-03-30'                      | DATE '2024-02-29'                      | 1.03225806          |
        | Feb 28 of a leap year is not a last day        | DATE '2024-02-28'                      | DATE '2024-01-31'                      | 0.90322581          |
        | same day of month ignores the time             | TIMESTAMP '2024-03-15 12:00:00'        | TIMESTAMP '2024-02-15 00:00:00'        | 1.0                 |
        | last days ignore the time                      | TIMESTAMP '2024-03-31 23:00:00'        | TIMESTAMP '2024-02-29 01:00:00'        | 1.0                 |
        | time of day enters the fraction                | TIMESTAMP '2024-03-01 12:00:00'        | TIMESTAMP '2024-02-02 00:00:00'        | 0.98387097          |
        | one second short of the end of month rule      | TIMESTAMP '2024-03-01 00:00:00'        | TIMESTAMP '2024-01-31 23:59:59'        | 1.00000037          |
        | negative fraction                              | DATE '2024-02-01'                      | DATE '2024-03-10'                      | -1.29032258         |
        | roundOff false keeps full precision            | DATE '2024-03-10'                      | DATE '2024-02-01', false               | 1.2903225806451613  |
        | roundOff false with time of day                | TIMESTAMP '2024-03-01 12:00:00'        | TIMESTAMP '2024-02-02 00:00:00', false | 0.9838709677419355  |
        | TIMESTAMP_NTZ arguments                        | TIMESTAMP_NTZ '2024-03-01 00:00:00'    | TIMESTAMP_NTZ '2024-01-31 12:00:00'    | 1.01612903          |
        | past the maximum literal date                  | DATE '+10000-01-31'                    | DATE '9999-12-31'                      | 1.0                 |
        | NULL argument                                  | DATE '2024-01-01'                      | NULL                                   | NULL                |

    Scenario: months_between reads each row's own dates and roundOff
      When query
        """
        SELECT months_between(a, b) AS rounded, months_between(a, b, false) AS exact
        FROM VALUES (1, DATE '2024-03-31', DATE '2024-02-29'), (2, DATE '2024-03-30', DATE '2024-02-29'),
          (3, DATE '2024-02-01', DATE '2024-03-10'), (4, CAST(NULL AS DATE), DATE '2024-01-01') AS t(i, a, b)
        ORDER BY i
        """
      Then query result ordered
        | rounded     | exact               |
        | 1.0         | 1.0                 |
        | 1.03225806  | 1.032258064516129   |
        | -1.29032258 | -1.2903225806451613 |
        | NULL        | NULL                |

  # Spark 4.2.0 DateTimeUtils.monthsBetween takes the day of month and the seconds of day in the
  # session zone (microsToDays / daysToMicros with zoneId).
  Rule: months_between in the session time zone

    Scenario Outline: months_between reads local dates and times: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT months_between(<a>, <b>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | zone                | a                                   | b                                          | result              |
        | Kolkata both local last days          | Asia/Kolkata        | TIMESTAMP '2024-03-31 23:00:00'     | TIMESTAMP '2024-02-29 01:00:00'            | 1.0                 |
        | Kolkata early local hour, last days   | Asia/Kolkata        | TIMESTAMP '2024-03-31 02:00:00'     | TIMESTAMP '2024-02-29 02:00:00'            | 1.0                 |
        | Kolkata local time in the fraction    | Asia/Kolkata        | TIMESTAMP '2024-03-01 12:00:00'     | TIMESTAMP '2024-02-02 00:00:00'            | 0.98387097          |
        | Kolkata NTZ is not shifted            | Asia/Kolkata        | TIMESTAMP_NTZ '2024-03-01 12:00:00' | TIMESTAMP_NTZ '2024-02-02 00:00:00'        | 0.98387097          |
        | Pago Pago both local last days        | Pacific/Pago_Pago   | TIMESTAMP '2024-03-31 23:00:00'     | TIMESTAMP '2024-02-29 01:00:00'            | 1.0                 |
        | Pago Pago late local hour             | Pacific/Pago_Pago   | TIMESTAMP '2024-03-31 20:00:00'     | TIMESTAMP '2024-02-15 20:00:00'            | 1.51612903          |
        | Pago Pago roundOff false              | Pacific/Pago_Pago   | TIMESTAMP '2024-03-01 12:00:00'     | TIMESTAMP '2024-02-02 00:00:00', false     | 0.9838709677419355  |
        | LA across the gap                     | America/Los_Angeles | TIMESTAMP '2024-03-11 00:00:00'     | TIMESTAMP '2024-03-09 12:00:00', false     | 0.04838709677419355 |
        | LA across the overlap                 | America/Los_Angeles | TIMESTAMP '2024-11-04 00:00:00'     | TIMESTAMP '2024-11-02 12:00:00', false     | 0.04838709677419355 |
        | Chatham both local last days          | Pacific/Chatham     | TIMESTAMP '2024-04-30 23:00:00'     | TIMESTAMP '2024-03-31 01:00:00'            | 1.0                 |
        | Chatham across its fall-back          | Pacific/Chatham     | TIMESTAMP '2024-04-07 12:00:00'     | TIMESTAMP '2024-03-06 12:00:00', false     | 1.0336021505376345  |

    Scenario: months_between reads each row in Kolkata local time
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SELECT months_between(CAST(a AS TIMESTAMP), CAST(b AS TIMESTAMP)) AS result
        FROM VALUES (1, '2024-03-31 23:00:00', '2024-02-29 01:00:00'), (2, '2024-03-01 12:00:00', '2024-02-02 00:00:00') AS t(i, a, b)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1.0        |
        | 0.98387097 |
