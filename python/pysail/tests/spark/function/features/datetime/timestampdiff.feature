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
        | timestampdiff MONTH accepts ordinary years outside nanosecond range           | MONTH   | 1000-01-15 12:00:00 | 1000-03-15 11:59:59 | 1      |
        | timestampdiff MONTH floors pre-epoch time-of-day correctly                     | MONTH   | 1969-11-30 23:59:59 | 1969-12-30 23:59:58 | 0      |
        | timestampdiff MONTH keeps complete pre-epoch months                            | MONTH   | 1969-11-30 23:59:59 | 1969-12-30 23:59:59 | 1      |

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

  Rule: Calendar difference columns preserve range and nullability

    Scenario: calendar month differences accept wide timestamp columns and NULL
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampdiff(MONTH, start_ts, end_ts) AS result
        FROM VALUES
          (1, TIMESTAMP '1000-01-15 12:00:00', TIMESTAMP '1000-03-15 11:59:59'),
          (2, TIMESTAMP '1000-03-15 11:59:59', TIMESTAMP '1000-01-15 12:00:00'),
          (3, CAST(NULL AS TIMESTAMP), TIMESTAMP '1000-03-15 11:59:59') AS t(id, start_ts, end_ts)
        ORDER BY id
        """
      Then query result ordered
        | result |
        | 1      |
        | -1     |
        | NULL   |

    Scenario: non-null timestamp operands produce a non-null calendar difference
      When query
        """
        SELECT timestampdiff(MONTH, TIMESTAMP '2024-01-15 12:00:00', TIMESTAMP '2024-03-15 11:59:59') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

  Rule: Timestamp differences use recurring timezone rules for local wall time

    # Spark's DateTimeUtils.timestampDiff and subtractTimestamps compare local datetimes.
    # Future timestamps must keep recurring DST rules, including southern-hemisphere rules.
    Scenario Outline: Future timestamp differences in <timezone>: <case>
      Given config spark.sql.session.timeZone = <timezone>
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT timestampdiff(SECOND, TIMESTAMP '<start>', TIMESTAMP '<end>') AS seconds,
               timestampdiff(DAY, TIMESTAMP '<start>', TIMESTAMP '<end>') AS days,
               CAST(TIMESTAMP '<end>' - TIMESTAMP '<start>' AS STRING) AS duration
        """
      Then query result
        | seconds   | days   | duration                              |
        | <seconds> | <days> | INTERVAL '<duration>' DAY TO SECOND |

      Examples:
        | timezone            | case                   | start                     | end                       | seconds     | days   | duration          |
        | America/Los_Angeles | same summer offset     | 2024-07-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 2398291200  | 27758  | 27758 00:00:00    |
        | America/Los_Angeles | reverse summer offset  | 2100-07-01 00:00:00+00:00 | 2024-07-01 00:00:00+00:00 | -2398291200 | -27758 | -27758 00:00:00   |
        | America/New_York    | same summer offset     | 2024-07-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 2398291200  | 27758  | 27758 00:00:00    |
        | Australia/Sydney    | same winter offset     | 2024-07-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 2398291200  | 27758  | 27758 00:00:00    |
        | America/Los_Angeles | winter to summer       | 2100-01-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 15642000    | 181    | 181 01:00:00      |
        | Australia/Sydney    | summer to winter       | 2100-01-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 15634800    | 180    | 180 23:00:00      |
        | America/Los_Angeles | future leap century    | 2400-01-01 00:00:00+00:00 | 2400-07-01 00:00:00+00:00 | 15728400    | 182    | 182 01:00:00      |
        | America/Los_Angeles | beyond Jiff calendar   | +10000-01-01 00:00:00+00:00 | +10000-07-01 00:00:00+00:00 | 15728400    | 182    | 182 01:00:00      |
        | UTC                 | no timezone adjustment | 2100-01-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 15638400    | 181    | 181 00:00:00      |
        | +05:30              | fixed offset           | 2100-01-01 00:00:00+00:00 | 2100-07-01 00:00:00+00:00 | 15638400    | 181    | 181 00:00:00      |

    # Epoch constructors cover supported timestamp values beyond Chrono's calendar range.
    Scenario: Future timestamp differences beyond the Chrono calendar retain DST
      Given config spark.sql.session.timeZone = America/Los_Angeles
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT timestampdiff(SECOND, start_ts, end_ts) AS seconds,
               timestampdiff(DAY, start_ts, end_ts) AS days,
               CAST(end_ts - start_ts AS STRING) AS duration
        FROM (
          SELECT timestamp_micros(8931564144000000000L) AS start_ts,
                 timestamp_micros(8931579782400000000L) AS end_ts
        )
        """
      Then query result
        | seconds  | days | duration                            |
        | 15642000 | 181  | INTERVAL '181 01:00:00' DAY TO SECOND |

    Scenario Outline: Future timestamp differences preserve microseconds at the Jiff boundary
      Given config spark.sql.session.timeZone = America/Los_Angeles
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT timestampdiff(MICROSECOND, timestamp_micros(<start>),
                             timestamp_micros(<end>)) AS micros
        """
      Then query result
        | micros |
        | 1      |

      Examples:
        | start               | end                 |
        | 253402207200000000L | 253402207200000001L |
        | 253402207200999998L | 253402207200999999L |
        | 253402207200999999L | 253402207201000000L |
