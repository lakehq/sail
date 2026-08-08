Feature: sequence() over DATE returns expected arrays

    Scenario Outline: sequence over DATE: <case>
      When query
        """
        SELECT sequence(<args>) AS seq
        """
      Then query result ordered
        | seq   |
        | <seq> |

      Examples:
        | case                                                       | args                                                  | seq                                                          |
        | sequence(date, date) uses default step of 1 day            | date '2024-01-01', date '2024-01-05'                  | [2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05] |
        | sequence(date, date, interval days) uses the provided step | date '2024-01-01', date '2024-01-10', interval 2 days | [2024-01-01, 2024-01-03, 2024-01-05, 2024-01-07, 2024-01-09] |

    Scenario: temporal sequence chooses descending default steps
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          CAST(sequence(
            DATE '2018-01-03',
            DATE '2018-01-01'
          ) AS STRING) AS dates,
          CAST(sequence(
            TIMESTAMP '2018-01-03 00:00:00',
            TIMESTAMP '2018-01-01 00:00:00'
          ) AS STRING) AS timestamps,
          typeof(sequence(
            TIMESTAMP_NTZ '2018-01-03 00:00:00',
            TIMESTAMP_NTZ '2018-01-01 00:00:00'
          )) AS ntz_type
        """
      Then query result
        | dates                                | timestamps                                                           | ntz_type             |
        | [2018-01-03, 2018-01-02, 2018-01-01] | [2018-01-03 00:00:00, 2018-01-02 00:00:00, 2018-01-01 00:00:00] | array<timestamp_ntz> |

    Scenario: sequence anchors calendar interval multiplication at start
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          CAST(sequence(
            DATE '2018-01-31',
            DATE '2018-04-30',
            INTERVAL 1 MONTH
          ) AS STRING) AS month_end,
          CAST(sequence(
            DATE '2018-01-31',
            DATE '2018-04-30',
            make_interval(0, 1, 0, -1)
          ) AS STRING) AS mixed_sign,
          CAST(sequence(
            TIMESTAMP '2018-01-01 00:00:00',
            TIMESTAMP '2018-03-01 00:04:06',
            make_interval(0, 1, 0, 0, 0, 2, 3)
          ) AS STRING) AS composite
        """
      Then query result
        | month_end                                        | mixed_sign                                       | composite                                                             |
        | [2018-01-31, 2018-02-28, 2018-03-31, 2018-04-30] | [2018-01-31, 2018-02-27, 2018-03-29, 2018-04-27] | [2018-01-01 00:00:00, 2018-02-01 00:02:03, 2018-03-01 00:04:06] |

    Scenario: timestamp sequence supports microseconds and equal zero steps
      When query
        """
        SELECT
          CAST(sequence(
            TIMESTAMP_NTZ '2018-01-01 00:00:00.000000',
            TIMESTAMP_NTZ '2018-01-01 00:00:00.000002',
            INTERVAL 1 MICROSECOND
          ) AS STRING) AS micros,
          CAST(sequence(
            TIMESTAMP_NTZ '2018-01-01 00:00:00',
            TIMESTAMP_NTZ '2018-01-01 00:00:00',
            INTERVAL 0 MICROSECONDS
          ) AS STRING) AS zero_equal
        """
      Then query result
        | micros                                                                                      | zero_equal            |
        | [2018-01-01 00:00:00, 2018-01-01 00:00:00.000001, 2018-01-01 00:00:00.000002] | [2018-01-01 00:00:00] |

    Scenario: timestamp sequence propagates null column inputs
      When query
        """
        SELECT id, CAST(sequence(lo, hi, stride) AS STRING) AS result
        FROM VALUES
          (1, TIMESTAMP_NTZ '2018-01-01 00:00:00', TIMESTAMP_NTZ '2018-01-02 00:00:00', INTERVAL 12 HOURS),
          (2, CAST(NULL AS TIMESTAMP_NTZ), TIMESTAMP_NTZ '2018-01-02 00:00:00', INTERVAL 12 HOURS),
          (3, TIMESTAMP_NTZ '2018-01-01 00:00:00', CAST(NULL AS TIMESTAMP_NTZ), INTERVAL 12 HOURS),
          (4, TIMESTAMP_NTZ '2018-01-01 00:00:00', TIMESTAMP_NTZ '2018-01-02 00:00:00', CAST(NULL AS INTERVAL DAY TO SECOND))
          AS t(id, lo, hi, stride)
        ORDER BY id
        """
      Then query result ordered
        | id | result                                                               |
        | 1  | [2018-01-01 00:00:00, 2018-01-01 12:00:00, 2018-01-02 00:00:00] |
        | 2  | NULL                                                                 |
        | 3  | NULL                                                                 |
        | 4  | NULL                                                                 |

    Scenario: sequence widening uses Spark temporal cast semantics
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CAST(sequence(
            TIMESTAMP_NTZ '2021-11-07 01:00:00',
            TIMESTAMP '2021-11-07 02:00:00',
            INTERVAL 30 MINUTES
          ) AS STRING) AS mixed_timestamp_types,
          CAST(sequence(
            TIMESTAMP '2021-11-07 00:30:00',
            '2021-11-07 01:30:00',
            INTERVAL 30 MINUTES
          ) AS STRING) AS string_stop
        """
      Then query result
        | mixed_timestamp_types                                                                                                                 | string_stop                                                          |
        | [2021-11-07 01:00:00, 2021-11-07 01:30:00, 2021-11-07 01:00:00, 2021-11-07 01:30:00, 2021-11-07 02:00:00] | [2021-11-07 00:30:00, 2021-11-07 01:00:00, 2021-11-07 01:30:00] |

    Scenario: timestamp sequence crosses daylight-saving transitions
      Given config spark.sql.session.timeZone = Europe/Prague
      When query
        """
        SELECT
          CAST(sequence(
            TIMESTAMP '2018-03-25 01:30:00',
            TIMESTAMP '2018-03-25 03:30:00',
            INTERVAL 30 MINUTES
          ) AS STRING) AS spring,
          CAST(sequence(
            TIMESTAMP '2018-10-28 01:30:00',
            TIMESTAMP '2018-10-28 03:30:00',
            INTERVAL 30 MINUTES
          ) AS STRING) AS autumn
        """
      Then query result
        | spring                                                          | autumn                                                                                                                                                              |
        | [2018-03-25 01:30:00, 2018-03-25 03:00:00, 2018-03-25 03:30:00] | [2018-10-28 01:30:00, 2018-10-28 02:00:00, 2018-10-28 02:30:00, 2018-10-28 02:00:00, 2018-10-28 02:30:00, 2018-10-28 03:00:00, 2018-10-28 03:30:00] |

    Scenario Outline: date sequence applies mixed day-hour intervals in the session timezone
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(sequence(
          DATE '2022-03-09',
          DATE '2022-03-15',
          make_interval(0, 0, 0, 4, 23)
        ) AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | result                   |
        | America/Los_Angeles | [2022-03-09, 2022-03-14] |
        | UTC                 | [2022-03-09, 2022-03-13] |
        | Z                   | [2022-03-09, 2022-03-13] |
        | +1:00               | [2022-03-09, 2022-03-13] |
        | +01:02:03           | [2022-03-09, 2022-03-13] |
        | PST                 | [2022-03-09, 2022-03-14] |
        | EST                 | [2022-03-09, 2022-03-13] |
        | MST                 | [2022-03-09, 2022-03-13] |
        | UT                  | [2022-03-09, 2022-03-13] |
        | UTC+8               | [2022-03-09, 2022-03-13] |
        | GMT+8:30            | [2022-03-09, 2022-03-13] |
        | +8                  | [2022-03-09, 2022-03-13] |
        | +0130               | [2022-03-09, 2022-03-13] |

    Scenario: temporal sequence uses Spark's microsecond-exclusive boundary
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          CAST(sequence(
            DATE '2024-01-02',
            DATE '2024-01-01',
            INTERVAL '-1 00:00:00.000001' DAY TO SECOND
          ) AS STRING) AS dates,
          CAST(sequence(
            TIMESTAMP_NTZ '2024-01-02 00:00:00',
            TIMESTAMP_NTZ '2024-01-01 00:00:00',
            INTERVAL '-1 00:00:00.000001' DAY TO SECOND
          ) AS STRING) AS timestamps
        """
      Then query result
        | dates                    | timestamps                                                        |
        | [2024-01-02, 2023-12-31] | [2024-01-02 00:00:00, 2023-12-31 23:59:59.999999] |

    Scenario: sequence wraps calendar interval estimates like Spark
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sequence(
          TIMESTAMP '1970-01-01 00:00:00',
          TIMESTAMP '1970-01-01 00:00:00',
          make_interval(0, 69000000, 0, -2100000000)
        ) AS STRING) AS result
        """
      Then query result
        | result                |
        | [1970-01-01 00:00:00] |

    Scenario: date sequence boundary errors show the original interval
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT sequence(
          DATE '1970-01-01',
          DATE '1970-02-01',
          INTERVAL '-0-1' YEAR TO MONTH
        ) AS result
        """
      Then query error Illegal sequence boundaries: 0 to 2678400000000 by -1

    Scenario Outline: date sequence sub-day errors identify the interval type
      When query
        """
        SELECT sequence(
          DATE '2021-07-01',
          DATE '2021-07-10',
          <step>
        ) AS result
        """
      Then query error sequence step must be an <interval_type> of day granularity if start and end values are dates

      Examples:
        | step                                | interval_type          |
        | make_interval(0, 0, 0, 0, 1)       | interval               |
        | INTERVAL '0-0' YEAR TO MONTH        | interval year to month |
        | INTERVAL '0 03:00:00' DAY TO SECOND | interval day to second |
