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
