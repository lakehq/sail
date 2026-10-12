Feature: extract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to extract yields the schema Spark declares
      When query
        """
        SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: a day-time interval is read field by field

    # `ExtractIntervalHours` and `ExtractIntervalMinutes` return the field, not the total:
    # `(micros / MICROS_PER_HOUR) % HOURS_PER_DAY` and `(micros / MICROS_PER_MINUTE) % MINUTES_PER_HOUR`
    # (`IntervalUtils.scala:52-62`), truncating toward zero, so a negative interval gives negative fields.
    Scenario Outline: <fn> reads the fields of <case>
      When query
        """
        SELECT
          CAST(<fn>(<day> FROM <interval>) AS INT) AS d,
          CAST(<fn>(<hour> FROM <interval>) AS INT) AS h,
          CAST(<fn>(<minute> FROM <interval>) AS INT) AS m,
          CAST(<fn>(<second> FROM <interval>) AS STRING) AS s
        """
      Then query result
        | d   | h   | m   | s   |
        | <d> | <h> | <m> | <s> |

      Examples:
        | case                     | fn      | day | hour | minute | second | interval                                                    | d  | h  | m   | s          |
        | a DAY TO SECOND literal  | extract | DAY | HOUR | MINUTE | SECOND | INTERVAL '1 02:30:15.25' DAY TO SECOND                      | 1  | 2  | 30  | 15.250000  |
        | a negated interval       | extract | DAY | HOUR | MINUTE | SECOND | -INTERVAL '1 02:30:15.25' DAY TO SECOND                     | -1 | -2 | -30 | -15.250000 |
        | 90 minutes               | extract | DAY | HOUR | MINUTE | SECOND | make_dt_interval(0, 0, 90, 0)                               | 0  | 1  | 30  | 0.000000   |
        | make_dt_interval         | extract | DAY | HOUR | MINUTE | SECOND | make_dt_interval(1, 26, 30, 15)                             | 2  | 2  | 30  | 15.000000  |
        | a timestamp difference   | extract | DAY | HOUR | MINUTE | SECOND | TIMESTAMP'2024-01-02 10:30:00' - TIMESTAMP'2024-01-01 09:00:00' | 1  | 1  | 30  | 0.000000   |
        | a date minus a timestamp | extract | DAY | HOUR | MINUTE | SECOND | DATE'2024-01-02' - TIMESTAMP'2024-01-01 09:30:00'           | 0  | 14 | 30  | 0.000000   |

    Scenario: date_part and datepart read the fields of a day-time interval
      When query
        """
        SELECT
          CAST(date_part('MINUTE', INTERVAL '1 02:30:15' DAY TO SECOND) AS INT) AS m,
          CAST(datepart('HOUR', INTERVAL '1 02:30:15' DAY TO SECOND) AS INT) AS h
        """
      Then query result
        | m  | h |
        | 30 | 2 |

    Scenario: extract reads the fields of a day-time interval column on every row
      When query
        """
        SELECT
          CAST(extract(MINUTE FROM i) AS INT) AS m,
          CAST(extract(HOUR FROM i) AS INT) AS h
        FROM (
          SELECT id, IF(id = 4, NULL, make_dt_interval(0, 0, CAST(id * 45 AS INT), 0)) AS i
          FROM range(0, 5, 1, 2)
        )
        ORDER BY id
        """
      Then query result ordered
        | m    | h    |
        | 0    | 0    |
        | 45   | 0    |
        | 30   | 1    |
        | 15   | 2    |
        | NULL | NULL |
