Feature: date_part and extract fields

  @function(nullability)
  Rule: Output schema

    Scenario: date_part second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT date_part('second', TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    Scenario: extract second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT extract(SECOND FROM TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    Scenario: date_part second of a nullable timestamp stays nullable
      When query
        """
        SELECT date_part('second', c) AS result FROM VALUES (TIMESTAMP '2024-01-01 00:00:05'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = true)
        """

    Scenario: date part aliases preserve non-null dynamic timestamp fields
      When query
        """
        SELECT datepart('YEAR', timestamp_micros(id - 1)) AS y,
               extract(SECOND FROM timestamp_micros(id - 1)) AS s
        FROM range(3)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = false)
         |-- s: decimal(8,6) (nullable = false)
        """
      And query result
        | y    | s         |
        | 1969 | 59.999999 |
        | 1970 | 0.000000  |
        | 1970 | 0.000001  |

    Scenario: nullable datepart fields retain input nulls
      When query
        """
        SELECT datepart('YEAR', c) AS y, extract(SECOND FROM c) AS s
        FROM VALUES (timestamp_micros(-1)), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = true)
         |-- s: decimal(8,6) (nullable = true)
        """
      And query result
        | y    | s         |
        | 1969 | 59.999999 |
        | NULL | NULL      |

    Scenario: null extraction field yields a nullable double
      When query
        """
        SELECT date_part(NULL, DATE '2024-01-01') AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
      And query result
        | result |
        | NULL   |

  Rule: Wide proleptic Gregorian fields

    # Spark 4.2 DateTimeUtils uses Java LocalDate, not Chrono's narrower range.
    Scenario: calendar fields cover the full Date32 range and ISO week boundaries
      When query
        """
        SELECT date_part('YRS', date_from_unix_date(d)) AS y,
               date_part('YEAROFWEEK', date_from_unix_date(d)) AS wy,
               date_part('QTR', date_from_unix_date(d)) AS q,
               date_part('MONS', date_from_unix_date(d)) AS m,
               date_part('W', date_from_unix_date(d)) AS w,
               date_part('DAYS', date_from_unix_date(d)) AS d,
               date_part('DAYOFWEEK', date_from_unix_date(d)) AS dow,
               date_part('DOW_ISO', date_from_unix_date(d)) AS iso,
               date_part('DOY', date_from_unix_date(d)) AS doy
        FROM VALUES (-2147483648), (2147483647), (-141438), (13149), (13150) AS t(d)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = false)
         |-- wy: integer (nullable = false)
         |-- q: integer (nullable = false)
         |-- m: integer (nullable = false)
         |-- w: integer (nullable = false)
         |-- d: integer (nullable = false)
         |-- dow: integer (nullable = false)
         |-- iso: integer (nullable = false)
         |-- doy: integer (nullable = false)
        """
      And query result
        | y        | wy       | q | m  | w  | d  | dow | iso | doy |
        | -5877641 | -5877641 | 2 | 6  | 26 | 23 | 3   | 2   | 174 |
        | 5881580  | 5881580  | 3 | 7  | 28 | 11 | 6   | 5   | 193 |
        | 1582     | 1582     | 4 | 10 | 40 | 4  | 2   | 1   | 277 |
        | 2006     | 2005     | 1 | 1  | 52 | 1  | 1   | 7   | 1   |
        | 2006     | 2006     | 1 | 1  | 1  | 2  | 2   | 1   | 2   |

    Scenario Outline: timestamp limits retain fields in a fixed-offset session <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT date_part('YEAR', timestamp_micros(u)) AS y,
               date_part('MONTH', timestamp_micros(u)) AS m,
               date_part('DAY', timestamp_micros(u)) AS d,
               date_part('HRS', timestamp_micros(u)) AS h,
               date_part('MINS', timestamp_micros(u)) AS mi,
               date_part('SECS', timestamp_micros(u)) AS s
        FROM VALUES (-9223372036854775808L), (9223372036854775807L) AS t(u)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = false)
         |-- m: integer (nullable = false)
         |-- d: integer (nullable = false)
         |-- h: integer (nullable = false)
         |-- mi: integer (nullable = false)
         |-- s: decimal(8,6) (nullable = false)
        """
      And query result
        | y       | m  | d     | h     | mi     | s         |
        | -290308 | 12 | <dlo> | <hlo> | <milo> | 5.224192  |
        | 294247  | 1  | <dhi> | <hhi> | <mihi> | 54.775807 |

      Examples:
        | zone   | dlo | hlo | milo | dhi | hhi | mihi |
        | UTC    | 21  | 19  | 59   | 10  | 4   | 0    |
        | +05:30 | 22  | 1   | 29   | 10  | 9   | 30   |
        | -12:00 | 21  | 7   | 59   | 9   | 16  | 0    |

    Scenario: timestamp NTZ fields do not shift with the session timezone
      Given config spark.sql.session.timeZone = +05:30
      When query
        """
        SELECT date_part('YEAR', t) AS y, date_part('HOUR', t) AS h,
               extract(SECOND FROM t) AS s
        FROM VALUES (TIMESTAMP_NTZ '-290308-12-21 19:59:05.224192'),
                    (TIMESTAMP_NTZ '+294247-01-10 04:00:54.775807'),
                    (TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS v(t)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = true)
         |-- h: integer (nullable = false)
         |-- s: decimal(8,6) (nullable = false)
        """
      And query result
        | y       | h  | s         |
        | -290308 | 19 | 5.224192  |
        | 294247  | 4  | 54.775807 |
        | 1969    | 23 | 59.999999 |

  Rule: Timezone and interval fields

    Scenario: timestamp fields read the session timezone across DST transitions
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT datepart('YEAR', timestamp_micros(u)) AS y,
               datepart('DOW', timestamp_micros(u)) AS dow,
               extract(HOUR FROM timestamp_micros(u)) AS h,
               extract(SECOND FROM timestamp_micros(u)) AS s
        FROM VALUES (-1L), (1710064799999999L), (1710064800000000L),
                    (1730624399999999L), (1730624400000000L) AS t(u)
        """
      Then query result
        | y    | dow | h  | s         |
        | 1969 | 4   | 15 | 59.999999 |
        | 2024 | 1   | 1  | 59.999999 |
        | 2024 | 1   | 3  | 0.000000  |
        | 2024 | 1   | 1  | 59.999999 |
        | 2024 | 1   | 1  | 0.000000  |

    Scenario: second extraction includes historical timezone offset seconds
      Given config spark.sql.session.timeZone = Africa/Monrovia
      When query
        """
        SELECT extract(SECOND FROM timestamp_micros(id - 1)) AS s FROM range(2)
        """
      Then query result
        | s         |
        | 29.999999 |
        | 30.000000 |

    Scenario: negative day-time intervals retain signed components and Spark types
      When query
        """
        SELECT date_part('DAY', i) AS d, date_part('HOUR', i) AS h,
               date_part('MINUTE', i) AS m, date_part('SECOND', i) AS s
        FROM VALUES (INTERVAL '-123 12:34:56.789123' DAY TO SECOND),
                    (INTERVAL '0 00:00:00.000001' DAY TO SECOND) AS t(i)
        """
      Then query schema
        """
        root
         |-- d: integer (nullable = false)
         |-- h: byte (nullable = false)
         |-- m: byte (nullable = false)
         |-- s: decimal(8,6) (nullable = false)
        """
      And query result
        | d    | h   | m   | s          |
        | -123 | -12 | -34 | -56.789123 |
        | 0    | 0   | 0   | 0.000001   |

    Scenario: negative year-month intervals retain signed components and Spark types
      When query
        """
        SELECT extract(YEAR FROM i) AS y, date_part('MONTH', i) AS m
        FROM VALUES (INTERVAL '-2-1' YEAR TO MONTH), (INTERVAL '2-1' YEAR TO MONTH) AS t(i)
        """
      Then query schema
        """
        root
         |-- y: integer (nullable = false)
         |-- m: byte (nullable = false)
        """
      And query result
        | y  | m  |
        | -2 | -1 |
        | 2  | 1  |

    Scenario Outline: invalid extraction fields are rejected <part>
      When query
        """
        SELECT date_part('<part>', DATE '2024-01-01')
        """
      Then query error (?i)date part|date_part|cannot extract

      Examples:
        | part          |
        |               |
        | not_supported |

    Scenario: a dynamic extraction field is rejected
      When query
        """
        SELECT date_part(CAST(id AS STRING), DATE '2024-01-01') FROM range(2)
        """
      Then query error (?i)constant|foldable

    Scenario: year extraction is not defined on day-time intervals
      When query
        """
        SELECT date_part('YEAR', INTERVAL '1 00:00:00' DAY TO SECOND)
        """
      Then query error (?i)cannot extract
