Feature: date_part / extract output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
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

    @sail-bug
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

  Rule: Basic date parts from timestamps

    Scenario: extract year from timestamp
      When query
      """
      SELECT date_part('YEAR', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: extract month from timestamp
      When query
      """
      SELECT date_part('MONTH', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 3      |

    Scenario: extract day from timestamp
      When query
      """
      SELECT date_part('DAY', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 15     |

    Scenario: extract hour from timestamp
      When query
      """
      SELECT date_part('HOUR', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 14     |

    Scenario: extract minute from timestamp
      When query
      """
      SELECT date_part('MINUTE', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 30     |

    Scenario: extract second from timestamp returns decimal
      When query
      """
      SELECT date_part('SECOND', TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result    |
      | 45.000000 |

    Scenario: extract second with microseconds
      When query
      """
      SELECT date_part('SECOND', TIMESTAMP '2024-03-15 14:30:45.123456') AS result
      """
      Then query result
      | result    |
      | 45.123456 |

  Rule: Date parts from date values

    Scenario: extract year from date
      When query
      """
      SELECT date_part('YEAR', DATE '2024-03-15') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: extract month from date
      When query
      """
      SELECT date_part('MONTH', DATE '2024-03-15') AS result
      """
      Then query result
      | result |
      | 3      |

    Scenario: extract hour from date is zero
      When query
      """
      SELECT date_part('HOUR', DATE '2024-03-15') AS result
      """
      Then query result
      | result |
      | 0      |

  Rule: Spark dayofweek is 1-indexed (Sunday=1, Saturday=7)

    @sail-bug
    Scenario: dayofweek on Sunday
      When query
      """
      SELECT date_part('DAYOFWEEK', DATE '2024-03-17') AS result
      """
      Then query result
      | result |
      | 1      |

    @sail-bug
    Scenario: dayofweek on Monday
      When query
      """
      SELECT date_part('DAYOFWEEK', DATE '2024-03-18') AS result
      """
      Then query result
      | result |
      | 2      |

    @sail-bug
    Scenario: dayofweek on Saturday
      When query
      """
      SELECT date_part('DAYOFWEEK', DATE '2024-03-23') AS result
      """
      Then query result
      | result |
      | 7      |

  Rule: Quarter, day of year, and week parts

    Scenario: quarter Q1
      When query
      """
      SELECT date_part('QUARTER', DATE '2024-01-15') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: quarter Q4
      When query
      """
      SELECT date_part('QUARTER', DATE '2024-12-15') AS result
      """
      Then query result
      | result |
      | 4      |

    Scenario: day of year on leap year
      When query
      """
      SELECT date_part('DOY', DATE '2024-03-01') AS result
      """
      Then query result
      | result |
      | 61     |

    Scenario: day of year on non-leap year
      When query
      """
      SELECT date_part('DOY', DATE '2023-03-01') AS result
      """
      Then query result
      | result |
      | 60     |

    Scenario: last day of leap year
      When query
      """
      SELECT date_part('DOY', DATE '2024-12-31') AS result
      """
      Then query result
      | result |
      | 366    |

    Scenario: last day of non-leap year
      When query
      """
      SELECT date_part('DOY', DATE '2023-12-31') AS result
      """
      Then query result
      | result |
      | 365    |

  Rule: Aliases datepart and extract

    Scenario: datepart alias works
      When query
      """
      SELECT datepart('YEAR', TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: extract with FROM syntax
      When query
      """
      SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: extract second returns decimal
      When query
      """
      SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-01-01 00:00:30.500000') AS result
      """
      Then query result
      | result    |
      | 30.500000 |

  Rule: Convenience functions

    Scenario: year function
      When query
      """
      SELECT year(TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: month function
      When query
      """
      SELECT month(TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 3      |

    Scenario: day function
      When query
      """
      SELECT day(TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 15     |

    Scenario: dayofmonth function equals day
      When query
      """
      SELECT dayofmonth(DATE '2024-03-15') AS result
      """
      Then query result
      | result |
      | 15     |

    Scenario: hour function
      When query
      """
      SELECT hour(TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 14     |

    Scenario: minute function
      When query
      """
      SELECT minute(TIMESTAMP '2024-03-15 14:30:45') AS result
      """
      Then query result
      | result |
      | 30     |

    Scenario: second function returns integer
      When query
      """
      SELECT second(TIMESTAMP '2024-03-15 14:30:45.123456') AS result
      """
      Then query result
      | result |
      | 45     |

    Scenario: quarter function
      When query
      """
      SELECT quarter(DATE '2024-07-04') AS result
      """
      Then query result
      | result |
      | 3      |

    Scenario: dayofyear function
      When query
      """
      SELECT dayofyear(DATE '2024-03-01') AS result
      """
      Then query result
      | result |
      | 61     |

    Scenario: dayofweek function Sunday is 1
      When query
      """
      SELECT dayofweek(DATE '2024-03-17') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: dayofweek function Wednesday is 4
      When query
      """
      SELECT dayofweek(DATE '2024-03-20') AS result
      """
      Then query result
      | result |
      | 4      |

    Scenario: weekday function Monday is 0
      When query
      """
      SELECT weekday(DATE '2024-03-18') AS result
      """
      Then query result
      | result |
      | 0      |

    @sail-bug
    Scenario: weekday function Sunday is 6
      When query
      """
      SELECT weekday(DATE '2024-03-17') AS result
      """
      Then query result
      | result |
      | 6      |

    Scenario: weekofyear function
      When query
      """
      SELECT weekofyear(DATE '2024-01-01') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: year function on date
      When query
      """
      SELECT year(DATE '2024-12-31') AS result
      """
      Then query result
      | result |
      | 2024   |

  Rule: Case insensitivity of part names

    Scenario: lowercase part name
      When query
      """
      SELECT date_part('year', TIMESTAMP '2024-03-15 10:00:00') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: mixed case part name
      When query
      """
      SELECT date_part('Month', DATE '2024-06-15') AS result
      """
      Then query result
      | result |
      | 6      |

  Rule: Second with microsecond precision edge cases

    Scenario: second at zero
      When query
      """
      SELECT second(TIMESTAMP '2024-01-01 00:00:00.000000') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: second at max
      When query
      """
      SELECT second(TIMESTAMP '2024-01-01 00:00:59.999999') AS result
      """
      Then query result
      | result |
      | 59     |

    Scenario: second with partial microseconds
      When query
      """
      SELECT second(TIMESTAMP '2024-01-01 12:00:30.100000') AS result
      """
      Then query result
      | result |
      | 30     |

  Rule: NULL handling

    Scenario: date_part with null timestamp
      When query
      """
      SELECT date_part('YEAR', CAST(NULL AS TIMESTAMP)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_part with null date
      When query
      """
      SELECT date_part('MONTH', CAST(NULL AS DATE)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: year function with null
      When query
      """
      SELECT year(CAST(NULL AS DATE)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: second function with null
      When query
      """
      SELECT second(CAST(NULL AS TIMESTAMP)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Multi-row extraction

    Scenario: extract parts from multiple rows
      When query
      """
      SELECT
        year(ts) AS y,
        month(ts) AS m,
        day(ts) AS d,
        hour(ts) AS h,
        minute(ts) AS mi
      FROM VALUES
        (TIMESTAMP '2024-01-15 08:30:00'),
        (TIMESTAMP '2024-06-20 14:45:00'),
        (TIMESTAMP '2024-12-31 23:59:59')
      AS t(ts)
      ORDER BY ts
      """
      Then query result ordered
      | y    | m  | d  | h  | mi |
      | 2024 | 1  | 15 | 8  | 30 |
      | 2024 | 6  | 20 | 14 | 45 |
      | 2024 | 12 | 31 | 23 | 59 |

    Scenario: dayofweek across a full week
      When query
      """
      SELECT
        d,
        dayofweek(d) AS dow
      FROM VALUES
        (DATE '2024-03-17'),
        (DATE '2024-03-18'),
        (DATE '2024-03-19'),
        (DATE '2024-03-20'),
        (DATE '2024-03-21'),
        (DATE '2024-03-22'),
        (DATE '2024-03-23')
      AS t(d)
      ORDER BY d
      """
      Then query result ordered
      | d          | dow |
      | 2024-03-17 | 1   |
      | 2024-03-18 | 2   |
      | 2024-03-19 | 3   |
      | 2024-03-20 | 4   |
      | 2024-03-21 | 5   |
      | 2024-03-22 | 6   |
      | 2024-03-23 | 7   |

    Scenario: mixed null and non-null rows
      When query
      """
      SELECT
        year(ts) AS y,
        month(ts) AS m
      FROM VALUES
        (TIMESTAMP '2024-03-15 10:00:00'),
        (CAST(NULL AS TIMESTAMP)),
        (TIMESTAMP '2023-07-04 12:00:00')
      AS t(ts)
      ORDER BY ts
      """
      Then query result ordered
      | y    | m    |
      | NULL | NULL |
      | 2023 | 7    |
      | 2024 | 3    |

  Rule: Boundary and edge cases

    Scenario: midnight timestamp
      When query
      """
      SELECT hour(TIMESTAMP '2024-01-01 00:00:00') AS h,
             minute(TIMESTAMP '2024-01-01 00:00:00') AS m
      """
      Then query result
      | h | m |
      | 0 | 0 |

    Scenario: end of day timestamp
      When query
      """
      SELECT hour(TIMESTAMP '2024-01-01 23:59:59') AS h,
             minute(TIMESTAMP '2024-01-01 23:59:59') AS m
      """
      Then query result
      | h  | m  |
      | 23 | 59 |

    Scenario: quarter boundaries
      When query
      """
      SELECT
        quarter(d) AS q
      FROM VALUES
        (DATE '2024-01-01'),
        (DATE '2024-03-31'),
        (DATE '2024-04-01'),
        (DATE '2024-06-30'),
        (DATE '2024-07-01'),
        (DATE '2024-09-30'),
        (DATE '2024-10-01'),
        (DATE '2024-12-31')
      AS t(d)
      ORDER BY d
      """
      Then query result ordered
      | q |
      | 1 |
      | 1 |
      | 2 |
      | 2 |
      | 3 |
      | 3 |
      | 4 |
      | 4 |

    Scenario: year from string coercion
      When query
      """
      SELECT year('2024-06-15') AS result
      """
      Then query result
      | result |
      | 2024   |

    Scenario: month from string coercion
      When query
      """
      SELECT month('2024-06-15') AS result
      """
      Then query result
      | result |
      | 6      |

  Rule: Fields are read in the session time zone

    # Spark 4.2.0 DatePart resolves to the same field expressions as Extract (Year, DayOfMonth, ...),
    # each evaluated in the session zone.

    Scenario: date_part of a zoned literal in Kiritimati
      Given config spark.sql.session.timeZone = Pacific/Kiritimati
      When query
      """
      SELECT
        date_part('DAY', TIMESTAMP '2024-12-31 12:00:00Z') AS d,
        date_part('YEAR', TIMESTAMP '2024-12-31 12:00:00Z') AS y
      """
      Then query result
      | d | y    |
      | 1 | 2025 |

    @sail-bug
    Scenario: date_part of an instant built with timestamp_millis in Kiritimati
      Given config spark.sql.session.timeZone = Pacific/Kiritimati
      When query
      """
      SELECT
        date_part('DAY', timestamp_millis(1735646400000)) AS d,
        date_part('YEAR', timestamp_millis(1735646400000)) AS y
      """
      Then query result
      | d | y    |
      | 1 | 2025 |

  Rule: Fields at the limits of the 0001-9999 range

    Scenario: date_part and extract at the range limits
      Given config spark.sql.session.timeZone = UTC
      When query
      """
      SELECT
        extract(YEAR FROM DATE '0001-01-01') AS y,
        extract(DOY FROM DATE '9999-12-31') AS doy,
        date_part('QUARTER', TIMESTAMP '0001-01-01 00:00:00') AS q,
        extract(HOUR FROM TIMESTAMP_NTZ '9999-12-31 23:59:59.999999') AS h,
        extract(SECOND FROM TIMESTAMP '9999-12-31 23:59:59.999999') AS s
      """
      Then query result
      | y | doy | q | h  | s         |
      | 1 | 365 | 1 | 23 | 59.999999 |
