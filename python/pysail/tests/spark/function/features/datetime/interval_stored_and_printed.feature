Feature: a interval stores and prints what Spark does

  # Measured on the Spark 4.2 JVM with `zz_david/verify/datetime_stored_vs_printed.py` and written
  # by `zz_david/verify/gen_stored_and_printed.py`. Each shape is read through FOUR lenses: the
  # number stored behind the value (off Arrow, with no renderer in between), the type the query
  # publishes, what `show` prints and what `CAST(... AS STRING)` prints. What makes a datetime value
  # what it is does not live in that number -- the session zone decides what a timestamp prints, the
  # field range what an interval prints, the precision what a time prints -- so asserting only the
  # printed value cannot tell a wrong VALUE from a right value printed wrong, and a fix that reaches
  # one renderer but not the schema stays visible. `-` marks a lens the engines do not offer.
  # Every zone here is a FIXED offset except where a daylight-saving jump is the subject: a named
  # zone reads the tz database, whose copies are updated apart in the JVM and in Sail.
  # An interval stores its micros or its months, and its field range rides in the field metadata.
  # Spark declares thirteen ranges plus the legacy calendar interval (`DataType.scala:195-208`).

  Rule: the stored value and the printed value both match Spark

    @sail-bug
    Scenario: a day interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '5' DAY AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: a day to hour interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '1 02' DAY TO HOUR AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 93600000000 | interval day to hour | INTERVAL '1 02' DAY TO HOUR | INTERVAL '1 02' DAY TO HOUR |

    @sail-bug
    Scenario: a day to minute interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '1 02:03' DAY TO MINUTE AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 93780000000 | interval day to minute | INTERVAL '1 02:03' DAY TO MINUTE | INTERVAL '1 02:03' DAY TO MINUTE |

    Scenario: a day to second interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '1 02:03:04' DAY TO SECOND AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 93784000000 | interval day to second | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |

    @sail-bug
    Scenario: an hour interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '25' HOUR AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 90000000000 | interval hour | INTERVAL '25' HOUR | INTERVAL '25' HOUR |

    @sail-bug
    Scenario: an hour to minute interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '01:30' HOUR TO MINUTE AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 5400000000 | interval hour to minute | INTERVAL '01:30' HOUR TO MINUTE | INTERVAL '01:30' HOUR TO MINUTE |

    @sail-bug
    Scenario: an hour to second interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '01:30:45' HOUR TO SECOND AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 5445000000 | interval hour to second | INTERVAL '01:30:45' HOUR TO SECOND | INTERVAL '01:30:45' HOUR TO SECOND |

    @sail-bug
    Scenario: a minute interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '90' MINUTE AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 5400000000 | interval minute | INTERVAL '90' MINUTE | INTERVAL '90' MINUTE |

    @sail-bug
    Scenario: a minute to second interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '03:04' MINUTE TO SECOND AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 184000000 | interval minute to second | INTERVAL '03:04' MINUTE TO SECOND | INTERVAL '03:04' MINUTE TO SECOND |

    @sail-bug
    Scenario: a second interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '5.5' SECOND AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 5500000 | interval second | INTERVAL '05.5' SECOND | INTERVAL '05.5' SECOND |

    @sail-bug
    Scenario: a year interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '2' YEAR AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year | INTERVAL '2' YEAR | INTERVAL '2' YEAR |

    @sail-bug
    Scenario: a month interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '14' MONTH AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval month | INTERVAL '14' MONTH | INTERVAL '14' MONTH |

    Scenario: a year to month interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '1-2' YEAR TO MONTH AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year to month | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |

    # The PySpark 3.5 client cannot deserialize the legacy calendar interval that Connect
    # sends back: `Unsupported data type calendar_interval`. The type itself is older.
    @spark-4
    Scenario: a calendar interval from make_interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_interval(1, 2, 3, 4, 5, 6, 7) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval | 1 years 2 months 25 days 5 hours 6 minutes 7 seconds | 1 years 2 months 25 days 5 hours 6 minutes 7 seconds |

    # The PySpark 3.5 client cannot deserialize the legacy calendar interval that Connect
    # sends back: `Unsupported data type calendar_interval`. The type itself is older.
    @spark-4
    Scenario: a calendar interval of days and time
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 2, 3, 4, 5) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval | 2 days 3 hours 4 minutes 5 seconds | 2 days 3 hours 4 minutes 5 seconds |

    @sail-bug
    Scenario: a date difference
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' - DATE'2024-01-01' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1209600000000 | interval day | INTERVAL '14' DAY | INTERVAL '14' DAY |

    Scenario: a timestamp difference
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' - TIMESTAMP'2024-01-01 00:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1245600000000 | interval day to second | INTERVAL '14 10:00:00' DAY TO SECOND | INTERVAL '14 10:00:00' DAY TO SECOND |

    @sail-bug
    Scenario: a negated difference
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT -(DATE'2024-01-15' - DATE'2024-01-01') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | -1209600000000 | interval day | INTERVAL '-14' DAY | INTERVAL '-14' DAY |

    @sail-bug
    Scenario: a difference plus a day
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT (DATE'2024-01-15' - DATE'2024-01-01') + INTERVAL '2' DAY AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1382400000000 | interval day | INTERVAL '16' DAY | INTERVAL '16' DAY |

    @sail-bug
    Scenario: abs of a day interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT abs(INTERVAL '-5' DAY) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: abs of a year interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT abs(INTERVAL '-2' YEAR) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year | INTERVAL '2' YEAR | INTERVAL '2' YEAR |

    @sail-bug
    Scenario: coalesce of day intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT coalesce(NULL, INTERVAL '5' DAY) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: nvl of year intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT nvl(NULL, INTERVAL '2' YEAR) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year | INTERVAL '2' YEAR | INTERVAL '2' YEAR |

    @sail-bug
    Scenario: greatest of day intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT greatest(INTERVAL '1' DAY, INTERVAL '5' DAY) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: if of hour intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT if(true, INTERVAL '25' HOUR, INTERVAL '2' HOUR) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 90000000000 | interval hour | INTERVAL '25' HOUR | INTERVAL '25' HOUR |

    @sail-bug
    Scenario: case of month intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CASE WHEN true THEN INTERVAL '14' MONTH ELSE INTERVAL '2' MONTH END AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval month | INTERVAL '14' MONTH | INTERVAL '14' MONTH |

    Scenario: a day interval times two
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '2' DAY * 2 AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 345600000000 | interval day to second | INTERVAL '4 00:00:00' DAY TO SECOND | INTERVAL '4 00:00:00' DAY TO SECOND |

    @sail-bug
    Scenario: a year interval times two
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '1' YEAR * 2 AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year to month | INTERVAL '2-0' YEAR TO MONTH | INTERVAL '2-0' YEAR TO MONTH |

    @sail-bug
    Scenario: a year interval divided
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT INTERVAL '4' YEAR / 2 AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year to month | INTERVAL '2-0' YEAR TO MONTH | INTERVAL '2-0' YEAR TO MONTH |

    @sail-bug
    Scenario: try_add of day intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_add(INTERVAL '2' DAY, INTERVAL '3' DAY) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: try_divide of a day interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_divide(INTERVAL '10' DAY, 2) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day to second | INTERVAL '5 00:00:00' DAY TO SECOND | INTERVAL '5 00:00:00' DAY TO SECOND |

    @sail-bug
    Scenario: an interval out of an array
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT array(INTERVAL '5' DAY)[0] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: an interval out of a map
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT map('k', INTERVAL '5' DAY)['k'] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: an interval out of a struct
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT named_struct('d', INTERVAL '5' DAY).d AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: an interval out of a sorted array
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT sort_array(array(INTERVAL '5' DAY, INTERVAL '9' DAY))[0] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: the sum of day intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT sum(i) AS v FROM VALUES (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 432000000000 | interval day | INTERVAL '5' DAY | INTERVAL '5' DAY |

    @sail-bug
    Scenario: the min of year intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT min(i) AS v FROM VALUES (INTERVAL '2' YEAR), (INTERVAL '3' YEAR) AS t(i)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | - | interval year | INTERVAL '2' YEAR | INTERVAL '2' YEAR |

    @sail-bug
    Scenario: the max of hour intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT max(i) AS v FROM VALUES (INTERVAL '25' HOUR), (INTERVAL '2' HOUR) AS t(i)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 90000000000 | interval hour | INTERVAL '25' HOUR | INTERVAL '25' HOUR |

    @sail-bug
    Scenario: lag over day intervals
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT lag(i) OVER (ORDER BY i DESC) AS v FROM VALUES (INTERVAL '5' DAY), (INTERVAL '9' DAY) AS t(i) ORDER BY i LIMIT 1
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 777600000000 | interval day | INTERVAL '9' DAY | INTERVAL '9' DAY |

    @sail-bug
    Scenario: the sum of date differences
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT sum(d) AS v FROM VALUES (DATE'2024-01-15' - DATE'2024-01-01') AS t(d)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1209600000000 | interval day | INTERVAL '14' DAY | INTERVAL '14' DAY |
