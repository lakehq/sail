@spark-4.2
Feature: a time stores and prints what Spark does

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
  # A time stores the units of the DAY. Spark has seven precisions, 0 to 6 (`TimeType.scala:51-53`),
  # and the type exists from Spark 4.2 only, behind `spark.sql.timeType.enabled`.

  Rule: the stored value and the printed value both match Spark

    Scenario: midnight
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '00:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 0 | time(6) | 00:00:00 | 00:00:00 |

    Scenario: a time
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '10:20:30' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    Scenario: a time with micros
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '10:20:30.123456' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230123456000 | time(6) | 10:20:30.123456 | 10:20:30.123456 |

    Scenario: the last microsecond of the day
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '23:59:59.999999' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 86399999999000 | time(6) | 23:59:59.999999 | 23:59:59.999999 |

    Scenario: a time at precision 0
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(0)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(0) | 10:20:30 | 10:20:30 |

    @sail-bug
    Scenario: a time at precision 1
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(1)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230100000000 | time(1) | 10:20:30.1 | 10:20:30.1 |

    @sail-bug
    Scenario: a time at precision 2
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(2)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230120000000 | time(2) | 10:20:30.12 | 10:20:30.12 |

    Scenario: a time at precision 3
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(3)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230123000000 | time(3) | 10:20:30.123 | 10:20:30.123 |

    @sail-bug
    Scenario: a time at precision 4
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(4)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230123400000 | time(4) | 10:20:30.1234 | 10:20:30.1234 |

    @sail-bug
    Scenario: a time at precision 5
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(5)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230123450000 | time(5) | 10:20:30.12345 | 10:20:30.12345 |

    Scenario: a time at precision 6
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '10:20:30.123456' AS TIME(6)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230123456000 | time(6) | 10:20:30.123456 | 10:20:30.123456 |

    Scenario: a time cast from a string
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST('10:20:30' AS TIME(6)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    Scenario: a time built by make_time
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_time(10, 20, 30.5) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230500000000 | time(6) | 10:20:30.5 | 10:20:30.5 |

    Scenario: a time read in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '10:20:30' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    Scenario: a time column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT t AS v FROM VALUES (TIME '10:20:30') AS t(t)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    Scenario: the least of two times
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT least(TIME '10:20:30', TIME '01:02:03') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 3723000000000 | time(6) | 01:02:03 | 01:02:03 |

    Scenario: a time out of an array
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT array(TIME '10:20:30')[0] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    Scenario: the max of a time column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT max(t) AS v FROM VALUES (TIME '10:20:30'), (TIME '01:02:03') AS t(t)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

    @sail-bug
    Scenario: a time plus a minute interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '10:20:30' + INTERVAL '90' MINUTE AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 42630000000000 | time(6) | 11:50:30 | 11:50:30 |

    @sail-bug
    Scenario: a difference of times
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '12:00:00' AS TIME(6)) - CAST(TIME '10:00:00' AS TIME(6)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 7200000000 | interval hour to second | INTERVAL '02:00:00' HOUR TO SECOND | INTERVAL '02:00:00' HOUR TO SECOND |

    Scenario: a time read in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '10:20:30' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 37230000000000 | time(6) | 10:20:30 | 10:20:30 |

  Rule: what Spark refuses and Sail answers

    # Found by sweeping the type surface, not by a failing query: Sail accepts more
    # than Spark here, and the value it invents looks perfectly reasonable.

    @sail-bug
    Scenario: a time cast from a timestamp is refused
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIME(6)) AS v
        """
      Then query error DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION
