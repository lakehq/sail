Feature: a timestamp stores and prints what Spark does

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
  # A timestamp stores the units since the epoch in UTC; a TIMESTAMP_NTZ stores the wall clock and
  # must NOT move with the session zone. Both zones are swept for that reason.

  Rule: the stored value and the printed value both match Spark

    Scenario: the epoch
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'1970-01-01 00:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 0 | timestamp | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 |

    Scenario: a timestamp
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp with micros
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00.123456' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800123456 | timestamp | 2024-01-15 10:00:00.123456 | 2024-01-15 10:00:00.123456 |

    Scenario: a timestamp before the epoch
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'1969-12-31 23:59:59' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | -1000000 | timestamp | 1969-12-31 23:59:59 | 1969-12-31 23:59:59 |

    Scenario: a timestamp_ntz
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIMESTAMP_NTZ) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp_ntz | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp read in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705330800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    @sail-bug
    Scenario: a timestamp_ntz read in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIMESTAMP_NTZ) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp_ntz | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp parsed in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705330800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp across a DST jump
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-03-10 03:30:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1710055800000000 | timestamp | 2024-03-10 03:30:00 | 2024-03-10 03:30:00 |

    Scenario: a timestamp cast from a date
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' AS TIMESTAMP) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705276800000000 | timestamp | 2024-01-15 00:00:00 | 2024-01-15 00:00:00 |

    Scenario: a timestamp cast from a date in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' AS TIMESTAMP) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705294800000000 | timestamp | 2024-01-15 00:00:00 | 2024-01-15 00:00:00 |

    Scenario: a timestamp built by make_timestamp
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, 0) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp from a unix second
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT timestamp_seconds(1705312800) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: date_trunc to the hour
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT date_trunc('HOUR', TIMESTAMP'2024-01-15 10:20:30') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp plus a month interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' + INTERVAL '1' MONTH AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1707991200000000 | timestamp | 2024-02-15 10:00:00 | 2024-02-15 10:00:00 |

    Scenario: a timestamp plus a day interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' + INTERVAL '2' DAY AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705485600000000 | timestamp | 2024-01-17 10:00:00 | 2024-01-17 10:00:00 |

    Scenario: a timestamp column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT ts AS v FROM VALUES (TIMESTAMP'2024-01-15 10:00:00') AS t(ts)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp out of an array
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT array(TIMESTAMP'2024-01-15 10:00:00')[0] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: the max of a timestamp column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT max(ts) AS v FROM VALUES (TIMESTAMP'2024-01-15 10:00:00'), (TIMESTAMP'2020-01-01 00:00:00') AS t(ts)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp read in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705330800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    @sail-bug
    Scenario: a timestamp_ntz read in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIMESTAMP_NTZ) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705312800000000 | timestamp_ntz | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp parsed in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIMESTAMP'2024-01-15 10:00:00' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705330800000000 | timestamp | 2024-01-15 10:00:00 | 2024-01-15 10:00:00 |

    Scenario: a timestamp cast from a date in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' AS TIMESTAMP) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 1705294800000000 | timestamp | 2024-01-15 00:00:00 | 2024-01-15 00:00:00 |
