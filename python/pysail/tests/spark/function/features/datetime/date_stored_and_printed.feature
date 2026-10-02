Feature: a date stores and prints what Spark does

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
  # A date stores the DAYS since the epoch, and nothing about it follows the session zone.

  Rule: the stored value and the printed value both match Spark

    Scenario: the epoch
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'1970-01-01' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 0 | date | 1970-01-01 | 1970-01-01 |

    Scenario: a date
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: the day before the epoch
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'1969-12-31' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | -1 | date | 1969-12-31 | 1969-12-31 |

    Scenario: a leap day
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-02-29' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19782 | date | 2024-02-29 | 2024-02-29 |

    Scenario: the first year
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'0001-01-01' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | -719162 | date | 0001-01-01 | 0001-01-01 |

    Scenario: the last year
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'9999-12-31' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 2932896 | date | 9999-12-31 | 9999-12-31 |

    Scenario: a date cast from a string
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST('2024-01-15' AS DATE) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date cast from a timestamp
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS DATE) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date built by make_date
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_date(2024, 2, 29) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19782 | date | 2024-02-29 | 2024-02-29 |

    Scenario: a date read in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date cast from a timestamp in another zone
      Given config spark.sql.session.timeZone = -05:00
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS DATE) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: date_add
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT date_add(DATE'2024-01-15', 20) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19757 | date | 2024-02-04 | 2024-02-04 |

    Scenario: date_sub
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT date_sub(DATE'2024-01-15', 20) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19717 | date | 2023-12-26 | 2023-12-26 |

    Scenario: add_months over the year
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT add_months(DATE'2024-01-15', 12) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 20103 | date | 2025-01-15 | 2025-01-15 |

    Scenario: last_day
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT last_day(DATE'2024-01-15') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19753 | date | 2024-01-31 | 2024-01-31 |

    Scenario: next_day
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT next_day(DATE'2024-01-15', 'MON') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19744 | date | 2024-01-22 | 2024-01-22 |

    Scenario: date_trunc to the month
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(date_trunc('MONTH', DATE'2024-01-15') AS DATE) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19723 | date | 2024-01-01 | 2024-01-01 |

    Scenario: a date plus a day interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' + INTERVAL '2' DAY AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19739 | date | 2024-01-17 | 2024-01-17 |

    Scenario: a date plus a month interval
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' + INTERVAL '1' MONTH AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19768 | date | 2024-02-15 | 2024-02-15 |

    Scenario: a date column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT d AS v FROM VALUES (DATE'2024-01-15') AS t(d)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: the greatest of two dates
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT greatest(DATE'2024-01-15', DATE'2024-01-01') AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date out of an array
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT array(DATE'2024-01-15')[0] AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: the max of a date column
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT max(d) AS v FROM VALUES (DATE'2024-01-15'), (DATE'2024-01-01') AS t(d)
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date read in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DATE'2024-01-15' AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |

    Scenario: a date cast from a timestamp in another zone, named
      Given config spark.sql.session.timeZone = America/New_York
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' AS DATE) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 19737 | date | 2024-01-15 | 2024-01-15 |
