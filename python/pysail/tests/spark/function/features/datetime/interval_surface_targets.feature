Feature: the interval surface, vs Spark 4.2.0

  # Generated from `zz_david/verify/interval_sweep.py` by measuring every shape on the Spark 4.2
  # JVM (session time zone UTC, ANSI on) and on this branch. Every expected value is the JVM's.
  # A scenario without a tag is one this branch already answers, kept as the regression guard;
  # `@sail-bug` marks what is still open, so `grep @sail-bug` is the pending list. Regenerate
  # with `gen_interval_targets.py` after a fix and remove the tags it drops.

  Rule: M -- a computed interval gets the range Spark computes for it

    # The type of the result, which decides every cell the value lands in next.

    Scenario: computed type -- date - date is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' - DATE'2024-01-01') AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- NULL - date is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(NULL - DATE'2024-01-01') AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- ts - ts is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(TIMESTAMP'2024-01-15 10:00:00' - TIMESTAMP'2024-01-01 00:00:00') AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- ts - date is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(TIMESTAMP'2024-01-15 10:00:00' - DATE'2024-01-01') AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- date - ts is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00') AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- ym + ym is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' YEAR + INTERVAL '1' YEAR) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- month + month is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' MONTH + INTERVAL '1' MONTH) AS v
        """
      Then query result
        | v |
        | interval month |

    Scenario: computed type -- day + day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' DAY + INTERVAL '1' DAY) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- day + hour is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' DAY + INTERVAL '1' HOUR) AS v
        """
      Then query result
        | v |
        | interval day to hour |

    Scenario: computed type -- hour + hour is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' HOUR + INTERVAL '1' HOUR) AS v
        """
      Then query result
        | v |
        | interval hour |

    Scenario: computed type -- day - day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' DAY - INTERVAL '1' DAY) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- year * 2 is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' YEAR * 2) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: computed type -- day * 2 is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' DAY * 2) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- day / 2 is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' DAY / 2) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- -day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(-INTERVAL '2' DAY) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- abs(-day) is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(abs(INTERVAL '-2' DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- abs(-year) is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(abs(INTERVAL '-2' YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- make_dt_interval is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(make_dt_interval(1, 2, 3, 4.5)) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- make_ym_interval is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(make_ym_interval(1, 2)) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: computed type -- try_add day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_add(INTERVAL '2' DAY, INTERVAL '1' DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- try_add year is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_add(INTERVAL '2' YEAR, INTERVAL '1' YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- try_subtract year is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_subtract(INTERVAL '2' YEAR, INTERVAL '1' YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- try_multiply day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_multiply(INTERVAL '2' DAY, 2)) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- try_divide day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_divide(INTERVAL '2' DAY, 2)) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: computed type -- try_multiply year is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_multiply(INTERVAL '2' YEAR, 2)) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: computed type -- try_divide year is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_divide(INTERVAL '3' YEAR, 2)) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: computed type -- coalesce of days is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(coalesce(NULL, INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    @sail-bug
    Scenario: computed type -- nvl of years is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(nvl(NULL, INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- greatest of days is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(greatest(INTERVAL '1' DAY, INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: computed type -- least of years is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(least(INTERVAL '1' YEAR, INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: computed type -- if of hours is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(if(true, INTERVAL '1' HOUR, INTERVAL '2' HOUR)) AS v
        """
      Then query result
        | v |
        | interval hour |

    Scenario: computed type -- case of months is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CASE WHEN true THEN INTERVAL '1' MONTH ELSE INTERVAL '2' MONTH END) AS v
        """
      Then query result
        | v |
        | interval month |

    Scenario: computed type -- time - time is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(TIME '12:00:00' AS TIME(6)) - CAST(TIME '10:00:00' AS TIME(6))) AS v
        """
      Then query result
        | v |
        | interval hour to second |

    Scenario: computed type -- date + day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '2' DAY) AS v
        """
      Then query result
        | v |
        | date |

    Scenario: computed type -- date + hour is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS v
        """
      Then query result
        | v |
        | timestamp |

    Scenario: computed type -- ts + month is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(TIMESTAMP'2024-01-15 00:00:00' + INTERVAL '1' MONTH) AS v
        """
      Then query result
        | v |
        | timestamp |

    Scenario: computed type -- sequence step is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(sequence(DATE'2024-01-01', DATE'2024-01-05', INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | array<date> |

    Scenario: computed type -- extract day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(extract(DAY FROM INTERVAL '1 02:03:04' DAY TO SECOND)) AS v
        """
      Then query result
        | v |
        | int |

    @sail-bug
    Scenario: computed type -- date_part hour is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(date_part('HOUR', INTERVAL '1 02:03:04' DAY TO SECOND)) AS v
        """
      Then query result
        | v |
        | tinyint |

    Scenario: computed render -- date - date answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' - DATE'2024-01-01' AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14' DAY |

    Scenario: computed render -- ts - ts answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' - TIMESTAMP'2024-01-01 00:00:00' AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14 10:00:00' DAY TO SECOND |

    Scenario: computed render -- ts - date answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 10:00:00' - DATE'2024-01-01' AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14 10:00:00' DAY TO SECOND |

    Scenario: computed render -- date - ts answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00' AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14 00:00:00' DAY TO SECOND |

    Scenario: computed render -- ym + ym answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' YEAR + INTERVAL '1' YEAR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' YEAR |

    Scenario: computed render -- month + month answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' MONTH + INTERVAL '1' MONTH AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' MONTH |

    Scenario: computed render -- day + day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' DAY + INTERVAL '1' DAY AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: computed render -- day + hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' DAY + INTERVAL '1' HOUR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2 01' DAY TO HOUR |

    Scenario: computed render -- hour + hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' HOUR + INTERVAL '1' HOUR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '03' HOUR |

    Scenario: computed render -- day - day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' DAY - INTERVAL '1' DAY AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    Scenario: computed render -- year * 2 answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' YEAR * 2 AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '4-0' YEAR TO MONTH |

    Scenario: computed render -- day * 2 answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' DAY * 2 AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '4 00:00:00' DAY TO SECOND |

    Scenario: computed render -- day / 2 answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' DAY / 2 AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 00:00:00' DAY TO SECOND |

    Scenario: computed render -- -day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(-INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '-2' DAY |

    Scenario: computed render -- abs(-day) answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(abs(INTERVAL '-2' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: computed render -- abs(-year) answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(abs(INTERVAL '-2' YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' YEAR |

    Scenario: computed render -- make_dt_interval answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(make_dt_interval(1, 2, 3, 4.5) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02:03:04.5' DAY TO SECOND |

    Scenario: computed render -- make_ym_interval answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(make_ym_interval(1, 2) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1-2' YEAR TO MONTH |

    Scenario: computed render -- try_add day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_add(INTERVAL '2' DAY, INTERVAL '1' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: computed render -- try_add year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_add(INTERVAL '2' YEAR, INTERVAL '1' YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' YEAR |

    Scenario: computed render -- try_subtract year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_subtract(INTERVAL '2' YEAR, INTERVAL '1' YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' YEAR |

    Scenario: computed render -- try_multiply day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_multiply(INTERVAL '2' DAY, 2) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '4 00:00:00' DAY TO SECOND |

    Scenario: computed render -- try_divide day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_divide(INTERVAL '2' DAY, 2) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 00:00:00' DAY TO SECOND |

    Scenario: computed render -- try_multiply year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_multiply(INTERVAL '2' YEAR, 2) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '4-0' YEAR TO MONTH |

    Scenario: computed render -- try_divide year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(try_divide(INTERVAL '3' YEAR, 2) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1-6' YEAR TO MONTH |

    Scenario: computed render -- coalesce of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(coalesce(NULL, INTERVAL '2' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    @sail-bug
    Scenario: computed render -- nvl of years answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(nvl(NULL, INTERVAL '2' YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' YEAR |

    Scenario: computed render -- greatest of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(greatest(INTERVAL '1' DAY, INTERVAL '2' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: computed render -- least of years answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(least(INTERVAL '1' YEAR, INTERVAL '2' YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' YEAR |

    Scenario: computed render -- if of hours answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(if(true, INTERVAL '1' HOUR, INTERVAL '2' HOUR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '01' HOUR |

    Scenario: computed render -- case of months answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CASE WHEN true THEN INTERVAL '1' MONTH ELSE INTERVAL '2' MONTH END AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' MONTH |

    Scenario: computed render -- time - time answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(TIME '12:00:00' AS TIME(6)) - CAST(TIME '10:00:00' AS TIME(6)) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '02:00:00' HOUR TO SECOND |

    Scenario: computed render -- date + day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' + INTERVAL '2' DAY AS STRING) AS v
        """
      Then query result
        | v |
        | 2024-01-17 |

    Scenario: computed render -- date + hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(DATE'2024-01-15' + INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        | v |
        | 2024-01-16 01:00:00 |

    Scenario: computed render -- ts + month answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP'2024-01-15 00:00:00' + INTERVAL '1' MONTH AS STRING) AS v
        """
      Then query result
        | v |
        | 2024-02-15 00:00:00 |

    Scenario: computed render -- sequence step answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(sequence(DATE'2024-01-01', DATE'2024-01-05', INTERVAL '2' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | [2024-01-01, 2024-01-03, 2024-01-05] |

    Scenario: computed render -- extract day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(extract(DAY FROM INTERVAL '1 02:03:04' DAY TO SECOND) AS STRING) AS v
        """
      Then query result
        | v |
        | 1 |

    @sail-bug
    Scenario: computed render -- date_part hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(date_part('HOUR', INTERVAL '1 02:03:04' DAY TO SECOND) AS STRING) AS v
        """
      Then query result
        | v |
        | 2 |

  Rule: N -- a cast reads and writes the range

    Scenario: cast -- day to int answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '14' DAY AS INT) AS STRING) AS v
        """
      Then query result
        | v |
        | 14 |

    Scenario: cast -- hour to int answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '26' HOUR AS INT) AS STRING) AS v
        """
      Then query result
        | v |
        | 26 |

    @sail-bug
    Scenario: cast -- year to int answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '2' YEAR AS INT) AS STRING) AS v
        """
      Then query result
        | v |
        | 2 |

    @sail-bug
    Scenario: cast -- month to int answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '14' MONTH AS INT) AS STRING) AS v
        """
      Then query result
        | v |
        | 14 |

    Scenario: cast -- dts to decimal answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAY TO SECOND AS DECIMAL(20,6)) AS STRING) AS v
        """
      Then query result
        | v |
        | 93784.500000 |

    Scenario: cast -- int to day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(3 AS INTERVAL DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    @sail-bug
    Scenario: cast -- string to day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST('3' AS INTERVAL DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: cast -- day to string answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '3' DAY AS STRING) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: cast -- dts narrowed to day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '1 02:03:04' DAY TO SECOND AS INTERVAL DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    Scenario: cast -- day widened to dts answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '3' DAY AS INTERVAL DAY TO SECOND) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3 00:00:00' DAY TO SECOND |

    Scenario: cast -- ym narrowed to year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(INTERVAL '1-2' YEAR TO MONTH AS INTERVAL YEAR) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1' YEAR |

    Scenario: cast -- difference to string answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(CAST(DATE'2024-01-15' - DATE'2024-01-01' AS STRING) AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14' DAY |

    Scenario: cast type -- day to int is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '14' DAY AS INT)) AS v
        """
      Then query result
        | v |
        | int |

    Scenario: cast type -- hour to int is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '26' HOUR AS INT)) AS v
        """
      Then query result
        | v |
        | int |

    Scenario: cast type -- year to int is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '2' YEAR AS INT)) AS v
        """
      Then query result
        | v |
        | int |

    Scenario: cast type -- month to int is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '14' MONTH AS INT)) AS v
        """
      Then query result
        | v |
        | int |

    Scenario: cast type -- dts to decimal is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '1 02:03:04.5' DAY TO SECOND AS DECIMAL(20,6))) AS v
        """
      Then query result
        | v |
        | decimal(20,6) |

    Scenario: cast type -- int to day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(3 AS INTERVAL DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: cast type -- string to day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST('3' AS INTERVAL DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: cast type -- day to string is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '3' DAY AS STRING)) AS v
        """
      Then query result
        | v |
        | string |

    Scenario: cast type -- dts narrowed to day is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '1 02:03:04' DAY TO SECOND AS INTERVAL DAY)) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: cast type -- day widened to dts is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '3' DAY AS INTERVAL DAY TO SECOND)) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: cast type -- ym narrowed to year is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(INTERVAL '1-2' YEAR TO MONTH AS INTERVAL YEAR)) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: cast type -- difference to string is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(CAST(DATE'2024-01-15' - DATE'2024-01-01' AS STRING)) AS v
        """
      Then query result
        | v |
        | string |

  Rule: O -- the consumers of an interval answer what Spark answers

    # `hash`, `to_json`, `to_csv` and the container renderings all read the range.

    Scenario: consumer -- hash of a difference answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT hash(DATE'2024-01-15' - DATE'2024-01-01') IS NOT NULL AS v
        """
      Then query result
        | v |
        | true |

    Scenario: consumer -- hash of a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT hash(INTERVAL '2' DAY) IS NOT NULL AS v
        """
      Then query result
        | v |
        | true |

    Scenario: consumer -- xxhash64 of a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT xxhash64(INTERVAL '2' DAY) IS NOT NULL AS v
        """
      Then query result
        | v |
        | true |

    Scenario: consumer -- to_json of a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_json(named_struct('d', INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | {"d":"INTERVAL '2' DAY"} |

    Scenario: consumer -- to_json of a year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_json(named_struct('y', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | {"y":"INTERVAL '2' YEAR"} |

    Scenario: consumer -- to_csv of a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_csv(named_struct('d', INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    @sail-bug
    Scenario: consumer -- array of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(array(INTERVAL '1' DAY, INTERVAL '2' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | [INTERVAL '1' DAY, INTERVAL '2' DAY] |

    Scenario: consumer -- map of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(map('k', INTERVAL '1' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | {k -> INTERVAL '1' DAY} |

    Scenario: consumer -- struct of a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(named_struct('d', INTERVAL '1' DAY) AS STRING) AS v
        """
      Then query result
        | v |
        | {INTERVAL '1' DAY} |

    Scenario: consumer -- typeof array of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(array(INTERVAL '1' DAY, INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | array<interval day> |

    Scenario: consumer -- typeof struct of a year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(named_struct('y', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | struct<y:interval year> |

    Scenario: consumer -- typeof array of day and hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(array(INTERVAL '1' DAY, INTERVAL '2' HOUR)) AS v
        """
      Then query result
        | v |
        | array<interval day to hour> |

    Scenario: consumer -- typeof map of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(map('k', INTERVAL '1' DAY)) AS v
        """
      Then query result
        | v |
        | map<string,interval day> |

    Scenario: consumer -- typeof struct of a day and a year answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(named_struct('d', INTERVAL '1' DAY, 'y', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | struct<d:interval day,y:interval year> |

    Scenario: consumer -- typeof struct() of an hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(struct(INTERVAL '1' HOUR)) AS v
        """
      Then query result
        | v |
        | struct<col1:interval hour> |

    Scenario: consumer -- typeof map_from_arrays of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(map_from_arrays(array('k'), array(INTERVAL '1' DAY))) AS v
        """
      Then query result
        | v |
        | map<string,interval day> |

    Scenario: consumer -- typeof sort of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(sort_array(array(INTERVAL '2' DAY, INTERVAL '1' DAY))) AS v
        """
      Then query result
        | v |
        | array<interval day> |

    Scenario: consumer -- try_divide year by zero type answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(try_divide(INTERVAL '2' YEAR, 0)) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: consumer -- try_divide year by zero is null answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_divide(INTERVAL '2' YEAR, 0) IS NULL AS v
        """
      Then query result
        | v |
        | true |

    Scenario: consumer -- try_divide day by zero is null answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_divide(INTERVAL '2' DAY, 0) IS NULL AS v
        """
      Then query result
        | v |
        | true |

    @sail-bug
    Scenario: consumer -- sort of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(sort_array(array(INTERVAL '2' DAY, INTERVAL '1' DAY)) AS STRING) AS v
        """
      Then query result
        | v |
        | [INTERVAL '1' DAY, INTERVAL '2' DAY] |

  Rule: P -- an aggregate, a window and a set operation keep the range

    # `Sum`, `Min` and the window functions declare the child's type; a union widens to the common range (`day to hour` for DAY beside HOUR), not to DAY TO SECOND.

    Scenario: aggregate -- sum of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(sum(i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: aggregate -- avg of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(avg(i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | v |
        | INTERVAL '1 12:00:00' DAY TO SECOND |

    Scenario: aggregate -- try_sum of days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_sum(i) IS NOT NULL AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | v |
        | true |

    Scenario: aggregate -- min of years answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(min(i) AS STRING) AS v FROM VALUES (INTERVAL '1' YEAR), (INTERVAL '2' YEAR) AS t(i)
        """
      Then query result
        | v |
        | INTERVAL '1' YEAR |

    Scenario: aggregate -- sum of differences answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(sum(d) AS STRING) AS v FROM VALUES (DATE'2024-01-15' - DATE'2024-01-01') AS t(d)
        """
      Then query result
        | v |
        | INTERVAL '14' DAY |

    Scenario: aggregate -- group by a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT count(*) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '1' DAY) AS t(i) GROUP BY i
        """
      Then query result
        | v |
        | 2 |

    Scenario: aggregate -- order by a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(i AS STRING) AS v FROM VALUES (INTERVAL '2' DAY), (INTERVAL '1' DAY) AS t(i) ORDER BY i LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    Scenario: aggregate -- distinct days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT count(DISTINCT i) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '1' DAY) AS t(i)
        """
      Then query result
        | v |
        | 1 |

    Scenario: aggregate -- join on a day answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT count(*) AS v FROM VALUES (INTERVAL '1' DAY) AS a(i) JOIN VALUES (INTERVAL '1' DAY) AS b(j) ON a.i = b.j
        """
      Then query result
        | v |
        | 1 |

    Scenario: aggregate -- window over days answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(lag(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    Scenario: aggregate -- union of ranges answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DISTINCT typeof(i) AS v FROM (SELECT INTERVAL '1' DAY AS i UNION ALL SELECT INTERVAL '2' DAY)
        """
      Then query result
        | v |
        | interval day |

    @sail-bug
    Scenario: aggregate -- union of day and hour answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT DISTINCT typeof(i) AS v FROM (SELECT INTERVAL '1' DAY AS i UNION ALL SELECT INTERVAL '2' HOUR)
        """
      Then query result
        | v |
        | interval day to hour |

  Rule: Q -- a declared interval keeps its range

    Scenario: declared type -- INTERVAL '2' YEAR is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '2' YEAR) AS v
        """
      Then query result
        | v |
        | interval year |

    Scenario: declared type -- INTERVAL '14' MONTH is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '14' MONTH) AS v
        """
      Then query result
        | v |
        | interval month |

    Scenario: declared type -- INTERVAL '1-2' YEAR TO MONTH is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '1-2' YEAR TO MONTH) AS v
        """
      Then query result
        | v |
        | interval year to month |

    Scenario: declared type -- INTERVAL '3' DAY is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '3' DAY) AS v
        """
      Then query result
        | v |
        | interval day |

    Scenario: declared type -- INTERVAL '25' HOUR is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '25' HOUR) AS v
        """
      Then query result
        | v |
        | interval hour |

    Scenario: declared type -- INTERVAL '90' MINUTE is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '90' MINUTE) AS v
        """
      Then query result
        | v |
        | interval minute |

    Scenario: declared type -- INTERVAL '5.5' SECOND is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '5.5' SECOND) AS v
        """
      Then query result
        | v |
        | interval second |

    Scenario: declared type -- INTERVAL '1 02' DAY TO HOUR is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '1 02' DAY TO HOUR) AS v
        """
      Then query result
        | v |
        | interval day to hour |

    Scenario: declared type -- INTERVAL '1 02:03' DAY TO MINUTE is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '1 02:03' DAY TO MINUTE) AS v
        """
      Then query result
        | v |
        | interval day to minute |

    Scenario: declared type -- INTERVAL '1 02:03:04' DAY TO SECOND is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '1 02:03:04' DAY TO SECOND) AS v
        """
      Then query result
        | v |
        | interval day to second |

    Scenario: declared type -- INTERVAL '02:03' HOUR TO MINUTE is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '02:03' HOUR TO MINUTE) AS v
        """
      Then query result
        | v |
        | interval hour to minute |

    Scenario: declared type -- INTERVAL '02:03:04' HOUR TO SECOND is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '02:03:04' HOUR TO SECOND) AS v
        """
      Then query result
        | v |
        | interval hour to second |

    Scenario: declared type -- INTERVAL '03:04' MINUTE TO SECOND is what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(INTERVAL '03:04' MINUTE TO SECOND) AS v
        """
      Then query result
        | v |
        | interval minute to second |

    Scenario: declared render -- INTERVAL '2' YEAR answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '2' YEAR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' YEAR |

    Scenario: declared render -- INTERVAL '14' MONTH answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '14' MONTH AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '14' MONTH |

    Scenario: declared render -- INTERVAL '1-2' YEAR TO MONTH answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '1-2' YEAR TO MONTH AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1-2' YEAR TO MONTH |

    Scenario: declared render -- INTERVAL '3' DAY answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '3' DAY AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: declared render -- INTERVAL '25' HOUR answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '25' HOUR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '25' HOUR |

    Scenario: declared render -- INTERVAL '90' MINUTE answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '90' MINUTE AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '90' MINUTE |

    Scenario: declared render -- INTERVAL '5.5' SECOND answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '5.5' SECOND AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '05.5' SECOND |

    Scenario: declared render -- INTERVAL '1 02' DAY TO HOUR answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '1 02' DAY TO HOUR AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02' DAY TO HOUR |

    Scenario: declared render -- INTERVAL '1 02:03' DAY TO MINUTE answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '1 02:03' DAY TO MINUTE AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02:03' DAY TO MINUTE |

    Scenario: declared render -- INTERVAL '1 02:03:04' DAY TO SECOND answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '1 02:03:04' DAY TO SECOND AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

    Scenario: declared render -- INTERVAL '02:03' HOUR TO MINUTE answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '02:03' HOUR TO MINUTE AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '02:03' HOUR TO MINUTE |

    Scenario: declared render -- INTERVAL '02:03:04' HOUR TO SECOND answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '02:03:04' HOUR TO SECOND AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '02:03:04' HOUR TO SECOND |

    Scenario: declared render -- INTERVAL '03:04' MINUTE TO SECOND answers what Spark says
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(INTERVAL '03:04' MINUTE TO SECOND AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '03:04' MINUTE TO SECOND |
