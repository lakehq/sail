Feature: a window over a date or an interval answers what Spark answers

  # Measured on the Spark 4.2 JVM with `zz_david/verify/window_interval_hunt.py`, which sweeps the
  # window surface of an interval: the ORDER BY key and the aggregated VALUE are separate leaves,
  # because only the key builds a frame. A day-time key is ordered by its microseconds, which order
  # identically, so the frame can be built on a type DataFusion accepts.

  Rule: a window over an interval

    Scenario: lag over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(lag(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: lead over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(lead(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: row_number over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT row_number() OVER (ORDER BY i) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i LIMIT 1
        """
      Then query result
        | v |
        | 1 |

    Scenario: rank over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT rank() OVER (ORDER BY i) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | 3 |

    Scenario: sum over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '6' DAY |

    Scenario: count over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT count(*) OVER (ORDER BY i) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | 3 |

    Scenario: first over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(first_value(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    @sail-bug
    Scenario: lag over a year-month key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(lag(i) OVER (ORDER BY i) AS STRING) AS v FROM VALUES (INTERVAL '1' YEAR), (INTERVAL '2' YEAR) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1' YEAR |

    Scenario: partition by an interval
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT count(*) OVER (PARTITION BY i) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) LIMIT 1
        """
      Then query result
        | v |
        | 1 |

    Scenario: lag of an interval over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(lag(i) OVER (ORDER BY d) AS STRING) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) ORDER BY d DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1' DAY |

    Scenario: sum of an interval over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY d) AS STRING) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) ORDER BY d DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: max of an interval over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(max(i) OVER (ORDER BY d) AS STRING) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) ORDER BY d DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: avg of an interval over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(avg(i) OVER (ORDER BY d) AS STRING) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) ORDER BY d DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '1 12:00:00' DAY TO SECOND |

    Scenario: rows frame over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '6' DAY |

    Scenario: range frame over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '6' DAY |

    @sail-bug
    Scenario: range offset over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY i RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS STRING) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) ORDER BY i DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '5' DAY |

    Scenario: range offset over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(sum(i) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS STRING) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) ORDER BY d DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '3' DAY |

    Scenario: lag over a date difference
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(lag(x) OVER (ORDER BY x) AS STRING) AS v FROM (SELECT DATE'2024-01-15' - DATE'2024-01-01' AS x UNION ALL SELECT DATE'2024-01-20' - DATE'2024-01-01') ORDER BY x DESC LIMIT 1
        """
      Then query result
        | v |
        | INTERVAL '14' DAY |

    Scenario: typeof lag over an interval key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT typeof(lag(i) OVER (ORDER BY i)) AS v FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(i) LIMIT 1
        """
      Then query result
        | v |
        | interval day |

    Scenario: typeof sum over a date key
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT typeof(sum(i) OVER (ORDER BY d)) AS v FROM VALUES (DATE'2024-01-01', INTERVAL '1' DAY), (DATE'2024-01-02', INTERVAL '2' DAY) AS t(d, i) LIMIT 1
        """
      Then query result
        | v |
        | interval day |
