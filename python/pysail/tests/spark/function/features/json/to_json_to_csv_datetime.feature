Feature: to_json and to_csv write a date or an interval the way Spark writes it

  # Measured on the Spark 4.2 JVM (session time zone UTC) with
  # `zz_david/verify/to_json_csv_datetime_hunt.py`, which sweeps the date and interval surface of
  # both writers: every range of both families, NULLs, a column, nesting and the format options
  # that ride on the same writer. Spark writes an interval as its `toString`, with the fields of
  # its range, so the range has to reach the writer through the field metadata.

  Rule: an interval is written with the fields of its range

    Scenario: to_json struct of day
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '2' DAY"} |

    Scenario: to_csv struct of day
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '2' DAY)) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    Scenario: to_json struct of hour
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '25' HOUR)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '25' HOUR"} |

    Scenario: to_csv struct of hour
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '25' HOUR)) AS v
        """
      Then query result
        | v |
        | INTERVAL '25' HOUR |

    Scenario: to_json struct of minute
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '90' MINUTE)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '90' MINUTE"} |

    Scenario: to_csv struct of minute
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '90' MINUTE)) AS v
        """
      Then query result
        | v |
        | INTERVAL '90' MINUTE |

    Scenario: to_json struct of second
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '5.5' SECOND)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '05.5' SECOND"} |

    Scenario: to_csv struct of second
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '5.5' SECOND)) AS v
        """
      Then query result
        | v |
        | INTERVAL '05.5' SECOND |

    Scenario: to_json struct of day to hour
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '1 02' DAY TO HOUR)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '1 02' DAY TO HOUR"} |

    Scenario: to_csv struct of day to hour
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '1 02' DAY TO HOUR)) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02' DAY TO HOUR |

    Scenario: to_json struct of day to second
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '1 02:03:04' DAY TO SECOND)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '1 02:03:04' DAY TO SECOND"} |

    Scenario: to_csv struct of day to second
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '1 02:03:04' DAY TO SECOND)) AS v
        """
      Then query result
        | v |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

    Scenario: to_json struct of hour to minute
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '02:03' HOUR TO MINUTE)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '02:03' HOUR TO MINUTE"} |

    Scenario: to_csv struct of hour to minute
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '02:03' HOUR TO MINUTE)) AS v
        """
      Then query result
        | v |
        | INTERVAL '02:03' HOUR TO MINUTE |

    Scenario: to_json struct of year
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '2' YEAR"} |

    Scenario: to_csv struct of year
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' YEAR |

    Scenario: to_json struct of month
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '14' MONTH)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '14' MONTH"} |

    Scenario: to_csv struct of month
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '14' MONTH)) AS v
        """
      Then query result
        | v |
        | INTERVAL '14' MONTH |

    Scenario: to_json struct of year to month
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', INTERVAL '1-2' YEAR TO MONTH)) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '1-2' YEAR TO MONTH"} |

    Scenario: to_csv struct of year to month
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', INTERVAL '1-2' YEAR TO MONTH)) AS v
        """
      Then query result
        | v |
        | INTERVAL '1-2' YEAR TO MONTH |

    Scenario: to_json struct of date
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', DATE'2024-01-15')) AS v
        """
      Then query result
        | v |
        | {"v":"2024-01-15"} |

    Scenario: to_csv struct of date
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', DATE'2024-01-15')) AS v
        """
      Then query result
        | v |
        | 2024-01-15 |

    @sail-bug
    Scenario: to_json struct of timestamp
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', TIMESTAMP'2024-01-15 10:00:00')) AS v
        """
      Then query result
        | v |
        | {"v":"2024-01-15T10:00:00.000Z"} |

    Scenario: to_csv struct of timestamp
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', TIMESTAMP'2024-01-15 10:00:00')) AS v
        """
      Then query result
        | v |
        | 2024-01-15T10:00:00.000Z |

    @sail-bug
    Scenario: to_json struct of timestamp_ntz
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIMESTAMP_NTZ))) AS v
        """
      Then query result
        | v |
        | {"v":"2024-01-15T10:00:00.000"} |

    Scenario: to_csv struct of timestamp_ntz
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', CAST(TIMESTAMP'2024-01-15 10:00:00' AS TIMESTAMP_NTZ))) AS v
        """
      Then query result
        | v |
        | 2024-01-15T10:00:00.000 |

    Scenario: to_json struct of date difference
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', (DATE'2024-01-15' - DATE'2024-01-01'))) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '14' DAY"} |

    Scenario: to_csv struct of date difference
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', (DATE'2024-01-15' - DATE'2024-01-01'))) AS v
        """
      Then query result
        | v |
        | INTERVAL '14' DAY |

    Scenario: to_json struct of timestamp difference
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', (TIMESTAMP'2024-01-15 10:00:00' - TIMESTAMP'2024-01-01 00:00:00'))) AS v
        """
      Then query result
        | v |
        | {"v":"INTERVAL '14 10:00:00' DAY TO SECOND"} |

    Scenario: to_csv struct of timestamp difference
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', (TIMESTAMP'2024-01-15 10:00:00' - TIMESTAMP'2024-01-01 00:00:00'))) AS v
        """
      Then query result
        | v |
        | INTERVAL '14 10:00:00' DAY TO SECOND |

    Scenario: to_json a NULL interval
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', CAST(NULL AS INTERVAL DAY))) AS v
        """
      Then query result
        | v |
        | {} |

    Scenario: to_json a NULL date
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', CAST(NULL AS DATE))) AS v
        """
      Then query result
        | v |
        | {} |

    Scenario: to_json an interval column
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', i)) AS v FROM VALUES (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | v |
        | {"v":"INTERVAL '2' DAY"} |

    Scenario: to_csv an interval column
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', i)) AS v FROM VALUES (INTERVAL '2' DAY) AS t(i)
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |

    @sail-bug
    Scenario: to_json an array of intervals
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', array(INTERVAL '2' DAY))) AS v
        """
      Then query result
        | v |
        | {"v":["INTERVAL '2' DAY"]} |

    Scenario: to_json a map of intervals
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', map('k', INTERVAL '2' DAY))) AS v
        """
      Then query result
        | v |
        | {"v":{"k":"INTERVAL '2' DAY"}} |

    Scenario: to_json a struct of a struct
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', named_struct('w', INTERVAL '2' DAY))) AS v
        """
      Then query result
        | v |
        | {"v":{"w":"INTERVAL '2' DAY"}} |

    Scenario: to_json two ranges side by side
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('d', INTERVAL '2' DAY, 'y', INTERVAL '2' YEAR)) AS v
        """
      Then query result
        | v |
        | {"d":"INTERVAL '2' DAY","y":"INTERVAL '2' YEAR"} |

    Scenario: to_json with a timestampFormat
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', TIMESTAMP'2024-01-15 10:00:00'), map('timestampFormat', 'yyyy/MM/dd')) AS v
        """
      Then query result
        | v |
        | {"v":"2024/01/15"} |

    Scenario: to_json with a dateFormat
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', DATE'2024-01-15'), map('dateFormat', 'yyyy/MM/dd')) AS v
        """
      Then query result
        | v |
        | {"v":"2024/01/15"} |

    Scenario: to_csv with a dateFormat
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', DATE'2024-01-15'), map('dateFormat', 'yyyy/MM/dd')) AS v
        """
      Then query result
        | v |
        | 2024/01/15 |

    @sail-bug
    Scenario: from_json of an interval
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(from_json('{"v":"INTERVAL \'2\' DAY"}', 'v INTERVAL DAY').v AS STRING) AS v
        """
      Then query result
        | v |
        | INTERVAL '2' DAY |
