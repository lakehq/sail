Feature: weekday output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to weekday yields the schema Spark declares
      When query
        """
        SELECT weekday('2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to weekday yields the schema Spark declares
      When query
        """
        SELECT weekday(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to weekday stays nullable
      When query
        """
        SELECT weekday(c) AS result FROM VALUES ('2009-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: weekday is Monday-based

    # Spark's weekday is Monday-based (Monday=0 .. Sunday=6). DataFusion's DOW is
    # Sunday-based (Sunday=0 .. Saturday=6), so the mapping is a rotation,
    # `(DOW + 6) % 7`, not a subtraction: `DOW - 1` sends Sunday to -1.

    @sail-bug
    Scenario: weekday of a sunday is 6
      When query
        """
        SELECT weekday(DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 6      |

    # The whole week through a column also exercises the columnar kernel, which a
    # constant-folded literal never reaches. Only the Sunday row is wrong, so a
    # single-value column would hide the bug entirely.
    @sail-bug
    Scenario: weekday over a full week from a column
      When query
        """
        SELECT weekday(c) AS result FROM VALUES
          (DATE '2024-03-11'), (DATE '2024-03-12'), (DATE '2024-03-13'),
          (DATE '2024-03-14'), (DATE '2024-03-15'), (DATE '2024-03-16'),
          (DATE '2024-03-17') AS t(c)
        """
      Then query result
        | result |
        | 0      |
        | 1      |
        | 2      |
        | 3      |
        | 4      |
        | 5      |
        | 6      |

    # dayofweek is Sunday-based and already correct; it pins that a weekday fix
    # must not be applied to the shared DOW helper in a way that breaks it.
    @sail-bug
    Scenario: weekday stays consistent with dayofweek
      When query
        """
        SELECT weekday(c) AS weekday, dayofweek(c) AS dayofweek FROM VALUES
          (DATE '2024-03-16'), (DATE '2024-03-17') AS t(c)
        """
      Then query result
        | weekday | dayofweek |
        | 5       | 7         |
        | 6       | 1         |

  Rule: null handling

    @sail-bug
    Scenario: weekday of null
      When query
        """
        SELECT weekday(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: weekday of a typed null
      When query
        """
        SELECT weekday(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: weekday of an all-null column
      When query
        """
        SELECT weekday(c) AS result FROM VALUES (NULL) AS t(c)
        """
      Then query result
        | result |
        | NULL   |

  Rule: accepted input types

    @sail-bug
    Scenario: weekday of a timestamp
      When query
        """
        SELECT weekday(TIMESTAMP '2024-03-17 12:34:56') AS result
        """
      Then query result
        | result |
        | 6      |

    @sail-bug
    Scenario: weekday of a timestamp_ntz
      When query
        """
        SELECT weekday(TIMESTAMP_NTZ '2024-03-17 12:34:56') AS result
        """
      Then query result
        | result |
        | 6      |

    # Spark rejects numeric input at analysis time with a data type mismatch.
    @sail-bug
    Scenario: weekday rejects a numeric argument
      When query
        """
        SELECT weekday(1) AS result
        """
      Then query error due to data type mismatch

  Rule: implicit string to date casting

    # Spark casts the string to a date leniently: non-zero-padded parts, surrounding
    # whitespace, and truncated year / year-month forms are all accepted.

    @sail-bug
    Scenario: weekday accepts a date string
      When query
        """
        SELECT weekday('2024-03-17') AS result
        """
      Then query result
        | result |
        | 6      |

    @sail-bug
    Scenario: weekday accepts a non-zero-padded date string
      When query
        """
        SELECT weekday('2024-3-7') AS result
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    Scenario: weekday accepts a whitespace padded date string
      When query
        """
        SELECT weekday('  2024-03-17  ') AS result
        """
      Then query result
        | result |
        | 6      |

    @sail-bug
    Scenario: weekday of a year-only string defaults to january first
      When query
        """
        SELECT weekday('2024') AS result
        """
      Then query result
        | result |
        | 0      |

    @sail-bug
    Scenario: weekday of a year-month string defaults to the first
      When query
        """
        SELECT weekday('2024-03') AS result
        """
      Then query result
        | result |
        | 4      |

  Rule: unparseable date strings honour ANSI mode

    @sail-bug
    Scenario: weekday of an unparseable string errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT weekday('not-a-date') AS result
        """
      Then query error The value .not-a-date. of the type .STRING. cannot be cast to .DATE.

    @sail-bug
    Scenario: weekday of an unparseable string is null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT weekday('not-a-date') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: weekday of an out-of-range date string is null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT weekday('2024-13-45') AS result
        """
      Then query result
        | result |
        | NULL   |

    # The unparseable value must resolve to NULL for its own row only. Sail aborts
    # the whole batch, so one bad row takes the entire query down.
    @sail-bug
    Scenario: weekday resolves an unparseable row to null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT weekday(c) AS result FROM VALUES
          ('2024-03-11'), ('not-a-date'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 0      |
        | NULL   |
        | NULL   |

  Rule: date range boundaries

    Scenario: weekday at the minimum date
      When query
        """
        SELECT weekday(DATE '0001-01-01') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: weekday at the maximum date
      When query
        """
        SELECT weekday(DATE '9999-12-31') AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: weekday of a leap day
      When query
        """
        SELECT weekday(DATE '2024-02-29') AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: weekday does not depend on the session time zone

    # A date literal is interpreted in the session time zone, so the local date -
    # and therefore the weekday - is the same in every zone. Verified against the
    # JVM in UTC, America/New_York, Asia/Kolkata, Pacific/Kiritimati, Pacific/Niue
    # and Australia/Lord_Howe.
    @sail-bug
    Scenario: weekday of a sunday in a non-utc session time zone
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT weekday(DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: return type

    Scenario: weekday returns a non-nullable integer for a non-null literal
      When query
        """
        SELECT weekday(DATE '2024-03-11') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: weekday returns a nullable integer for a nullable column
      When query
        """
        SELECT weekday(c) AS result FROM VALUES (DATE '2024-03-11'), (NULL) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # Spark 4.2.0 Cast.forceNullable: (TimestampNTZType, DateType) falls into `case (_, DateType)
    # => true`, so the implicit cast of a TIMESTAMP_NTZ to DATE makes the result nullable.
    @sail-bug
    Scenario: weekday returns a nullable integer for a non-null timestamp_ntz literal
      When query
        """
        SELECT weekday(TIMESTAMP_NTZ '2024-01-01 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
