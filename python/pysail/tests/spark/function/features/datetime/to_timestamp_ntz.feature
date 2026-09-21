Feature: to_timestamp_ntz

  Rule: Result values (migrated from test_to_timestamp_ntz.txt doctests)

    Scenario: to_timestamp_ntz doctest #1 (result) — input LTZ timestamps under Amsterdam
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT ts FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | ts                  |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

    Scenario: to_timestamp_ntz doctest #2 (result)
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT to_timestamp_ntz(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | r                   |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

  Rule: Output schema (migrated from test_to_timestamp_ntz.txt printSchema doctests)

    # The inline-table TIMESTAMP column is non-nullable and the conversion cannot fail, so
    # Spark keeps the result non-nullable. Sail widens it.
    @sail-bug
    @function(nullability)
    Scenario: to_timestamp_ntz doctest #3 (schema)
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT to_timestamp_ntz(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp_ntz (nullable = false)
        """
