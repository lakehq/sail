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

  Rule: to_timestamp_ntz does NOT accept numeric input, unlike to_timestamp
    # ParseToTimestamp.inputTypes adds NumericType to the accepted set only when the
    # target `dataType.isInstanceOf[TimestampType]`, and TimestampNTZType is a SIBLING of
    # TimestampType, not a subclass. So for an NTZ target the number is implicitly cast to
    # STRING first and then parsed, and the text '1' is not a timestamp. Plain
    # to_timestamp instead casts the number to TIMESTAMP with SECONDS semantics and
    # succeeds. An implementation that routes both through one numeric path diverges on
    # exactly one of them, so the pair below is what discriminates.
    # Measured on Spark JVM 4.2.0.

    Scenario: to_timestamp accepts a number, as seconds since the epoch
      # The contrasting half of the pair. That Sail reads the number as MICROseconds is
      # covered by its own scenarios in to_timestamp.feature; the value 0 is used here
      # precisely because it is identical under both readings, so this half stays green
      # and only the to_timestamp_ntz half below carries the divergence.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(0) AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

    @sail-bug
    Scenario Outline: to_timestamp_ntz rejects a non-parseable type under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ntz(<input>) AS result
        """
      Then query error cannot be cast to "TIMESTAMP_NTZ" because it is malformed

      Examples:
        | case    | input                      |
        | zero    | 0                          |
        | integer | 1                          |
        | double  | 1.5                        |
        | decimal | CAST(1.5 AS DECIMAL(10,2)) |
        | boolean | true                       |
        | binary  | X'48656C6C6F'              |

    @sail-bug
    Scenario Outline: to_timestamp_ntz turns the same input into NULL without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp_ntz(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case    | input                      |
        | zero    | 0                          |
        | integer | 1                          |
        | double  | 1.5                        |
        | decimal | CAST(1.5 AS DECIMAL(10,2)) |
        | boolean | true                       |
        | binary  | X'48656C6C6F'              |
