Feature: try_to_date (never-throwing variant of to_date)
  # try_to_date parses a string to a date and "always returns null on an invalid input
  # with/without ANSI SQL mode enabled" (DESCRIBE FUNCTION EXTENDED, Spark 4.2.0,
  # org.apache.spark.sql.catalyst.expressions.TryToDateExpressionBuilder, Since 4.0.0).
  #
  # Sail does not register this function at all: every scenario below fails with
  # "unknown function: try_to_date", so the whole file is @sail-bug. The gold data already
  # carried Spark's answers for three of these queries
  # (crates/sail-spark-connect/tests/gold_data/function/datetime.json).
  #
  # All expected values were captured on Spark JVM 4.2.0 with the session time zone set
  # to UTC, which is what the test harness uses.

  @sail-bug
  Rule: Valid input parses, and the value error is swallowed

    Scenario Outline: Valid input: <case>
      When query
        """
        SELECT try_to_date(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                       | result       |
        | date only                       | '2016-12-31'               | 2016-12-31   |
        | date with explicit format       | '2016-12-31', 'yyyy-MM-dd' | 2016-12-31   |
        | timestamp string truncates      | '2016-12-31 00:12:00'      | 2016-12-31   |
        | single-digit month and day      | '2024-1-5'                 | 2024-01-05   |
        | leading plus sign on the year   | '+2024-01-15'              | 2024-01-15   |
        | leap day                        | '2024-02-29'               | 2024-02-29   |
        | first representable date        | '0001-01-01'               | 0001-01-01   |
        | last four-digit year            | '9999-12-31'               | 9999-12-31   |
        | five-digit year gets a plus     | '10000-01-01'              | +10000-01-01 |
        | year zero                       | '0000-12-31'               | 0000-12-31   |
        | negative year                   | '-0001-01-01'              | -0001-01-01  |
        | ISO T separator                 | '2024-01-15T12:00:00'      | 2024-01-15   |
        | trailing zone designator        | '2024-01-15 12:00:00Z'     | 2024-01-15   |
        | trailing numeric offset         | '2024-01-15 12:00:00+05:30' | 2024-01-15  |
        | leading whitespace is trimmed   | ' 2024-01-15'              | 2024-01-15   |
        | surrounding whitespace trimmed  | '  2024-01-15  '           | 2024-01-15   |
        | trailing whitespace is trimmed  | '2024-01-15 '              | 2024-01-15   |

  @sail-bug
  Rule: Invalid input is NULL in BOTH ANSI modes
    # This is the whole point of the try_ variant: unlike to_date, the ANSI setting does
    # not change the outcome. Asserting only one ANSI mode would not discriminate it from
    # plain to_date, so both are asserted for the same inputs.

    Scenario Outline: Invalid input with ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_date(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                   | input           |
        | not a date at all      | 'foo'           |
        | day out of range       | '2023-02-29'    |
        | trailing garbage       | '2024-01-15xyz' |
        | empty string           | ''              |
        | whitespace only        | '   '           |
        | numeric input          | 1               |
        | boolean input          | true            |
        | binary input           | X'48656C6C6F'   |
        | TIME input             | TIME '12:30:00' |

    Scenario Outline: Invalid input with ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_to_date(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                   | input           |
        | not a date at all      | 'foo'           |
        | day out of range       | '2023-02-29'    |
        | trailing garbage       | '2024-01-15xyz' |
        | empty string           | ''              |
        | whitespace only        | '   '           |
        | numeric input          | 1               |
        | boolean input          | true            |
        | binary input           | X'48656C6C6F'   |
        | TIME input             | TIME '12:30:00' |

  @sail-bug
  Rule: Datetime input types are converted, not parsed

    Scenario Outline: Datetime input: <case>
      When query
        """
        SELECT try_to_date(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case          | input                                 | result     |
        | DATE          | DATE '2024-01-15'                     | 2024-01-15 |
        | TIMESTAMP     | TIMESTAMP '2024-01-15 12:00:00'       | 2024-01-15 |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-15 12:00:00'   | 2024-01-15 |

  @sail-bug
  Rule: A malformed PATTERN still raises — only VALUE errors are swallowed
    # The try_ prefix does not make the function total: an unrecognized datetime pattern
    # is an error in both ANSI modes. The NULL-value case below is what discriminates a
    # lazy formatter (Spark) from an eager one: with a NULL value Spark never builds the
    # formatter, so the very same bad pattern yields NULL instead of raising.

    Scenario: an unrecognized pattern raises even for the try_ variant
      When query
        """
        SELECT try_to_date('2016-12-31', 'qqq') AS result
        """
      Then query error Unrecognized datetime pattern

    Scenario: a NULL value with a bad pattern is NULL, because the pattern is never read
      When query
        """
        SELECT try_to_date(CAST(NULL AS STRING), 'qqq') AS result
        """
      Then query result
        | result |
        | NULL   |

  @sail-bug
  Rule: NULL and empty format handling

    Scenario Outline: NULL and empty formats: <case>
      When query
        """
        SELECT try_to_date(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                     | args                                        |
        | untyped NULL value       | NULL                                        |
        | typed NULL value         | CAST(NULL AS STRING)                        |
        | NULL format              | '2016-12-31', CAST(NULL AS STRING)          |
        | both NULL                | CAST(NULL AS STRING), CAST(NULL AS STRING)  |
        | empty format             | '2016-12-31', ''                            |
        | whitespace-only format   | '2016-12-31', '   '                         |

  @sail-bug
  Rule: Argument count

    Scenario Outline: Argument count: <case>
      When query
        """
        SELECT try_to_date(<args>)
        """
      Then query error (?i)try_to_date.? requires

      Examples:
        | case      | args                                     |
        | zero args |                                          |
        | three args | '2016-12-31', 'yyyy-MM-dd', 'yyyy-MM-dd' |

  @sail-bug
  Rule: The value and the format may come from a column
    # A behaviour-governing argument given as a literal is constant-folded, so the literal
    # scenarios above never exercise the columnar kernel. These pass the same arguments
    # through columns, with rows that differ from each other so that a row-0 broadcast
    # would be visible.

    Scenario: the value comes from a column and is resolved per row
      When query
        """
        SELECT try_to_date(c) AS result FROM VALUES ('2016-12-31'), ('nope'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result
        | result     |
        | 2016-12-31 |
        | NULL       |
        | NULL       |

    Scenario: paired value and format columns are resolved per row
      When query
        """
        SELECT try_to_date(a, b) AS result FROM VALUES
          ('2016-12-31', 'yyyy-MM-dd'),
          ('31/12/2016', 'dd/MM/yyyy'),
          ('nope', 'yyyy-MM-dd') AS t(a, b)
        """
      Then query result
        | result     |
        | 2016-12-31 |
        | 2016-12-31 |
        | NULL       |

    Scenario: a non-foldable format still parses
      When query
        """
        SELECT try_to_date('2016-12-31', IF(rand() < 2, 'yyyy-MM-dd', 'x')) AS result
        """
      Then query result
        | result     |
        | 2016-12-31 |

  @sail-bug
  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal yields a nullable date
      When query
        """
        SELECT try_to_date('2016-12-31 00:12:00') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null string column yields a nullable date
      When query
        """
        SELECT try_to_date('2016-12-31 00:12:00') AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT try_to_date(c) AS result FROM VALUES ('2016-12-31 00:12:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null DATE input is NOT nullable
      # The load-bearing half of the pair: try_to_date does not force the result nullable
      # when the input needs no parsing, so a hardcoded `true` would fail here.
      When query
        """
        SELECT try_to_date(DATE '2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a non-null TIMESTAMP input is NOT nullable
      When query
        """
        SELECT try_to_date(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """
