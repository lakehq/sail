@spark-4
Feature: try_to_date
  Safe variant of to_date that returns NULL on parse failure
  instead of throwing an exception. Strict to_date throws.

  Rule: Single-argument form parses with default formats

    @sail-bug
    Scenario: ISO date string
      When query
      """
      SELECT try_to_date('2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: Beginning of epoch
      When query
      """
      SELECT try_to_date('1970-01-01') AS result
      """
      Then query result
      | result     |
      | 1970-01-01 |

    @sail-bug
    Scenario: Leap year valid date
      When query
      """
      SELECT try_to_date('2024-02-29') AS result
      """
      Then query result
      | result     |
      | 2024-02-29 |

    @sail-bug
    Scenario: Non-leap year invalid date returns NULL
      When query
      """
      SELECT try_to_date('2023-02-29') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_date('not-a-date') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_date('') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Trailing garbage parses prefix (matches Spark lenient behavior)
      When query
      """
      SELECT try_to_date('2024-01-15 garbage') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: Out-of-range month returns NULL
      When query
      """
      SELECT try_to_date('2024-13-01') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Out-of-range day returns NULL
      When query
      """
      SELECT try_to_date('2024-01-32') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Year boundary low
      When query
      """
      SELECT try_to_date('0001-01-01') AS result
      """
      Then query result
      | result     |
      | 0001-01-01 |

    @sail-bug
    Scenario: Year boundary high
      When query
      """
      SELECT try_to_date('9999-12-31') AS result
      """
      Then query result
      | result     |
      | 9999-12-31 |

    @sail-bug
    Scenario: NULL input
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Cast from timestamp preserves date
      When query
      """
      SELECT try_to_date(TIMESTAMP '2024-01-15 10:30:00') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: Cast from date is identity
      When query
      """
      SELECT try_to_date(DATE '2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

  Rule: Two-argument form parses with format string

    @sail-bug
    Scenario: Spark yyyy-MM-dd format
      When query
      """
      SELECT try_to_date('2024-01-15', 'yyyy-MM-dd') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: US-style MM/dd/yyyy format
      When query
      """
      SELECT try_to_date('01/15/2024', 'MM/dd/yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: European dd-MM-yyyy format
      When query
      """
      SELECT try_to_date('15-01-2024', 'dd-MM-yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    @sail-bug
    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_date('2024-01-15', 'MM/dd/yyyy') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Garbage with format returns NULL
      When query
      """
      SELECT try_to_date('garbage', 'yyyy-MM-dd') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: NULL value with format returns NULL
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING), 'yyyy-MM-dd') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: NULL format returns NULL
      When query
      """
      SELECT try_to_date('2024-01-15', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Per-row format (column-expression format)

    @sail-bug
    Scenario: Different format per row all parse
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('15/01/2024', 'dd/MM/yyyy'),
        ('Jan 15 2024', 'MMM dd yyyy') AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | 2024-01-15 |
      | 2024-01-15 |

    @sail-bug
    Scenario: Per-row format with one invalid row returns NULL only there
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('garbage', 'yyyy-MM-dd'),
        ('15/01/2024', 'dd/MM/yyyy') AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |
      | 2024-01-15 |

    @sail-bug
    Scenario: NULL format propagates to NULL result for that row
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('2024-01-16', CAST(NULL AS STRING)) AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |

  Rule: Non-finite floating-point string literals return NULL

    @sail-bug
    Scenario: NaN string returns NULL
      When query
      """
      SELECT try_to_date('NaN') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Infinity string returns NULL
      When query
      """
      SELECT try_to_date('Infinity') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: Negative Infinity string returns NULL
      When query
      """
      SELECT try_to_date('-Infinity') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Multi-row arrays handle per-row failures

    @sail-bug
    Scenario: Mixed valid and invalid in batch
      When query
      """
      SELECT try_to_date(d) AS result FROM VALUES ('2024-01-15'), ('garbage'), ('2024-02-29'), (NULL) AS t(d)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |
      | 2024-02-29 |
      | NULL       |

  Rule: Without a format, try_to_date follows the lenient STRING to DATE cast and never raises
    # Spark 4.2.0 datetimeExpressions.scala: TryToDateExpressionBuilder builds
    # ParseToDate(ansiEnabled = false), a non-ANSI Cast(left, DateType), whatever the session
    # ANSI setting. SparkDateTimeUtils.stringToDate trims, ignores what follows ' ' or 'T' after
    # the day, and reaches +5881580-07-11.

    @sail-bug
    Scenario: try_to_date applies the lenient cast per row with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT i, try_to_date(v) AS result
        FROM VALUES
          (1, ' 2024-01-15 '),
          (2, '2024-01-16T10:30'),
          (3, '294248-01-01'),
          (4, '2024-02-30'),
          (5, 'T10:30:45'),
          (6, '2024')
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result        |
        | 1 | 2024-01-15    |
        | 2 | 2024-01-16    |
        | 3 | +294248-01-01 |
        | 4 | NULL          |
        | 5 | NULL          |
        | 6 | 2024-01-01    |

    @sail-bug
    Scenario Outline: try_to_date of <case> input returns NULL with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_to_date(<value>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case    | value |
        | integer | 1     |
        | boolean | true  |

  Rule: An invalid pattern is an error even for the try_ variant
    # Spark 4.2.0 datetimeExpressions.scala: the formatter is built outside the parse-error
    # handler of ToTimestamp, so an illegal pattern raises INVALID_DATETIME_PATTERN.

    @sail-bug
    Scenario: try_to_date with an illegal pattern fails
      When query
        """
        SELECT try_to_date('2024-01-15', 'yyyy-MM-dd-qqq') AS result
        """
      Then query error INVALID_DATETIME_PATTERN

  Rule: A formatted parse keeps its local date in a non-UTC session time zone

    @sail-bug
    Scenario: try_to_date with a time format keeps the parsed local date in a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT try_to_date('2024-06-15 23:30', 'yyyy-MM-dd HH:mm') AS result
        """
      Then query result
        | result     |
        | 2024-06-15 |

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
