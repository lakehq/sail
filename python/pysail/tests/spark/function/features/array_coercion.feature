@array_coercion
Feature: array() type coercion with mixed element types

  Spark resolves a single element type for `array(...)` via its type-coercion
  rules. The result depends on `spark.sql.ansi.enabled`:

  - ANSI=false: legacy "string promotion" is ON, so a STRING mixed with a
    numeric/date/timestamp coerces the whole array to STRING.
  - ANSI=true (Spark 4.x default, and the value pinned by the test harness):
    string promotion is OFF. `array('a', 1)` resolves to a numeric common type
    and Spark raises CAST_INVALID_INPUT at runtime when the string is not a
    valid number.

  Type-incompatible combinations (string+boolean, string+binary, date+int) are
  rejected by Spark in BOTH modes with DATATYPE_MISMATCH.

  Rule: Homogeneous arrays preserve their element type

    Scenario: array of integers
      When query
      """
      SELECT array(1, 2, 3) AS result
      """
      Then query result
      | result    |
      | [1, 2, 3] |

    Scenario: array of strings
      When query
      """
      SELECT array('a', 'b', 'c') AS result
      """
      Then query result
      | result    |
      | [a, b, c] |

    Scenario: array of doubles
      When query
      """
      SELECT array(1.0, 2.5, 3.5) AS result
      """
      Then query result
      | result          |
      | [1.0, 2.5, 3.5] |

  Rule: Mixed numeric types coerce to the widest numeric type

    Scenario: integer and double coerce to double
      When query
      """
      SELECT array(1, 2.5) AS result
      """
      Then query result
      | result     |
      | [1.0, 2.5] |

    Scenario: tinyint and bigint coerce to bigint
      When query
      """
      SELECT array(CAST(1 AS TINYINT), CAST(2 AS BIGINT)) AS result
      """
      Then query result
      | result |
      | [1, 2] |

  Rule: NULL elements are preserved during numeric coercion

    Scenario: integer and NULL
      When query
      """
      SELECT array(1, NULL) AS result
      """
      Then query result
      | result    |
      | [1, NULL] |

    Scenario: all-NULL array
      When query
      """
      SELECT array(NULL, NULL) AS result
      """
      Then query result
      | result       |
      | [NULL, NULL] |

    Scenario: leading NULL with numeric widening
      When query
      """
      SELECT array(NULL, 1, 2.5) AS result
      """
      Then query result
      | result           |
      | [NULL, 1.0, 2.5] |

  Rule: Mixed string and numeric — ANSI=true rejects, ANSI=false promotes to string

    # String promotion is disabled under ANSI: Spark picks a numeric common type
    # and fails casting the non-numeric string at runtime.
    Scenario: string and integer under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', 1) AS result
      """
      Then query error .*

    Scenario: string and integer coerce to string (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', 1) AS result
      """
      Then query result
      | result |
      | [a, 1] |

    Scenario: string and double under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', 1.5) AS result
      """
      Then query error .*

    Scenario: string and double coerce to string (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', 1.5) AS result
      """
      Then query result
      | result   |
      | [a, 1.5] |

    Scenario: string and decimal under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', CAST(1.5 AS DECIMAL(10,2))) AS result
      """
      Then query error .*

    Scenario: string and decimal coerce to string preserving scale (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', CAST(1.5 AS DECIMAL(10,2))) AS result
      """
      Then query result
      | result    |
      | [a, 1.50] |

    Scenario: multiple strings and numerics under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', 1, 2.5, 'b') AS result
      """
      Then query error .*

    Scenario: multiple strings and numerics coerce to string (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', 1, 2.5, 'b') AS result
      """
      Then query result
      | result         |
      | [a, 1, 2.5, b] |

    Scenario: string, numeric and NULL under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', 1, NULL, 1.0) AS result
      """
      Then query error .*

    Scenario: string, numeric and NULL coerce to string with NULL preserved (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', 1, NULL, 1.0) AS result
      """
      Then query result
      | result            |
      | [a, 1, NULL, 1.0] |

  Rule: Numeric-parseable strings cast cleanly even under ANSI

    # '1' is a valid number, so the numeric common type succeeds under ANSI;
    # only non-numeric strings like 'a' trigger CAST_INVALID_INPUT.
    Scenario: parseable string and integer under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('1', 2) AS result
      """
      Then query result
      | result |
      | [1, 2] |

    Scenario: parseable string and integer coerce to string (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('1', 2) AS result
      """
      Then query result
      | result |
      | [1, 2] |

  Rule: String and date/timestamp — ANSI=true rejects, ANSI=false promotes to string

    Scenario: string and date under ANSI errors
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT array('a', DATE '2024-01-15') AS result
      """
      Then query error .*

    Scenario: string and date coerce to string (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', DATE '2024-01-15') AS result
      """
      Then query result
      | result          |
      | [a, 2024-01-15] |

    # Independent bug: even when string promotion is correct (ANSI=false), Sail
    # serializes TIMESTAMP via ISO-8601 ("2024-01-15T12:00:00Z") instead of
    # Spark's "2024-01-15 12:00:00".
    @sail-bug
    Scenario: string and timestamp coerce to string with Spark formatting (non-ANSI)
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', TIMESTAMP '2024-01-15 12:00:00') AS result
      """
      Then query result
      | result                   |
      | [a, 2024-01-15 12:00:00] |

  Rule: Type-incompatible elements are rejected by Spark in both ANSI modes

    # Spark raises DATATYPE_MISMATCH (no string promotion for boolean/binary,
    # no numeric->date coercion).
    Scenario: string and boolean rejected
      When query
      """
      SELECT array('a', true) AS result
      """
      Then query error .*

    # Still divergent: DataFusion's `comparison_coercion` resolves STRING+BINARY to
    # BINARY, so Sail casts the string to binary instead of rejecting like Spark.
    @sail-bug
    Scenario: string and binary rejected
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT array('a', X'4869') AS result
      """
      Then query error .*

    # Still divergent: `comparison_coercion` resolves DATE+INT to DATE, so Sail
    # coerces the integer to an epoch-day date instead of rejecting like Spark.
    @sail-bug
    Scenario: date and integer rejected
      When query
      """
      SELECT array(DATE '2024-01-15', 1) AS result
      """
      Then query error .*
