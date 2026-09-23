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
