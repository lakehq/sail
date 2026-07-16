Feature: DATE_TRUNC preserves timestamp type

  Rule: date_trunc on timestamp preserves type

    Scenario: date_trunc on timestamp column preserves timestamp type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP '2026-02-02 00:00:00 UTC'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz column preserves timestamp_ntz type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz literal preserves timestamp_ntz type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    Scenario: date_trunc YEAR on timestamp values
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-01 00:00:00 |

    Scenario: date_trunc DAY on timestamp with America/Los_Angeles timezone
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2026-03-15 02:30:00 America/Los_Angeles') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 00:00:00 |

    Scenario: date_trunc HOUR on timestamp with America/New_York timezone
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2026-03-15 14:45:30 America/New_York') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 18:00:00 |

  Rule: date_trunc — the argument may come from a column

    @column_args
    Scenario: date_trunc with the argument as a literal
      When query
        """
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359') AS result
        """
      Then query result ordered
        | result              |
        | 2015-03-01 00:00:00 |

    # Sail rejects the column: Sail errors: Granularity of `date_trunc` must be non-null scalar Utf8
    @column_args @sail-bug
    Scenario: date_trunc takes argument 1 from a column holding two different values
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-03-01 00:00:00 |

    # Sail rejects the column: Sail errors: Granularity of `date_trunc` must be non-null scalar Utf8
    @column_args @sail-bug
    Scenario: date_trunc takes argument 1 from a column
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'YEAR') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-01-01 00:00:00 |
