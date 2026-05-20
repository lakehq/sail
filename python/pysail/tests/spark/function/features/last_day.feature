@last_day
Feature: last_day comprehensive tests

  Rule: Argument count validation

    Scenario: last_day zero arguments errors
      When query
        """
        SELECT last_day() AS result
        """
      Then query error .*

    Scenario: last_day two arguments errors
      When query
        """
        SELECT last_day(DATE'2024-01-01', 'extra') AS result
        """
      Then query error .*

  Rule: NULL handling

    Scenario: last_day NULL date
      When query
        """
        SELECT last_day(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: last_day NULL string
      When query
        """
        SELECT last_day(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic usage per month

    Scenario: last_day January
      When query
        """
        SELECT last_day(DATE'2024-01-15') AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day February leap year
      When query
        """
        SELECT last_day(DATE'2024-02-01') AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day February non-leap year
      When query
        """
        SELECT last_day(DATE'2023-02-01') AS result
        """
      Then query result
        | result     |
        | 2023-02-28 |

    Scenario: last_day March
      When query
        """
        SELECT last_day(DATE'2024-03-01') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day April
      When query
        """
        SELECT last_day(DATE'2024-04-15') AS result
        """
      Then query result
        | result     |
        | 2024-04-30 |

    Scenario: last_day December
      When query
        """
        SELECT last_day(DATE'2024-12-05') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

  Rule: String input coercion

    Scenario: last_day with string input
      When query
        """
        SELECT last_day('2024-03-15') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day with invalid string errors
      When query
        """
        SELECT last_day('not-a-date') AS result
        """
      Then query error .*

  Rule: Boundary dates

    Scenario: last_day epoch
      When query
        """
        SELECT last_day(DATE'1970-01-01') AS result
        """
      Then query result
        | result     |
        | 1970-01-31 |

    Scenario: last_day minimum date
      When query
        """
        SELECT last_day(DATE'0001-01-01') AS result
        """
      Then query result
        | result     |
        | 0001-01-31 |

    Scenario: last_day maximum date
      When query
        """
        SELECT last_day(DATE'9999-12-01') AS result
        """
      Then query result
        | result     |
        | 9999-12-31 |

  Rule: Multi-row

    Scenario: last_day multi-row
      When query
        """
        SELECT last_day(d) AS result FROM VALUES (DATE'2024-01-15'), (CAST(NULL AS DATE)), (DATE'2024-02-29') AS t(d)
        """
      Then query result
        | result     |
        | 2024-01-31 |
        | NULL       |
        | 2024-02-29 |

  Rule: Timestamp implicit coercion to Date

    # Spark implicitly casts Timestamp / Timestamp_NTZ to Date before applying
    # last_day. Regression test for issue #1735 — Sail previously rejected
    # these types at plan time.

    Scenario: last_day accepts TIMESTAMP input (Spark casts to Date)
      When query
        """
        SELECT last_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP)) AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day accepts TIMESTAMP_NTZ input (Spark casts to Date)
      When query
        """
        SELECT last_day(CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)) AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day on TIMESTAMP with non-midnight time still truncates
      When query
        """
        SELECT last_day(CAST('2024-02-29 23:59:59' AS TIMESTAMP)) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day on TIMESTAMP column from VALUES
      When query
        """
        SELECT last_day(ts) AS result FROM VALUES
          (CAST('2024-01-15 10:30:00' AS TIMESTAMP)),
          (CAST('2024-12-01 00:00:00' AS TIMESTAMP))
          AS t(ts)
        """
      Then query result
        | result     |
        | 2024-01-31 |
        | 2024-12-31 |

  Rule: Gregorian calendar edge cases (century leap rules + min date)

    # Century rule: divisible by 100 is NOT leap unless also divisible by 400.
    # 1900 → non-leap (Feb=28 days). 2000 → leap (Feb=29 days).
    # Exercises the leap-year algorithm correctness, not just the generic path.

    Scenario: last_day February 1900 (century non-leap, rule of 100)
      When query
        """
        SELECT last_day(DATE '1900-02-15') AS result
        """
      Then query result
        | result     |
        | 1900-02-28 |

    Scenario: last_day February 2000 (century leap, rule of 400)
      When query
        """
        SELECT last_day(DATE '2000-02-15') AS result
        """
      Then query result
        | result     |
        | 2000-02-29 |

    Scenario: last_day year 1 AD (min date boundary)
      When query
        """
        SELECT last_day(DATE '0001-01-15') AS result
        """
      Then query result
        | result     |
        | 0001-01-31 |

  Rule: Filter pushdown — preimage rewrite preserves semantics for all 5 operators

    Scenario: last_day equality with valid last day returns whole month
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-15'),
          (DATE '2024-04-30'),
          (DATE '2024-05-01'),
          (DATE '2024-05-31')
          AS t(d)
        WHERE last_day(d) = DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-04-01 |
        | 2024-04-15 |
        | 2024-04-30 |

    Scenario: last_day strict less than excludes target month
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-30'),
          (DATE '2024-05-01')
          AS t(d)
        WHERE last_day(d) < DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-03-31 |

    Scenario: last_day less or equal includes target month
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-30'),
          (DATE '2024-05-01')
          AS t(d)
        WHERE last_day(d) <= DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-03-31 |
        | 2024-04-01 |
        | 2024-04-30 |

    Scenario: last_day strict greater than excludes target month
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-30'),
          (DATE '2024-05-01'),
          (DATE '2024-05-31')
          AS t(d)
        WHERE last_day(d) > DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-05-01 |
        | 2024-05-31 |

    Scenario: last_day greater or equal includes target month
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-30'),
          (DATE '2024-05-01'),
          (DATE '2024-05-31')
          AS t(d)
        WHERE last_day(d) >= DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-04-01 |
        | 2024-04-30 |
        | 2024-05-01 |
        | 2024-05-31 |

  Rule: Filter pushdown — unsatisfiable literal returns empty (no over-approx)

    # Canary against over-approximation: D not being a month-end must
    # leave the predicate unrewritten so per-row eval yields zero rows.

    Scenario: last_day equality with mid-month date returns empty
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-04-01'),
          (DATE '2024-04-15'),
          (DATE '2024-04-30')
          AS t(d)
        WHERE last_day(d) = DATE '2024-04-15'
        """
      Then query result
        | d |

    Scenario: last_day equality with first of month returns empty
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-04-15'),
          (DATE '2024-04-30')
          AS t(d)
        WHERE last_day(d) = DATE '2024-04-01'
        """
      Then query result
        | d |

    Scenario: last_day equality with day 30 in 31-day month returns empty
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-12-30'),
          (DATE '2024-12-31')
          AS t(d)
        WHERE last_day(d) = DATE '2024-12-30'
        """
      Then query result
        | d |

  Rule: Filter pushdown — boundary cases for valid last days

    Scenario: last_day equality at leap-year February 29
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-02-01'),
          (DATE '2024-02-29'),
          (DATE '2024-03-01')
          AS t(d)
        WHERE last_day(d) = DATE '2024-02-29'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-02-01 |
        | 2024-02-29 |

    Scenario: last_day equality at non-leap February 28
      When query
        """
        SELECT d FROM VALUES
          (DATE '2025-02-01'),
          (DATE '2025-02-28'),
          (DATE '2025-03-01')
          AS t(d)
        WHERE last_day(d) = DATE '2025-02-28'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2025-02-01 |
        | 2025-02-28 |

    Scenario: last_day equality at year-end December 31
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-11-30'),
          (DATE '2024-12-01'),
          (DATE '2024-12-31'),
          (DATE '2025-01-01')
          AS t(d)
        WHERE last_day(d) = DATE '2024-12-31'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-12-01 |
        | 2024-12-31 |

  Rule: Filter pushdown — NULL handling

    Scenario: last_day equality drops NULL rows
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-04-15'),
          (CAST(NULL AS DATE)),
          (DATE '2024-04-30')
          AS t(d)
        WHERE last_day(d) = DATE '2024-04-30'
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-04-15 |
        | 2024-04-30 |

    Scenario: last_day equality with NULL literal returns empty
      When query
        """
        SELECT d FROM VALUES
          (DATE '2024-04-15'),
          (DATE '2024-04-30')
          AS t(d)
        WHERE last_day(d) = CAST(NULL AS DATE)
        """
      Then query result
        | d |

  Rule: Filter pushdown — Timestamp input via implicit cast

    Scenario: last_day on Timestamp column matches whole month
      When query
        """
        SELECT ts FROM VALUES
          (CAST('2024-04-30 00:00:00' AS TIMESTAMP)),
          (CAST('2024-04-30 23:59:59' AS TIMESTAMP)),
          (CAST('2024-05-01 00:00:00' AS TIMESTAMP)),
          (CAST('2024-05-15 12:00:00' AS TIMESTAMP))
          AS t(ts)
        WHERE last_day(ts) = DATE '2024-04-30'
        ORDER BY ts
        """
      Then query result ordered
        | ts                  |
        | 2024-04-30 00:00:00 |
        | 2024-04-30 23:59:59 |

    Scenario: last_day on Timestamp_NTZ column matches whole month
      When query
        """
        SELECT ts FROM VALUES
          (CAST('2024-04-15 10:30:00' AS TIMESTAMP_NTZ)),
          (CAST('2024-05-01 00:00:00' AS TIMESTAMP_NTZ))
          AS t(ts)
        WHERE last_day(ts) = DATE '2024-04-30'
        ORDER BY ts
        """
      Then query result ordered
        | ts                  |
        | 2024-04-15 10:30:00 |

  Rule: Plan snapshot — filter pushdown on Parquet (preimage)

    @sail-only
    Scenario: EXPLAIN literal Date filter on Parquet — baseline
      Given variable location for temporary directory explain_last_day_baseline
      Given final statement
        """
        DROP TABLE IF EXISTS explain_last_day_baseline_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_last_day_baseline_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-15'),
          (DATE '2024-04-30'),
          (DATE '2024-05-31')
        AS t(d)
        """
      When query
        """
        EXPLAIN ANALYZE SELECT d FROM explain_last_day_baseline_parquet WHERE d >= DATE '2024-04-01' AND d < DATE '2024-05-01'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN last_day filter on Parquet shows preimage pushdown
      Given variable location for temporary directory explain_last_day_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_last_day_pushdown_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_last_day_pushdown_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-01'),
          (DATE '2024-04-15'),
          (DATE '2024-04-30'),
          (DATE '2024-05-31')
        AS t(d)
        """
      When query
        """
        EXPLAIN SELECT d FROM explain_last_day_pushdown_parquet WHERE last_day(d) = DATE '2024-04-30'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN last_day with mid-month literal does NOT rewrite
      Given variable location for temporary directory explain_last_day_unsat
      Given final statement
        """
        DROP TABLE IF EXISTS explain_last_day_unsat_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_last_day_unsat_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (DATE '2024-04-15'),
          (DATE '2024-04-30')
        AS t(d)
        """
      When query
        """
        EXPLAIN SELECT d FROM explain_last_day_unsat_parquet WHERE last_day(d) = DATE '2024-04-15'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN last_day less or equal rewrites to upper-bound predicate
      Given variable location for temporary directory explain_last_day_lte
      Given final statement
        """
        DROP TABLE IF EXISTS explain_last_day_lte_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_last_day_lte_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-30'),
          (DATE '2024-05-31')
        AS t(d)
        """
      When query
        """
        EXPLAIN SELECT d FROM explain_last_day_lte_parquet WHERE last_day(d) <= DATE '2024-04-30'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN last_day not equal rewrites to disjunction over the gap month
      Given variable location for temporary directory explain_last_day_neq
      Given final statement
        """
        DROP TABLE IF EXISTS explain_last_day_neq_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_last_day_neq_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (DATE '2024-03-31'),
          (DATE '2024-04-15'),
          (DATE '2024-04-30'),
          (DATE '2024-05-31')
        AS t(d)
        """
      When query
        """
        EXPLAIN SELECT d FROM explain_last_day_neq_parquet WHERE last_day(d) != DATE '2024-04-30'
        """
      Then query plan matches snapshot
