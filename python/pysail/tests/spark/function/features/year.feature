Feature: year

  Rule: Basic year extraction

    Scenario: year of a DATE
      When query
        """
        SELECT year(DATE '2024-03-15') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of DATE '2024-01-01' (first day)
      When query
        """
        SELECT year(DATE '2024-01-01') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of DATE '2024-12-31' (last day)
      When query
        """
        SELECT year(DATE '2024-12-31') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of TIMESTAMP
      When query
        """
        SELECT year(TIMESTAMP '2024-03-15 12:30:00') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of TIMESTAMP_NTZ
      When query
        """
        SELECT year(TIMESTAMP_NTZ '2024-03-15 12:30:00') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of NULL
      When query
        """
        SELECT year(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of DATE '0001-01-01' (minimum date)
      When query
        """
        SELECT year(DATE '0001-01-01') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: year of DATE '9999-12-31' (maximum date)
      When query
        """
        SELECT year(DATE '9999-12-31') AS result
        """
      Then query result
        | result |
        | 9999   |

    Scenario: year of leap day
      When query
        """
        SELECT year(DATE '2024-02-29') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: multi-row with different years
      When query
        """
        SELECT year(d) AS result
        FROM VALUES
          (DATE '2020-06-15'),
          (DATE '2022-01-01'),
          (DATE '2024-12-31')
          AS t(d)
        """
      Then query result
        | result |
        | 2020   |
        | 2022   |
        | 2024   |

    Scenario: multi-row with NULLs mixed in
      When query
        """
        SELECT year(d) AS result
        FROM VALUES
          (DATE '2023-03-01'),
          (CAST(NULL AS DATE)),
          (DATE '2025-07-04')
          AS t(d)
        """
      Then query result
        | result |
        | 2023   |
        | NULL   |
        | 2025   |

  Rule: Preimage — row-result correctness (validates that filter produces right rows)

    Scenario: WHERE year(col) = 2023 returns only 2023 rows
      When query
        """
        SELECT d AS result
        FROM VALUES
          (DATE '2022-12-31'),
          (DATE '2023-01-01'),
          (DATE '2023-06-15'),
          (DATE '2023-12-31'),
          (DATE '2024-01-01')
          AS t(d)
        WHERE year(d) = 2023
        """
      Then query result ordered
        | result     |
        | 2023-01-01 |
        | 2023-06-15 |
        | 2023-12-31 |

    Scenario: WHERE year(col) <= 2022 returns 2022 and earlier
      When query
        """
        SELECT d AS result
        FROM VALUES
          (DATE '2021-11-11'),
          (DATE '2022-12-31'),
          (DATE '2023-01-01'),
          (DATE '2024-06-01')
          AS t(d)
        WHERE year(d) <= 2022
        """
      Then query result ordered
        | result     |
        | 2021-11-11 |
        | 2022-12-31 |

    Scenario: WHERE year(col) != 2023 excludes 2023 rows
      When query
        """
        SELECT d AS result
        FROM VALUES
          (DATE '2022-12-31'),
          (DATE '2023-06-15'),
          (DATE '2024-01-01')
          AS t(d)
        WHERE year(d) != 2023
        """
      Then query result ordered
        | result     |
        | 2022-12-31 |
        | 2024-01-01 |

    Scenario: WHERE year(col) IS NOT NULL excludes NULLs
      When query
        """
        SELECT year(d) AS result
        FROM VALUES
          (DATE '2023-01-01'),
          (CAST(NULL AS DATE)),
          (DATE '2024-06-01')
          AS t(d)
        WHERE year(d) IS NOT NULL
        """
      Then query result ordered
        | result |
        | 2023   |
        | 2024   |

    Scenario: WHERE year(col) = 9999 returns only year-9999 rows
      When query
        """
        SELECT d AS result
        FROM VALUES
          (DATE '9998-12-31'),
          (DATE '9999-01-01'),
          (DATE '9999-12-31')
          AS t(d)
        WHERE year(d) = 9999
        """
      Then query result ordered
        | result     |
        | 9999-01-01 |
        | 9999-12-31 |

  Rule: NULL handling

    Scenario: year of untyped NULL returns NULL
      When query
        """
        SELECT year(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of NULL TIMESTAMP returns NULL
      When query
        """
        SELECT year(CAST(NULL AS TIMESTAMP)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of NULL TIMESTAMP_NTZ returns NULL
      When query
        """
        SELECT year(CAST(NULL AS TIMESTAMP_NTZ)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of NULL STRING returns NULL
      When query
        """
        SELECT year(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: TIMESTAMP and TIMESTAMP_NTZ boundary values

    Scenario: year of TIMESTAMP at minimum date
      When query
        """
        SELECT year(TIMESTAMP '0001-01-01 00:00:00') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: year of TIMESTAMP at maximum date
      When query
        """
        SELECT year(TIMESTAMP '9999-12-31 23:59:59') AS result
        """
      Then query result
        | result |
        | 9999   |

    Scenario: year of TIMESTAMP at Unix epoch
      When query
        """
        SELECT year(TIMESTAMP '1970-01-01 00:00:00') AS result
        """
      Then query result
        | result |
        | 1970   |

    Scenario: year of TIMESTAMP with sub-second precision stays in same year
      When query
        """
        SELECT year(TIMESTAMP '2024-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of TIMESTAMP_NTZ at minimum date
      When query
        """
        SELECT year(TIMESTAMP_NTZ '0001-01-01 00:00:00') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: year of TIMESTAMP_NTZ at maximum date
      When query
        """
        SELECT year(TIMESTAMP_NTZ '9999-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result |
        | 9999   |

  Rule: String input coercion

    Scenario: year of string date literal
      When query
        """
        SELECT year('2024-03-15') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of string datetime literal
      When query
        """
        SELECT year('2024-03-15 10:30:00') AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: year of string with timezone offset
      When query
        """
        SELECT year('2024-03-15 10:30:00+05:30') AS result
        """
      Then query result
        | result |
        | 2024   |

    @sail-bug
    Scenario: year of year-only string
      When query
        """
        SELECT year('2024') AS result
        """
      Then query result
        | result |
        | 2024   |

    @sail-bug
    Scenario: year of invalid string returns NULL when ANSI is off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT year('not-a-date') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: year of empty string returns NULL when ANSI is off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT year('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of invalid string errors when ANSI is on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT year('not-a-date') AS result
        """
      Then query error .*

  Rule: Arity errors

    @sail-only
    Scenario: year with zero arguments raises error
      When query
        """
        SELECT year() AS result
        """
      Then query error (?i).*year.*requires 1 argument.*

    @sail-only
    Scenario: year with two arguments raises error
      When query
        """
        SELECT year(DATE '2024-01-15', DATE '2024-01-15') AS result
        """
      Then query error (?i).*year.*requires 1 argument.*

  Rule: Type errors

    @sail-only
    Scenario: year of integer raises type mismatch error
      When query
        """
        SELECT year(1) AS result
        """
      Then query error (?i).*year.*date.*timestamp.*

  Rule: Preimage — plan snapshots (VALUES, in-memory)
    @sail-only
    Scenario: EXPLAIN WHERE year(col) = 2023 rewrites to date range (no UDF in plan)
      When query
        """
        EXPLAIN SELECT d FROM VALUES
          (DATE '2023-06-15')
          AS t(d)
        WHERE year(d) = 2023
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE year(col) <= 2022 rewrites to upper-bound date predicate
      When query
        """
        EXPLAIN SELECT d FROM VALUES
          (DATE '2022-06-15')
          AS t(d)
        WHERE year(d) <= 2022
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE year(col) != 2023 rewrites to disjunction
      When query
        """
        EXPLAIN SELECT d FROM VALUES
          (DATE '2022-01-01')
          AS t(d)
        WHERE year(d) != 2023
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE year(col) = 9999 also rewrites (NaiveDate supports year 10000)
      When query
        """
        EXPLAIN SELECT d FROM VALUES
          (DATE '9999-01-01')
          AS t(d)
        WHERE year(d) = 9999
        """
      Then query plan matches snapshot
