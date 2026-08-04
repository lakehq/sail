Feature: year

  Rule: Basic year extraction

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT year(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | input                               | result |
        | year of a DATE                           | DATE '2024-03-15'                   | 2024   |
        | year of DATE '2024-01-01' (first day)    | DATE '2024-01-01'                   | 2024   |
        | year of DATE '2024-12-31' (last day)     | DATE '2024-12-31'                   | 2024   |
        | year of TIMESTAMP                        | TIMESTAMP '2024-03-15 12:30:00'     | 2024   |
        | year of TIMESTAMP_NTZ                    | TIMESTAMP_NTZ '2024-03-15 12:30:00' | 2024   |
        | year of NULL                             | CAST(NULL AS DATE)                  | NULL   |
        | year of DATE '0001-01-01' (minimum date) | DATE '0001-01-01'                   | 1      |
        | year of DATE '9999-12-31' (maximum date) | DATE '9999-12-31'                   | 9999   |
        | year of leap day                         | DATE '2024-02-29'                   | 2024   |

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

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT year(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                    | input                       |
        | year of untyped NULL returns NULL       | NULL                        |
        | year of NULL TIMESTAMP returns NULL     | CAST(NULL AS TIMESTAMP)     |
        | year of NULL TIMESTAMP_NTZ returns NULL | CAST(NULL AS TIMESTAMP_NTZ) |
        | year of NULL STRING returns NULL        | CAST(NULL AS STRING)        |

  Rule: TIMESTAMP and TIMESTAMP_NTZ boundary values

    Scenario Outline: Boundary: <case>
      When query
        """
        SELECT year(<type> <value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                           | type          | value                        | result |
        | year of TIMESTAMP at minimum date                              | TIMESTAMP     | '0001-01-01 00:00:00'        | 1      |
        | year of TIMESTAMP at maximum date                              | TIMESTAMP     | '9999-12-31 23:59:59'        | 9999   |
        | year of TIMESTAMP at Unix epoch                                | TIMESTAMP     | '1970-01-01 00:00:00'        | 1970   |
        | year of TIMESTAMP with sub-second precision stays in same year | TIMESTAMP     | '2024-12-31 23:59:59.999999' | 2024   |
        | year of TIMESTAMP_NTZ at minimum date                          | TIMESTAMP_NTZ | '0001-01-01 00:00:00'        | 1      |
        | year of TIMESTAMP_NTZ at maximum date                          | TIMESTAMP_NTZ | '9999-12-31 23:59:59.999999' | 9999   |

  Rule: String input coercion

    Scenario Outline: String coercion: <case>
      When query
        """
        SELECT year(<input>) AS result
        """
      Then query result
        | result |
        | 2024   |

      Examples:
        | case                                | input                       |
        | year of string date literal         | '2024-03-15'                |
        | year of string datetime literal     | '2024-03-15 10:30:00'       |
        | year of string with timezone offset | '2024-03-15 10:30:00+05:30' |

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

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null date literal yields a non-nullable integer
      When query
        """
        SELECT year(DATE '2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null date column yields a non-nullable integer
      When query
        """
        SELECT year(CAST(CAST(id AS TIMESTAMP) AS DATE)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable date column stays nullable
      When query
        """
        SELECT year(c) AS result FROM VALUES (DATE '2024-01-15'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
