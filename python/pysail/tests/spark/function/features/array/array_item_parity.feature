Feature: array(...)[index] parity with Spark

  # Values measured on the Spark JVM (Spark Connect, UTC). `main` returns the same wrong answer
  # as this branch for the first two, so they are known Sail bugs and not regressions.
  Rule: An index that is not a literal picks one element per row

    @sail-bug
    Scenario: a column index over array(...)
      When query
        """
        SELECT array(1, 2, 3)[id] AS r FROM range(2)
        """
      Then query result ordered
        | r |
        | 1 |
        | 2 |

  Rule: With ANSI off the elements of array(...) are typed together before the access

    # Spark types `array(1, 1 / 0)` as ARRAY<DOUBLE> because `/` returns DOUBLE; Sail keeps INT.
    @sail-bug
    Scenario: the division widens the picked element
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(1, 1 / 0)[0] AS r
        """
      Then query schema
        """
        root
         |-- r: double (nullable = false)
        """

  Rule: A year-month interval element keeps its unit when cast to a number

    # CAST(INTERVAL '2' YEAR AS INT) is 2 (years). `main` returns the months, 24; the
    # element access now carries the interval field range, so this branch matches Spark.
    Scenario: INTERVAL YEAR element cast to INT
      When query
        """
        SELECT CAST(array(INTERVAL '2' YEAR)[0] AS INT) AS r
        """
      Then query result ordered
        | r |
        | 2 |
