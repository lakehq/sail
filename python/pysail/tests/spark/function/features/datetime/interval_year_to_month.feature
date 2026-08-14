Feature: INTERVAL YEAR TO MONTH operations

  Rule: Arithmetic

    # Results are cast to STRING because a year-month interval cannot cross the Spark Connect
    # Arrow boundary: returning one directly fails on both engines with
    # UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION.

    Scenario: adding two year-month intervals
      When query
        """
        SELECT CAST(INTERVAL '1' YEAR + INTERVAL '2' MONTH AS STRING) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '1-2' YEAR TO MONTH |

    # Sail fails to plan these: "Cannot get result type for temporal operation
    # Interval(YearMonth) * Int32". The day-time equivalents work, and so does
    # try_multiply on a year-month interval, so the gap is specific to the
    # `*` and `/` operators on Interval(YearMonth).
    @sail-bug
    Scenario Outline: multiplying and dividing a year-month interval: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        FROM VALUES (2) AS t(y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | expr                        | result                       |
        | interval * column  | INTERVAL '1' YEAR * y       | INTERVAL '2-0' YEAR TO MONTH |
        | column * interval  | y * INTERVAL '1' YEAR       | INTERVAL '2-0' YEAR TO MONTH |
        | interval * literal | make_ym_interval(1, 6) * 2  | INTERVAL '3-0' YEAR TO MONTH |
        | interval / column  | INTERVAL '2' YEAR / y       | INTERVAL '1-0' YEAR TO MONTH |
