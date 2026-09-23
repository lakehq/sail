Feature: to_json and to_csv render an interval the way Spark does

  # `to_json` and `to_csv` are renderers of their own: neither goes through `show` nor
  # through `CAST(... AS STRING)`, so a value can print right in every lens of
  # `interval_stored_and_printed.feature` and still be written wrong -- or dropped without an
  # error -- here. This file asserts only the string the two functions return, so that the
  # lenses stay apart: the stored value, the type and the two printed forms live in that file.
  # Spark renders every datetime type through `ToStringBase` (`Cast.scala`).
  # Measured on the Spark 4.2 JVM over Spark Connect with the session zone pinned to UTC.

  Rule: to_json renders an interval the way Spark does

    @sail-bug
    Scenario Outline: to_json of <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                      | expr                                  | result                                        |
        | interval day              | INTERVAL '5' DAY                      | {"v":"INTERVAL '5' DAY"}                      |
        | interval day to hour      | INTERVAL '1 02' DAY TO HOUR           | {"v":"INTERVAL '1 02' DAY TO HOUR"}           |
        | interval day to minute    | INTERVAL '1 02:03' DAY TO MINUTE      | {"v":"INTERVAL '1 02:03' DAY TO MINUTE"}      |
        | interval day to second    | INTERVAL '1 02:03:04.5' DAY TO SECOND | {"v":"INTERVAL '1 02:03:04.5' DAY TO SECOND"} |
        | interval hour             | INTERVAL '7' HOUR                     | {"v":"INTERVAL '07' HOUR"}                    |
        | interval hour to minute   | INTERVAL '07:08' HOUR TO MINUTE       | {"v":"INTERVAL '07:08' HOUR TO MINUTE"}       |
        | interval hour to second   | INTERVAL '07:08:09.5' HOUR TO SECOND  | {"v":"INTERVAL '07:08:09.5' HOUR TO SECOND"}  |
        | interval minute           | INTERVAL '9' MINUTE                   | {"v":"INTERVAL '09' MINUTE"}                  |
        | interval minute to second | INTERVAL '09:10.5' MINUTE TO SECOND   | {"v":"INTERVAL '09:10.5' MINUTE TO SECOND"}   |
        | interval second           | INTERVAL '11.5' SECOND                | {"v":"INTERVAL '11.5' SECOND"}                |
        | interval year             | INTERVAL '3' YEAR                     | {"v":"INTERVAL '3' YEAR"}                     |
        | interval month            | INTERVAL '4' MONTH                    | {"v":"INTERVAL '4' MONTH"}                    |
        | interval year to month    | INTERVAL '3-4' YEAR TO MONTH          | {"v":"INTERVAL '3-4' YEAR TO MONTH"}          |
        | interval (calendar)       | make_interval(0, 1, 0, 2, 3, 0, 0)    | {"v":"1 months 2 days 3 hours"}               |
        | date difference           | DATE '2024-03-05' - DATE '2024-01-01' | {"v":"INTERVAL '64' DAY"}                     |

    Scenario Outline: to_json of a NULL value: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_json(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case              | expr                       | result |
        | null interval day | CAST(NULL AS INTERVAL DAY) | {}     |

  Rule: to_csv renders an interval the way Spark does

    @sail-bug
    Scenario Outline: to_csv of <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                      | expr                                  | result                                |
        | interval day              | INTERVAL '5' DAY                      | INTERVAL '5' DAY                      |
        | interval day to hour      | INTERVAL '1 02' DAY TO HOUR           | INTERVAL '1 02' DAY TO HOUR           |
        | interval day to minute    | INTERVAL '1 02:03' DAY TO MINUTE      | INTERVAL '1 02:03' DAY TO MINUTE      |
        | interval day to second    | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND |
        | interval hour             | INTERVAL '7' HOUR                     | INTERVAL '07' HOUR                    |
        | interval hour to minute   | INTERVAL '07:08' HOUR TO MINUTE       | INTERVAL '07:08' HOUR TO MINUTE       |
        | interval hour to second   | INTERVAL '07:08:09.5' HOUR TO SECOND  | INTERVAL '07:08:09.5' HOUR TO SECOND  |
        | interval minute           | INTERVAL '9' MINUTE                   | INTERVAL '09' MINUTE                  |
        | interval minute to second | INTERVAL '09:10.5' MINUTE TO SECOND   | INTERVAL '09:10.5' MINUTE TO SECOND   |
        | interval second           | INTERVAL '11.5' SECOND                | INTERVAL '11.5' SECOND                |
        | interval year             | INTERVAL '3' YEAR                     | INTERVAL '3' YEAR                     |
        | interval month            | INTERVAL '4' MONTH                    | INTERVAL '4' MONTH                    |
        | interval year to month    | INTERVAL '3-4' YEAR TO MONTH          | INTERVAL '3-4' YEAR TO MONTH          |
        | interval (calendar)       | make_interval(0, 1, 0, 2, 3, 0, 0)    | 1 months 2 days 3 hours               |
        | date difference           | DATE '2024-03-05' - DATE '2024-01-01' | INTERVAL '64' DAY                     |

    @sail-bug
    Scenario Outline: to_csv of a NULL value: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT to_csv(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case              | expr                       | result |
        | null interval day | CAST(NULL AS INTERVAL DAY) | NULL   |
