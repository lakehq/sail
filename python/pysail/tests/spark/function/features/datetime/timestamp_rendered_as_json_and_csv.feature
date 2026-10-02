Feature: to_json and to_csv render a timestamp the way Spark does

  # `to_json` and `to_csv` are renderers of their own: neither goes through `show` nor
  # through `CAST(... AS STRING)`, so a value can print right in every lens of
  # `timestamp_stored_and_printed.feature` and still be written wrong -- or dropped without an
  # error -- here. This file asserts only the string the two functions return, so that the
  # lenses stay apart: the stored value, the type and the two printed forms live in that file.
  # Spark renders every datetime type through `ToStringBase` (`Cast.scala`).
  # Measured on the Spark 4.2 JVM over Spark Connect with the session zone pinned to UTC.

  Rule: to_json renders a timestamp the way Spark does

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
        | case          | expr                                  | result                           |
        | timestamp     | TIMESTAMP '2024-03-05 06:07:08.9'     | {"v":"2024-03-05T06:07:08.900Z"} |
        | timestamp_ntz | TIMESTAMP_NTZ '2024-03-05 06:07:08.9' | {"v":"2024-03-05T06:07:08.900"}  |

  Rule: to_csv renders a timestamp the way Spark does

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
        | case          | expr                                  | result                   |
        | timestamp     | TIMESTAMP '2024-03-05 06:07:08.9'     | 2024-03-05T06:07:08.900Z |
        | timestamp_ntz | TIMESTAMP_NTZ '2024-03-05 06:07:08.9' | 2024-03-05T06:07:08.900  |
