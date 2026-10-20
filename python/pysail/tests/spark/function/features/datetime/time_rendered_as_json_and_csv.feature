@spark-4.2
Feature: to_json and to_csv render a time the way Spark does

  # `to_json` and `to_csv` are renderers of their own: neither goes through `show` nor
  # through `CAST(... AS STRING)`, so a value can print right in every lens of
  # `time_stored_and_printed.feature` and still be written wrong -- or dropped without an
  # error -- here. This file asserts only the string the two functions return, so that the
  # lenses stay apart: the stored value, the type and the two printed forms live in that file.
  # Spark renders every datetime type through `ToStringBase` (`Cast.scala`).
  # Measured on the Spark 4.2 JVM over Spark Connect with the session zone pinned to UTC.
  # Tagged `@spark-4.2` because the `time` type only exists from Spark 4.2 on.

  Rule: to_json renders a time the way Spark does

    @sail-bug
    Scenario Outline: to_json of <case>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_json(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case    | expr                               | result                  |
        | time(0) | CAST('06:07:08' AS TIME(0))        | {"v":"06:07:08"}        |
        | time(1) | CAST('06:07:08.1' AS TIME(1))      | {"v":"06:07:08.1"}      |
        | time(2) | CAST('06:07:08.12' AS TIME(2))     | {"v":"06:07:08.12"}     |
        | time(3) | CAST('06:07:08.123' AS TIME(3))    | {"v":"06:07:08.123"}    |
        | time(4) | CAST('06:07:08.1234' AS TIME(4))   | {"v":"06:07:08.1234"}   |
        | time(5) | CAST('06:07:08.12345' AS TIME(5))  | {"v":"06:07:08.12345"}  |
        | time(6) | CAST('06:07:08.123456' AS TIME(6)) | {"v":"06:07:08.123456"} |

  Rule: to_csv renders a time the way Spark does

    @sail-bug
    Scenario Outline: to_csv of <case>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_csv(named_struct('v', <expr>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case    | expr                               | result          |
        | time(0) | CAST('06:07:08' AS TIME(0))        | 06:07:08        |
        | time(1) | CAST('06:07:08.1' AS TIME(1))      | 06:07:08.1      |
        | time(2) | CAST('06:07:08.12' AS TIME(2))     | 06:07:08.12     |
        | time(3) | CAST('06:07:08.123' AS TIME(3))    | 06:07:08.123    |
        | time(4) | CAST('06:07:08.1234' AS TIME(4))   | 06:07:08.1234   |
        | time(5) | CAST('06:07:08.12345' AS TIME(5))  | 06:07:08.12345  |
        | time(6) | CAST('06:07:08.123456' AS TIME(6)) | 06:07:08.123456 |
