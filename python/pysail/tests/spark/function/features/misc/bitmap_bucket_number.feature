Feature: bitmap_bucket_number output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to bitmap_bucket_number yields the schema Spark declares
      When query
        """
        SELECT bitmap_bucket_number(123) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to bitmap_bucket_number yields the schema Spark declares
      When query
        """
        SELECT bitmap_bucket_number(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to bitmap_bucket_number stays nullable
      When query
        """
        SELECT bitmap_bucket_number(c) AS result FROM VALUES (123), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: the argument of bitmap_bucket_number is read as a BIGINT

    # `inputTypes = Seq(LongType)` under `ImplicitCastInputTypes` (`bitmapExpressions.scala`): a
    # number or a string is cast to BIGINT, and a BOOLEAN, DATE, TIMESTAMP or INTERVAL is refused
    # at analysis in both ANSI modes.
    Scenario Outline: bitmap_bucket_number refuses a <case> argument with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT bitmap_bucket_number(<argument>) AS result
        """
      Then query error (?i)cannot resolve|data type mismatch

      Examples:
        | case      | ansi  | argument                       |
        | boolean   | false | true                           |
        | boolean   | true  | true                           |
        | date      | false | DATE'2020-01-01'               |
        | date      | true  | DATE'2020-01-01'               |
        | timestamp | false | TIMESTAMP'2020-01-01 00:00:00' |
        | timestamp | true  | TIMESTAMP'2020-01-01 00:00:00' |
        | interval  | false | INTERVAL '1' DAY               |
        | interval  | true  | INTERVAL '1' DAY               |

    Scenario: bitmap_bucket_number casts a string argument to BIGINT
      When query
        """
        SELECT bitmap_bucket_number('123') AS result
        """
      Then query result
        | result |
        | 1      |
