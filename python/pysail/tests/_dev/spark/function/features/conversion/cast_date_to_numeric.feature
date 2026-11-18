Feature: CAST date to numeric types follows Arrow cast rules

  Sail follows Arrow's cast rules for explicit casts, in both legacy and ANSI mode.
  Arrow casts a DATE to INT or BIGINT as the number of days since 1970-01-01,
  and does not support casting a DATE to other numeric types or BOOLEAN.
  Spark returns NULL in legacy mode and raises an error in ANSI mode.

  Rule: CAST date to INT or BIGINT returns days since epoch

    This is an explicit and intentional divergence from Spark.

    @sail-only
    Scenario Outline: <mode>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<value> AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |
      Given config spark.sql.ansi.enabled = false

      Examples:
        | mode   | ansi  | case                               | value              | type   | result |
        | Legacy | false | cast date to int                   | DATE '2023-01-15'  | INT    | 19372  |
        | Legacy | false | cast date to bigint                | DATE '2023-01-15'  | BIGINT | 19372  |
        | Legacy | false | cast null date to int returns null | CAST(NULL AS DATE) | INT    | NULL   |
        | ANSI   | true  | cast date to int                   | DATE '2023-01-15'  | INT    | 19372  |
        | ANSI   | true  | cast date to bigint                | DATE '2023-01-15'  | BIGINT | 19372  |

  Rule: CAST date to other numeric types or BOOLEAN is not supported by Arrow

    This is an explicit and intentional divergence from Spark.

    @sail-only
    Scenario Outline: <case>
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS <type>) AS result
        """
      Then query error (?i)unsupported cast

      Examples:
        | case                    | type          |
        | cast date to smallint   | SMALLINT      |
        | cast date to tinyint    | TINYINT       |
        | cast date to float      | FLOAT         |
        | cast date to double     | DOUBLE        |
        | cast date to decimal    | DECIMAL(10,2) |
        | cast date to boolean    | BOOLEAN       |
