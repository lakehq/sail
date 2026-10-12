Feature: CAST date to numeric types returns null

  In Spark legacy mode, casting a DATE to any numeric type returns NULL.
  In ANSI mode, a plain CAST raises an error. TRY_CAST also raises an error, regardless
  of the ANSI setting: `canAnsiCast` has no DATE <-> NumericType/BooleanType rule at all
  (only the legacy `canCast` does), and TRY_CAST always analyzes against `canAnsiCast`.

  Rule: CAST date to numeric types returns null (legacy mode)

    Scenario Outline: Legacy: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<value> AS <type>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                               | value              | type          |
        | cast date to int returns null      | DATE '2023-01-15'  | INT           |
        | cast date to bigint returns null   | DATE '2023-01-15'  | BIGINT        |
        | cast date to smallint returns null | DATE '2023-01-15'  | SMALLINT      |
        | cast date to tinyint returns null  | DATE '2023-01-15'  | TINYINT       |
        | cast date to float returns null    | DATE '2023-01-15'  | FLOAT         |
        | cast date to double returns null   | DATE '2023-01-15'  | DOUBLE        |
        | cast date to decimal returns null  | DATE '2023-01-15'  | DECIMAL(10,2) |
        | cast date to boolean returns null  | DATE '2023-01-15'  | BOOLEAN       |
        | cast null date to int returns null | CAST(NULL AS DATE) | INT           |

  Rule: CAST date to numeric in ANSI mode raises error

    @sail-only
    Scenario Outline: ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(DATE '2023-01-15' AS <type>) AS result
        """
      Then query error cannot cast date
      Given config spark.sql.ansi.enabled = false

      Examples:
        | case                                           | type    |
        | cast date to int in ANSI mode raises error     | INT     |
        | cast date to double in ANSI mode raises error  | DOUBLE  |
        | cast date to boolean in ANSI mode raises error | BOOLEAN |

  Rule: TRY_CAST date to numeric always raises, regardless of ANSI

    # `canAnsiCast` has no DATE <-> NumericType/BooleanType rule at all (only the legacy
    # `canCast` does, Cast.scala:237,239,243,271), and TRY_CAST always analyzes against
    # `canAnsiCast` -- so, unlike a plain CAST, TRY_CAST never falls back to returning
    # NULL for this pair even with ANSI off.
    @sail-only
    Scenario Outline: TRY_CAST: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(DATE '2023-01-15' AS <type>) AS result
        """
      Then query error cannot cast date

      Examples:
        | case                                            | type    | ansi  |
        | TRY_CAST date to int raises under ANSI off      | INT     | false |
        | TRY_CAST date to int raises under ANSI on       | INT     | true  |
        | TRY_CAST date to boolean raises under ANSI off  | BOOLEAN | false |
