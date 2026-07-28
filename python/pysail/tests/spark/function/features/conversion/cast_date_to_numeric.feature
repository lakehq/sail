@cast_date_to_numeric
Feature: CAST date to numeric types returns null

  In Spark legacy mode, casting a DATE to any numeric type returns NULL.
  In ANSI mode, the cast raises an error. TRY_CAST always returns NULL.

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
