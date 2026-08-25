@arithmetic_matrix
Feature: arithmetic operator operand-type matrix (+ - * / %) vs Spark 4.2.0

  # Auto-generated full cartesian product of the operand types below crossed with each
  # operator, under BOTH ANSI modes, measured against Spark JVM 4.2.0 (the expected
  # column is Spark's). Value pairs assert the result `typeof`; pairs Spark rejects at
  # analysis assert an error. Both modes are pinned because Spark's numeric/string
  # coercion is ANSI-dependent (e.g. `int + float` -> float off / double on;
  # `int + str` -> double off / bigint on). `@sail-bug` marks the cells where Sail
  # still diverges — clean them up as the coercion/overflow follow-ups land.

# ============================ ANSI OFF ============================

  Rule: `+` operand-type matrix (ANSI off)

    Scenario Outline: plus ansi-off: valid pair type: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint + tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint + int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint + bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint + float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | float |
        | tinyint + double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint + dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | tinyint + str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint + date | CAST(6 AS TINYINT) | DATE'2024-01-15' | date |
        | tinyint + null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint + unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int + tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int + int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int + bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int + float | CAST(6 AS INT) | CAST(2 AS FLOAT) | float |
        | int + double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int + dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | int + str | CAST(6 AS INT) | '2' | double |
        | int + date | CAST(6 AS INT) | DATE'2024-01-15' | date |
        | int + null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int + unull | CAST(6 AS INT) | NULL | int |
        | bigint + tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint + int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint + bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint + float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | float |
        | bigint + double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint + dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,2) |
        | bigint + str | CAST(6 AS BIGINT) | '2' | double |
        | bigint + null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint + unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float + tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | float |
        | float + int | CAST(6 AS FLOAT) | CAST(2 AS INT) | float |
        | float + bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | float |
        | float + float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float + double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float + dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float + str | CAST(6 AS FLOAT) | '2' | double |
        | float + null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | float |
        | float + unull | CAST(6 AS FLOAT) | NULL | float |
        | double + tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double + int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double + bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double + float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double + double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double + dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double + str | CAST(6 AS DOUBLE) | '2' | double |
        | double + null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double + unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec + tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(11,2) |
        | dec + int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(13,2) |
        | dec + bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(23,2) |
        | dec + float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec + double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec + dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | dec + str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec + null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(13,2) |
        | dec + unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(11,2) |
        | str + tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str + int | '6' | CAST(2 AS INT) | double |
        | str + bigint | '6' | CAST(2 AS BIGINT) | double |
        | str + float | '6' | CAST(2 AS FLOAT) | double |
        | str + double | '6' | CAST(2 AS DOUBLE) | double |
        | str + dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str + str | '6' | '2' | double |
        | str + ival_d | '6' | INTERVAL '2' DAY | string |
        | str + null | '6' | CAST(NULL AS INT) | double |
        | str + unull | '6' | NULL | double |
        | date + tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) | date |
        | date + int | DATE'2024-01-15' | CAST(2 AS INT) | date |
        | date + ival_d | DATE'2024-01-15' | INTERVAL '2' DAY | date |
        | date + ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH | date |
        | date + null | DATE'2024-01-15' | CAST(NULL AS INT) | date |
        | ts + ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY | timestamp |
        | ts + ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH | timestamp |
        | ival_d + str | INTERVAL '2' DAY | '2' | string |
        | ival_d + date | INTERVAL '2' DAY | DATE'2024-01-15' | date |
        | ival_d + ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | ival_m + date | INTERVAL '2' MONTH | DATE'2024-01-15' | date |
        | ival_m + ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | null + tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null + int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null + bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null + float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | float |
        | null + double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null + dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | null + str | CAST(NULL AS INT) | '2' | double |
        | null + date | CAST(NULL AS INT) | DATE'2024-01-15' | date |
        | null + null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null + unull | CAST(NULL AS INT) | NULL | int |
        | unull + tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull + int | NULL | CAST(2 AS INT) | int |
        | unull + bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull + float | NULL | CAST(2 AS FLOAT) | float |
        | unull + double | NULL | CAST(2 AS DOUBLE) | double |
        | unull + dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | unull + str | NULL | '2' | double |
        | unull + null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: plus ansi-off: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | date + unull | DATE'2024-01-15' | NULL | timestamp |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL | timestamp |
        | ival_d + ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY | interval day |
        | ival_d + unull | INTERVAL '2' DAY | NULL | interval day |
        | ival_m + ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH | interval month |
        | ival_m + unull | INTERVAL '2' MONTH | NULL | interval month |
        | unull + date | NULL | DATE'2024-01-15' | timestamp |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | unull + ival_d | NULL | INTERVAL '2' DAY | interval day |
        | unull + ival_m | NULL | INTERVAL '2' MONTH | interval month |
        | unull + unull | NULL | NULL | double |

    Scenario Outline: plus ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool + bool | true | true |
        | bool + tinyint | true | CAST(2 AS TINYINT) |
        | bool + int | true | CAST(2 AS INT) |
        | bool + bigint | true | CAST(2 AS BIGINT) |
        | bool + float | true | CAST(2 AS FLOAT) |
        | bool + double | true | CAST(2 AS DOUBLE) |
        | bool + dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool + str | true | '2' |
        | bool + date | true | DATE'2024-01-15' |
        | bool + ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool + ival_d | true | INTERVAL '2' DAY |
        | bool + ival_m | true | INTERVAL '2' MONTH |
        | bool + bin | true | CAST('2' AS BINARY) |
        | bool + null | true | CAST(NULL AS INT) |
        | bool + unull | true | NULL |
        | tinyint + bool | CAST(6 AS TINYINT) | true |
        | tinyint + ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint + ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint + ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint + bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int + bool | CAST(6 AS INT) | true |
        | int + ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int + ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int + ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int + bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint + bool | CAST(6 AS BIGINT) | true |
        | bigint + date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint + ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint + ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint + ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint + bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float + bool | CAST(6 AS FLOAT) | true |
        | float + date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float + ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float + ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float + ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float + bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double + bool | CAST(6 AS DOUBLE) | true |
        | double + date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double + ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double + ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double + ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double + bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec + bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec + date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec + ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec + ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec + ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec + bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str + bool | '6' | true |
        | str + date | '6' | DATE'2024-01-15' |
        | str + ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str + ival_m | '6' | INTERVAL '2' MONTH |
        | str + bin | '6' | CAST('2' AS BINARY) |
        | date + bool | DATE'2024-01-15' | true |
        | date + bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date + float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date + double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date + dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date + str | DATE'2024-01-15' | '2' |
        | date + date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date + ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date + bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | ts + bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts + tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts + int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts + bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts + float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts + double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts + dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts + str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts + date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts + ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts + bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts + null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ival_d + bool | INTERVAL '2' DAY | true |
        | ival_d + tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d + int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d + bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d + float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d + double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d + dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d + ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d + bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d + null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_m + bool | INTERVAL '2' MONTH | true |
        | ival_m + tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m + int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m + bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m + float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m + double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m + dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m + str | INTERVAL '2' MONTH | '2' |
        | ival_m + ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m + bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m + null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | bin + bool | CAST('6' AS BINARY) | true |
        | bin + tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin + int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin + bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin + float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin + double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin + dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin + str | CAST('6' AS BINARY) | '2' |
        | bin + date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin + ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin + ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin + ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin + bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin + null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin + unull | CAST('6' AS BINARY) | NULL |
        | null + bool | CAST(NULL AS INT) | true |
        | null + ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null + ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null + ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null + bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull + bool | NULL | true |
        | unull + bin | NULL | CAST('2' AS BINARY) |

  Rule: `-` operand-type matrix (ANSI off)

    Scenario Outline: minus ansi-off: valid pair type: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint - tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint - int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint - bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint - float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | float |
        | tinyint - double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint - dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | tinyint - str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint - null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint - unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int - tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int - int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int - bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int - float | CAST(6 AS INT) | CAST(2 AS FLOAT) | float |
        | int - double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int - dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | int - str | CAST(6 AS INT) | '2' | double |
        | int - null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int - unull | CAST(6 AS INT) | NULL | int |
        | bigint - tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint - int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint - bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint - float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | float |
        | bigint - double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint - dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,2) |
        | bigint - str | CAST(6 AS BIGINT) | '2' | double |
        | bigint - null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint - unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float - tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | float |
        | float - int | CAST(6 AS FLOAT) | CAST(2 AS INT) | float |
        | float - bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | float |
        | float - float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float - double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float - dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float - str | CAST(6 AS FLOAT) | '2' | double |
        | float - null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | float |
        | float - unull | CAST(6 AS FLOAT) | NULL | float |
        | double - tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double - int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double - bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double - float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double - double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double - dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double - str | CAST(6 AS DOUBLE) | '2' | double |
        | double - null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double - unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec - tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(11,2) |
        | dec - int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(13,2) |
        | dec - bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(23,2) |
        | dec - float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec - double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec - dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | dec - str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec - null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(13,2) |
        | dec - unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(11,2) |
        | str - tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str - int | '6' | CAST(2 AS INT) | double |
        | str - bigint | '6' | CAST(2 AS BIGINT) | double |
        | str - float | '6' | CAST(2 AS FLOAT) | double |
        | str - double | '6' | CAST(2 AS DOUBLE) | double |
        | str - dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str - str | '6' | '2' | double |
        | str - null | '6' | CAST(NULL AS INT) | double |
        | str - unull | '6' | NULL | double |
        | date - tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) | date |
        | date - int | DATE'2024-01-15' | CAST(2 AS INT) | date |
        | date - ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | date - ival_d | DATE'2024-01-15' | INTERVAL '2' DAY | date |
        | date - ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH | date |
        | date - null | DATE'2024-01-15' | CAST(NULL AS INT) | date |
        | ts - date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' | interval day to second |
        | ts - ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | ts - ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY | timestamp |
        | ts - ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH | timestamp |
        | ts - unull | TIMESTAMP'2024-01-15 12:00:00' | NULL | interval day to second |
        | null - tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null - int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null - bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null - float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | float |
        | null - double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null - dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | null - str | CAST(NULL AS INT) | '2' | double |
        | null - null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null - unull | CAST(NULL AS INT) | NULL | int |
        | unull - tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull - int | NULL | CAST(2 AS INT) | int |
        | unull - bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull - float | NULL | CAST(2 AS FLOAT) | float |
        | unull - double | NULL | CAST(2 AS DOUBLE) | double |
        | unull - dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | unull - str | NULL | '2' | double |
        | unull - ts | NULL | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | unull - null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: minus ansi-off: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | str - date | '6' | DATE'2024-01-15' | interval day |
        | str - ival_d | '6' | INTERVAL '2' DAY | string |
        | date - date | DATE'2024-01-15' | DATE'2024-01-15' | interval day |
        | date - unull | DATE'2024-01-15' | NULL | interval day |
        | ival_d - ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY | interval day |
        | ival_d - unull | INTERVAL '2' DAY | NULL | interval day |
        | ival_m - ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH | interval month |
        | ival_m - unull | INTERVAL '2' MONTH | NULL | interval month |
        | unull - date | NULL | DATE'2024-01-15' | interval day |
        | unull - ival_d | NULL | INTERVAL '2' DAY | interval day |
        | unull - ival_m | NULL | INTERVAL '2' MONTH | interval month |
        | unull - unull | NULL | NULL | double |

    Scenario Outline: minus ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool - bool | true | true |
        | bool - tinyint | true | CAST(2 AS TINYINT) |
        | bool - int | true | CAST(2 AS INT) |
        | bool - bigint | true | CAST(2 AS BIGINT) |
        | bool - float | true | CAST(2 AS FLOAT) |
        | bool - double | true | CAST(2 AS DOUBLE) |
        | bool - dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool - str | true | '2' |
        | bool - date | true | DATE'2024-01-15' |
        | bool - ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool - ival_d | true | INTERVAL '2' DAY |
        | bool - ival_m | true | INTERVAL '2' MONTH |
        | bool - bin | true | CAST('2' AS BINARY) |
        | bool - null | true | CAST(NULL AS INT) |
        | bool - unull | true | NULL |
        | tinyint - bool | CAST(6 AS TINYINT) | true |
        | tinyint - date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint - ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint - ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint - ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint - bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int - bool | CAST(6 AS INT) | true |
        | int - date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int - ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int - ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int - ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int - bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint - bool | CAST(6 AS BIGINT) | true |
        | bigint - date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint - ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint - ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint - ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint - bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float - bool | CAST(6 AS FLOAT) | true |
        | float - date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float - ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float - ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float - ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float - bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double - bool | CAST(6 AS DOUBLE) | true |
        | double - date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double - ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double - ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double - ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double - bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec - bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec - date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec - ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec - ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec - ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec - bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str - bool | '6' | true |
        | str - ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str - ival_m | '6' | INTERVAL '2' MONTH |
        | str - bin | '6' | CAST('2' AS BINARY) |
        | date - bool | DATE'2024-01-15' | true |
        | date - bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date - float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date - double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date - dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date - str | DATE'2024-01-15' | '2' |
        | date - bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | ts - bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts - tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts - int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts - bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts - float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts - double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts - dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts - str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts - bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts - null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ival_d - bool | INTERVAL '2' DAY | true |
        | ival_d - tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d - int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d - bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d - float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d - double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d - dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d - str | INTERVAL '2' DAY | '2' |
        | ival_d - date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d - ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d - ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d - bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d - null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_m - bool | INTERVAL '2' MONTH | true |
        | ival_m - tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m - int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m - bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m - float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m - double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m - dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m - str | INTERVAL '2' MONTH | '2' |
        | ival_m - date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m - ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m - ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m - bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m - null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | bin - bool | CAST('6' AS BINARY) | true |
        | bin - tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin - int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin - bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin - float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin - double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin - dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin - str | CAST('6' AS BINARY) | '2' |
        | bin - date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin - ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin - ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin - ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin - bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin - null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin - unull | CAST('6' AS BINARY) | NULL |
        | null - bool | CAST(NULL AS INT) | true |
        | null - date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null - ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null - ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null - ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null - bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull - bool | NULL | true |
        | unull - bin | NULL | CAST('2' AS BINARY) |

  Rule: `*` operand-type matrix (ANSI off)

    Scenario Outline: times ansi-off: valid pair type: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint * tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint * int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint * bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint * float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | float |
        | tinyint * double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint * dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(14,2) |
        | tinyint * str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint * ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY | interval day to second |
        | tinyint * null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint * unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int * tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int * int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int * bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int * float | CAST(6 AS INT) | CAST(2 AS FLOAT) | float |
        | int * double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int * dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(21,2) |
        | int * str | CAST(6 AS INT) | '2' | double |
        | int * ival_d | CAST(6 AS INT) | INTERVAL '2' DAY | interval day to second |
        | int * null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int * unull | CAST(6 AS INT) | NULL | int |
        | bigint * tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint * int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint * bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint * float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | float |
        | bigint * double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint * dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(31,2) |
        | bigint * str | CAST(6 AS BIGINT) | '2' | double |
        | bigint * ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY | interval day to second |
        | bigint * null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint * unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float * tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | float |
        | float * int | CAST(6 AS FLOAT) | CAST(2 AS INT) | float |
        | float * bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | float |
        | float * float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float * double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float * dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float * str | CAST(6 AS FLOAT) | '2' | double |
        | float * ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY | interval day to second |
        | float * null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | float |
        | float * unull | CAST(6 AS FLOAT) | NULL | float |
        | double * tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double * int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double * bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double * float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double * double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double * dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double * str | CAST(6 AS DOUBLE) | '2' | double |
        | double * ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY | interval day to second |
        | double * null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double * unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec * tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(14,2) |
        | dec * int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(21,2) |
        | dec * bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(31,2) |
        | dec * float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec * double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec * dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(21,4) |
        | dec * str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec * ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY | interval day to second |
        | dec * null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(21,2) |
        | dec * unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(21,4) |
        | str * tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str * int | '6' | CAST(2 AS INT) | double |
        | str * bigint | '6' | CAST(2 AS BIGINT) | double |
        | str * float | '6' | CAST(2 AS FLOAT) | double |
        | str * double | '6' | CAST(2 AS DOUBLE) | double |
        | str * dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str * str | '6' | '2' | double |
        | str * ival_d | '6' | INTERVAL '2' DAY | interval day to second |
        | str * null | '6' | CAST(NULL AS INT) | double |
        | str * unull | '6' | NULL | double |
        | ival_d * tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) | interval day to second |
        | ival_d * int | INTERVAL '2' DAY | CAST(2 AS INT) | interval day to second |
        | ival_d * bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) | interval day to second |
        | ival_d * float | INTERVAL '2' DAY | CAST(2 AS FLOAT) | interval day to second |
        | ival_d * double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) | interval day to second |
        | ival_d * dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) | interval day to second |
        | ival_d * str | INTERVAL '2' DAY | '2' | interval day to second |
        | ival_d * null | INTERVAL '2' DAY | CAST(NULL AS INT) | interval day to second |
        | ival_d * unull | INTERVAL '2' DAY | NULL | interval day to second |
        | null * tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null * int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null * bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null * float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | float |
        | null * double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null * dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(21,2) |
        | null * str | CAST(NULL AS INT) | '2' | double |
        | null * ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY | interval day to second |
        | null * null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null * unull | CAST(NULL AS INT) | NULL | int |
        | unull * tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull * int | NULL | CAST(2 AS INT) | int |
        | unull * bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull * float | NULL | CAST(2 AS FLOAT) | float |
        | unull * double | NULL | CAST(2 AS DOUBLE) | double |
        | unull * dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(21,4) |
        | unull * str | NULL | '2' | double |
        | unull * ival_d | NULL | INTERVAL '2' DAY | interval day to second |
        | unull * null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: times ansi-off: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint * ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH | interval year to month |
        | int * ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH | interval year to month |
        | bigint * ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH | interval year to month |
        | float * ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH | interval year to month |
        | double * ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH | interval year to month |
        | dec * ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH | interval year to month |
        | str * ival_m | '6' | INTERVAL '2' MONTH | interval year to month |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) | interval year to month |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) | interval year to month |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) | interval year to month |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) | interval year to month |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) | interval year to month |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) | interval year to month |
        | ival_m * str | INTERVAL '2' MONTH | '2' | interval year to month |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) | interval year to month |
        | ival_m * unull | INTERVAL '2' MONTH | NULL | interval year to month |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH | interval year to month |
        | unull * ival_m | NULL | INTERVAL '2' MONTH | interval year to month |
        | unull * unull | NULL | NULL | double |

    Scenario Outline: times ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool * bool | true | true |
        | bool * tinyint | true | CAST(2 AS TINYINT) |
        | bool * int | true | CAST(2 AS INT) |
        | bool * bigint | true | CAST(2 AS BIGINT) |
        | bool * float | true | CAST(2 AS FLOAT) |
        | bool * double | true | CAST(2 AS DOUBLE) |
        | bool * dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool * str | true | '2' |
        | bool * date | true | DATE'2024-01-15' |
        | bool * ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool * ival_d | true | INTERVAL '2' DAY |
        | bool * ival_m | true | INTERVAL '2' MONTH |
        | bool * bin | true | CAST('2' AS BINARY) |
        | bool * null | true | CAST(NULL AS INT) |
        | bool * unull | true | NULL |
        | tinyint * bool | CAST(6 AS TINYINT) | true |
        | tinyint * date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint * ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint * bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int * bool | CAST(6 AS INT) | true |
        | int * date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int * ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int * bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint * bool | CAST(6 AS BIGINT) | true |
        | bigint * date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint * ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint * bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float * bool | CAST(6 AS FLOAT) | true |
        | float * date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float * ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float * bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double * bool | CAST(6 AS DOUBLE) | true |
        | double * date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double * ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double * bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec * bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec * date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec * ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec * bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str * bool | '6' | true |
        | str * date | '6' | DATE'2024-01-15' |
        | str * ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str * bin | '6' | CAST('2' AS BINARY) |
        | date * bool | DATE'2024-01-15' | true |
        | date * tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date * int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date * bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date * float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date * double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date * dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date * str | DATE'2024-01-15' | '2' |
        | date * date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date * ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date * ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date * ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date * bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date * null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date * unull | DATE'2024-01-15' | NULL |
        | ts * bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts * tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts * int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts * bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts * float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts * double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts * dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts * str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts * date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts * ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts * ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts * ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts * bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts * null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts * unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d * bool | INTERVAL '2' DAY | true |
        | ival_d * date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d * ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d * ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d * ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d * bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_m * bool | INTERVAL '2' MONTH | true |
        | ival_m * date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m * ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m * ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m * ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m * bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | bin * bool | CAST('6' AS BINARY) | true |
        | bin * tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin * int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin * bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin * float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin * double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin * dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin * str | CAST('6' AS BINARY) | '2' |
        | bin * date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin * ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin * ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin * ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin * bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin * null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin * unull | CAST('6' AS BINARY) | NULL |
        | null * bool | CAST(NULL AS INT) | true |
        | null * date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null * ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null * bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull * bool | NULL | true |
        | unull * date | NULL | DATE'2024-01-15' |
        | unull * ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull * bin | NULL | CAST('2' AS BINARY) |

  Rule: `/` operand-type matrix (ANSI off)

    Scenario Outline: divide ansi-off: valid pair type: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint / tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | double |
        | tinyint / int | CAST(6 AS TINYINT) | CAST(2 AS INT) | double |
        | tinyint / bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | double |
        | tinyint / float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint / double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint / dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(16,11) |
        | tinyint / str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint / null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | double |
        | tinyint / unull | CAST(6 AS TINYINT) | NULL | double |
        | int / tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | double |
        | int / int | CAST(6 AS INT) | CAST(2 AS INT) | double |
        | int / bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | double |
        | int / float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int / double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int / dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,11) |
        | int / str | CAST(6 AS INT) | '2' | double |
        | int / null | CAST(6 AS INT) | CAST(NULL AS INT) | double |
        | int / unull | CAST(6 AS INT) | NULL | double |
        | bigint / tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | double |
        | bigint / int | CAST(6 AS BIGINT) | CAST(2 AS INT) | double |
        | bigint / bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | double |
        | bigint / float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint / double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint / dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(33,11) |
        | bigint / str | CAST(6 AS BIGINT) | '2' | double |
        | bigint / null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | double |
        | bigint / unull | CAST(6 AS BIGINT) | NULL | double |
        | float / tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float / int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float / bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float / float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | double |
        | float / double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float / dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float / str | CAST(6 AS FLOAT) | '2' | double |
        | float / null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float / unull | CAST(6 AS FLOAT) | NULL | double |
        | double / tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double / int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double / bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double / float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double / double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double / dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double / str | CAST(6 AS DOUBLE) | '2' | double |
        | double / null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double / unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec / tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(14,6) |
        | dec / int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(21,13) |
        | dec / bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(31,23) |
        | dec / float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec / double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec / dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(23,13) |
        | dec / str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec / null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(21,13) |
        | dec / unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(23,13) |
        | str / tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str / int | '6' | CAST(2 AS INT) | double |
        | str / bigint | '6' | CAST(2 AS BIGINT) | double |
        | str / float | '6' | CAST(2 AS FLOAT) | double |
        | str / double | '6' | CAST(2 AS DOUBLE) | double |
        | str / dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str / str | '6' | '2' | double |
        | str / null | '6' | CAST(NULL AS INT) | double |
        | str / unull | '6' | NULL | double |
        | ival_d / tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) | interval day to second |
        | ival_d / int | INTERVAL '2' DAY | CAST(2 AS INT) | interval day to second |
        | ival_d / bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) | interval day to second |
        | ival_d / float | INTERVAL '2' DAY | CAST(2 AS FLOAT) | interval day to second |
        | ival_d / double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) | interval day to second |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) | interval day to second |
        | ival_d / str | INTERVAL '2' DAY | '2' | interval day to second |
        | ival_d / null | INTERVAL '2' DAY | CAST(NULL AS INT) | interval day to second |
        | ival_d / unull | INTERVAL '2' DAY | NULL | interval day to second |
        | null / tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | double |
        | null / int | CAST(NULL AS INT) | CAST(2 AS INT) | double |
        | null / bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | double |
        | null / float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null / double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null / dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,11) |
        | null / str | CAST(NULL AS INT) | '2' | double |
        | null / null | CAST(NULL AS INT) | CAST(NULL AS INT) | double |
        | null / unull | CAST(NULL AS INT) | NULL | double |
        | unull / tinyint | NULL | CAST(2 AS TINYINT) | double |
        | unull / int | NULL | CAST(2 AS INT) | double |
        | unull / bigint | NULL | CAST(2 AS BIGINT) | double |
        | unull / float | NULL | CAST(2 AS FLOAT) | double |
        | unull / double | NULL | CAST(2 AS DOUBLE) | double |
        | unull / dec | NULL | CAST(2 AS DECIMAL(10,2)) | double |
        | unull / str | NULL | '2' | double |
        | unull / null | NULL | CAST(NULL AS INT) | double |
        | unull / unull | NULL | NULL | double |

    @sail-bug
    Scenario Outline: divide ansi-off: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) | interval year to month |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) | interval year to month |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) | interval year to month |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) | interval year to month |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) | interval year to month |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) | interval year to month |
        | ival_m / str | INTERVAL '2' MONTH | '2' | interval year to month |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) | interval year to month |
        | ival_m / unull | INTERVAL '2' MONTH | NULL | interval year to month |

    Scenario Outline: divide ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool / bool | true | true |
        | bool / tinyint | true | CAST(2 AS TINYINT) |
        | bool / int | true | CAST(2 AS INT) |
        | bool / bigint | true | CAST(2 AS BIGINT) |
        | bool / float | true | CAST(2 AS FLOAT) |
        | bool / double | true | CAST(2 AS DOUBLE) |
        | bool / dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool / str | true | '2' |
        | bool / date | true | DATE'2024-01-15' |
        | bool / ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool / ival_d | true | INTERVAL '2' DAY |
        | bool / ival_m | true | INTERVAL '2' MONTH |
        | bool / bin | true | CAST('2' AS BINARY) |
        | bool / null | true | CAST(NULL AS INT) |
        | bool / unull | true | NULL |
        | tinyint / bool | CAST(6 AS TINYINT) | true |
        | tinyint / date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint / ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint / ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint / ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint / bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int / bool | CAST(6 AS INT) | true |
        | int / date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int / ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int / ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int / ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int / bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint / bool | CAST(6 AS BIGINT) | true |
        | bigint / date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint / ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint / ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint / ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint / bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float / bool | CAST(6 AS FLOAT) | true |
        | float / date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float / ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float / ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float / ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float / bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double / bool | CAST(6 AS DOUBLE) | true |
        | double / date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double / ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double / ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double / ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double / bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec / bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec / date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec / ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec / ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec / ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec / bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str / bool | '6' | true |
        | str / date | '6' | DATE'2024-01-15' |
        | str / ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str / ival_d | '6' | INTERVAL '2' DAY |
        | str / ival_m | '6' | INTERVAL '2' MONTH |
        | str / bin | '6' | CAST('2' AS BINARY) |
        | date / bool | DATE'2024-01-15' | true |
        | date / tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date / int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date / bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date / float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date / double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date / dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date / str | DATE'2024-01-15' | '2' |
        | date / date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date / ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date / ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date / ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date / bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date / null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date / unull | DATE'2024-01-15' | NULL |
        | ts / bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts / tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts / int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts / bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts / float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts / double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts / dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts / str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts / date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts / ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts / ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts / ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts / bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts / null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts / unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d / bool | INTERVAL '2' DAY | true |
        | ival_d / date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d / ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d / ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d / ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d / bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_m / bool | INTERVAL '2' MONTH | true |
        | ival_m / date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m / ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m / ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m / ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m / bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | bin / bool | CAST('6' AS BINARY) | true |
        | bin / tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin / int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin / bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin / float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin / double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin / dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin / str | CAST('6' AS BINARY) | '2' |
        | bin / date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin / ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin / ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin / ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin / bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin / null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin / unull | CAST('6' AS BINARY) | NULL |
        | null / bool | CAST(NULL AS INT) | true |
        | null / date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null / ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null / ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null / ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null / bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull / bool | NULL | true |
        | unull / date | NULL | DATE'2024-01-15' |
        | unull / ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull / ival_d | NULL | INTERVAL '2' DAY |
        | unull / ival_m | NULL | INTERVAL '2' MONTH |
        | unull / bin | NULL | CAST('2' AS BINARY) |

  Rule: `%` operand-type matrix (ANSI off)

    Scenario Outline: modulo ansi-off: valid pair type: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint % tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint % int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint % bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint % float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | float |
        | tinyint % double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint % dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(5,2) |
        | tinyint % str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint % null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | int % tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int % int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int % bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int % float | CAST(6 AS INT) | CAST(2 AS FLOAT) | float |
        | int % double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int % dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | int % str | CAST(6 AS INT) | '2' | double |
        | int % null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int % unull | CAST(6 AS INT) | NULL | int |
        | bigint % tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint % int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint % bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint % float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | float |
        | bigint % double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint % dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | bigint % str | CAST(6 AS BIGINT) | '2' | double |
        | bigint % null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint % unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float % tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | float |
        | float % int | CAST(6 AS FLOAT) | CAST(2 AS INT) | float |
        | float % bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | float |
        | float % float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float % double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float % dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float % str | CAST(6 AS FLOAT) | '2' | double |
        | float % null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | float |
        | float % unull | CAST(6 AS FLOAT) | NULL | float |
        | double % tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double % int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double % bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double % float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double % double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double % dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double % str | CAST(6 AS DOUBLE) | '2' | double |
        | double % null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double % unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec % tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(5,2) |
        | dec % int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(10,2) |
        | dec % bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(10,2) |
        | dec % float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec % double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec % dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | dec % str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec % null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(10,2) |
        | dec % unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(10,2) |
        | str % tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str % int | '6' | CAST(2 AS INT) | double |
        | str % bigint | '6' | CAST(2 AS BIGINT) | double |
        | str % float | '6' | CAST(2 AS FLOAT) | double |
        | str % double | '6' | CAST(2 AS DOUBLE) | double |
        | str % dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str % str | '6' | '2' | double |
        | str % null | '6' | CAST(NULL AS INT) | double |
        | str % unull | '6' | NULL | double |
        | null % tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null % int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null % bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null % float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | float |
        | null % double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null % dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | null % str | CAST(NULL AS INT) | '2' | double |
        | null % null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null % unull | CAST(NULL AS INT) | NULL | int |
        | unull % tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull % int | NULL | CAST(2 AS INT) | int |
        | unull % bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull % float | NULL | CAST(2 AS FLOAT) | float |
        | unull % double | NULL | CAST(2 AS DOUBLE) | double |
        | unull % dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | unull % str | NULL | '2' | double |
        | unull % null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: modulo ansi-off: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint % unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | unull % unull | NULL | NULL | double |

    Scenario Outline: modulo ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool % bool | true | true |
        | bool % tinyint | true | CAST(2 AS TINYINT) |
        | bool % int | true | CAST(2 AS INT) |
        | bool % bigint | true | CAST(2 AS BIGINT) |
        | bool % float | true | CAST(2 AS FLOAT) |
        | bool % double | true | CAST(2 AS DOUBLE) |
        | bool % dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool % str | true | '2' |
        | bool % date | true | DATE'2024-01-15' |
        | bool % ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool % ival_d | true | INTERVAL '2' DAY |
        | bool % ival_m | true | INTERVAL '2' MONTH |
        | bool % bin | true | CAST('2' AS BINARY) |
        | bool % null | true | CAST(NULL AS INT) |
        | bool % unull | true | NULL |
        | tinyint % bool | CAST(6 AS TINYINT) | true |
        | tinyint % date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint % ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint % ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint % ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint % bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int % bool | CAST(6 AS INT) | true |
        | int % date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int % ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int % ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int % ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int % bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint % bool | CAST(6 AS BIGINT) | true |
        | bigint % date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint % ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint % ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint % ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint % bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float % bool | CAST(6 AS FLOAT) | true |
        | float % date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float % ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float % ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float % ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float % bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double % bool | CAST(6 AS DOUBLE) | true |
        | double % date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double % ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double % ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double % ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double % bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec % bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec % date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec % ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec % ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec % ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec % bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str % bool | '6' | true |
        | str % date | '6' | DATE'2024-01-15' |
        | str % ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str % ival_d | '6' | INTERVAL '2' DAY |
        | str % ival_m | '6' | INTERVAL '2' MONTH |
        | str % bin | '6' | CAST('2' AS BINARY) |
        | date % bool | DATE'2024-01-15' | true |
        | date % tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date % int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date % bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date % float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date % double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date % dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date % str | DATE'2024-01-15' | '2' |
        | date % date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date % ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date % ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date % ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date % bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date % null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date % unull | DATE'2024-01-15' | NULL |
        | ts % bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts % tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts % int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts % bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts % float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts % double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts % dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts % str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts % date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts % ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts % ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts % ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts % bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts % null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts % unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d % bool | INTERVAL '2' DAY | true |
        | ival_d % tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d % int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d % bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d % float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d % double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d % dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d % str | INTERVAL '2' DAY | '2' |
        | ival_d % date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d % ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d % ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d % ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d % bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d % null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d % unull | INTERVAL '2' DAY | NULL |
        | ival_m % bool | INTERVAL '2' MONTH | true |
        | ival_m % tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m % int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m % bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m % float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m % double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m % dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m % str | INTERVAL '2' MONTH | '2' |
        | ival_m % date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m % ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m % ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m % ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m % bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m % null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m % unull | INTERVAL '2' MONTH | NULL |
        | bin % bool | CAST('6' AS BINARY) | true |
        | bin % tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin % int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin % bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin % float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin % double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin % dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin % str | CAST('6' AS BINARY) | '2' |
        | bin % date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin % ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin % ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin % ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin % bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin % null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin % unull | CAST('6' AS BINARY) | NULL |
        | null % bool | CAST(NULL AS INT) | true |
        | null % date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null % ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null % ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null % ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null % bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull % bool | NULL | true |
        | unull % date | NULL | DATE'2024-01-15' |
        | unull % ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull % ival_d | NULL | INTERVAL '2' DAY |
        | unull % ival_m | NULL | INTERVAL '2' MONTH |
        | unull % bin | NULL | CAST('2' AS BINARY) |

# ============================ ANSI ON ============================

  Rule: `+` operand-type matrix (ANSI on)

    Scenario Outline: plus ansi-on: valid pair type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint + tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint + int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint + bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint + float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint + double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint + dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | tinyint + str | CAST(6 AS TINYINT) | '2' | bigint |
        | tinyint + date | CAST(6 AS TINYINT) | DATE'2024-01-15' | date |
        | tinyint + null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint + unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int + tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int + int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int + bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int + float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int + double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int + dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | int + str | CAST(6 AS INT) | '2' | bigint |
        | int + date | CAST(6 AS INT) | DATE'2024-01-15' | date |
        | int + null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int + unull | CAST(6 AS INT) | NULL | int |
        | bigint + tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint + int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint + bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint + float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint + double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint + dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,2) |
        | bigint + str | CAST(6 AS BIGINT) | '2' | bigint |
        | bigint + null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint + unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float + tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float + int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float + bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float + float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float + double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float + dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float + str | CAST(6 AS FLOAT) | '2' | double |
        | float + null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float + unull | CAST(6 AS FLOAT) | NULL | float |
        | double + tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double + int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double + bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double + float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double + double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double + dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double + str | CAST(6 AS DOUBLE) | '2' | double |
        | double + null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double + unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec + tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(11,2) |
        | dec + int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(13,2) |
        | dec + bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(23,2) |
        | dec + float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec + double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec + dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | dec + str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec + null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(13,2) |
        | dec + unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(11,2) |
        | str + tinyint | '6' | CAST(2 AS TINYINT) | bigint |
        | str + int | '6' | CAST(2 AS INT) | bigint |
        | str + bigint | '6' | CAST(2 AS BIGINT) | bigint |
        | str + float | '6' | CAST(2 AS FLOAT) | double |
        | str + double | '6' | CAST(2 AS DOUBLE) | double |
        | str + dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str + ival_d | '6' | INTERVAL '2' DAY | string |
        | str + null | '6' | CAST(NULL AS INT) | bigint |
        | date + tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) | date |
        | date + int | DATE'2024-01-15' | CAST(2 AS INT) | date |
        | date + ival_d | DATE'2024-01-15' | INTERVAL '2' DAY | date |
        | date + ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH | date |
        | date + null | DATE'2024-01-15' | CAST(NULL AS INT) | date |
        | ts + ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY | timestamp |
        | ts + ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH | timestamp |
        | ival_d + str | INTERVAL '2' DAY | '2' | string |
        | ival_d + date | INTERVAL '2' DAY | DATE'2024-01-15' | date |
        | ival_d + ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | ival_m + date | INTERVAL '2' MONTH | DATE'2024-01-15' | date |
        | ival_m + ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | null + tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null + int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null + bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null + float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null + double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null + dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | null + str | CAST(NULL AS INT) | '2' | bigint |
        | null + date | CAST(NULL AS INT) | DATE'2024-01-15' | date |
        | null + null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null + unull | CAST(NULL AS INT) | NULL | int |
        | unull + tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull + int | NULL | CAST(2 AS INT) | int |
        | unull + bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull + float | NULL | CAST(2 AS FLOAT) | float |
        | unull + double | NULL | CAST(2 AS DOUBLE) | double |
        | unull + dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | unull + null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: plus ansi-on: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | date + unull | DATE'2024-01-15' | NULL | timestamp |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL | timestamp |
        | ival_d + ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY | interval day |
        | ival_d + unull | INTERVAL '2' DAY | NULL | interval day |
        | ival_m + ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH | interval month |
        | ival_m + unull | INTERVAL '2' MONTH | NULL | interval month |
        | unull + date | NULL | DATE'2024-01-15' | timestamp |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' | timestamp |
        | unull + ival_d | NULL | INTERVAL '2' DAY | interval day |
        | unull + ival_m | NULL | INTERVAL '2' MONTH | interval month |
        | unull + unull | NULL | NULL | double |

    Scenario Outline: plus ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool + bool | true | true |
        | bool + tinyint | true | CAST(2 AS TINYINT) |
        | bool + int | true | CAST(2 AS INT) |
        | bool + bigint | true | CAST(2 AS BIGINT) |
        | bool + float | true | CAST(2 AS FLOAT) |
        | bool + double | true | CAST(2 AS DOUBLE) |
        | bool + dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool + str | true | '2' |
        | bool + date | true | DATE'2024-01-15' |
        | bool + ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool + ival_d | true | INTERVAL '2' DAY |
        | bool + ival_m | true | INTERVAL '2' MONTH |
        | bool + bin | true | CAST('2' AS BINARY) |
        | bool + null | true | CAST(NULL AS INT) |
        | bool + unull | true | NULL |
        | tinyint + bool | CAST(6 AS TINYINT) | true |
        | tinyint + ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint + ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint + ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint + bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int + bool | CAST(6 AS INT) | true |
        | int + ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int + ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int + ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int + bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint + bool | CAST(6 AS BIGINT) | true |
        | bigint + date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint + ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint + ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint + ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint + bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float + bool | CAST(6 AS FLOAT) | true |
        | float + date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float + ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float + ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float + ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float + bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double + bool | CAST(6 AS DOUBLE) | true |
        | double + date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double + ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double + ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double + ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double + bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec + bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec + date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec + ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec + ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec + ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec + bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str + bool | '6' | true |
        | str + str | '6' | '2' |
        | str + date | '6' | DATE'2024-01-15' |
        | str + ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str + ival_m | '6' | INTERVAL '2' MONTH |
        | str + bin | '6' | CAST('2' AS BINARY) |
        | str + unull | '6' | NULL |
        | date + bool | DATE'2024-01-15' | true |
        | date + bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date + float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date + double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date + dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date + str | DATE'2024-01-15' | '2' |
        | date + date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date + ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date + bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | ts + bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts + tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts + int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts + bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts + float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts + double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts + dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts + str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts + date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts + ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts + bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts + null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ival_d + bool | INTERVAL '2' DAY | true |
        | ival_d + tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d + int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d + bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d + float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d + double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d + dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d + ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d + bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d + null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_m + bool | INTERVAL '2' MONTH | true |
        | ival_m + tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m + int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m + bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m + float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m + double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m + dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m + str | INTERVAL '2' MONTH | '2' |
        | ival_m + ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m + bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m + null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | bin + bool | CAST('6' AS BINARY) | true |
        | bin + tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin + int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin + bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin + float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin + double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin + dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin + str | CAST('6' AS BINARY) | '2' |
        | bin + date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin + ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin + ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin + ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin + bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin + null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin + unull | CAST('6' AS BINARY) | NULL |
        | null + bool | CAST(NULL AS INT) | true |
        | null + ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null + ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null + ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null + bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull + bool | NULL | true |
        | unull + str | NULL | '2' |
        | unull + bin | NULL | CAST('2' AS BINARY) |

  Rule: `-` operand-type matrix (ANSI on)

    Scenario Outline: minus ansi-on: valid pair type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint - tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint - int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint - bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint - float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint - double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint - dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | tinyint - str | CAST(6 AS TINYINT) | '2' | bigint |
        | tinyint - null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint - unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int - tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int - int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int - bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int - float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int - double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int - dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | int - str | CAST(6 AS INT) | '2' | bigint |
        | int - null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int - unull | CAST(6 AS INT) | NULL | int |
        | bigint - tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint - int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint - bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint - float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint - double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint - dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,2) |
        | bigint - str | CAST(6 AS BIGINT) | '2' | bigint |
        | bigint - null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint - unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float - tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float - int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float - bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float - float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float - double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float - dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float - str | CAST(6 AS FLOAT) | '2' | double |
        | float - null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float - unull | CAST(6 AS FLOAT) | NULL | float |
        | double - tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double - int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double - bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double - float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double - double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double - dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double - str | CAST(6 AS DOUBLE) | '2' | double |
        | double - null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double - unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec - tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(11,2) |
        | dec - int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(13,2) |
        | dec - bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(23,2) |
        | dec - float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec - double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec - dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | dec - str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec - null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(13,2) |
        | dec - unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(11,2) |
        | str - tinyint | '6' | CAST(2 AS TINYINT) | bigint |
        | str - int | '6' | CAST(2 AS INT) | bigint |
        | str - bigint | '6' | CAST(2 AS BIGINT) | bigint |
        | str - float | '6' | CAST(2 AS FLOAT) | double |
        | str - double | '6' | CAST(2 AS DOUBLE) | double |
        | str - dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str - null | '6' | CAST(NULL AS INT) | bigint |
        | date - tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) | date |
        | date - int | DATE'2024-01-15' | CAST(2 AS INT) | date |
        | date - ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | date - ival_d | DATE'2024-01-15' | INTERVAL '2' DAY | date |
        | date - ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH | date |
        | date - null | DATE'2024-01-15' | CAST(NULL AS INT) | date |
        | ts - date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' | interval day to second |
        | ts - ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | ts - ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY | timestamp |
        | ts - ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH | timestamp |
        | ts - unull | TIMESTAMP'2024-01-15 12:00:00' | NULL | interval day to second |
        | null - tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null - int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null - bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null - float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null - double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null - dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(13,2) |
        | null - str | CAST(NULL AS INT) | '2' | bigint |
        | null - null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null - unull | CAST(NULL AS INT) | NULL | int |
        | unull - tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull - int | NULL | CAST(2 AS INT) | int |
        | unull - bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull - float | NULL | CAST(2 AS FLOAT) | float |
        | unull - double | NULL | CAST(2 AS DOUBLE) | double |
        | unull - dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(11,2) |
        | unull - ts | NULL | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | unull - null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: minus ansi-on: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | str - date | '6' | DATE'2024-01-15' | interval day |
        | str - ts | '6' | TIMESTAMP'2024-01-15 12:00:00' | interval day to second |
        | str - ival_d | '6' | INTERVAL '2' DAY | string |
        | date - str | DATE'2024-01-15' | '2' | interval day |
        | date - date | DATE'2024-01-15' | DATE'2024-01-15' | interval day |
        | date - unull | DATE'2024-01-15' | NULL | interval day |
        | ts - str | TIMESTAMP'2024-01-15 12:00:00' | '2' | interval day to second |
        | ival_d - ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY | interval day |
        | ival_d - unull | INTERVAL '2' DAY | NULL | interval day |
        | ival_m - ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH | interval month |
        | ival_m - unull | INTERVAL '2' MONTH | NULL | interval month |
        | unull - date | NULL | DATE'2024-01-15' | interval day |
        | unull - ival_d | NULL | INTERVAL '2' DAY | interval day |
        | unull - ival_m | NULL | INTERVAL '2' MONTH | interval month |
        | unull - unull | NULL | NULL | double |

    Scenario Outline: minus ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool - bool | true | true |
        | bool - tinyint | true | CAST(2 AS TINYINT) |
        | bool - int | true | CAST(2 AS INT) |
        | bool - bigint | true | CAST(2 AS BIGINT) |
        | bool - float | true | CAST(2 AS FLOAT) |
        | bool - double | true | CAST(2 AS DOUBLE) |
        | bool - dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool - str | true | '2' |
        | bool - date | true | DATE'2024-01-15' |
        | bool - ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool - ival_d | true | INTERVAL '2' DAY |
        | bool - ival_m | true | INTERVAL '2' MONTH |
        | bool - bin | true | CAST('2' AS BINARY) |
        | bool - null | true | CAST(NULL AS INT) |
        | bool - unull | true | NULL |
        | tinyint - bool | CAST(6 AS TINYINT) | true |
        | tinyint - date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint - ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint - ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint - ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint - bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int - bool | CAST(6 AS INT) | true |
        | int - date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int - ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int - ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int - ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int - bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint - bool | CAST(6 AS BIGINT) | true |
        | bigint - date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint - ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint - ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint - ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint - bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float - bool | CAST(6 AS FLOAT) | true |
        | float - date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float - ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float - ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float - ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float - bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double - bool | CAST(6 AS DOUBLE) | true |
        | double - date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double - ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double - ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double - ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double - bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec - bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec - date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec - ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec - ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec - ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec - bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str - bool | '6' | true |
        | str - str | '6' | '2' |
        | str - ival_m | '6' | INTERVAL '2' MONTH |
        | str - bin | '6' | CAST('2' AS BINARY) |
        | str - unull | '6' | NULL |
        | date - bool | DATE'2024-01-15' | true |
        | date - bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date - float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date - double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date - dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date - bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | ts - bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts - tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts - int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts - bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts - float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts - double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts - dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts - bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts - null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ival_d - bool | INTERVAL '2' DAY | true |
        | ival_d - tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d - int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d - bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d - float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d - double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d - dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d - str | INTERVAL '2' DAY | '2' |
        | ival_d - date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d - ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d - ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d - bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d - null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_m - bool | INTERVAL '2' MONTH | true |
        | ival_m - tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m - int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m - bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m - float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m - double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m - dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m - str | INTERVAL '2' MONTH | '2' |
        | ival_m - date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m - ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m - ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m - bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m - null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | bin - bool | CAST('6' AS BINARY) | true |
        | bin - tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin - int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin - bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin - float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin - double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin - dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin - str | CAST('6' AS BINARY) | '2' |
        | bin - date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin - ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin - ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin - ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin - bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin - null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin - unull | CAST('6' AS BINARY) | NULL |
        | null - bool | CAST(NULL AS INT) | true |
        | null - date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null - ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null - ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null - ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null - bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull - bool | NULL | true |
        | unull - str | NULL | '2' |
        | unull - bin | NULL | CAST('2' AS BINARY) |

  Rule: `*` operand-type matrix (ANSI on)

    Scenario Outline: times ansi-on: valid pair type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint * tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint * int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint * bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint * float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint * double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint * dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(14,2) |
        | tinyint * str | CAST(6 AS TINYINT) | '2' | bigint |
        | tinyint * ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY | interval day to second |
        | tinyint * null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint * unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int * tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int * int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int * bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int * float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int * double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int * dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(21,2) |
        | int * str | CAST(6 AS INT) | '2' | bigint |
        | int * ival_d | CAST(6 AS INT) | INTERVAL '2' DAY | interval day to second |
        | int * null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int * unull | CAST(6 AS INT) | NULL | int |
        | bigint * tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint * int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint * bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint * float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint * double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint * dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(31,2) |
        | bigint * str | CAST(6 AS BIGINT) | '2' | bigint |
        | bigint * ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY | interval day to second |
        | bigint * null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint * unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float * tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float * int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float * bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float * float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float * double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float * dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float * str | CAST(6 AS FLOAT) | '2' | double |
        | float * ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY | interval day to second |
        | float * null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float * unull | CAST(6 AS FLOAT) | NULL | float |
        | double * tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double * int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double * bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double * float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double * double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double * dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double * str | CAST(6 AS DOUBLE) | '2' | double |
        | double * ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY | interval day to second |
        | double * null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double * unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec * tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(14,2) |
        | dec * int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(21,2) |
        | dec * bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(31,2) |
        | dec * float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec * double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec * dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(21,4) |
        | dec * str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec * ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY | interval day to second |
        | dec * null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(21,2) |
        | dec * unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(21,4) |
        | str * tinyint | '6' | CAST(2 AS TINYINT) | bigint |
        | str * int | '6' | CAST(2 AS INT) | bigint |
        | str * bigint | '6' | CAST(2 AS BIGINT) | bigint |
        | str * float | '6' | CAST(2 AS FLOAT) | double |
        | str * double | '6' | CAST(2 AS DOUBLE) | double |
        | str * dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str * ival_d | '6' | INTERVAL '2' DAY | interval day to second |
        | str * null | '6' | CAST(NULL AS INT) | bigint |
        | ival_d * tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) | interval day to second |
        | ival_d * int | INTERVAL '2' DAY | CAST(2 AS INT) | interval day to second |
        | ival_d * bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) | interval day to second |
        | ival_d * float | INTERVAL '2' DAY | CAST(2 AS FLOAT) | interval day to second |
        | ival_d * double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) | interval day to second |
        | ival_d * dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) | interval day to second |
        | ival_d * str | INTERVAL '2' DAY | '2' | interval day to second |
        | ival_d * null | INTERVAL '2' DAY | CAST(NULL AS INT) | interval day to second |
        | ival_d * unull | INTERVAL '2' DAY | NULL | interval day to second |
        | null * tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null * int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null * bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null * float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null * double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null * dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(21,2) |
        | null * str | CAST(NULL AS INT) | '2' | bigint |
        | null * ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY | interval day to second |
        | null * null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null * unull | CAST(NULL AS INT) | NULL | int |
        | unull * tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull * int | NULL | CAST(2 AS INT) | int |
        | unull * bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull * float | NULL | CAST(2 AS FLOAT) | float |
        | unull * double | NULL | CAST(2 AS DOUBLE) | double |
        | unull * dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(21,4) |
        | unull * ival_d | NULL | INTERVAL '2' DAY | interval day to second |
        | unull * null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: times ansi-on: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint * ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH | interval year to month |
        | int * ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH | interval year to month |
        | bigint * ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH | interval year to month |
        | float * ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH | interval year to month |
        | double * ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH | interval year to month |
        | dec * ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH | interval year to month |
        | str * ival_m | '6' | INTERVAL '2' MONTH | interval year to month |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) | interval year to month |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) | interval year to month |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) | interval year to month |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) | interval year to month |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) | interval year to month |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) | interval year to month |
        | ival_m * str | INTERVAL '2' MONTH | '2' | interval year to month |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) | interval year to month |
        | ival_m * unull | INTERVAL '2' MONTH | NULL | interval year to month |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH | interval year to month |
        | unull * ival_m | NULL | INTERVAL '2' MONTH | interval year to month |
        | unull * unull | NULL | NULL | double |

    Scenario Outline: times ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool * bool | true | true |
        | bool * tinyint | true | CAST(2 AS TINYINT) |
        | bool * int | true | CAST(2 AS INT) |
        | bool * bigint | true | CAST(2 AS BIGINT) |
        | bool * float | true | CAST(2 AS FLOAT) |
        | bool * double | true | CAST(2 AS DOUBLE) |
        | bool * dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool * str | true | '2' |
        | bool * date | true | DATE'2024-01-15' |
        | bool * ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool * ival_d | true | INTERVAL '2' DAY |
        | bool * ival_m | true | INTERVAL '2' MONTH |
        | bool * bin | true | CAST('2' AS BINARY) |
        | bool * null | true | CAST(NULL AS INT) |
        | bool * unull | true | NULL |
        | tinyint * bool | CAST(6 AS TINYINT) | true |
        | tinyint * date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint * ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint * bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int * bool | CAST(6 AS INT) | true |
        | int * date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int * ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int * bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint * bool | CAST(6 AS BIGINT) | true |
        | bigint * date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint * ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint * bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float * bool | CAST(6 AS FLOAT) | true |
        | float * date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float * ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float * bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double * bool | CAST(6 AS DOUBLE) | true |
        | double * date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double * ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double * bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec * bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec * date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec * ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec * bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str * bool | '6' | true |
        | str * str | '6' | '2' |
        | str * date | '6' | DATE'2024-01-15' |
        | str * ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str * bin | '6' | CAST('2' AS BINARY) |
        | str * unull | '6' | NULL |
        | date * bool | DATE'2024-01-15' | true |
        | date * tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date * int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date * bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date * float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date * double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date * dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date * str | DATE'2024-01-15' | '2' |
        | date * date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date * ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date * ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date * ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date * bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date * null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date * unull | DATE'2024-01-15' | NULL |
        | ts * bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts * tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts * int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts * bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts * float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts * double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts * dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts * str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts * date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts * ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts * ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts * ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts * bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts * null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts * unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d * bool | INTERVAL '2' DAY | true |
        | ival_d * date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d * ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d * ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d * ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d * bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_m * bool | INTERVAL '2' MONTH | true |
        | ival_m * date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m * ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m * ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m * ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m * bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | bin * bool | CAST('6' AS BINARY) | true |
        | bin * tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin * int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin * bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin * float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin * double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin * dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin * str | CAST('6' AS BINARY) | '2' |
        | bin * date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin * ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin * ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin * ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin * bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin * null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin * unull | CAST('6' AS BINARY) | NULL |
        | null * bool | CAST(NULL AS INT) | true |
        | null * date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null * ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null * bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull * bool | NULL | true |
        | unull * str | NULL | '2' |
        | unull * date | NULL | DATE'2024-01-15' |
        | unull * ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull * bin | NULL | CAST('2' AS BINARY) |

  Rule: `/` operand-type matrix (ANSI on)

    Scenario Outline: divide ansi-on: valid pair type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint / tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | double |
        | tinyint / int | CAST(6 AS TINYINT) | CAST(2 AS INT) | double |
        | tinyint / bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | double |
        | tinyint / float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint / double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint / dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(16,11) |
        | tinyint / str | CAST(6 AS TINYINT) | '2' | double |
        | tinyint / null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | double |
        | tinyint / unull | CAST(6 AS TINYINT) | NULL | double |
        | int / tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | double |
        | int / int | CAST(6 AS INT) | CAST(2 AS INT) | double |
        | int / bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | double |
        | int / float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int / double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int / dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,11) |
        | int / str | CAST(6 AS INT) | '2' | double |
        | int / null | CAST(6 AS INT) | CAST(NULL AS INT) | double |
        | int / unull | CAST(6 AS INT) | NULL | double |
        | bigint / tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | double |
        | bigint / int | CAST(6 AS BIGINT) | CAST(2 AS INT) | double |
        | bigint / bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | double |
        | bigint / float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint / double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint / dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(33,11) |
        | bigint / str | CAST(6 AS BIGINT) | '2' | double |
        | bigint / null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | double |
        | bigint / unull | CAST(6 AS BIGINT) | NULL | double |
        | float / tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float / int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float / bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float / float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | double |
        | float / double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float / dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float / str | CAST(6 AS FLOAT) | '2' | double |
        | float / null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float / unull | CAST(6 AS FLOAT) | NULL | double |
        | double / tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double / int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double / bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double / float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double / double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double / dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double / str | CAST(6 AS DOUBLE) | '2' | double |
        | double / null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double / unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec / tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(14,6) |
        | dec / int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(21,13) |
        | dec / bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(31,23) |
        | dec / float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec / double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec / dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(23,13) |
        | dec / str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec / null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(21,13) |
        | dec / unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(23,13) |
        | str / tinyint | '6' | CAST(2 AS TINYINT) | double |
        | str / int | '6' | CAST(2 AS INT) | double |
        | str / bigint | '6' | CAST(2 AS BIGINT) | double |
        | str / float | '6' | CAST(2 AS FLOAT) | double |
        | str / double | '6' | CAST(2 AS DOUBLE) | double |
        | str / dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str / null | '6' | CAST(NULL AS INT) | double |
        | ival_d / tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) | interval day to second |
        | ival_d / int | INTERVAL '2' DAY | CAST(2 AS INT) | interval day to second |
        | ival_d / bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) | interval day to second |
        | ival_d / float | INTERVAL '2' DAY | CAST(2 AS FLOAT) | interval day to second |
        | ival_d / double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) | interval day to second |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) | interval day to second |
        | ival_d / str | INTERVAL '2' DAY | '2' | interval day to second |
        | ival_d / null | INTERVAL '2' DAY | CAST(NULL AS INT) | interval day to second |
        | ival_d / unull | INTERVAL '2' DAY | NULL | interval day to second |
        | null / tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | double |
        | null / int | CAST(NULL AS INT) | CAST(2 AS INT) | double |
        | null / bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | double |
        | null / float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null / double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null / dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(23,11) |
        | null / str | CAST(NULL AS INT) | '2' | double |
        | null / null | CAST(NULL AS INT) | CAST(NULL AS INT) | double |
        | null / unull | CAST(NULL AS INT) | NULL | double |
        | unull / tinyint | NULL | CAST(2 AS TINYINT) | double |
        | unull / int | NULL | CAST(2 AS INT) | double |
        | unull / bigint | NULL | CAST(2 AS BIGINT) | double |
        | unull / float | NULL | CAST(2 AS FLOAT) | double |
        | unull / double | NULL | CAST(2 AS DOUBLE) | double |
        | unull / dec | NULL | CAST(2 AS DECIMAL(10,2)) | double |
        | unull / null | NULL | CAST(NULL AS INT) | double |
        | unull / unull | NULL | NULL | double |

    @sail-bug
    Scenario Outline: divide ansi-on: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) | interval year to month |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) | interval year to month |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) | interval year to month |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) | interval year to month |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) | interval year to month |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) | interval year to month |
        | ival_m / str | INTERVAL '2' MONTH | '2' | interval year to month |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) | interval year to month |
        | ival_m / unull | INTERVAL '2' MONTH | NULL | interval year to month |

    Scenario Outline: divide ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool / bool | true | true |
        | bool / tinyint | true | CAST(2 AS TINYINT) |
        | bool / int | true | CAST(2 AS INT) |
        | bool / bigint | true | CAST(2 AS BIGINT) |
        | bool / float | true | CAST(2 AS FLOAT) |
        | bool / double | true | CAST(2 AS DOUBLE) |
        | bool / dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool / str | true | '2' |
        | bool / date | true | DATE'2024-01-15' |
        | bool / ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool / ival_d | true | INTERVAL '2' DAY |
        | bool / ival_m | true | INTERVAL '2' MONTH |
        | bool / bin | true | CAST('2' AS BINARY) |
        | bool / null | true | CAST(NULL AS INT) |
        | bool / unull | true | NULL |
        | tinyint / bool | CAST(6 AS TINYINT) | true |
        | tinyint / date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint / ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint / ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint / ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint / bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int / bool | CAST(6 AS INT) | true |
        | int / date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int / ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int / ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int / ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int / bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint / bool | CAST(6 AS BIGINT) | true |
        | bigint / date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint / ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint / ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint / ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint / bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float / bool | CAST(6 AS FLOAT) | true |
        | float / date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float / ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float / ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float / ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float / bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double / bool | CAST(6 AS DOUBLE) | true |
        | double / date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double / ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double / ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double / ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double / bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec / bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec / date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec / ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec / ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec / ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec / bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str / bool | '6' | true |
        | str / str | '6' | '2' |
        | str / date | '6' | DATE'2024-01-15' |
        | str / ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str / ival_d | '6' | INTERVAL '2' DAY |
        | str / ival_m | '6' | INTERVAL '2' MONTH |
        | str / bin | '6' | CAST('2' AS BINARY) |
        | str / unull | '6' | NULL |
        | date / bool | DATE'2024-01-15' | true |
        | date / tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date / int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date / bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date / float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date / double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date / dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date / str | DATE'2024-01-15' | '2' |
        | date / date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date / ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date / ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date / ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date / bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date / null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date / unull | DATE'2024-01-15' | NULL |
        | ts / bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts / tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts / int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts / bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts / float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts / double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts / dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts / str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts / date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts / ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts / ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts / ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts / bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts / null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts / unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d / bool | INTERVAL '2' DAY | true |
        | ival_d / date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d / ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d / ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d / ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d / bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_m / bool | INTERVAL '2' MONTH | true |
        | ival_m / date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m / ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m / ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m / ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m / bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | bin / bool | CAST('6' AS BINARY) | true |
        | bin / tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin / int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin / bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin / float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin / double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin / dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin / str | CAST('6' AS BINARY) | '2' |
        | bin / date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin / ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin / ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin / ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin / bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin / null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin / unull | CAST('6' AS BINARY) | NULL |
        | null / bool | CAST(NULL AS INT) | true |
        | null / date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null / ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null / ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null / ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null / bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull / bool | NULL | true |
        | unull / str | NULL | '2' |
        | unull / date | NULL | DATE'2024-01-15' |
        | unull / ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull / ival_d | NULL | INTERVAL '2' DAY |
        | unull / ival_m | NULL | INTERVAL '2' MONTH |
        | unull / bin | NULL | CAST('2' AS BINARY) |

  Rule: `%` operand-type matrix (ANSI on)

    Scenario Outline: modulo ansi-on: valid pair type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | tinyint % tinyint | CAST(6 AS TINYINT) | CAST(2 AS TINYINT) | tinyint |
        | tinyint % int | CAST(6 AS TINYINT) | CAST(2 AS INT) | int |
        | tinyint % bigint | CAST(6 AS TINYINT) | CAST(2 AS BIGINT) | bigint |
        | tinyint % float | CAST(6 AS TINYINT) | CAST(2 AS FLOAT) | double |
        | tinyint % double | CAST(6 AS TINYINT) | CAST(2 AS DOUBLE) | double |
        | tinyint % dec | CAST(6 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) | decimal(5,2) |
        | tinyint % str | CAST(6 AS TINYINT) | '2' | bigint |
        | tinyint % null | CAST(6 AS TINYINT) | CAST(NULL AS INT) | int |
        | tinyint % unull | CAST(6 AS TINYINT) | NULL | tinyint |
        | int % tinyint | CAST(6 AS INT) | CAST(2 AS TINYINT) | int |
        | int % int | CAST(6 AS INT) | CAST(2 AS INT) | int |
        | int % bigint | CAST(6 AS INT) | CAST(2 AS BIGINT) | bigint |
        | int % float | CAST(6 AS INT) | CAST(2 AS FLOAT) | double |
        | int % double | CAST(6 AS INT) | CAST(2 AS DOUBLE) | double |
        | int % dec | CAST(6 AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | int % str | CAST(6 AS INT) | '2' | bigint |
        | int % null | CAST(6 AS INT) | CAST(NULL AS INT) | int |
        | int % unull | CAST(6 AS INT) | NULL | int |
        | bigint % tinyint | CAST(6 AS BIGINT) | CAST(2 AS TINYINT) | bigint |
        | bigint % int | CAST(6 AS BIGINT) | CAST(2 AS INT) | bigint |
        | bigint % bigint | CAST(6 AS BIGINT) | CAST(2 AS BIGINT) | bigint |
        | bigint % float | CAST(6 AS BIGINT) | CAST(2 AS FLOAT) | double |
        | bigint % double | CAST(6 AS BIGINT) | CAST(2 AS DOUBLE) | double |
        | bigint % dec | CAST(6 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | bigint % str | CAST(6 AS BIGINT) | '2' | bigint |
        | bigint % null | CAST(6 AS BIGINT) | CAST(NULL AS INT) | bigint |
        | bigint % unull | CAST(6 AS BIGINT) | NULL | bigint |
        | float % tinyint | CAST(6 AS FLOAT) | CAST(2 AS TINYINT) | double |
        | float % int | CAST(6 AS FLOAT) | CAST(2 AS INT) | double |
        | float % bigint | CAST(6 AS FLOAT) | CAST(2 AS BIGINT) | double |
        | float % float | CAST(6 AS FLOAT) | CAST(2 AS FLOAT) | float |
        | float % double | CAST(6 AS FLOAT) | CAST(2 AS DOUBLE) | double |
        | float % dec | CAST(6 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) | double |
        | float % str | CAST(6 AS FLOAT) | '2' | double |
        | float % null | CAST(6 AS FLOAT) | CAST(NULL AS INT) | double |
        | float % unull | CAST(6 AS FLOAT) | NULL | float |
        | double % tinyint | CAST(6 AS DOUBLE) | CAST(2 AS TINYINT) | double |
        | double % int | CAST(6 AS DOUBLE) | CAST(2 AS INT) | double |
        | double % bigint | CAST(6 AS DOUBLE) | CAST(2 AS BIGINT) | double |
        | double % float | CAST(6 AS DOUBLE) | CAST(2 AS FLOAT) | double |
        | double % double | CAST(6 AS DOUBLE) | CAST(2 AS DOUBLE) | double |
        | double % dec | CAST(6 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) | double |
        | double % str | CAST(6 AS DOUBLE) | '2' | double |
        | double % null | CAST(6 AS DOUBLE) | CAST(NULL AS INT) | double |
        | double % unull | CAST(6 AS DOUBLE) | NULL | double |
        | dec % tinyint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) | decimal(5,2) |
        | dec % int | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS INT) | decimal(10,2) |
        | dec % bigint | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) | decimal(10,2) |
        | dec % float | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) | double |
        | dec % double | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) | double |
        | dec % dec | CAST(6 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | dec % str | CAST(6 AS DECIMAL(10,2)) | '2' | double |
        | dec % null | CAST(6 AS DECIMAL(10,2)) | CAST(NULL AS INT) | decimal(10,2) |
        | dec % unull | CAST(6 AS DECIMAL(10,2)) | NULL | decimal(10,2) |
        | str % tinyint | '6' | CAST(2 AS TINYINT) | bigint |
        | str % int | '6' | CAST(2 AS INT) | bigint |
        | str % bigint | '6' | CAST(2 AS BIGINT) | bigint |
        | str % float | '6' | CAST(2 AS FLOAT) | double |
        | str % double | '6' | CAST(2 AS DOUBLE) | double |
        | str % dec | '6' | CAST(2 AS DECIMAL(10,2)) | double |
        | str % null | '6' | CAST(NULL AS INT) | bigint |
        | null % tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) | int |
        | null % int | CAST(NULL AS INT) | CAST(2 AS INT) | int |
        | null % bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) | bigint |
        | null % float | CAST(NULL AS INT) | CAST(2 AS FLOAT) | double |
        | null % double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) | double |
        | null % dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | null % str | CAST(NULL AS INT) | '2' | bigint |
        | null % null | CAST(NULL AS INT) | CAST(NULL AS INT) | int |
        | null % unull | CAST(NULL AS INT) | NULL | int |
        | unull % tinyint | NULL | CAST(2 AS TINYINT) | tinyint |
        | unull % int | NULL | CAST(2 AS INT) | int |
        | unull % bigint | NULL | CAST(2 AS BIGINT) | bigint |
        | unull % float | NULL | CAST(2 AS FLOAT) | float |
        | unull % double | NULL | CAST(2 AS DOUBLE) | double |
        | unull % dec | NULL | CAST(2 AS DECIMAL(10,2)) | decimal(10,2) |
        | unull % null | NULL | CAST(NULL AS INT) | int |

    @sail-bug
    Scenario Outline: modulo ansi-on: valid pair type (Sail diverges): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query result
        | t        |
        | <result> |

      Examples:
        | case | l | r | result |
        | unull % unull | NULL | NULL | double |

    Scenario Outline: modulo ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error .*

      Examples:
        | case | l | r |
        | bool % bool | true | true |
        | bool % tinyint | true | CAST(2 AS TINYINT) |
        | bool % int | true | CAST(2 AS INT) |
        | bool % bigint | true | CAST(2 AS BIGINT) |
        | bool % float | true | CAST(2 AS FLOAT) |
        | bool % double | true | CAST(2 AS DOUBLE) |
        | bool % dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool % str | true | '2' |
        | bool % date | true | DATE'2024-01-15' |
        | bool % ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool % ival_d | true | INTERVAL '2' DAY |
        | bool % ival_m | true | INTERVAL '2' MONTH |
        | bool % bin | true | CAST('2' AS BINARY) |
        | bool % null | true | CAST(NULL AS INT) |
        | bool % unull | true | NULL |
        | tinyint % bool | CAST(6 AS TINYINT) | true |
        | tinyint % date | CAST(6 AS TINYINT) | DATE'2024-01-15' |
        | tinyint % ts | CAST(6 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint % ival_d | CAST(6 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint % ival_m | CAST(6 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint % bin | CAST(6 AS TINYINT) | CAST('2' AS BINARY) |
        | int % bool | CAST(6 AS INT) | true |
        | int % date | CAST(6 AS INT) | DATE'2024-01-15' |
        | int % ts | CAST(6 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int % ival_d | CAST(6 AS INT) | INTERVAL '2' DAY |
        | int % ival_m | CAST(6 AS INT) | INTERVAL '2' MONTH |
        | int % bin | CAST(6 AS INT) | CAST('2' AS BINARY) |
        | bigint % bool | CAST(6 AS BIGINT) | true |
        | bigint % date | CAST(6 AS BIGINT) | DATE'2024-01-15' |
        | bigint % ts | CAST(6 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint % ival_d | CAST(6 AS BIGINT) | INTERVAL '2' DAY |
        | bigint % ival_m | CAST(6 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint % bin | CAST(6 AS BIGINT) | CAST('2' AS BINARY) |
        | float % bool | CAST(6 AS FLOAT) | true |
        | float % date | CAST(6 AS FLOAT) | DATE'2024-01-15' |
        | float % ts | CAST(6 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float % ival_d | CAST(6 AS FLOAT) | INTERVAL '2' DAY |
        | float % ival_m | CAST(6 AS FLOAT) | INTERVAL '2' MONTH |
        | float % bin | CAST(6 AS FLOAT) | CAST('2' AS BINARY) |
        | double % bool | CAST(6 AS DOUBLE) | true |
        | double % date | CAST(6 AS DOUBLE) | DATE'2024-01-15' |
        | double % ts | CAST(6 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double % ival_d | CAST(6 AS DOUBLE) | INTERVAL '2' DAY |
        | double % ival_m | CAST(6 AS DOUBLE) | INTERVAL '2' MONTH |
        | double % bin | CAST(6 AS DOUBLE) | CAST('2' AS BINARY) |
        | dec % bool | CAST(6 AS DECIMAL(10,2)) | true |
        | dec % date | CAST(6 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec % ts | CAST(6 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec % ival_d | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec % ival_m | CAST(6 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec % bin | CAST(6 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | str % bool | '6' | true |
        | str % str | '6' | '2' |
        | str % date | '6' | DATE'2024-01-15' |
        | str % ts | '6' | TIMESTAMP'2024-01-15 12:00:00' |
        | str % ival_d | '6' | INTERVAL '2' DAY |
        | str % ival_m | '6' | INTERVAL '2' MONTH |
        | str % bin | '6' | CAST('2' AS BINARY) |
        | str % unull | '6' | NULL |
        | date % bool | DATE'2024-01-15' | true |
        | date % tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date % int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date % bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date % float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date % double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date % dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date % str | DATE'2024-01-15' | '2' |
        | date % date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date % ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date % ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date % ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date % bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date % null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date % unull | DATE'2024-01-15' | NULL |
        | ts % bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts % tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts % int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts % bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts % float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts % double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts % dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts % str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts % date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts % ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts % ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts % ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts % bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts % null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts % unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ival_d % bool | INTERVAL '2' DAY | true |
        | ival_d % tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d % int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d % bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d % float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d % double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d % dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d % str | INTERVAL '2' DAY | '2' |
        | ival_d % date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d % ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d % ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d % ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d % bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d % null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d % unull | INTERVAL '2' DAY | NULL |
        | ival_m % bool | INTERVAL '2' MONTH | true |
        | ival_m % tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m % int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m % bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m % float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m % double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m % dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m % str | INTERVAL '2' MONTH | '2' |
        | ival_m % date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m % ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m % ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m % ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m % bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m % null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m % unull | INTERVAL '2' MONTH | NULL |
        | bin % bool | CAST('6' AS BINARY) | true |
        | bin % tinyint | CAST('6' AS BINARY) | CAST(2 AS TINYINT) |
        | bin % int | CAST('6' AS BINARY) | CAST(2 AS INT) |
        | bin % bigint | CAST('6' AS BINARY) | CAST(2 AS BIGINT) |
        | bin % float | CAST('6' AS BINARY) | CAST(2 AS FLOAT) |
        | bin % double | CAST('6' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin % dec | CAST('6' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin % str | CAST('6' AS BINARY) | '2' |
        | bin % date | CAST('6' AS BINARY) | DATE'2024-01-15' |
        | bin % ts | CAST('6' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin % ival_d | CAST('6' AS BINARY) | INTERVAL '2' DAY |
        | bin % ival_m | CAST('6' AS BINARY) | INTERVAL '2' MONTH |
        | bin % bin | CAST('6' AS BINARY) | CAST('2' AS BINARY) |
        | bin % null | CAST('6' AS BINARY) | CAST(NULL AS INT) |
        | bin % unull | CAST('6' AS BINARY) | NULL |
        | null % bool | CAST(NULL AS INT) | true |
        | null % date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null % ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null % ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null % ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null % bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | unull % bool | NULL | true |
        | unull % str | NULL | '2' |
        | unull % date | NULL | DATE'2024-01-15' |
        | unull % ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull % ival_d | NULL | INTERVAL '2' DAY |
        | unull % ival_m | NULL | INTERVAL '2' MONTH |
        | unull % bin | NULL | CAST('2' AS BINARY) |
