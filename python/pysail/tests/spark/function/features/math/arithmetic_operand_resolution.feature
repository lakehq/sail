Feature: arithmetic operand pairs Spark resolves (+ - * / %) vs Spark 4.2.0

  # The companion of `arithmetic_operand_rejection.feature`, and the reason it exists: a
  # rejection matrix alone cannot catch OVER-rejection, since a guard that rejected
  # everything would keep all its rows green. Every pair here resolves in Spark 4.2.0, so
  # narrowing a guard too far turns a row red.
  #
  # It asserts RESOLUTION ONLY and does not pin the result type: that is the coercion
  # contract, pinned in `arithmetic_result_type.feature`. Nullability is likewise out of scope.
  #
  # Version note: the verdicts are Spark 4.2.0's, which is what Sail targets. Three cells
  # changed in 4.1 (SPARK-52782, `BinaryArithmeticWithDatetimeResolver.scala:88`) and were
  # measured on 4.0.1 to confirm: `NULL + ts` and `ts + NULL` are REJECTED there but resolve
  # from 4.1 on, and `NULL + date` resolves to `date` on 4.0 versus `timestamp` from 4.1.
  # Only a JVM oracle sweep against a pre-4.1 Spark would see the difference; the rows are
  # left ungated because they assert resolution, not the type, and because 4.2 is the target.
  #
  # Same 28-token alphabet as the rejection file. Every pair Spark resolves, Sail resolves
  # too; the few `@sail-bug` scenarios at the end pin a value or a name, or a pair Sail
  # accepts and Spark refuses, each with its cause next to it.

  Rule: `+` operand pairs that resolve (ANSI off)

    Scenario Outline: plus ansi-off: pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + null | NULL | CAST(NULL AS INT) |
        | unull + tinyint | NULL | CAST(2 AS TINYINT) |
        | unull + smallint | NULL | CAST(2 AS SMALLINT) |
        | unull + int | NULL | CAST(2 AS INT) |
        | unull + bigint | NULL | CAST(2 AS BIGINT) |
        | unull + float | NULL | CAST(2 AS FLOAT) |
        | unull + double | NULL | CAST(2 AS DOUBLE) |
        | unull + dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull + ival_d | NULL | INTERVAL '2' DAY |
        | unull + ival_dt | NULL | INTERVAL '25' HOUR |
        | unull + ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull + ival_m | NULL | INTERVAL '2' MONTH |
        | unull + ival_y | NULL | INTERVAL '2' YEAR |
        | unull + ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull + calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null + unull | CAST(NULL AS INT) | NULL |
        | null + null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null + tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null + smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null + int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null + bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null + float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null + double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null + dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null + date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | tinyint + unull | CAST(2 AS TINYINT) | NULL |
        | tinyint + null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint + tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint + smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint + int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint + bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint + float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint + double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint + dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint + date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | smallint + unull | CAST(2 AS SMALLINT) | NULL |
        | smallint + null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint + tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint + smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint + int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint + bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint + float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint + double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint + dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint + date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | int + unull | CAST(2 AS INT) | NULL |
        | int + null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int + tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int + smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int + int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int + bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int + float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int + double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int + dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int + date | CAST(2 AS INT) | DATE'2024-01-15' |
        | bigint + unull | CAST(2 AS BIGINT) | NULL |
        | bigint + null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint + tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint + smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint + int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint + bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint + float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint + double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint + dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | float + unull | CAST(2 AS FLOAT) | NULL |
        | float + null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float + tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float + smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float + int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float + bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float + float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float + double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float + dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | double + unull | CAST(2 AS DOUBLE) | NULL |
        | double + null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double + tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double + smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double + int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double + bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double + float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double + double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double + dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | dec + unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec + null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec + tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec + smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec + int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec + bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec + float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec + double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec + dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | str + ival_d | '2' | INTERVAL '2' DAY |
        | str + ival_dt | '2' | INTERVAL '25' HOUR |
        | str + ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date + null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date + tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date + smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date + int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date + ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date + ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date + ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date + ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date + ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date + ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date + calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | ts + ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts + ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts + ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts + ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts + ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts + ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts + calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz + ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz + ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz + ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz + ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz + ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz + ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz + calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ival_d + unull | INTERVAL '2' DAY | NULL |
        | ival_d + str | INTERVAL '2' DAY | '2' |
        | ival_d + date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d + ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d + ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d + ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d + ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d + ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt + unull | INTERVAL '25' HOUR | NULL |
        | ival_dt + str | INTERVAL '25' HOUR | '2' |
        | ival_dt + date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt + ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt + ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt + ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt + ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt + ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds + unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds + str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds + date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds + ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds + ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds + ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds + ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds + ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m + unull | INTERVAL '2' MONTH | NULL |
        | ival_m + date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m + ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m + ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m + ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m + ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m + ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y + unull | INTERVAL '2' YEAR | NULL |
        | ival_y + date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y + ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y + ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y + ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y + ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y + ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym + unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym + date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym + ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym + ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym + ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym + ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym + ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | calendar + unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar + date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar + ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar + ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar + calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | unull + date | NULL | DATE'2024-01-15' |
        | date + unull | DATE'2024-01-15' | NULL |
        | unull + str | NULL | '2' |
        | null + str | CAST(NULL AS INT) | '2' |
        | tinyint + str | CAST(2 AS TINYINT) | '2' |
        | smallint + str | CAST(2 AS SMALLINT) | '2' |
        | int + str | CAST(2 AS INT) | '2' |
        | bigint + str | CAST(2 AS BIGINT) | '2' |
        | float + str | CAST(2 AS FLOAT) | '2' |
        | double + str | CAST(2 AS DOUBLE) | '2' |
        | dec + str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str + unull | '2' | NULL |
        | str + null | '2' | CAST(NULL AS INT) |
        | str + tinyint | '2' | CAST(2 AS TINYINT) |
        | str + smallint | '2' | CAST(2 AS SMALLINT) |
        | str + int | '2' | CAST(2 AS INT) |
        | str + bigint | '2' | CAST(2 AS BIGINT) |
        | str + float | '2' | CAST(2 AS FLOAT) |
        | str + double | '2' | CAST(2 AS DOUBLE) |
        | str + dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str + str | '2' | '2' |
        | str + calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | calendar + str | make_interval(0,1,0,1,0,0,0) | '2' |

    @spark-4
    Scenario Outline: plus ansi-off: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + unull | NULL | NULL |

    @spark-4.1
    Scenario Outline: plus ansi-off: pair resolves, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + time | NULL | TIME '12:00:00' |
        | time + unull | TIME '12:00:00' | NULL |
        | time + ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time + ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time + ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d + time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt + time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds + time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |

    @spark-4.1
    Scenario Outline: plus ansi-off: post-4.0 datetime pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull + ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts_ntz + unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |

  Rule: `+` operand pairs that resolve (ANSI on)

    Scenario Outline: plus ansi-on: pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + null | NULL | CAST(NULL AS INT) |
        | unull + tinyint | NULL | CAST(2 AS TINYINT) |
        | unull + smallint | NULL | CAST(2 AS SMALLINT) |
        | unull + int | NULL | CAST(2 AS INT) |
        | unull + bigint | NULL | CAST(2 AS BIGINT) |
        | unull + float | NULL | CAST(2 AS FLOAT) |
        | unull + double | NULL | CAST(2 AS DOUBLE) |
        | unull + dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull + ival_d | NULL | INTERVAL '2' DAY |
        | unull + ival_dt | NULL | INTERVAL '25' HOUR |
        | unull + ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull + ival_m | NULL | INTERVAL '2' MONTH |
        | unull + ival_y | NULL | INTERVAL '2' YEAR |
        | unull + ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull + calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null + unull | CAST(NULL AS INT) | NULL |
        | null + null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null + tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null + smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null + int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null + bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null + float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null + double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null + dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null + date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | tinyint + unull | CAST(2 AS TINYINT) | NULL |
        | tinyint + null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint + tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint + smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint + int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint + bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint + float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint + double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint + dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint + date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | smallint + unull | CAST(2 AS SMALLINT) | NULL |
        | smallint + null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint + tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint + smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint + int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint + bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint + float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint + double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint + dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint + date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | int + unull | CAST(2 AS INT) | NULL |
        | int + null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int + tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int + smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int + int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int + bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int + float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int + double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int + dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int + date | CAST(2 AS INT) | DATE'2024-01-15' |
        | bigint + unull | CAST(2 AS BIGINT) | NULL |
        | bigint + null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint + tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint + smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint + int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint + bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint + float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint + double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint + dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | float + unull | CAST(2 AS FLOAT) | NULL |
        | float + null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float + tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float + smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float + int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float + bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float + float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float + double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float + dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | double + unull | CAST(2 AS DOUBLE) | NULL |
        | double + null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double + tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double + smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double + int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double + bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double + float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double + double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double + dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | dec + unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec + null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec + tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec + smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec + int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec + bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec + float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec + double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec + dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | str + ival_d | '2' | INTERVAL '2' DAY |
        | str + ival_dt | '2' | INTERVAL '25' HOUR |
        | str + ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date + null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date + tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date + smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date + int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date + ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date + ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date + ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date + ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date + ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date + ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date + calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | ts + ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts + ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts + ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts + ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts + ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts + ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts + calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz + ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz + ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz + ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz + ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz + ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz + ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz + calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ival_d + unull | INTERVAL '2' DAY | NULL |
        | ival_d + str | INTERVAL '2' DAY | '2' |
        | ival_d + date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d + ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d + ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d + ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d + ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d + ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt + unull | INTERVAL '25' HOUR | NULL |
        | ival_dt + str | INTERVAL '25' HOUR | '2' |
        | ival_dt + date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt + ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt + ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt + ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt + ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt + ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds + unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds + str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds + date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds + ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds + ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds + ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds + ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds + ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m + unull | INTERVAL '2' MONTH | NULL |
        | ival_m + date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m + ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m + ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m + ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m + ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m + ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y + unull | INTERVAL '2' YEAR | NULL |
        | ival_y + date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y + ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y + ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y + ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y + ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y + ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym + unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym + date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym + ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym + ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym + ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym + ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym + ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | calendar + unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar + date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar + ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar + ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar + calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | unull + date | NULL | DATE'2024-01-15' |
        | date + unull | DATE'2024-01-15' | NULL |
        | null + str | CAST(NULL AS INT) | '2' |
        | tinyint + str | CAST(2 AS TINYINT) | '2' |
        | smallint + str | CAST(2 AS SMALLINT) | '2' |
        | int + str | CAST(2 AS INT) | '2' |
        | bigint + str | CAST(2 AS BIGINT) | '2' |
        | float + str | CAST(2 AS FLOAT) | '2' |
        | double + str | CAST(2 AS DOUBLE) | '2' |
        | dec + str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str + null | '2' | CAST(NULL AS INT) |
        | str + tinyint | '2' | CAST(2 AS TINYINT) |
        | str + smallint | '2' | CAST(2 AS SMALLINT) |
        | str + int | '2' | CAST(2 AS INT) |
        | str + bigint | '2' | CAST(2 AS BIGINT) |
        | str + float | '2' | CAST(2 AS FLOAT) |
        | str + double | '2' | CAST(2 AS DOUBLE) |
        | str + dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str + calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | calendar + str | make_interval(0,1,0,1,0,0,0) | '2' |

    @spark-4
    Scenario Outline: plus ansi-on: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + unull | NULL | NULL |

    @spark-4.1
    Scenario Outline: plus ansi-on: pair resolves, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + time | NULL | TIME '12:00:00' |
        | time + unull | TIME '12:00:00' | NULL |
        | time + ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time + ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time + ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d + time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt + time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds + time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |

    @spark-4.1
    Scenario Outline: plus ansi-on: post-4.0 datetime pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull + ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts_ntz + unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |

  Rule: `-` operand pairs that resolve (ANSI off)

    Scenario Outline: minus ansi-off: pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - null | NULL | CAST(NULL AS INT) |
        | unull - tinyint | NULL | CAST(2 AS TINYINT) |
        | unull - smallint | NULL | CAST(2 AS SMALLINT) |
        | unull - int | NULL | CAST(2 AS INT) |
        | unull - bigint | NULL | CAST(2 AS BIGINT) |
        | unull - float | NULL | CAST(2 AS FLOAT) |
        | unull - double | NULL | CAST(2 AS DOUBLE) |
        | unull - dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull - date | NULL | DATE'2024-01-15' |
        | unull - ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull - ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull - ival_d | NULL | INTERVAL '2' DAY |
        | unull - ival_dt | NULL | INTERVAL '25' HOUR |
        | unull - ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull - ival_m | NULL | INTERVAL '2' MONTH |
        | unull - ival_y | NULL | INTERVAL '2' YEAR |
        | unull - ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull - calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null - unull | CAST(NULL AS INT) | NULL |
        | null - null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null - tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null - smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null - int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null - bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null - float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null - double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null - dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint - unull | CAST(2 AS TINYINT) | NULL |
        | tinyint - null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint - tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint - smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint - int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint - bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint - float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint - double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint - dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint - unull | CAST(2 AS SMALLINT) | NULL |
        | smallint - null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint - tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint - smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint - int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint - bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint - float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint - double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint - dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | int - unull | CAST(2 AS INT) | NULL |
        | int - null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int - tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int - smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int - int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int - bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int - float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int - double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int - dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint - unull | CAST(2 AS BIGINT) | NULL |
        | bigint - null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint - tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint - smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint - int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint - bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint - float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint - double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint - dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | float - unull | CAST(2 AS FLOAT) | NULL |
        | float - null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float - tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float - smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float - int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float - bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float - float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float - double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float - dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | double - unull | CAST(2 AS DOUBLE) | NULL |
        | double - null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double - tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double - smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double - int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double - bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double - float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double - double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double - dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | dec - unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec - null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec - tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec - smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec - int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec - bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec - float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec - double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec - dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | date - unull | DATE'2024-01-15' | NULL |
        | date - null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date - tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date - smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date - int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date - date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date - ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date - ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date - ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date - ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date - ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date - ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date - ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date - ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date - calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | ts - unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts - date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts - ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts - ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts - ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts - ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts - ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts - ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts - ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts - ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts - calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz - unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz - date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz - ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz - ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz - ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz - ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz - ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz - ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz - ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz - ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz - calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ival_d - unull | INTERVAL '2' DAY | NULL |
        | ival_d - ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d - ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d - ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt - unull | INTERVAL '25' HOUR | NULL |
        | ival_dt - ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt - ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt - ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds - unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds - ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds - ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds - ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m - unull | INTERVAL '2' MONTH | NULL |
        | ival_m - ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m - ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m - ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y - unull | INTERVAL '2' YEAR | NULL |
        | ival_y - ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y - ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y - ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym - unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym - ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym - ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym - ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | calendar - unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar - calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | unull - str | NULL | '2' |
        | null - str | CAST(NULL AS INT) | '2' |
        | tinyint - str | CAST(2 AS TINYINT) | '2' |
        | smallint - str | CAST(2 AS SMALLINT) | '2' |
        | int - str | CAST(2 AS INT) | '2' |
        | bigint - str | CAST(2 AS BIGINT) | '2' |
        | float - str | CAST(2 AS FLOAT) | '2' |
        | double - str | CAST(2 AS DOUBLE) | '2' |
        | dec - str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str - unull | '2' | NULL |
        | str - null | '2' | CAST(NULL AS INT) |
        | str - tinyint | '2' | CAST(2 AS TINYINT) |
        | str - smallint | '2' | CAST(2 AS SMALLINT) |
        | str - int | '2' | CAST(2 AS INT) |
        | str - bigint | '2' | CAST(2 AS BIGINT) |
        | str - float | '2' | CAST(2 AS FLOAT) |
        | str - double | '2' | CAST(2 AS DOUBLE) |
        | str - dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str - str | '2' | '2' |
        | str - date | '2' | DATE'2024-01-15' |
        | str - ival_d | '2' | INTERVAL '2' DAY |
        | str - ival_dt | '2' | INTERVAL '25' HOUR |
        | str - ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str - calendar | '2' | make_interval(0,1,0,1,0,0,0) |

    @spark-4
    Scenario Outline: minus ansi-off: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - unull | NULL | NULL |

    @spark-4.1
    Scenario Outline: minus ansi-off: pair resolves, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - time | NULL | TIME '12:00:00' |
        | time - unull | TIME '12:00:00' | NULL |
        | time - time | TIME '12:00:00' | TIME '12:00:00' |
        | time - ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time - ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time - ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |

  Rule: `-` operand pairs that resolve (ANSI on)

    Scenario Outline: minus ansi-on: pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - null | NULL | CAST(NULL AS INT) |
        | unull - tinyint | NULL | CAST(2 AS TINYINT) |
        | unull - smallint | NULL | CAST(2 AS SMALLINT) |
        | unull - int | NULL | CAST(2 AS INT) |
        | unull - bigint | NULL | CAST(2 AS BIGINT) |
        | unull - float | NULL | CAST(2 AS FLOAT) |
        | unull - double | NULL | CAST(2 AS DOUBLE) |
        | unull - dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull - date | NULL | DATE'2024-01-15' |
        | unull - ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull - ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull - ival_d | NULL | INTERVAL '2' DAY |
        | unull - ival_dt | NULL | INTERVAL '25' HOUR |
        | unull - ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull - ival_m | NULL | INTERVAL '2' MONTH |
        | unull - ival_y | NULL | INTERVAL '2' YEAR |
        | unull - ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull - calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null - unull | CAST(NULL AS INT) | NULL |
        | null - null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null - tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null - smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null - int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null - bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null - float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null - double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null - dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint - unull | CAST(2 AS TINYINT) | NULL |
        | tinyint - null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint - tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint - smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint - int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint - bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint - float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint - double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint - dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint - unull | CAST(2 AS SMALLINT) | NULL |
        | smallint - null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint - tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint - smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint - int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint - bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint - float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint - double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint - dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | int - unull | CAST(2 AS INT) | NULL |
        | int - null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int - tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int - smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int - int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int - bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int - float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int - double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int - dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint - unull | CAST(2 AS BIGINT) | NULL |
        | bigint - null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint - tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint - smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint - int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint - bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint - float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint - double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint - dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | float - unull | CAST(2 AS FLOAT) | NULL |
        | float - null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float - tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float - smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float - int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float - bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float - float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float - double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float - dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | double - unull | CAST(2 AS DOUBLE) | NULL |
        | double - null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double - tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double - smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double - int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double - bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double - float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double - double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double - dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | dec - unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec - null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec - tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec - smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec - int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec - bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec - float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec - double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec - dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | date - unull | DATE'2024-01-15' | NULL |
        | date - null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date - tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date - smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date - int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date - date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date - ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date - ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date - ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date - ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date - ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date - ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date - ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date - ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date - calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | ts - unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts - date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts - ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts - ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts - ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts - ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts - ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts - ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts - ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts - ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts - calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz - unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz - date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz - ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz - ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz - ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz - ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz - ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz - ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz - ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz - ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz - calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ival_d - unull | INTERVAL '2' DAY | NULL |
        | ival_d - ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d - ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d - ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt - unull | INTERVAL '25' HOUR | NULL |
        | ival_dt - ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt - ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt - ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds - unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds - ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds - ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds - ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m - unull | INTERVAL '2' MONTH | NULL |
        | ival_m - ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m - ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m - ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y - unull | INTERVAL '2' YEAR | NULL |
        | ival_y - ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y - ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y - ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym - unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym - ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym - ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym - ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | calendar - unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar - calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | null - str | CAST(NULL AS INT) | '2' |
        | tinyint - str | CAST(2 AS TINYINT) | '2' |
        | smallint - str | CAST(2 AS SMALLINT) | '2' |
        | int - str | CAST(2 AS INT) | '2' |
        | bigint - str | CAST(2 AS BIGINT) | '2' |
        | float - str | CAST(2 AS FLOAT) | '2' |
        | double - str | CAST(2 AS DOUBLE) | '2' |
        | dec - str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str - null | '2' | CAST(NULL AS INT) |
        | str - tinyint | '2' | CAST(2 AS TINYINT) |
        | str - smallint | '2' | CAST(2 AS SMALLINT) |
        | str - int | '2' | CAST(2 AS INT) |
        | str - bigint | '2' | CAST(2 AS BIGINT) |
        | str - float | '2' | CAST(2 AS FLOAT) |
        | str - double | '2' | CAST(2 AS DOUBLE) |
        | str - dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str - date | '2' | DATE'2024-01-15' |
        | str - ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str - ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str - ival_d | '2' | INTERVAL '2' DAY |
        | str - ival_dt | '2' | INTERVAL '25' HOUR |
        | str - ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str - calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | date - str | DATE'2024-01-15' | '2' |
        | ts - str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts_ntz - str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |

    @spark-4
    Scenario Outline: minus ansi-on: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - unull | NULL | NULL |

    @spark-4.1
    Scenario Outline: minus ansi-on: pair resolves, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull - time | NULL | TIME '12:00:00' |
        | time - unull | TIME '12:00:00' | NULL |
        | time - time | TIME '12:00:00' | TIME '12:00:00' |
        | time - ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time - ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time - ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str - time | '2' | TIME '12:00:00' |
        | time - str | TIME '12:00:00' | '2' |

  Rule: `*` operand pairs that resolve (ANSI off)

    Scenario Outline: times ansi-off: pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull * null | NULL | CAST(NULL AS INT) |
        | unull * tinyint | NULL | CAST(2 AS TINYINT) |
        | unull * smallint | NULL | CAST(2 AS SMALLINT) |
        | unull * int | NULL | CAST(2 AS INT) |
        | unull * bigint | NULL | CAST(2 AS BIGINT) |
        | unull * float | NULL | CAST(2 AS FLOAT) |
        | unull * double | NULL | CAST(2 AS DOUBLE) |
        | unull * dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull * ival_d | NULL | INTERVAL '2' DAY |
        | unull * ival_dt | NULL | INTERVAL '25' HOUR |
        | unull * ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null * unull | CAST(NULL AS INT) | NULL |
        | null * null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null * tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null * smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null * int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null * bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null * float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null * double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null * dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null * ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null * ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null * ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint * unull | CAST(2 AS TINYINT) | NULL |
        | tinyint * null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint * tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint * smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint * int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint * bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint * float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint * double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint * dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint * ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint * ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint * ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint * unull | CAST(2 AS SMALLINT) | NULL |
        | smallint * null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint * tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint * smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint * int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint * bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint * float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint * double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint * dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint * ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint * ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint * ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int * unull | CAST(2 AS INT) | NULL |
        | int * null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int * tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int * smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int * int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int * bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int * float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int * double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int * dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int * ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int * ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int * ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint * unull | CAST(2 AS BIGINT) | NULL |
        | bigint * null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint * tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint * smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint * int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint * bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint * float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint * double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint * dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint * ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint * ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint * ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float * unull | CAST(2 AS FLOAT) | NULL |
        | float * null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float * tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float * smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float * int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float * bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float * float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float * double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float * dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | float * ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float * ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float * ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double * unull | CAST(2 AS DOUBLE) | NULL |
        | double * null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double * tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double * smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double * int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double * bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double * float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double * double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double * dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | double * ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double * ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double * ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec * unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec * null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec * tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec * smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec * int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec * bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec * float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec * double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec * dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | dec * ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec * ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec * ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str * ival_d | '2' | INTERVAL '2' DAY |
        | str * ival_dt | '2' | INTERVAL '25' HOUR |
        | str * ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d * unull | INTERVAL '2' DAY | NULL |
        | ival_d * null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d * tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d * smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d * int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d * bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d * float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d * double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d * dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d * str | INTERVAL '2' DAY | '2' |
        | ival_dt * unull | INTERVAL '25' HOUR | NULL |
        | ival_dt * null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt * tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt * smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt * int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt * bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt * float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt * double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt * dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt * str | INTERVAL '25' HOUR | '2' |
        | ival_ds * unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds * null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds * tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds * smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds * int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds * bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds * float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds * double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds * dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds * str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | unull * ival_m | NULL | INTERVAL '2' MONTH |
        | unull * ival_y | NULL | INTERVAL '2' YEAR |
        | unull * ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null * ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null * ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint * ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint * ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint * ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint * ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint * ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint * ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | int * ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int * ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int * ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint * ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint * ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint * ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | float * ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float * ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float * ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | double * ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double * ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double * ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | dec * ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec * ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec * ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * unull | INTERVAL '2' MONTH | NULL |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m * smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_y * unull | INTERVAL '2' YEAR | NULL |
        | ival_y * null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y * tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y * smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y * int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y * bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y * float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y * double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y * dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym * unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym * null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym * tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym * smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym * int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym * bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym * float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym * double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym * dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | str * ival_m | '2' | INTERVAL '2' MONTH |
        | str * ival_y | '2' | INTERVAL '2' YEAR |
        | str * ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * str | INTERVAL '2' MONTH | '2' |
        | ival_y * str | INTERVAL '2' YEAR | '2' |
        | ival_ym * str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | unull * str | NULL | '2' |
        | unull * calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null * str | CAST(NULL AS INT) | '2' |
        | null * calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint * str | CAST(2 AS TINYINT) | '2' |
        | tinyint * calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint * str | CAST(2 AS SMALLINT) | '2' |
        | smallint * calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | int * str | CAST(2 AS INT) | '2' |
        | int * calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | bigint * str | CAST(2 AS BIGINT) | '2' |
        | bigint * calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | float * str | CAST(2 AS FLOAT) | '2' |
        | float * calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | double * str | CAST(2 AS DOUBLE) | '2' |
        | double * calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | dec * str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | dec * calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | str * unull | '2' | NULL |
        | str * null | '2' | CAST(NULL AS INT) |
        | str * tinyint | '2' | CAST(2 AS TINYINT) |
        | str * smallint | '2' | CAST(2 AS SMALLINT) |
        | str * int | '2' | CAST(2 AS INT) |
        | str * bigint | '2' | CAST(2 AS BIGINT) |
        | str * float | '2' | CAST(2 AS FLOAT) |
        | str * double | '2' | CAST(2 AS DOUBLE) |
        | str * dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str * str | '2' | '2' |
        | str * calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | calendar * unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar * null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar * tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar * smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar * int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar * bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar * float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar * double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar * dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar * str | make_interval(0,1,0,1,0,0,0) | '2' |

    @spark-4
    Scenario Outline: times ansi-off: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull * unull | NULL | NULL |

  Rule: `*` operand pairs that resolve (ANSI on)

    Scenario Outline: times ansi-on: pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull * null | NULL | CAST(NULL AS INT) |
        | unull * tinyint | NULL | CAST(2 AS TINYINT) |
        | unull * smallint | NULL | CAST(2 AS SMALLINT) |
        | unull * int | NULL | CAST(2 AS INT) |
        | unull * bigint | NULL | CAST(2 AS BIGINT) |
        | unull * float | NULL | CAST(2 AS FLOAT) |
        | unull * double | NULL | CAST(2 AS DOUBLE) |
        | unull * dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull * ival_d | NULL | INTERVAL '2' DAY |
        | unull * ival_dt | NULL | INTERVAL '25' HOUR |
        | unull * ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null * unull | CAST(NULL AS INT) | NULL |
        | null * null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null * tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null * smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null * int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null * bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null * float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null * double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null * dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null * ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null * ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null * ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint * unull | CAST(2 AS TINYINT) | NULL |
        | tinyint * null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint * tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint * smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint * int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint * bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint * float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint * double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint * dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint * ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint * ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint * ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint * unull | CAST(2 AS SMALLINT) | NULL |
        | smallint * null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint * tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint * smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint * int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint * bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint * float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint * double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint * dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint * ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint * ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint * ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int * unull | CAST(2 AS INT) | NULL |
        | int * null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int * tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int * smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int * int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int * bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int * float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int * double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int * dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int * ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int * ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int * ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint * unull | CAST(2 AS BIGINT) | NULL |
        | bigint * null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint * tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint * smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint * int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint * bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint * float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint * double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint * dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint * ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint * ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint * ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float * unull | CAST(2 AS FLOAT) | NULL |
        | float * null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float * tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float * smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float * int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float * bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float * float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float * double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float * dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | float * ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float * ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float * ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double * unull | CAST(2 AS DOUBLE) | NULL |
        | double * null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double * tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double * smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double * int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double * bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double * float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double * double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double * dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | double * ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double * ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double * ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec * unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec * null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec * tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec * smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec * int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec * bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec * float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec * double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec * dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | dec * ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec * ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec * ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str * ival_d | '2' | INTERVAL '2' DAY |
        | str * ival_dt | '2' | INTERVAL '25' HOUR |
        | str * ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d * unull | INTERVAL '2' DAY | NULL |
        | ival_d * null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d * tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d * smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d * int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d * bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d * float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d * double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d * dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d * str | INTERVAL '2' DAY | '2' |
        | ival_dt * unull | INTERVAL '25' HOUR | NULL |
        | ival_dt * null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt * tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt * smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt * int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt * bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt * float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt * double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt * dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt * str | INTERVAL '25' HOUR | '2' |
        | ival_ds * unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds * null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds * tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds * smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds * int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds * bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds * float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds * double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds * dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds * str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | unull * ival_m | NULL | INTERVAL '2' MONTH |
        | unull * ival_y | NULL | INTERVAL '2' YEAR |
        | unull * ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null * ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null * ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint * ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint * ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint * ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint * ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint * ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint * ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | int * ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int * ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int * ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint * ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint * ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint * ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | float * ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float * ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float * ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | double * ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double * ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double * ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | dec * ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec * ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec * ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * unull | INTERVAL '2' MONTH | NULL |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m * smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_y * unull | INTERVAL '2' YEAR | NULL |
        | ival_y * null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y * tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y * smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y * int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y * bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y * float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y * double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y * dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym * unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym * null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym * tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym * smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym * int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym * bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym * float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym * double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym * dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | str * ival_m | '2' | INTERVAL '2' MONTH |
        | str * ival_y | '2' | INTERVAL '2' YEAR |
        | str * ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * str | INTERVAL '2' MONTH | '2' |
        | ival_y * str | INTERVAL '2' YEAR | '2' |
        | ival_ym * str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | unull * calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null * str | CAST(NULL AS INT) | '2' |
        | null * calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint * str | CAST(2 AS TINYINT) | '2' |
        | tinyint * calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint * str | CAST(2 AS SMALLINT) | '2' |
        | smallint * calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | int * str | CAST(2 AS INT) | '2' |
        | int * calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | bigint * str | CAST(2 AS BIGINT) | '2' |
        | bigint * calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | float * str | CAST(2 AS FLOAT) | '2' |
        | float * calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | double * str | CAST(2 AS DOUBLE) | '2' |
        | double * calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | dec * str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | dec * calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | str * null | '2' | CAST(NULL AS INT) |
        | str * tinyint | '2' | CAST(2 AS TINYINT) |
        | str * smallint | '2' | CAST(2 AS SMALLINT) |
        | str * int | '2' | CAST(2 AS INT) |
        | str * bigint | '2' | CAST(2 AS BIGINT) |
        | str * float | '2' | CAST(2 AS FLOAT) |
        | str * double | '2' | CAST(2 AS DOUBLE) |
        | str * dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str * calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | calendar * unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar * null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar * tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar * smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar * int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar * bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar * float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar * double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar * dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar * str | make_interval(0,1,0,1,0,0,0) | '2' |

    @spark-4
    Scenario Outline: times ansi-on: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull * unull | NULL | NULL |

  Rule: `/` operand pairs that resolve (ANSI off)

    Scenario Outline: divide ansi-off: pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull / null | NULL | CAST(NULL AS INT) |
        | unull / tinyint | NULL | CAST(2 AS TINYINT) |
        | unull / smallint | NULL | CAST(2 AS SMALLINT) |
        | unull / int | NULL | CAST(2 AS INT) |
        | unull / bigint | NULL | CAST(2 AS BIGINT) |
        | unull / float | NULL | CAST(2 AS FLOAT) |
        | unull / double | NULL | CAST(2 AS DOUBLE) |
        | unull / dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull / str | NULL | '2' |
        | null / unull | CAST(NULL AS INT) | NULL |
        | null / null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null / tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null / smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null / int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null / bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null / float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null / double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null / dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null / str | CAST(NULL AS INT) | '2' |
        | tinyint / unull | CAST(2 AS TINYINT) | NULL |
        | tinyint / null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint / tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint / smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint / int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint / bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint / float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint / double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint / dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint / str | CAST(2 AS TINYINT) | '2' |
        | smallint / unull | CAST(2 AS SMALLINT) | NULL |
        | smallint / null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint / tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint / smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint / int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint / bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint / float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint / double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint / dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint / str | CAST(2 AS SMALLINT) | '2' |
        | int / unull | CAST(2 AS INT) | NULL |
        | int / null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int / tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int / smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int / int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int / bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int / float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int / double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int / dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int / str | CAST(2 AS INT) | '2' |
        | bigint / unull | CAST(2 AS BIGINT) | NULL |
        | bigint / null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint / tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint / smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint / int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint / bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint / float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint / double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint / dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint / str | CAST(2 AS BIGINT) | '2' |
        | float / unull | CAST(2 AS FLOAT) | NULL |
        | float / null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float / tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float / smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float / int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float / bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float / float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float / double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float / dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | float / str | CAST(2 AS FLOAT) | '2' |
        | double / unull | CAST(2 AS DOUBLE) | NULL |
        | double / null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double / tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double / smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double / int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double / bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double / float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double / double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double / dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | double / str | CAST(2 AS DOUBLE) | '2' |
        | dec / unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec / null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec / tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec / smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec / int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec / bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec / float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec / double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec / dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | dec / str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str / unull | '2' | NULL |
        | str / null | '2' | CAST(NULL AS INT) |
        | str / tinyint | '2' | CAST(2 AS TINYINT) |
        | str / smallint | '2' | CAST(2 AS SMALLINT) |
        | str / int | '2' | CAST(2 AS INT) |
        | str / bigint | '2' | CAST(2 AS BIGINT) |
        | str / float | '2' | CAST(2 AS FLOAT) |
        | str / double | '2' | CAST(2 AS DOUBLE) |
        | str / str | '2' | '2' |
        | ival_d / unull | INTERVAL '2' DAY | NULL |
        | ival_d / null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d / tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d / smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d / int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d / bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d / float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d / double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d / str | INTERVAL '2' DAY | '2' |
        | ival_dt / unull | INTERVAL '25' HOUR | NULL |
        | ival_dt / null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt / tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt / smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt / int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt / bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt / float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt / double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt / str | INTERVAL '25' HOUR | '2' |
        | ival_ds / unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds / null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds / tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds / smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds / int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds / bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds / float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds / double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds / str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | calendar / unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar / null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar / tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar / smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar / int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar / bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar / float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar / double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar / str | make_interval(0,1,0,1,0,0,0) | '2' |
        | ival_m / unull | INTERVAL '2' MONTH | NULL |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m / smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_y / unull | INTERVAL '2' YEAR | NULL |
        | ival_y / null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y / tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y / smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y / int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y / bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y / float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y / double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y / dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym / unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym / null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym / tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym / smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym / int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym / bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym / float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym / double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym / dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt / dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds / dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / str | INTERVAL '2' MONTH | '2' |
        | ival_y / str | INTERVAL '2' YEAR | '2' |
        | ival_ym / str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | calendar / dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | str / dec | '2' | CAST(2 AS DECIMAL(10,2)) |

    @spark-4
    Scenario Outline: divide ansi-off: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull / unull | NULL | NULL |

  Rule: `/` operand pairs that resolve (ANSI on)

    Scenario Outline: divide ansi-on: pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull / null | NULL | CAST(NULL AS INT) |
        | unull / tinyint | NULL | CAST(2 AS TINYINT) |
        | unull / smallint | NULL | CAST(2 AS SMALLINT) |
        | unull / int | NULL | CAST(2 AS INT) |
        | unull / bigint | NULL | CAST(2 AS BIGINT) |
        | unull / float | NULL | CAST(2 AS FLOAT) |
        | unull / double | NULL | CAST(2 AS DOUBLE) |
        | unull / dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | null / unull | CAST(NULL AS INT) | NULL |
        | null / null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null / tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null / smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null / int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null / bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null / float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null / double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null / dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null / str | CAST(NULL AS INT) | '2' |
        | tinyint / unull | CAST(2 AS TINYINT) | NULL |
        | tinyint / null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint / tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint / smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint / int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint / bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint / float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint / double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint / dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint / str | CAST(2 AS TINYINT) | '2' |
        | smallint / unull | CAST(2 AS SMALLINT) | NULL |
        | smallint / null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint / tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint / smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint / int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint / bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint / float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint / double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint / dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint / str | CAST(2 AS SMALLINT) | '2' |
        | int / unull | CAST(2 AS INT) | NULL |
        | int / null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int / tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int / smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int / int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int / bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int / float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int / double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int / dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int / str | CAST(2 AS INT) | '2' |
        | bigint / unull | CAST(2 AS BIGINT) | NULL |
        | bigint / null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint / tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint / smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint / int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint / bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint / float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint / double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint / dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint / str | CAST(2 AS BIGINT) | '2' |
        | float / unull | CAST(2 AS FLOAT) | NULL |
        | float / null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float / tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float / smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float / int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float / bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float / float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float / double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float / dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | float / str | CAST(2 AS FLOAT) | '2' |
        | double / unull | CAST(2 AS DOUBLE) | NULL |
        | double / null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double / tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double / smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double / int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double / bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double / float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double / double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double / dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | double / str | CAST(2 AS DOUBLE) | '2' |
        | dec / unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec / null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec / tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec / smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec / int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec / bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec / float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec / double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec / dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | str / null | '2' | CAST(NULL AS INT) |
        | str / tinyint | '2' | CAST(2 AS TINYINT) |
        | str / smallint | '2' | CAST(2 AS SMALLINT) |
        | str / int | '2' | CAST(2 AS INT) |
        | str / bigint | '2' | CAST(2 AS BIGINT) |
        | str / float | '2' | CAST(2 AS FLOAT) |
        | str / double | '2' | CAST(2 AS DOUBLE) |
        | ival_d / unull | INTERVAL '2' DAY | NULL |
        | ival_d / null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d / tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d / smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d / int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d / bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d / float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d / double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d / str | INTERVAL '2' DAY | '2' |
        | ival_dt / unull | INTERVAL '25' HOUR | NULL |
        | ival_dt / null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt / tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt / smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt / int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt / bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt / float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt / double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt / str | INTERVAL '25' HOUR | '2' |
        | ival_ds / unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds / null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds / tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds / smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds / int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds / bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds / float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds / double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds / str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | calendar / unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar / null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar / tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar / smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar / int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar / bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar / float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar / double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar / str | make_interval(0,1,0,1,0,0,0) | '2' |
        | ival_m / unull | INTERVAL '2' MONTH | NULL |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m / smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_y / unull | INTERVAL '2' YEAR | NULL |
        | ival_y / null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y / tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y / smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y / int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y / bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y / float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y / double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y / dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym / unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym / null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym / tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym / smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym / int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym / bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym / float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym / double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym / dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt / dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds / dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / str | INTERVAL '2' MONTH | '2' |
        | ival_y / str | INTERVAL '2' YEAR | '2' |
        | ival_ym / str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | calendar / dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | dec / str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str / dec | '2' | CAST(2 AS DECIMAL(10,2)) |

    @spark-4
    Scenario Outline: divide ansi-on: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull / unull | NULL | NULL |

  Rule: `%` operand pairs that resolve (ANSI off)

    Scenario Outline: modulo ansi-off: pair resolves: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull % null | NULL | CAST(NULL AS INT) |
        | unull % tinyint | NULL | CAST(2 AS TINYINT) |
        | unull % smallint | NULL | CAST(2 AS SMALLINT) |
        | unull % int | NULL | CAST(2 AS INT) |
        | unull % bigint | NULL | CAST(2 AS BIGINT) |
        | unull % float | NULL | CAST(2 AS FLOAT) |
        | unull % double | NULL | CAST(2 AS DOUBLE) |
        | unull % dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | unull % str | NULL | '2' |
        | null % unull | CAST(NULL AS INT) | NULL |
        | null % null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null % tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null % smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null % int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null % bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null % float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null % double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null % dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | null % str | CAST(NULL AS INT) | '2' |
        | tinyint % unull | CAST(2 AS TINYINT) | NULL |
        | tinyint % null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint % tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint % smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint % int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint % bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint % float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint % double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint % dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint % str | CAST(2 AS TINYINT) | '2' |
        | smallint % unull | CAST(2 AS SMALLINT) | NULL |
        | smallint % null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint % tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint % smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint % int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint % bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint % float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint % double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint % dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint % str | CAST(2 AS SMALLINT) | '2' |
        | int % unull | CAST(2 AS INT) | NULL |
        | int % null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int % tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int % smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int % int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int % bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int % float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int % double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int % dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | int % str | CAST(2 AS INT) | '2' |
        | bigint % unull | CAST(2 AS BIGINT) | NULL |
        | bigint % null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint % tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint % smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint % int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint % bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint % float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint % double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint % dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint % str | CAST(2 AS BIGINT) | '2' |
        | float % unull | CAST(2 AS FLOAT) | NULL |
        | float % null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float % tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float % smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float % int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float % bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float % float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float % double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float % dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | float % str | CAST(2 AS FLOAT) | '2' |
        | double % unull | CAST(2 AS DOUBLE) | NULL |
        | double % null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double % tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double % smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double % int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double % bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double % float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double % double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double % dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | double % str | CAST(2 AS DOUBLE) | '2' |
        | dec % unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec % null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec % tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec % smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec % int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec % bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec % float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec % double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec % dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | dec % str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str % unull | '2' | NULL |
        | str % null | '2' | CAST(NULL AS INT) |
        | str % tinyint | '2' | CAST(2 AS TINYINT) |
        | str % smallint | '2' | CAST(2 AS SMALLINT) |
        | str % int | '2' | CAST(2 AS INT) |
        | str % bigint | '2' | CAST(2 AS BIGINT) |
        | str % float | '2' | CAST(2 AS FLOAT) |
        | str % double | '2' | CAST(2 AS DOUBLE) |
        | str % dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str % str | '2' | '2' |

    @spark-4
    Scenario Outline: modulo ansi-off: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull % unull | NULL | NULL |

  Rule: `%` operand pairs that resolve (ANSI on)

    Scenario Outline: modulo ansi-on: pair resolves: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull % null | NULL | CAST(NULL AS INT) |
        | unull % tinyint | NULL | CAST(2 AS TINYINT) |
        | unull % smallint | NULL | CAST(2 AS SMALLINT) |
        | unull % int | NULL | CAST(2 AS INT) |
        | unull % bigint | NULL | CAST(2 AS BIGINT) |
        | unull % float | NULL | CAST(2 AS FLOAT) |
        | unull % double | NULL | CAST(2 AS DOUBLE) |
        | unull % dec | NULL | CAST(2 AS DECIMAL(10,2)) |
        | null % unull | CAST(NULL AS INT) | NULL |
        | null % null | CAST(NULL AS INT) | CAST(NULL AS INT) |
        | null % tinyint | CAST(NULL AS INT) | CAST(2 AS TINYINT) |
        | null % smallint | CAST(NULL AS INT) | CAST(2 AS SMALLINT) |
        | null % int | CAST(NULL AS INT) | CAST(2 AS INT) |
        | null % bigint | CAST(NULL AS INT) | CAST(2 AS BIGINT) |
        | null % float | CAST(NULL AS INT) | CAST(2 AS FLOAT) |
        | null % double | CAST(NULL AS INT) | CAST(2 AS DOUBLE) |
        | null % dec | CAST(NULL AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | tinyint % unull | CAST(2 AS TINYINT) | NULL |
        | tinyint % null | CAST(2 AS TINYINT) | CAST(NULL AS INT) |
        | tinyint % tinyint | CAST(2 AS TINYINT) | CAST(2 AS TINYINT) |
        | tinyint % smallint | CAST(2 AS TINYINT) | CAST(2 AS SMALLINT) |
        | tinyint % int | CAST(2 AS TINYINT) | CAST(2 AS INT) |
        | tinyint % bigint | CAST(2 AS TINYINT) | CAST(2 AS BIGINT) |
        | tinyint % float | CAST(2 AS TINYINT) | CAST(2 AS FLOAT) |
        | tinyint % double | CAST(2 AS TINYINT) | CAST(2 AS DOUBLE) |
        | tinyint % dec | CAST(2 AS TINYINT) | CAST(2 AS DECIMAL(10,2)) |
        | smallint % unull | CAST(2 AS SMALLINT) | NULL |
        | smallint % null | CAST(2 AS SMALLINT) | CAST(NULL AS INT) |
        | smallint % tinyint | CAST(2 AS SMALLINT) | CAST(2 AS TINYINT) |
        | smallint % smallint | CAST(2 AS SMALLINT) | CAST(2 AS SMALLINT) |
        | smallint % int | CAST(2 AS SMALLINT) | CAST(2 AS INT) |
        | smallint % bigint | CAST(2 AS SMALLINT) | CAST(2 AS BIGINT) |
        | smallint % float | CAST(2 AS SMALLINT) | CAST(2 AS FLOAT) |
        | smallint % double | CAST(2 AS SMALLINT) | CAST(2 AS DOUBLE) |
        | smallint % dec | CAST(2 AS SMALLINT) | CAST(2 AS DECIMAL(10,2)) |
        | int % unull | CAST(2 AS INT) | NULL |
        | int % null | CAST(2 AS INT) | CAST(NULL AS INT) |
        | int % tinyint | CAST(2 AS INT) | CAST(2 AS TINYINT) |
        | int % smallint | CAST(2 AS INT) | CAST(2 AS SMALLINT) |
        | int % int | CAST(2 AS INT) | CAST(2 AS INT) |
        | int % bigint | CAST(2 AS INT) | CAST(2 AS BIGINT) |
        | int % float | CAST(2 AS INT) | CAST(2 AS FLOAT) |
        | int % double | CAST(2 AS INT) | CAST(2 AS DOUBLE) |
        | int % dec | CAST(2 AS INT) | CAST(2 AS DECIMAL(10,2)) |
        | bigint % unull | CAST(2 AS BIGINT) | NULL |
        | bigint % null | CAST(2 AS BIGINT) | CAST(NULL AS INT) |
        | bigint % tinyint | CAST(2 AS BIGINT) | CAST(2 AS TINYINT) |
        | bigint % smallint | CAST(2 AS BIGINT) | CAST(2 AS SMALLINT) |
        | bigint % int | CAST(2 AS BIGINT) | CAST(2 AS INT) |
        | bigint % bigint | CAST(2 AS BIGINT) | CAST(2 AS BIGINT) |
        | bigint % float | CAST(2 AS BIGINT) | CAST(2 AS FLOAT) |
        | bigint % double | CAST(2 AS BIGINT) | CAST(2 AS DOUBLE) |
        | bigint % dec | CAST(2 AS BIGINT) | CAST(2 AS DECIMAL(10,2)) |
        | float % unull | CAST(2 AS FLOAT) | NULL |
        | float % null | CAST(2 AS FLOAT) | CAST(NULL AS INT) |
        | float % tinyint | CAST(2 AS FLOAT) | CAST(2 AS TINYINT) |
        | float % smallint | CAST(2 AS FLOAT) | CAST(2 AS SMALLINT) |
        | float % int | CAST(2 AS FLOAT) | CAST(2 AS INT) |
        | float % bigint | CAST(2 AS FLOAT) | CAST(2 AS BIGINT) |
        | float % float | CAST(2 AS FLOAT) | CAST(2 AS FLOAT) |
        | float % double | CAST(2 AS FLOAT) | CAST(2 AS DOUBLE) |
        | float % dec | CAST(2 AS FLOAT) | CAST(2 AS DECIMAL(10,2)) |
        | double % unull | CAST(2 AS DOUBLE) | NULL |
        | double % null | CAST(2 AS DOUBLE) | CAST(NULL AS INT) |
        | double % tinyint | CAST(2 AS DOUBLE) | CAST(2 AS TINYINT) |
        | double % smallint | CAST(2 AS DOUBLE) | CAST(2 AS SMALLINT) |
        | double % int | CAST(2 AS DOUBLE) | CAST(2 AS INT) |
        | double % bigint | CAST(2 AS DOUBLE) | CAST(2 AS BIGINT) |
        | double % float | CAST(2 AS DOUBLE) | CAST(2 AS FLOAT) |
        | double % double | CAST(2 AS DOUBLE) | CAST(2 AS DOUBLE) |
        | double % dec | CAST(2 AS DOUBLE) | CAST(2 AS DECIMAL(10,2)) |
        | dec % unull | CAST(2 AS DECIMAL(10,2)) | NULL |
        | dec % null | CAST(2 AS DECIMAL(10,2)) | CAST(NULL AS INT) |
        | dec % tinyint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS TINYINT) |
        | dec % smallint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS SMALLINT) |
        | dec % int | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS INT) |
        | dec % bigint | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS BIGINT) |
        | dec % float | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS FLOAT) |
        | dec % double | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DOUBLE) |
        | dec % dec | CAST(2 AS DECIMAL(10,2)) | CAST(2 AS DECIMAL(10,2)) |
        | null % str | CAST(NULL AS INT) | '2' |
        | tinyint % str | CAST(2 AS TINYINT) | '2' |
        | smallint % str | CAST(2 AS SMALLINT) | '2' |
        | int % str | CAST(2 AS INT) | '2' |
        | bigint % str | CAST(2 AS BIGINT) | '2' |
        | float % str | CAST(2 AS FLOAT) | '2' |
        | double % str | CAST(2 AS DOUBLE) | '2' |
        | dec % str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str % null | '2' | CAST(NULL AS INT) |
        | str % tinyint | '2' | CAST(2 AS TINYINT) |
        | str % smallint | '2' | CAST(2 AS SMALLINT) |
        | str % int | '2' | CAST(2 AS INT) |
        | str % bigint | '2' | CAST(2 AS BIGINT) |
        | str % float | '2' | CAST(2 AS FLOAT) |
        | str % double | '2' | CAST(2 AS DOUBLE) |
        | str % dec | '2' | CAST(2 AS DECIMAL(10,2)) |

    @spark-4
    Scenario Outline: modulo ansi-on: pair resolves, VARIANT or untyped NULL pair operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case | l | r |
        | unull % unull | NULL | NULL |

  # Sail's unsigned widths have no Spark SQL spelling, so the fixture cannot run on the JVM --
  # but the VERDICTS below were measured against Spark 4.2.0 on an equivalent PyArrow Parquet
  # file and match it exactly.
  #
  # Spark's Parquet reader WIDENS every unsigned by one step so all values stay
  # representable: UINT_8 -> SMALLINT, UINT_16 -> INT, UINT_32 -> BIGINT,
  # UINT_64 -> DECIMAL(20,0). `DateAdd`/`DateSub` accept only BYTE/SHORT/INT
  # (`datetimeExpressions.scala:331,371`), so uint8 and uint16 are valid offsets and
  # uint32 and uint64 are not. Measured on `struct<u8:smallint,u16:int,u32:bigint,
  # u64:decimal(20,0)>`: Spark resolves the first two and rejects the last two.
  #
  # Note Sail's own Arrow->Spark mapping reports the same-width signed type instead
  # (`u32:int`), one step narrower than Spark's and lossy above 2^31. That divergence is
  # pre-existing and lives in `data_type_arrow.rs`; the guard deliberately follows Spark's
  # widening rather than Sail's reporting, so the accept/reject decision matches.
  @sail-only
  Rule: an unsigned Parquet column is a date offset only at the widths Spark accepts

    Scenario Outline: date plus an unsigned Parquet column resolves: <case>
      Given variable location for temporary directory unsigned_offset_ok
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_ok
        """
      Given statement template
        """
        CREATE TABLE unsigned_ok
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT8) AS u8, CAST(2 AS UINT16) AS u16
        """
      When query
        """
        SELECT CAST(DATE'2024-01-15' + <col> AS STRING) AS r FROM unsigned_ok
        """
      Then query result
        | r          |
        | 2024-01-17 |

      Examples:
        | case | col |
        | uint8 offset | u8 |
        | uint16 offset | u16 |

    # Spark widens UINT_64 to DECIMAL(20,0), which `DateAdd` does not accept; Sail promotes the
    # sum to a decimal that does not cast back to a date, so it rejects it too. UINT_32 diverges:
    # Spark widens it to BIGINT and refuses, Sail reads it as INT and accepts -- see the
    # `@sail-bug` scenario below.
    Scenario Outline: date plus a wider unsigned Parquet column is rejected: <case>
      Given variable location for temporary directory unsigned_offset_reject
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_reject
        """
      Given statement template
        """
        CREATE TABLE unsigned_reject
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT32) AS u32, CAST(2 AS UINT64) AS u64
        """
      When query
        """
        SELECT DATE'2024-01-15' + <col> AS r FROM unsigned_reject
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | col |
        | uint64 offset | u64 |

    # Spark's reader widens UINT_32 to BIGINT, which `DateAdd` refuses. Sail refuses it too now
    # that its date offset guard is `DateAdd`'s own accept set.
    Scenario: date plus a uint32 Parquet column is rejected
      Given variable location for temporary directory unsigned_offset_u32
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_u32
        """
      Given statement template
        """
        CREATE TABLE unsigned_u32
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT32) AS u32
        """
      When query
        """
        SELECT DATE'2024-01-15' + u32 AS r FROM unsigned_u32
        """
      Then query error (?i)cannot resolve

  Rule: a date shifted by a difference of two dates resolves

    # Spark types `datediff` as INT and `date - date` as INTERVAL DAY, and a DATE takes both. Sail
    # now types them the same way, so these pin that a DATE still takes both offsets.
    Scenario Outline: a date shifted by a day difference: <case>
      When query
        """
        SELECT <expr> AS r
        """
      Then query result
        | r   |
        | <r> |

      Examples:
        | case                  | expr                                                             | r          |
        | date plus datediff    | DATE'2024-01-01' + datediff(DATE'2024-01-01', DATE'2023-12-25')  | 2024-01-08 |
        | date minus date_diff  | DATE'2024-01-01' - date_diff(DATE'2024-01-01', DATE'2023-12-25') | 2023-12-25 |
        | date plus a date diff | DATE'2024-02-01' + (DATE'2024-01-10' - DATE'2024-01-01')         | 2024-02-10 |

    # The root of the rule above, asserted directly.
    Scenario Outline: a day difference has Spark's result type: <case>
      When query
        """
        SELECT typeof(<expr>) AS t
        """
      Then query result
        | t   |
        | <t> |

      Examples:
        | case      | expr                                          | t   |
        | datediff  | datediff(DATE'2024-01-10', DATE'2024-01-01')  | int |
        | date_diff | date_diff(DATE'2024-01-10', DATE'2024-01-01') | int |

    # TODO: Spark's `date - date` is a `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616`).
    #  Sail answers an INT day count, because an Arrow `Duration` carries no field range and a
    #  `Duration` is read by seconds downstream (`CAST(date - date AS INT)` would answer 1209600).
    #  Carrying the range needs the interval metadata work of `fix/interval`.
    @sail-bug
    Scenario: a difference of two dates has Spark's declared field range
      When query
        """
        SELECT typeof(DATE'2024-01-10' - DATE'2024-01-01') AS t
        """
      Then query result
        | t            |
        | interval day |

  Rule: an untyped NULL beside a datetime takes the type Spark gives it

    # A bare `NULL` is `NullType`, and Spark does NOT leave it there. For `+`, whichever side is
    # the NULL is cast to a day-time interval -- `BinaryArithmeticWithDatetimeResolver.scala:88,91`,
    # `a.copy(right = Cast(a.right, DayTimeIntervalType.DEFAULT))` -- and for `-` it takes the
    # OTHER operand's own type (`:119,121`). Sail left it as `Null`, which DataFusion cannot
    # coerce, so it REFUSED twelve pairs Spark answers. These assert the pair resolves at all,
    # which is the divergence that mattered; the exact type is asserted below.
    #
    # ANSI is not an axis: all twenty-four cells were measured under both modes on the JVM and
    # neither the verdict nor the type changes, so one mode is the whole contract.
    Scenario Outline: an untyped NULL beside a <case> resolves
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case                 | expression                                |
        | date + null          | DATE'2024-01-15' + NULL                   |
        | null + date          | NULL + DATE'2024-01-15'                   |
        | timestamp + null     | TIMESTAMP'2024-01-15 01:02:03' + NULL     |
        | null + timestamp     | NULL + TIMESTAMP'2024-01-15 01:02:03'     |
        | timestamp_ntz + null | TIMESTAMP_NTZ'2024-01-15 01:02:03' + NULL |
        | null + timestamp_ntz | NULL + TIMESTAMP_NTZ'2024-01-15 01:02:03' |
        | time + null          | TIME '01:02:03' + NULL                    |
        | null + time          | NULL + TIME '01:02:03'                    |
        | date - null          | DATE'2024-01-15' - NULL                   |
        | null - date          | NULL - DATE'2024-01-15'                   |
        | timestamp - null     | TIMESTAMP'2024-01-15 01:02:03' - NULL     |
        | null - timestamp     | NULL - TIMESTAMP'2024-01-15 01:02:03'     |
        | timestamp_ntz - null | TIMESTAMP_NTZ'2024-01-15 01:02:03' - NULL |
        | time - null          | TIME '01:02:03' - NULL                    |
        | null - time          | NULL - TIME '01:02:03'                    |

    # The cast the resolver inserts decides the result type, and Sail agrees on all nine: `+`
    # gives back the datetime -- a DATE promoted to a TIMESTAMP, because the interval the resolver
    # inserts is DAY TO SECOND and `:69` widens the date for anything past DAY -- and `-` gives a
    # day-time interval.
    Scenario Outline: an untyped NULL beside a <case> is typed <type>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case                 | expression                                | type                   |
        | date + null          | DATE'2024-01-15' + NULL                   | timestamp              |
        | null + date          | NULL + DATE'2024-01-15'                   | timestamp              |
        | timestamp + null     | TIMESTAMP'2024-01-15 01:02:03' + NULL     | timestamp              |
        | null + timestamp     | NULL + TIMESTAMP'2024-01-15 01:02:03'     | timestamp              |
        | timestamp_ntz + null | TIMESTAMP_NTZ'2024-01-15 01:02:03' + NULL | timestamp_ntz          |
        | null + timestamp_ntz | NULL + TIMESTAMP_NTZ'2024-01-15 01:02:03' | timestamp_ntz          |
        | time + null          | TIME '01:02:03' + NULL                    | time(6)                |
        | null + time          | NULL + TIME '01:02:03'                    | time(6)                |
        | timestamp - null     | TIMESTAMP'2024-01-15 01:02:03' - NULL     | interval day to second |
        | null - timestamp     | NULL - TIMESTAMP'2024-01-15 01:02:03'     | interval day to second |
        | timestamp_ntz - null | TIMESTAMP_NTZ'2024-01-15 01:02:03' - NULL | interval day to second |

    # What is left is not about the NULL at all: `date - date` is `INTERVAL DAY` and
    # `time - time` is `INTERVAL HOUR TO SECOND`, and Sail spells every day-time interval
    # `DAY TO SECOND` because an Arrow `Duration` carries no declared field range. Same root as
    # `date - date` itself; it goes with `fix/interval`.
    @sail-bug
    Scenario Outline: an untyped NULL beside a <case> is typed <type>, which Sail does not spell
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | case        | expression              | type                    |
        | date - null | DATE'2024-01-15' - NULL | interval day            |
        | null - date | NULL - DATE'2024-01-15' | interval day            |
        | time - null | TIME '01:02:03' - NULL  | interval hour to second |
        | null - time | NULL - TIME '01:02:03'  | interval hour to second |

  Rule: an untyped NULL beside a calendar interval

    # `CalendarInterval + NULL` has no arm of its own: the NULL is cast to a day-time interval for
    # `+` and to the other operand's type for `-` (`BinaryArithmeticWithDatetimeResolver.scala:88,
    # 91,119,121`), and `calendar + day-time interval` is refused. Whether Spark gets there depends on
    # whether the interval operand is still unresolved in that pass: `make_interval(0,1,0,1,0,0,0)`,
    # `-make_interval(...)`, `make_interval(...) * 2` resolve; a column or `CAST('1 day' AS INTERVAL)`
    # does not. Sail resolves them all rather than refuse a query Spark answers.
    # ANSI is not an axis: every row below was measured in both modes on the JVM.
    Scenario Outline: <expression> resolves with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> IS NULL AS resolved FROM (SELECT make_interval(0, 1, 0, 1, 0, 0, 0) AS c)
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | ansi  | expression                                                      |
        | false | c - NULL                                                        |
        | true  | CAST('1 day' AS INTERVAL) - NULL                                |
        | false | make_interval(0, 1, 0, 1, 0, 0, 0) + NULL                       |
        | true  | NULL - make_interval(0, 1, 0, 1, 0, 0, 0.5)                     |
        | false | NULL + make_interval(0, 1, 0, 1, 0, 0, CAST(0 AS DECIMAL(10,6))) |
        | true  | make_interval(CAST(0 AS BIGINT), 1, 0, 1, 0, 0, CAST(0 AS DECIMAL(18,6))) + NULL |
        | false | -make_interval(0, 1, 0, 1, 0, 0, 0) + NULL                      |
        | true  | NULL + make_interval(0, 1, 0, 1, 0, 0, 0) * 2                   |
        | false | NULL - coalesce(make_interval(0, 1, 0, 1, 0, 0, 0), NULL)       |

    # TODO: Spark refuses these, since the interval operand is already resolved; see above.
    @sail-bug
    Scenario Outline: <expression> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v FROM (SELECT make_interval(0, 1, 0, 1, 0, 0, 0) AS c)
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi  | expression                                                       |
        | false | c + NULL                                                         |
        | true  | NULL + c                                                         |
        | false | NULL - c                                                         |
        | true  | coalesce(c, c) + NULL                                            |
        | false | CAST('1 day' AS INTERVAL) + NULL                                 |
        | true  | NULL - CAST('1 day' AS INTERVAL)                                 |
        | false | make_interval(0, 1) + NULL                                       |
        | true  | NULL + make_interval(0, 1, 0, 1, 0, 0, CAST(0 AS DECIMAL(18,6))) |
        | false | NULL - make_interval(0, 1, 0, 1, 0, 0, CAST(0 AS DECIMAL(18,6))) |

  Rule: a DATE minus a TIMESTAMP is subtracted as two timestamps

    # `BinaryArithmeticWithDatetimeResolver.scala:139-141` sends the pair to `SubtractTimestamps`
    # whenever EITHER side is a timestamp -- that arm comes before the `SubtractDates` one -- so
    # the DATE is read as a timestamp, midnight in the SESSION time zone, and the result is a
    # DAY TO SECOND interval.
    #
    # Sail had two faults in this one cell. DataFusion's own coercion reads the DATE as midnight
    # UTC, so under any other zone the answer was off by the offset: a WRONG VALUE, not a refusal.
    # And it produced `Duration(Nanosecond)`, which has no Spark type, so the query died reporting
    # its own schema (`cast Duration(Nanosecond) to Spark data type`). The time zone is the
    # discriminating axis here: run only in UTC and both faults are invisible.
    Scenario Outline: <case> in <timezone> is a day-time interval
      Given config spark.sql.session.timeZone = <timezone>
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case         | timezone         | expression                                            | type                   | value                                |
        | date - ts    | UTC              | DATE'2024-01-15' - TIMESTAMP'2024-01-15 06:00:00'     | interval day to second | INTERVAL '-0 06:00:00' DAY TO SECOND |
        | date - ts    | America/New_York | DATE'2024-01-15' - TIMESTAMP'2024-01-15 06:00:00'     | interval day to second | INTERVAL '-0 06:00:00' DAY TO SECOND |
        | ts - date    | UTC              | TIMESTAMP'2024-01-15 06:00:00' - DATE'2024-01-15'     | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND  |
        | ts - date    | America/New_York | TIMESTAMP'2024-01-15 06:00:00' - DATE'2024-01-15'     | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND  |
        | date - ntz   | UTC              | DATE'2024-01-15' - TIMESTAMP_NTZ'2024-01-15 06:00:00' | interval day to second | INTERVAL '-0 06:00:00' DAY TO SECOND |
        | date - ntz   | America/New_York | DATE'2024-01-15' - TIMESTAMP_NTZ'2024-01-15 06:00:00' | interval day to second | INTERVAL '-0 06:00:00' DAY TO SECOND |
        | ntz - date   | UTC              | TIMESTAMP_NTZ'2024-01-15 06:00:00' - DATE'2024-01-15' | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND  |
        | ntz - date   | America/New_York | TIMESTAMP_NTZ'2024-01-15 06:00:00' - DATE'2024-01-15' | interval day to second | INTERVAL '0 06:00:00' DAY TO SECOND  |
