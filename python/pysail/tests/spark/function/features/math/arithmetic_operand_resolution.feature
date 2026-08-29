Feature: arithmetic operand pairs Spark resolves (+ - * / %) vs Spark 4.2.0

  # The companion of `arithmetic_operand_rejection.feature`, and the reason it exists: a
  # rejection matrix on its own cannot catch OVER-rejection, because a guard that rejected
  # everything would keep all of its rows green. Here every pair Spark 4.2.0 RESOLVES must keep
  # resolving, so narrowing a guard too far turns a row red.
  #
  # This file asserts RESOLUTION ONLY -- that the pair plans at all -- and deliberately does not
  # pin the resulting type. The result type is the arithmetic COERCION contract, a separate
  # concern with its own branch; pinning it here would drag type divergences that this change
  # neither causes nor fixes into a rejection-only matrix. Nullability is likewise out of scope.
  #
  # Same 28-token alphabet and same full cartesian product as the rejection file. GEOGRAPHY is
  # outside the alphabet: it shares GEOMETRY's Arrow representation, and its naming case is
  # pinned in the rejection file instead.
  #
  # Two kinds of row, and only ONE of them is the over-rejection detector:
  #
  #   * the 1377 untagged rows -- pairs BOTH engines resolve today. These are the guard rail:
  #     narrow a guard too far and one of them turns red.
  #   * the 413 `@sail-bug` rows -- pairs Spark resolves and Sail does not. They cannot detect
  #     over-rejection (a pair Sail already rejects cannot become over-rejected); they are an
  #     inventory of Spark functions Sail has not implemented, measured against the JVM, that
  #     announces itself the day someone implements one. Verified cell by cell: none is
  #     rejected by a plan-time guard -- all 413 fail inside DataFusion, so none is a
  #     regression this change introduces. By cause: string promotion 177 | year-month
  #     interval * and / 162 | calendar interval * and / 38 | untyped NULL with a datetime 18
  #     | TIME +- interval 18.

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

    @sail-bug
    Scenario Outline: plus ansi-off: pair resolves (Sail rejects it): <case>
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
        | unull + str | NULL | '2' |
        | unull + date | NULL | DATE'2024-01-15' |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull + ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
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
        | date + unull | DATE'2024-01-15' | NULL |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts_ntz + unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
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

    @sail-bug
    @spark-4.1
    Scenario Outline: plus ansi-off: pair resolves, TIME operand (Sail rejects it): <case>
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
        | time + ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time + ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time + ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d + time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt + time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds + time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |

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

    @sail-bug
    Scenario Outline: plus ansi-on: pair resolves (Sail rejects it): <case>
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
        | unull + date | NULL | DATE'2024-01-15' |
        | unull + ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull + ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
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
        | date + unull | DATE'2024-01-15' | NULL |
        | ts + unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts_ntz + unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
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

    @sail-bug
    @spark-4.1
    Scenario Outline: plus ansi-on: pair resolves, TIME operand (Sail rejects it): <case>
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
        | time + ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time + ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time + ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d + time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt + time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds + time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |

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

    @sail-bug
    Scenario Outline: minus ansi-off: pair resolves (Sail rejects it): <case>
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

    @sail-bug
    @spark-4.1
    Scenario Outline: minus ansi-off: pair resolves, TIME operand (Sail rejects it): <case>
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

    @sail-bug
    Scenario Outline: minus ansi-on: pair resolves (Sail rejects it): <case>
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

    @sail-bug
    @spark-4.1
    Scenario Outline: minus ansi-on: pair resolves, TIME operand (Sail rejects it): <case>
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
        | str - time | '2' | TIME '12:00:00' |
        | time - str | TIME '12:00:00' | '2' |
        | time - ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time - ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time - ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |

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

    @sail-bug
    Scenario Outline: times ansi-off: pair resolves (Sail rejects it): <case>
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
        | unull * str | NULL | '2' |
        | unull * ival_m | NULL | INTERVAL '2' MONTH |
        | unull * ival_y | NULL | INTERVAL '2' YEAR |
        | unull * ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull * calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null * str | CAST(NULL AS INT) | '2' |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null * ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null * ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null * calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint * str | CAST(2 AS TINYINT) | '2' |
        | tinyint * ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint * ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint * ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint * calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint * str | CAST(2 AS SMALLINT) | '2' |
        | smallint * ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint * ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint * ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint * calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | int * str | CAST(2 AS INT) | '2' |
        | int * ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int * ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int * ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int * calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | bigint * str | CAST(2 AS BIGINT) | '2' |
        | bigint * ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint * ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint * ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint * calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | float * str | CAST(2 AS FLOAT) | '2' |
        | float * ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float * ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float * ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float * calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | double * str | CAST(2 AS DOUBLE) | '2' |
        | double * ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double * ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double * ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double * calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | dec * str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | dec * ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec * ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec * ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
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
        | str * ival_m | '2' | INTERVAL '2' MONTH |
        | str * ival_y | '2' | INTERVAL '2' YEAR |
        | str * ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str * calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | ival_m * unull | INTERVAL '2' MONTH | NULL |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m * smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m * str | INTERVAL '2' MONTH | '2' |
        | ival_y * unull | INTERVAL '2' YEAR | NULL |
        | ival_y * null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y * tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y * smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y * int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y * bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y * float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y * double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y * dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y * str | INTERVAL '2' YEAR | '2' |
        | ival_ym * unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym * null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym * tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym * smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym * int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym * bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym * float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym * double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym * dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym * str | INTERVAL '1-2' YEAR TO MONTH | '2' |
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

    @sail-bug
    Scenario Outline: times ansi-on: pair resolves (Sail rejects it): <case>
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
        | unull * ival_m | NULL | INTERVAL '2' MONTH |
        | unull * ival_y | NULL | INTERVAL '2' YEAR |
        | unull * ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull * calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | null * str | CAST(NULL AS INT) | '2' |
        | null * ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null * ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null * ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null * calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint * str | CAST(2 AS TINYINT) | '2' |
        | tinyint * ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint * ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint * ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint * calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint * str | CAST(2 AS SMALLINT) | '2' |
        | smallint * ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint * ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint * ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint * calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | int * str | CAST(2 AS INT) | '2' |
        | int * ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int * ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int * ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int * calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | bigint * str | CAST(2 AS BIGINT) | '2' |
        | bigint * ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint * ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint * ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint * calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | float * str | CAST(2 AS FLOAT) | '2' |
        | float * ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float * ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float * ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float * calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | double * str | CAST(2 AS DOUBLE) | '2' |
        | double * ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double * ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double * ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double * calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | dec * str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | dec * ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec * ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec * ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec * calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | str * null | '2' | CAST(NULL AS INT) |
        | str * tinyint | '2' | CAST(2 AS TINYINT) |
        | str * smallint | '2' | CAST(2 AS SMALLINT) |
        | str * int | '2' | CAST(2 AS INT) |
        | str * bigint | '2' | CAST(2 AS BIGINT) |
        | str * float | '2' | CAST(2 AS FLOAT) |
        | str * double | '2' | CAST(2 AS DOUBLE) |
        | str * dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | str * ival_m | '2' | INTERVAL '2' MONTH |
        | str * ival_y | '2' | INTERVAL '2' YEAR |
        | str * ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str * calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | ival_m * unull | INTERVAL '2' MONTH | NULL |
        | ival_m * null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m * tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m * smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m * int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m * bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m * float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m * double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m * dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m * str | INTERVAL '2' MONTH | '2' |
        | ival_y * unull | INTERVAL '2' YEAR | NULL |
        | ival_y * null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y * tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y * smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y * int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y * bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y * float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y * double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y * dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y * str | INTERVAL '2' YEAR | '2' |
        | ival_ym * unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym * null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym * tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym * smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym * int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym * bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym * float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym * double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym * dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym * str | INTERVAL '1-2' YEAR TO MONTH | '2' |
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

    @sail-bug
    Scenario Outline: divide ansi-off: pair resolves (Sail rejects it): <case>
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
        | str / dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt / dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds / dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / unull | INTERVAL '2' MONTH | NULL |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m / smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / str | INTERVAL '2' MONTH | '2' |
        | ival_y / unull | INTERVAL '2' YEAR | NULL |
        | ival_y / null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y / tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y / smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y / int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y / bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y / float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y / double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y / dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y / str | INTERVAL '2' YEAR | '2' |
        | ival_ym / unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym / null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym / tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym / smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym / int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym / bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym / float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym / double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym / dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym / str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | calendar / dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |

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

    @sail-bug
    Scenario Outline: divide ansi-on: pair resolves (Sail rejects it): <case>
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
        | dec / str | CAST(2 AS DECIMAL(10,2)) | '2' |
        | str / dec | '2' | CAST(2 AS DECIMAL(10,2)) |
        | ival_d / dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt / dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds / dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / unull | INTERVAL '2' MONTH | NULL |
        | ival_m / null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m / tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m / smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m / int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m / bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m / float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m / double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m / dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m / str | INTERVAL '2' MONTH | '2' |
        | ival_y / unull | INTERVAL '2' YEAR | NULL |
        | ival_y / null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y / tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y / smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y / int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y / bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y / float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y / double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y / dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y / str | INTERVAL '2' YEAR | '2' |
        | ival_ym / unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym / null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym / tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym / smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym / int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym / bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym / float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym / double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym / dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym / str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | calendar / dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |

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

    @sail-bug
    Scenario Outline: modulo ansi-off: pair resolves (Sail rejects it): <case>
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

    @sail-bug
    Scenario Outline: modulo ansi-on: pair resolves (Sail rejects it): <case>
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

  # Sail's unsigned integer widths have no Spark SQL spelling, so this cannot run against the JVM
  # oracle -- but it is not a synthetic case. Sail WRITES unsigned columns to Parquet as real
  # `uint8`/`uint16`/`uint32`/`uint64` (verified with `pyarrow.parquet.read_schema`) and READS
  # them back with the unsigned Arrow type intact (`typeof` says `unsigned int`), while
  # reporting them to the client as the Spark types `data_type_arrow.rs` maps them to:
  # UInt8 -> BYTE, UInt16 -> SHORT, UInt32 -> INT, UInt64 -> BIGINT. ClickBench's `hits.parquet`
  # has such a column (`EventDate` is `UInt16`), so this is a path real data takes.
  #
  # `DateAdd`/`DateSub` accept BYTE, SHORT and INT and reject BIGINT
  # (`datetimeExpressions.scala:331,371`; they are `ExpectsInputTypes`, so nothing widens a
  # BIGINT into range). So the offset must be judged by the Spark type the column is reported
  # as, never by its Arrow name -- otherwise the engine refuses an offset the user sees as a
  # plain INT. The uint32-vs-uint64 pair is the discriminating case.
  @sail-only
  Rule: an unsigned Parquet column is a date offset by the Spark type it is reported as

    Scenario: unsigned columns read from Parquet keep their reported Spark types
      Given variable location for temporary directory unsigned_date_offset
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_offsets
        """
      Given statement template
        """
        CREATE TABLE unsigned_offsets
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT8) AS u8, CAST(2 AS UINT16) AS u16,
                  CAST(2 AS UINT32) AS u32, CAST(2 AS UINT64) AS u64
        """
      When query
        """
        SELECT typeof(u8) AS a, typeof(u16) AS b, typeof(u32) AS c, typeof(u64) AS d
        FROM unsigned_offsets
        """
      Then query result
        | a               | b                | c            | d               |
        | unsigned tinyint | unsigned smallint | unsigned int | unsigned bigint |

    Scenario Outline: a date offset from an unsigned Parquet column resolves: <case>
      Given variable location for temporary directory unsigned_offset_add
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_offsets_add
        """
      Given statement template
        """
        CREATE TABLE unsigned_offsets_add
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT8) AS u8, CAST(2 AS UINT16) AS u16, CAST(2 AS UINT32) AS u32
        """
      When query
        """
        SELECT CAST(DATE'2024-01-15' + <col> AS STRING) AS r FROM unsigned_offsets_add
        """
      Then query result
        | r          |
        | 2024-01-17 |

      Examples:
        | case | col |
        | uint8 offset | u8 |
        | uint16 offset | u16 |
        | uint32 offset | u32 |

    # UInt64 is reported as BIGINT, which `DateAdd` does not accept.
    Scenario: a uint64 Parquet column is rejected as a date offset, like the BIGINT it reports
      Given variable location for temporary directory unsigned_offset_reject
      Given final statement
        """
        DROP TABLE IF EXISTS unsigned_offsets_reject
        """
      Given statement template
        """
        CREATE TABLE unsigned_offsets_reject
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT CAST(2 AS UINT64) AS u64
        """
      When query
        """
        SELECT DATE'2024-01-15' + u64 AS r FROM unsigned_offsets_reject
        """
      Then query error (?i)cannot resolve
