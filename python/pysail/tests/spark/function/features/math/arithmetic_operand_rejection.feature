Feature: arithmetic operand-type REJECTION matrix (+ - * / %) vs Spark 4.2.0

  # Every operand pair Spark 4.2.0 REJECTS at analysis, for all five operators under both ANSI
  # modes: the full cartesian product of its type system, 28 tokens, one row per cell, each
  # measured against the JVM. The pairs Spark RESOLVES live in
  # `arithmetic_operand_resolution.feature`; together the two cover all 7840 cells.
  #
  # The alphabet follows Spark's BRANCHING, not its type list: `ival_d` (DAY), `ival_dt`
  # (HOUR) and `ival_ds` (DAY TO SECOND) are distinct because the datetime resolver branches
  # on the field range (`BinaryArithmeticWithDatetimeResolver.scala:68-69`), and `calendar`
  # is distinct from the ANSI intervals. `char`/`varchar` are absent: both engines report
  # them as `string`.
  #
  # The assertion is `cannot resolve` and nothing wider, which is what makes it
  # discriminating: DataFusion's own failures read `Cannot coerce` / `Cannot get result
  # type`, so a row reaching the fallback instead of the guard FAILS here. Spark says
  # `Cannot resolve <expr> due to data type mismatch`, so it holds on both engines.

  Rule: `+` operand-type rejection (ANSI off)

    Scenario Outline: plus ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + bool | NULL | true |
        | unull + bin | NULL | CAST('2' AS BINARY) |
        | unull + array | NULL | array(1,2) |
        | unull + map | NULL | map('a',1) |
        | unull + struct | NULL | named_struct('a',1) |
        | null + bool | CAST(NULL AS INT) | true |
        | null + bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null + ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null + ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null + ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null + ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null + ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null + ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null + ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null + ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null + calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null + array | CAST(NULL AS INT) | array(1,2) |
        | null + map | CAST(NULL AS INT) | map('a',1) |
        | null + struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool + unull | true | NULL |
        | bool + null | true | CAST(NULL AS INT) |
        | bool + bool | true | true |
        | bool + tinyint | true | CAST(2 AS TINYINT) |
        | bool + smallint | true | CAST(2 AS SMALLINT) |
        | bool + int | true | CAST(2 AS INT) |
        | bool + bigint | true | CAST(2 AS BIGINT) |
        | bool + float | true | CAST(2 AS FLOAT) |
        | bool + double | true | CAST(2 AS DOUBLE) |
        | bool + dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool + str | true | '2' |
        | bool + bin | true | CAST('2' AS BINARY) |
        | bool + date | true | DATE'2024-01-15' |
        | bool + ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool + ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool + ival_d | true | INTERVAL '2' DAY |
        | bool + ival_dt | true | INTERVAL '25' HOUR |
        | bool + ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool + ival_m | true | INTERVAL '2' MONTH |
        | bool + ival_y | true | INTERVAL '2' YEAR |
        | bool + ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool + calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool + array | true | array(1,2) |
        | bool + map | true | map('a',1) |
        | bool + struct | true | named_struct('a',1) |
        | tinyint + bool | CAST(2 AS TINYINT) | true |
        | tinyint + bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint + ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint + ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint + ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint + ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint + ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint + ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint + ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint + ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint + calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint + array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint + map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint + struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint + bool | CAST(2 AS SMALLINT) | true |
        | smallint + bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint + ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint + ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint + ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint + ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint + ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint + ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint + ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint + ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint + calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint + array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint + map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint + struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int + bool | CAST(2 AS INT) | true |
        | int + bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int + ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int + ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int + ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int + ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int + ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int + ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int + ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int + ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int + calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int + array | CAST(2 AS INT) | array(1,2) |
        | int + map | CAST(2 AS INT) | map('a',1) |
        | int + struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint + bool | CAST(2 AS BIGINT) | true |
        | bigint + bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint + date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint + ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint + ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint + ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint + ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint + ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint + ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint + ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint + ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint + calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint + array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint + map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint + struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float + bool | CAST(2 AS FLOAT) | true |
        | float + bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float + date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float + ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float + ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float + ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float + ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float + ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float + ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float + ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float + ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float + calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float + array | CAST(2 AS FLOAT) | array(1,2) |
        | float + map | CAST(2 AS FLOAT) | map('a',1) |
        | float + struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double + bool | CAST(2 AS DOUBLE) | true |
        | double + bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double + date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double + ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double + ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double + ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double + ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double + ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double + ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double + ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double + ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double + calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double + array | CAST(2 AS DOUBLE) | array(1,2) |
        | double + map | CAST(2 AS DOUBLE) | map('a',1) |
        | double + struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec + bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec + bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec + date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec + ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec + ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec + ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec + ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec + ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec + ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec + ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec + ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec + calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec + array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec + map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec + struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str + bool | '2' | true |
        | str + bin | '2' | CAST('2' AS BINARY) |
        | str + date | '2' | DATE'2024-01-15' |
        | str + ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str + ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str + ival_m | '2' | INTERVAL '2' MONTH |
        | str + ival_y | '2' | INTERVAL '2' YEAR |
        | str + ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str + array | '2' | array(1,2) |
        | str + map | '2' | map('a',1) |
        | str + struct | '2' | named_struct('a',1) |
        | bin + unull | CAST('2' AS BINARY) | NULL |
        | bin + null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin + bool | CAST('2' AS BINARY) | true |
        | bin + tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin + smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin + int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin + bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin + float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin + double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin + dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin + str | CAST('2' AS BINARY) | '2' |
        | bin + bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin + date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin + ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin + ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin + ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin + ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin + ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin + ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin + ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin + ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin + calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin + array | CAST('2' AS BINARY) | array(1,2) |
        | bin + map | CAST('2' AS BINARY) | map('a',1) |
        | bin + struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date + bool | DATE'2024-01-15' | true |
        | date + bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date + float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date + double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date + dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date + str | DATE'2024-01-15' | '2' |
        | date + bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date + date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date + ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date + ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date + array | DATE'2024-01-15' | array(1,2) |
        | date + map | DATE'2024-01-15' | map('a',1) |
        | date + struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts + null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts + bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts + tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts + smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts + int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts + bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts + float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts + double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts + dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts + str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts + bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts + date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts + ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts + ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts + array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts + map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts + struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz + null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz + bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz + tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz + smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz + int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz + bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz + float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz + double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz + dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz + str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz + bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz + date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz + ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz + ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz + array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz + map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz + struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d + null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d + bool | INTERVAL '2' DAY | true |
        | ival_d + tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d + smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d + int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d + bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d + float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d + double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d + dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d + bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d + ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d + ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d + ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d + calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d + array | INTERVAL '2' DAY | array(1,2) |
        | ival_d + map | INTERVAL '2' DAY | map('a',1) |
        | ival_d + struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt + null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt + bool | INTERVAL '25' HOUR | true |
        | ival_dt + tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt + smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt + int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt + bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt + float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt + double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt + dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt + bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt + ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt + ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt + ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt + calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt + array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt + map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt + struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds + null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds + bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds + tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds + smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds + int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds + bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds + float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds + double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds + dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds + bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds + ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds + ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds + ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds + calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds + array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds + map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds + struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m + null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m + bool | INTERVAL '2' MONTH | true |
        | ival_m + tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m + smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m + int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m + bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m + float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m + double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m + dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m + str | INTERVAL '2' MONTH | '2' |
        | ival_m + bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m + ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m + ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m + ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m + calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m + array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m + map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m + struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y + null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y + bool | INTERVAL '2' YEAR | true |
        | ival_y + tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y + smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y + int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y + bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y + float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y + double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y + dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y + str | INTERVAL '2' YEAR | '2' |
        | ival_y + bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y + ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y + ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y + ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y + calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y + array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y + map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y + struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym + null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym + bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym + tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym + smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym + int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym + bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym + float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym + double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym + dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym + str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym + bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym + ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym + ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym + ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym + calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym + array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym + map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym + struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar + null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar + bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar + tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar + smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar + int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar + bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar + float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar + double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar + dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar + bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar + ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar + ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar + ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar + ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar + ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar + ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar + array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar + map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar + struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array + unull | array(1,2) | NULL |
        | array + null | array(1,2) | CAST(NULL AS INT) |
        | array + bool | array(1,2) | true |
        | array + tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array + smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array + int | array(1,2) | CAST(2 AS INT) |
        | array + bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array + float | array(1,2) | CAST(2 AS FLOAT) |
        | array + double | array(1,2) | CAST(2 AS DOUBLE) |
        | array + dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array + str | array(1,2) | '2' |
        | array + bin | array(1,2) | CAST('2' AS BINARY) |
        | array + date | array(1,2) | DATE'2024-01-15' |
        | array + ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array + ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array + ival_d | array(1,2) | INTERVAL '2' DAY |
        | array + ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array + ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array + ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array + ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array + ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array + calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array + array | array(1,2) | array(1,2) |
        | array + map | array(1,2) | map('a',1) |
        | array + struct | array(1,2) | named_struct('a',1) |
        | map + unull | map('a',1) | NULL |
        | map + null | map('a',1) | CAST(NULL AS INT) |
        | map + bool | map('a',1) | true |
        | map + tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map + smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map + int | map('a',1) | CAST(2 AS INT) |
        | map + bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map + float | map('a',1) | CAST(2 AS FLOAT) |
        | map + double | map('a',1) | CAST(2 AS DOUBLE) |
        | map + dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map + str | map('a',1) | '2' |
        | map + bin | map('a',1) | CAST('2' AS BINARY) |
        | map + date | map('a',1) | DATE'2024-01-15' |
        | map + ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map + ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map + ival_d | map('a',1) | INTERVAL '2' DAY |
        | map + ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map + ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map + ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map + ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map + ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map + calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map + array | map('a',1) | array(1,2) |
        | map + map | map('a',1) | map('a',1) |
        | map + struct | map('a',1) | named_struct('a',1) |
        | struct + unull | named_struct('a',1) | NULL |
        | struct + null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct + bool | named_struct('a',1) | true |
        | struct + tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct + smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct + int | named_struct('a',1) | CAST(2 AS INT) |
        | struct + bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct + float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct + double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct + dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct + str | named_struct('a',1) | '2' |
        | struct + bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct + date | named_struct('a',1) | DATE'2024-01-15' |
        | struct + ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct + ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct + ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct + ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct + ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct + ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct + ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct + ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct + calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct + array | named_struct('a',1) | array(1,2) |
        | struct + map | named_struct('a',1) | map('a',1) |
        | struct + struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: plus ansi-off: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + variant | NULL | parse_json('{"a":1}') |
        | null + variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool + variant | true | parse_json('{"a":1}') |
        | tinyint + variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint + variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int + variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint + variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float + variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double + variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec + variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str + variant | '2' | parse_json('{"a":1}') |
        | bin + variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date + variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts + variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz + variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d + variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt + variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds + variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m + variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y + variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym + variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar + variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array + variant | array(1,2) | parse_json('{"a":1}') |
        | map + variant | map('a',1) | parse_json('{"a":1}') |
        | struct + variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant + unull | parse_json('{"a":1}') | NULL |
        | variant + null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant + bool | parse_json('{"a":1}') | true |
        | variant + tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant + smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant + int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant + bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant + float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant + double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant + dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant + str | parse_json('{"a":1}') | '2' |
        | variant + bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant + date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant + ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant + ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant + ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant + ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant + ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant + ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant + ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant + ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant + calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant + array | parse_json('{"a":1}') | array(1,2) |
        | variant + map | parse_json('{"a":1}') | map('a',1) |
        | variant + struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant + variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: plus ansi-off: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | null + time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool + time | true | TIME '12:00:00' |
        | tinyint + time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint + time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int + time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint + time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float + time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double + time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec + time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str + time | '2' | TIME '12:00:00' |
        | bin + time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date + time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts + time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz + time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time + null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time + bool | TIME '12:00:00' | true |
        | time + tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time + smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time + int | TIME '12:00:00' | CAST(2 AS INT) |
        | time + bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time + float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time + double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time + dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time + str | TIME '12:00:00' | '2' |
        | time + bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time + date | TIME '12:00:00' | DATE'2024-01-15' |
        | time + ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time + ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time + time | TIME '12:00:00' | TIME '12:00:00' |
        | time + ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time + ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time + ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time + calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time + array | TIME '12:00:00' | array(1,2) |
        | time + map | TIME '12:00:00' | map('a',1) |
        | time + struct | TIME '12:00:00' | named_struct('a',1) |
        | time + variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_m + time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y + time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym + time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar + time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array + time | array(1,2) | TIME '12:00:00' |
        | map + time | map('a',1) | TIME '12:00:00' |
        | struct + time | named_struct('a',1) | TIME '12:00:00' |
        | variant + time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: plus ansi-off: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null + geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool + geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint + geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint + geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int + geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint + geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float + geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double + geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec + geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str + geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin + geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date + geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts + geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz + geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time + geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d + geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt + geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds + geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m + geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y + geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym + geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar + geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array + geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map + geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct + geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant + geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom + unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom + null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom + bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom + tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom + smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom + int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom + bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom + float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom + double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom + dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom + str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom + bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom + date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom + ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom + ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom + time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom + ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom + ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom + ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom + ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom + ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom + ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom + calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom + array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom + map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom + struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom + variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom + geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `+` operand-type rejection (ANSI on)

    Scenario Outline: plus ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + bool | NULL | true |
        | unull + str | NULL | '2' |
        | unull + bin | NULL | CAST('2' AS BINARY) |
        | unull + array | NULL | array(1,2) |
        | unull + map | NULL | map('a',1) |
        | unull + struct | NULL | named_struct('a',1) |
        | null + bool | CAST(NULL AS INT) | true |
        | null + bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null + ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null + ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null + ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null + ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null + ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null + ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null + ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null + ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null + calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null + array | CAST(NULL AS INT) | array(1,2) |
        | null + map | CAST(NULL AS INT) | map('a',1) |
        | null + struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool + unull | true | NULL |
        | bool + null | true | CAST(NULL AS INT) |
        | bool + bool | true | true |
        | bool + tinyint | true | CAST(2 AS TINYINT) |
        | bool + smallint | true | CAST(2 AS SMALLINT) |
        | bool + int | true | CAST(2 AS INT) |
        | bool + bigint | true | CAST(2 AS BIGINT) |
        | bool + float | true | CAST(2 AS FLOAT) |
        | bool + double | true | CAST(2 AS DOUBLE) |
        | bool + dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool + str | true | '2' |
        | bool + bin | true | CAST('2' AS BINARY) |
        | bool + date | true | DATE'2024-01-15' |
        | bool + ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool + ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool + ival_d | true | INTERVAL '2' DAY |
        | bool + ival_dt | true | INTERVAL '25' HOUR |
        | bool + ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool + ival_m | true | INTERVAL '2' MONTH |
        | bool + ival_y | true | INTERVAL '2' YEAR |
        | bool + ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool + calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool + array | true | array(1,2) |
        | bool + map | true | map('a',1) |
        | bool + struct | true | named_struct('a',1) |
        | tinyint + bool | CAST(2 AS TINYINT) | true |
        | tinyint + bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint + ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint + ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint + ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint + ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint + ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint + ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint + ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint + ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint + calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint + array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint + map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint + struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint + bool | CAST(2 AS SMALLINT) | true |
        | smallint + bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint + ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint + ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint + ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint + ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint + ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint + ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint + ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint + ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint + calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint + array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint + map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint + struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int + bool | CAST(2 AS INT) | true |
        | int + bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int + ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int + ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int + ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int + ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int + ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int + ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int + ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int + ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int + calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int + array | CAST(2 AS INT) | array(1,2) |
        | int + map | CAST(2 AS INT) | map('a',1) |
        | int + struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint + bool | CAST(2 AS BIGINT) | true |
        | bigint + bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint + date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint + ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint + ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint + ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint + ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint + ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint + ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint + ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint + ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint + calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint + array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint + map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint + struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float + bool | CAST(2 AS FLOAT) | true |
        | float + bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float + date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float + ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float + ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float + ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float + ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float + ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float + ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float + ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float + ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float + calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float + array | CAST(2 AS FLOAT) | array(1,2) |
        | float + map | CAST(2 AS FLOAT) | map('a',1) |
        | float + struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double + bool | CAST(2 AS DOUBLE) | true |
        | double + bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double + date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double + ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double + ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double + ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double + ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double + ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double + ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double + ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double + ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double + calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double + array | CAST(2 AS DOUBLE) | array(1,2) |
        | double + map | CAST(2 AS DOUBLE) | map('a',1) |
        | double + struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec + bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec + bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec + date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec + ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec + ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec + ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec + ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec + ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec + ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec + ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec + ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec + calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec + array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec + map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec + struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str + unull | '2' | NULL |
        | str + bool | '2' | true |
        | str + str | '2' | '2' |
        | str + bin | '2' | CAST('2' AS BINARY) |
        | str + date | '2' | DATE'2024-01-15' |
        | str + ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str + ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str + ival_m | '2' | INTERVAL '2' MONTH |
        | str + ival_y | '2' | INTERVAL '2' YEAR |
        | str + ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str + array | '2' | array(1,2) |
        | str + map | '2' | map('a',1) |
        | str + struct | '2' | named_struct('a',1) |
        | bin + unull | CAST('2' AS BINARY) | NULL |
        | bin + null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin + bool | CAST('2' AS BINARY) | true |
        | bin + tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin + smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin + int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin + bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin + float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin + double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin + dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin + str | CAST('2' AS BINARY) | '2' |
        | bin + bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin + date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin + ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin + ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin + ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin + ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin + ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin + ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin + ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin + ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin + calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin + array | CAST('2' AS BINARY) | array(1,2) |
        | bin + map | CAST('2' AS BINARY) | map('a',1) |
        | bin + struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date + bool | DATE'2024-01-15' | true |
        | date + bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date + float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date + double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date + dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date + str | DATE'2024-01-15' | '2' |
        | date + bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date + date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date + ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date + ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date + array | DATE'2024-01-15' | array(1,2) |
        | date + map | DATE'2024-01-15' | map('a',1) |
        | date + struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts + null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts + bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts + tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts + smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts + int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts + bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts + float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts + double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts + dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts + str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts + bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts + date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts + ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts + ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts + array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts + map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts + struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz + null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz + bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz + tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz + smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz + int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz + bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz + float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz + double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz + dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz + str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz + bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz + date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz + ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz + ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz + array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz + map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz + struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d + null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d + bool | INTERVAL '2' DAY | true |
        | ival_d + tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d + smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d + int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d + bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d + float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d + double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d + dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d + bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d + ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d + ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d + ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d + calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d + array | INTERVAL '2' DAY | array(1,2) |
        | ival_d + map | INTERVAL '2' DAY | map('a',1) |
        | ival_d + struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt + null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt + bool | INTERVAL '25' HOUR | true |
        | ival_dt + tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt + smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt + int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt + bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt + float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt + double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt + dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt + bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt + ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt + ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt + ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt + calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt + array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt + map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt + struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds + null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds + bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds + tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds + smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds + int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds + bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds + float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds + double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds + dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds + bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds + ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds + ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds + ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds + calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds + array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds + map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds + struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m + null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m + bool | INTERVAL '2' MONTH | true |
        | ival_m + tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m + smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m + int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m + bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m + float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m + double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m + dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m + str | INTERVAL '2' MONTH | '2' |
        | ival_m + bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m + ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m + ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m + ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m + calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m + array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m + map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m + struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y + null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y + bool | INTERVAL '2' YEAR | true |
        | ival_y + tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y + smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y + int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y + bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y + float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y + double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y + dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y + str | INTERVAL '2' YEAR | '2' |
        | ival_y + bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y + ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y + ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y + ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y + calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y + array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y + map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y + struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym + null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym + bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym + tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym + smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym + int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym + bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym + float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym + double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym + dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym + str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym + bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym + ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym + ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym + ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym + calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym + array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym + map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym + struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar + null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar + bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar + tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar + smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar + int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar + bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar + float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar + double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar + dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar + bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar + ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar + ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar + ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar + ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar + ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar + ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar + array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar + map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar + struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array + unull | array(1,2) | NULL |
        | array + null | array(1,2) | CAST(NULL AS INT) |
        | array + bool | array(1,2) | true |
        | array + tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array + smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array + int | array(1,2) | CAST(2 AS INT) |
        | array + bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array + float | array(1,2) | CAST(2 AS FLOAT) |
        | array + double | array(1,2) | CAST(2 AS DOUBLE) |
        | array + dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array + str | array(1,2) | '2' |
        | array + bin | array(1,2) | CAST('2' AS BINARY) |
        | array + date | array(1,2) | DATE'2024-01-15' |
        | array + ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array + ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array + ival_d | array(1,2) | INTERVAL '2' DAY |
        | array + ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array + ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array + ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array + ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array + ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array + calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array + array | array(1,2) | array(1,2) |
        | array + map | array(1,2) | map('a',1) |
        | array + struct | array(1,2) | named_struct('a',1) |
        | map + unull | map('a',1) | NULL |
        | map + null | map('a',1) | CAST(NULL AS INT) |
        | map + bool | map('a',1) | true |
        | map + tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map + smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map + int | map('a',1) | CAST(2 AS INT) |
        | map + bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map + float | map('a',1) | CAST(2 AS FLOAT) |
        | map + double | map('a',1) | CAST(2 AS DOUBLE) |
        | map + dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map + str | map('a',1) | '2' |
        | map + bin | map('a',1) | CAST('2' AS BINARY) |
        | map + date | map('a',1) | DATE'2024-01-15' |
        | map + ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map + ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map + ival_d | map('a',1) | INTERVAL '2' DAY |
        | map + ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map + ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map + ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map + ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map + ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map + calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map + array | map('a',1) | array(1,2) |
        | map + map | map('a',1) | map('a',1) |
        | map + struct | map('a',1) | named_struct('a',1) |
        | struct + unull | named_struct('a',1) | NULL |
        | struct + null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct + bool | named_struct('a',1) | true |
        | struct + tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct + smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct + int | named_struct('a',1) | CAST(2 AS INT) |
        | struct + bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct + float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct + double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct + dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct + str | named_struct('a',1) | '2' |
        | struct + bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct + date | named_struct('a',1) | DATE'2024-01-15' |
        | struct + ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct + ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct + ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct + ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct + ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct + ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct + ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct + ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct + calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct + array | named_struct('a',1) | array(1,2) |
        | struct + map | named_struct('a',1) | map('a',1) |
        | struct + struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: plus ansi-on: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + variant | NULL | parse_json('{"a":1}') |
        | null + variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool + variant | true | parse_json('{"a":1}') |
        | tinyint + variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint + variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int + variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint + variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float + variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double + variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec + variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str + variant | '2' | parse_json('{"a":1}') |
        | bin + variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date + variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts + variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz + variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d + variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt + variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds + variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m + variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y + variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym + variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar + variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array + variant | array(1,2) | parse_json('{"a":1}') |
        | map + variant | map('a',1) | parse_json('{"a":1}') |
        | struct + variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant + unull | parse_json('{"a":1}') | NULL |
        | variant + null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant + bool | parse_json('{"a":1}') | true |
        | variant + tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant + smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant + int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant + bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant + float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant + double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant + dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant + str | parse_json('{"a":1}') | '2' |
        | variant + bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant + date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant + ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant + ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant + ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant + ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant + ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant + ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant + ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant + ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant + calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant + array | parse_json('{"a":1}') | array(1,2) |
        | variant + map | parse_json('{"a":1}') | map('a',1) |
        | variant + struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant + variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: plus ansi-on: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | null + time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool + time | true | TIME '12:00:00' |
        | tinyint + time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint + time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int + time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint + time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float + time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double + time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec + time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str + time | '2' | TIME '12:00:00' |
        | bin + time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date + time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts + time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz + time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time + null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time + bool | TIME '12:00:00' | true |
        | time + tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time + smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time + int | TIME '12:00:00' | CAST(2 AS INT) |
        | time + bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time + float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time + double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time + dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time + str | TIME '12:00:00' | '2' |
        | time + bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time + date | TIME '12:00:00' | DATE'2024-01-15' |
        | time + ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time + ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time + time | TIME '12:00:00' | TIME '12:00:00' |
        | time + ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time + ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time + ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time + calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time + array | TIME '12:00:00' | array(1,2) |
        | time + map | TIME '12:00:00' | map('a',1) |
        | time + struct | TIME '12:00:00' | named_struct('a',1) |
        | time + variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_m + time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y + time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym + time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar + time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array + time | array(1,2) | TIME '12:00:00' |
        | map + time | map('a',1) | TIME '12:00:00' |
        | struct + time | named_struct('a',1) | TIME '12:00:00' |
        | variant + time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: plus ansi-on: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) + (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull + geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null + geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool + geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint + geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint + geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int + geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint + geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float + geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double + geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec + geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str + geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin + geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date + geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts + geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz + geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time + geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d + geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt + geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds + geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m + geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y + geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym + geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar + geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array + geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map + geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct + geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant + geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom + unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom + null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom + bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom + tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom + smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom + int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom + bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom + float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom + double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom + dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom + str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom + bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom + date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom + ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom + ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom + time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom + ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom + ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom + ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom + ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom + ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom + ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom + calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom + array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom + map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom + struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom + variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom + geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `-` operand-type rejection (ANSI off)

    Scenario Outline: minus ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - bool | NULL | true |
        | unull - bin | NULL | CAST('2' AS BINARY) |
        | unull - array | NULL | array(1,2) |
        | unull - map | NULL | map('a',1) |
        | unull - struct | NULL | named_struct('a',1) |
        | null - bool | CAST(NULL AS INT) | true |
        | null - bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null - date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null - ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null - ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null - ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null - ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null - ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null - ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null - ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null - ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null - calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null - array | CAST(NULL AS INT) | array(1,2) |
        | null - map | CAST(NULL AS INT) | map('a',1) |
        | null - struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool - unull | true | NULL |
        | bool - null | true | CAST(NULL AS INT) |
        | bool - bool | true | true |
        | bool - tinyint | true | CAST(2 AS TINYINT) |
        | bool - smallint | true | CAST(2 AS SMALLINT) |
        | bool - int | true | CAST(2 AS INT) |
        | bool - bigint | true | CAST(2 AS BIGINT) |
        | bool - float | true | CAST(2 AS FLOAT) |
        | bool - double | true | CAST(2 AS DOUBLE) |
        | bool - dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool - str | true | '2' |
        | bool - bin | true | CAST('2' AS BINARY) |
        | bool - date | true | DATE'2024-01-15' |
        | bool - ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool - ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool - ival_d | true | INTERVAL '2' DAY |
        | bool - ival_dt | true | INTERVAL '25' HOUR |
        | bool - ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool - ival_m | true | INTERVAL '2' MONTH |
        | bool - ival_y | true | INTERVAL '2' YEAR |
        | bool - ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool - calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool - array | true | array(1,2) |
        | bool - map | true | map('a',1) |
        | bool - struct | true | named_struct('a',1) |
        | tinyint - bool | CAST(2 AS TINYINT) | true |
        | tinyint - bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint - date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint - ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint - ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint - ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint - ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint - ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint - ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint - ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint - ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint - calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint - array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint - map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint - struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint - bool | CAST(2 AS SMALLINT) | true |
        | smallint - bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint - date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint - ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint - ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint - ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint - ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint - ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint - ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint - ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint - ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint - calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint - array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint - map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint - struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int - bool | CAST(2 AS INT) | true |
        | int - bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int - date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int - ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int - ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int - ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int - ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int - ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int - ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int - ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int - ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int - calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int - array | CAST(2 AS INT) | array(1,2) |
        | int - map | CAST(2 AS INT) | map('a',1) |
        | int - struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint - bool | CAST(2 AS BIGINT) | true |
        | bigint - bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint - date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint - ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint - ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint - ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint - ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint - ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint - ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint - ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint - ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint - calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint - array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint - map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint - struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float - bool | CAST(2 AS FLOAT) | true |
        | float - bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float - date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float - ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float - ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float - ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float - ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float - ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float - ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float - ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float - ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float - calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float - array | CAST(2 AS FLOAT) | array(1,2) |
        | float - map | CAST(2 AS FLOAT) | map('a',1) |
        | float - struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double - bool | CAST(2 AS DOUBLE) | true |
        | double - bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double - date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double - ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double - ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double - ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double - ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double - ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double - ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double - ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double - ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double - calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double - array | CAST(2 AS DOUBLE) | array(1,2) |
        | double - map | CAST(2 AS DOUBLE) | map('a',1) |
        | double - struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec - bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec - bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec - date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec - ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec - ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec - ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec - ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec - ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec - ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec - ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec - ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec - calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec - array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec - map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec - struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str - bool | '2' | true |
        | str - bin | '2' | CAST('2' AS BINARY) |
        | str - ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str - ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str - ival_m | '2' | INTERVAL '2' MONTH |
        | str - ival_y | '2' | INTERVAL '2' YEAR |
        | str - ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str - array | '2' | array(1,2) |
        | str - map | '2' | map('a',1) |
        | str - struct | '2' | named_struct('a',1) |
        | bin - unull | CAST('2' AS BINARY) | NULL |
        | bin - null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin - bool | CAST('2' AS BINARY) | true |
        | bin - tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin - smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin - int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin - bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin - float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin - double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin - dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin - str | CAST('2' AS BINARY) | '2' |
        | bin - bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin - date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin - ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin - ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin - ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin - ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin - ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin - ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin - ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin - ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin - calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin - array | CAST('2' AS BINARY) | array(1,2) |
        | bin - map | CAST('2' AS BINARY) | map('a',1) |
        | bin - struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date - bool | DATE'2024-01-15' | true |
        | date - bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date - float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date - double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date - dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date - str | DATE'2024-01-15' | '2' |
        | date - bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date - array | DATE'2024-01-15' | array(1,2) |
        | date - map | DATE'2024-01-15' | map('a',1) |
        | date - struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts - null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts - bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts - tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts - smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts - int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts - bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts - float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts - double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts - dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts - str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts - bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts - array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts - map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts - struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz - null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz - bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz - tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz - smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz - int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz - bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz - float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz - double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz - dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz - str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz - bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz - array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz - map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz - struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d - null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d - bool | INTERVAL '2' DAY | true |
        | ival_d - tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d - smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d - int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d - bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d - float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d - double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d - dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d - str | INTERVAL '2' DAY | '2' |
        | ival_d - bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d - date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d - ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d - ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d - ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d - ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d - ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d - calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d - array | INTERVAL '2' DAY | array(1,2) |
        | ival_d - map | INTERVAL '2' DAY | map('a',1) |
        | ival_d - struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt - null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt - bool | INTERVAL '25' HOUR | true |
        | ival_dt - tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt - smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt - int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt - bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt - float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt - double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt - dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt - str | INTERVAL '25' HOUR | '2' |
        | ival_dt - bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt - date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt - ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt - ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt - ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt - ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt - ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt - calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt - array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt - map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt - struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds - null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds - bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds - tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds - smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds - int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds - bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds - float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds - double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds - dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds - str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds - bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds - date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds - ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds - ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds - ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds - ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds - ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds - calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds - array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds - map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds - struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m - null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m - bool | INTERVAL '2' MONTH | true |
        | ival_m - tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m - smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m - int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m - bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m - float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m - double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m - dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m - str | INTERVAL '2' MONTH | '2' |
        | ival_m - bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m - date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m - ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m - ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m - ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m - ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m - ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m - calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m - array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m - map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m - struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y - null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y - bool | INTERVAL '2' YEAR | true |
        | ival_y - tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y - smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y - int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y - bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y - float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y - double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y - dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y - str | INTERVAL '2' YEAR | '2' |
        | ival_y - bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y - date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y - ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y - ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y - ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y - ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y - ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y - calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y - array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y - map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y - struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym - null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym - bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym - tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym - smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym - int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym - bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym - float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym - double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym - dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym - str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym - bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym - date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym - ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym - ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym - ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym - ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym - ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym - calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym - array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym - map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym - struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar - null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar - bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar - tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar - smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar - int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar - bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar - float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar - double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar - dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar - str | make_interval(0,1,0,1,0,0,0) | '2' |
        | calendar - bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar - date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar - ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar - ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar - ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar - ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar - ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar - ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar - ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar - ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar - array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar - map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar - struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array - unull | array(1,2) | NULL |
        | array - null | array(1,2) | CAST(NULL AS INT) |
        | array - bool | array(1,2) | true |
        | array - tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array - smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array - int | array(1,2) | CAST(2 AS INT) |
        | array - bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array - float | array(1,2) | CAST(2 AS FLOAT) |
        | array - double | array(1,2) | CAST(2 AS DOUBLE) |
        | array - dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array - str | array(1,2) | '2' |
        | array - bin | array(1,2) | CAST('2' AS BINARY) |
        | array - date | array(1,2) | DATE'2024-01-15' |
        | array - ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array - ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array - ival_d | array(1,2) | INTERVAL '2' DAY |
        | array - ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array - ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array - ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array - ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array - ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array - calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array - array | array(1,2) | array(1,2) |
        | array - map | array(1,2) | map('a',1) |
        | array - struct | array(1,2) | named_struct('a',1) |
        | map - unull | map('a',1) | NULL |
        | map - null | map('a',1) | CAST(NULL AS INT) |
        | map - bool | map('a',1) | true |
        | map - tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map - smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map - int | map('a',1) | CAST(2 AS INT) |
        | map - bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map - float | map('a',1) | CAST(2 AS FLOAT) |
        | map - double | map('a',1) | CAST(2 AS DOUBLE) |
        | map - dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map - str | map('a',1) | '2' |
        | map - bin | map('a',1) | CAST('2' AS BINARY) |
        | map - date | map('a',1) | DATE'2024-01-15' |
        | map - ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map - ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map - ival_d | map('a',1) | INTERVAL '2' DAY |
        | map - ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map - ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map - ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map - ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map - ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map - calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map - array | map('a',1) | array(1,2) |
        | map - map | map('a',1) | map('a',1) |
        | map - struct | map('a',1) | named_struct('a',1) |
        | struct - unull | named_struct('a',1) | NULL |
        | struct - null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct - bool | named_struct('a',1) | true |
        | struct - tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct - smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct - int | named_struct('a',1) | CAST(2 AS INT) |
        | struct - bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct - float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct - double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct - dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct - str | named_struct('a',1) | '2' |
        | struct - bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct - date | named_struct('a',1) | DATE'2024-01-15' |
        | struct - ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct - ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct - ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct - ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct - ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct - ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct - ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct - ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct - calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct - array | named_struct('a',1) | array(1,2) |
        | struct - map | named_struct('a',1) | map('a',1) |
        | struct - struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: minus ansi-off: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - variant | NULL | parse_json('{"a":1}') |
        | null - variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool - variant | true | parse_json('{"a":1}') |
        | tinyint - variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint - variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int - variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint - variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float - variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double - variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec - variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str - variant | '2' | parse_json('{"a":1}') |
        | bin - variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date - variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts - variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz - variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d - variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt - variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds - variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m - variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y - variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym - variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar - variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array - variant | array(1,2) | parse_json('{"a":1}') |
        | map - variant | map('a',1) | parse_json('{"a":1}') |
        | struct - variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant - unull | parse_json('{"a":1}') | NULL |
        | variant - null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant - bool | parse_json('{"a":1}') | true |
        | variant - tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant - smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant - int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant - bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant - float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant - double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant - dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant - str | parse_json('{"a":1}') | '2' |
        | variant - bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant - date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant - ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant - ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant - ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant - ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant - ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant - ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant - ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant - ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant - calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant - array | parse_json('{"a":1}') | array(1,2) |
        | variant - map | parse_json('{"a":1}') | map('a',1) |
        | variant - struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant - variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: minus ansi-off: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | null - time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool - time | true | TIME '12:00:00' |
        | tinyint - time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint - time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int - time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint - time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float - time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double - time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec - time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str - time | '2' | TIME '12:00:00' |
        | bin - time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date - time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts - time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz - time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time - null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time - bool | TIME '12:00:00' | true |
        | time - tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time - smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time - int | TIME '12:00:00' | CAST(2 AS INT) |
        | time - bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time - float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time - double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time - dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time - str | TIME '12:00:00' | '2' |
        | time - bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time - date | TIME '12:00:00' | DATE'2024-01-15' |
        | time - ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time - ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time - ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time - ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time - ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time - calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time - array | TIME '12:00:00' | array(1,2) |
        | time - map | TIME '12:00:00' | map('a',1) |
        | time - struct | TIME '12:00:00' | named_struct('a',1) |
        | time - variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d - time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt - time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds - time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m - time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y - time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym - time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar - time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array - time | array(1,2) | TIME '12:00:00' |
        | map - time | map('a',1) | TIME '12:00:00' |
        | struct - time | named_struct('a',1) | TIME '12:00:00' |
        | variant - time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: minus ansi-off: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null - geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool - geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint - geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint - geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int - geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint - geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float - geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double - geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec - geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str - geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin - geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date - geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts - geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz - geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time - geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d - geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt - geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds - geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m - geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y - geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym - geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar - geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array - geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map - geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct - geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant - geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom - unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom - null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom - bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom - tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom - smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom - int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom - bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom - float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom - double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom - dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom - str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom - bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom - date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom - ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom - ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom - time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom - ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom - ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom - ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom - ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom - ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom - ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom - calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom - array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom - map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom - struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom - variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom - geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `-` operand-type rejection (ANSI on)

    Scenario Outline: minus ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - bool | NULL | true |
        | unull - str | NULL | '2' |
        | unull - bin | NULL | CAST('2' AS BINARY) |
        | unull - array | NULL | array(1,2) |
        | unull - map | NULL | map('a',1) |
        | unull - struct | NULL | named_struct('a',1) |
        | null - bool | CAST(NULL AS INT) | true |
        | null - bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null - date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null - ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null - ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null - ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null - ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null - ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null - ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null - ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null - ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null - calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null - array | CAST(NULL AS INT) | array(1,2) |
        | null - map | CAST(NULL AS INT) | map('a',1) |
        | null - struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool - unull | true | NULL |
        | bool - null | true | CAST(NULL AS INT) |
        | bool - bool | true | true |
        | bool - tinyint | true | CAST(2 AS TINYINT) |
        | bool - smallint | true | CAST(2 AS SMALLINT) |
        | bool - int | true | CAST(2 AS INT) |
        | bool - bigint | true | CAST(2 AS BIGINT) |
        | bool - float | true | CAST(2 AS FLOAT) |
        | bool - double | true | CAST(2 AS DOUBLE) |
        | bool - dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool - str | true | '2' |
        | bool - bin | true | CAST('2' AS BINARY) |
        | bool - date | true | DATE'2024-01-15' |
        | bool - ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool - ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool - ival_d | true | INTERVAL '2' DAY |
        | bool - ival_dt | true | INTERVAL '25' HOUR |
        | bool - ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool - ival_m | true | INTERVAL '2' MONTH |
        | bool - ival_y | true | INTERVAL '2' YEAR |
        | bool - ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool - calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool - array | true | array(1,2) |
        | bool - map | true | map('a',1) |
        | bool - struct | true | named_struct('a',1) |
        | tinyint - bool | CAST(2 AS TINYINT) | true |
        | tinyint - bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint - date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint - ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint - ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint - ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint - ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint - ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint - ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint - ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint - ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint - calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint - array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint - map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint - struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint - bool | CAST(2 AS SMALLINT) | true |
        | smallint - bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint - date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint - ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint - ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint - ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint - ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint - ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint - ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint - ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint - ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint - calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint - array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint - map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint - struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int - bool | CAST(2 AS INT) | true |
        | int - bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int - date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int - ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int - ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int - ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int - ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int - ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int - ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int - ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int - ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int - calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int - array | CAST(2 AS INT) | array(1,2) |
        | int - map | CAST(2 AS INT) | map('a',1) |
        | int - struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint - bool | CAST(2 AS BIGINT) | true |
        | bigint - bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint - date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint - ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint - ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint - ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint - ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint - ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint - ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint - ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint - ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint - calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint - array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint - map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint - struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float - bool | CAST(2 AS FLOAT) | true |
        | float - bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float - date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float - ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float - ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float - ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float - ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float - ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float - ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float - ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float - ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float - calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float - array | CAST(2 AS FLOAT) | array(1,2) |
        | float - map | CAST(2 AS FLOAT) | map('a',1) |
        | float - struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double - bool | CAST(2 AS DOUBLE) | true |
        | double - bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double - date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double - ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double - ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double - ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double - ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double - ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double - ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double - ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double - ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double - calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double - array | CAST(2 AS DOUBLE) | array(1,2) |
        | double - map | CAST(2 AS DOUBLE) | map('a',1) |
        | double - struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec - bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec - bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec - date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec - ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec - ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec - ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec - ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec - ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec - ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec - ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec - ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec - calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec - array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec - map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec - struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str - unull | '2' | NULL |
        | str - bool | '2' | true |
        | str - str | '2' | '2' |
        | str - bin | '2' | CAST('2' AS BINARY) |
        | str - ival_m | '2' | INTERVAL '2' MONTH |
        | str - ival_y | '2' | INTERVAL '2' YEAR |
        | str - ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str - array | '2' | array(1,2) |
        | str - map | '2' | map('a',1) |
        | str - struct | '2' | named_struct('a',1) |
        | bin - unull | CAST('2' AS BINARY) | NULL |
        | bin - null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin - bool | CAST('2' AS BINARY) | true |
        | bin - tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin - smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin - int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin - bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin - float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin - double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin - dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin - str | CAST('2' AS BINARY) | '2' |
        | bin - bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin - date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin - ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin - ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin - ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin - ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin - ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin - ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin - ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin - ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin - calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin - array | CAST('2' AS BINARY) | array(1,2) |
        | bin - map | CAST('2' AS BINARY) | map('a',1) |
        | bin - struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date - bool | DATE'2024-01-15' | true |
        | date - bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date - float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date - double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date - dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date - bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date - array | DATE'2024-01-15' | array(1,2) |
        | date - map | DATE'2024-01-15' | map('a',1) |
        | date - struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts - null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts - bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts - tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts - smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts - int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts - bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts - float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts - double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts - dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts - bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts - array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts - map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts - struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz - null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz - bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz - tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz - smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz - int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz - bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz - float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz - double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz - dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz - bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz - array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz - map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz - struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d - null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d - bool | INTERVAL '2' DAY | true |
        | ival_d - tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d - smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d - int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d - bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d - float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d - double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d - dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d - str | INTERVAL '2' DAY | '2' |
        | ival_d - bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d - date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d - ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d - ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d - ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d - ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d - ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d - calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d - array | INTERVAL '2' DAY | array(1,2) |
        | ival_d - map | INTERVAL '2' DAY | map('a',1) |
        | ival_d - struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt - null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt - bool | INTERVAL '25' HOUR | true |
        | ival_dt - tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt - smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt - int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt - bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt - float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt - double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt - dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt - str | INTERVAL '25' HOUR | '2' |
        | ival_dt - bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt - date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt - ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt - ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt - ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt - ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt - ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt - calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt - array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt - map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt - struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds - null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds - bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds - tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds - smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds - int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds - bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds - float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds - double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds - dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds - str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds - bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds - date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds - ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds - ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds - ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds - ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds - ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds - calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds - array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds - map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds - struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m - null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m - bool | INTERVAL '2' MONTH | true |
        | ival_m - tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m - smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m - int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m - bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m - float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m - double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m - dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m - str | INTERVAL '2' MONTH | '2' |
        | ival_m - bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m - date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m - ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m - ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m - ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m - ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m - ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m - calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m - array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m - map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m - struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y - null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y - bool | INTERVAL '2' YEAR | true |
        | ival_y - tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y - smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y - int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y - bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y - float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y - double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y - dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y - str | INTERVAL '2' YEAR | '2' |
        | ival_y - bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y - date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y - ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y - ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y - ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y - ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y - ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y - calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y - array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y - map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y - struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym - null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym - bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym - tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym - smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym - int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym - bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym - float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym - double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym - dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym - str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym - bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym - date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym - ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym - ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym - ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym - ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym - ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym - calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym - array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym - map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym - struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar - null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar - bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar - tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar - smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar - int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar - bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar - float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar - double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar - dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar - str | make_interval(0,1,0,1,0,0,0) | '2' |
        | calendar - bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar - date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar - ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar - ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar - ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar - ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar - ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar - ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar - ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar - ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar - array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar - map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar - struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array - unull | array(1,2) | NULL |
        | array - null | array(1,2) | CAST(NULL AS INT) |
        | array - bool | array(1,2) | true |
        | array - tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array - smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array - int | array(1,2) | CAST(2 AS INT) |
        | array - bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array - float | array(1,2) | CAST(2 AS FLOAT) |
        | array - double | array(1,2) | CAST(2 AS DOUBLE) |
        | array - dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array - str | array(1,2) | '2' |
        | array - bin | array(1,2) | CAST('2' AS BINARY) |
        | array - date | array(1,2) | DATE'2024-01-15' |
        | array - ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array - ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array - ival_d | array(1,2) | INTERVAL '2' DAY |
        | array - ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array - ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array - ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array - ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array - ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array - calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array - array | array(1,2) | array(1,2) |
        | array - map | array(1,2) | map('a',1) |
        | array - struct | array(1,2) | named_struct('a',1) |
        | map - unull | map('a',1) | NULL |
        | map - null | map('a',1) | CAST(NULL AS INT) |
        | map - bool | map('a',1) | true |
        | map - tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map - smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map - int | map('a',1) | CAST(2 AS INT) |
        | map - bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map - float | map('a',1) | CAST(2 AS FLOAT) |
        | map - double | map('a',1) | CAST(2 AS DOUBLE) |
        | map - dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map - str | map('a',1) | '2' |
        | map - bin | map('a',1) | CAST('2' AS BINARY) |
        | map - date | map('a',1) | DATE'2024-01-15' |
        | map - ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map - ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map - ival_d | map('a',1) | INTERVAL '2' DAY |
        | map - ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map - ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map - ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map - ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map - ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map - calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map - array | map('a',1) | array(1,2) |
        | map - map | map('a',1) | map('a',1) |
        | map - struct | map('a',1) | named_struct('a',1) |
        | struct - unull | named_struct('a',1) | NULL |
        | struct - null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct - bool | named_struct('a',1) | true |
        | struct - tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct - smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct - int | named_struct('a',1) | CAST(2 AS INT) |
        | struct - bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct - float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct - double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct - dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct - str | named_struct('a',1) | '2' |
        | struct - bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct - date | named_struct('a',1) | DATE'2024-01-15' |
        | struct - ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct - ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct - ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct - ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct - ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct - ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct - ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct - ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct - calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct - array | named_struct('a',1) | array(1,2) |
        | struct - map | named_struct('a',1) | map('a',1) |
        | struct - struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: minus ansi-on: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - variant | NULL | parse_json('{"a":1}') |
        | null - variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool - variant | true | parse_json('{"a":1}') |
        | tinyint - variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint - variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int - variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint - variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float - variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double - variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec - variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str - variant | '2' | parse_json('{"a":1}') |
        | bin - variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date - variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts - variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz - variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d - variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt - variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds - variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m - variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y - variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym - variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar - variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array - variant | array(1,2) | parse_json('{"a":1}') |
        | map - variant | map('a',1) | parse_json('{"a":1}') |
        | struct - variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant - unull | parse_json('{"a":1}') | NULL |
        | variant - null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant - bool | parse_json('{"a":1}') | true |
        | variant - tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant - smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant - int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant - bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant - float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant - double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant - dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant - str | parse_json('{"a":1}') | '2' |
        | variant - bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant - date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant - ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant - ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant - ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant - ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant - ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant - ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant - ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant - ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant - calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant - array | parse_json('{"a":1}') | array(1,2) |
        | variant - map | parse_json('{"a":1}') | map('a',1) |
        | variant - struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant - variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: minus ansi-on: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | null - time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool - time | true | TIME '12:00:00' |
        | tinyint - time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint - time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int - time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint - time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float - time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double - time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec - time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | bin - time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date - time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts - time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz - time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time - null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time - bool | TIME '12:00:00' | true |
        | time - tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time - smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time - int | TIME '12:00:00' | CAST(2 AS INT) |
        | time - bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time - float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time - double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time - dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time - bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time - date | TIME '12:00:00' | DATE'2024-01-15' |
        | time - ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time - ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time - ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time - ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time - ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time - calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time - array | TIME '12:00:00' | array(1,2) |
        | time - map | TIME '12:00:00' | map('a',1) |
        | time - struct | TIME '12:00:00' | named_struct('a',1) |
        | time - variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d - time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt - time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds - time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m - time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y - time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym - time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar - time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array - time | array(1,2) | TIME '12:00:00' |
        | map - time | map('a',1) | TIME '12:00:00' |
        | struct - time | named_struct('a',1) | TIME '12:00:00' |
        | variant - time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: minus ansi-on: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) - (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull - geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null - geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool - geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint - geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint - geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int - geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint - geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float - geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double - geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec - geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str - geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin - geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date - geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts - geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz - geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time - geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d - geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt - geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds - geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m - geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y - geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym - geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar - geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array - geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map - geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct - geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant - geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom - unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom - null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom - bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom - tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom - smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom - int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom - bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom - float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom - double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom - dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom - str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom - bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom - date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom - ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom - ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom - time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom - ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom - ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom - ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom - ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom - ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom - ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom - calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom - array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom - map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom - struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom - variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom - geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `*` operand-type rejection (ANSI off)

    Scenario Outline: times ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * bool | NULL | true |
        | unull * bin | NULL | CAST('2' AS BINARY) |
        | unull * date | NULL | DATE'2024-01-15' |
        | unull * ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull * ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull * array | NULL | array(1,2) |
        | unull * map | NULL | map('a',1) |
        | unull * struct | NULL | named_struct('a',1) |
        | null * bool | CAST(NULL AS INT) | true |
        | null * bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null * date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null * ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null * ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null * array | CAST(NULL AS INT) | array(1,2) |
        | null * map | CAST(NULL AS INT) | map('a',1) |
        | null * struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool * unull | true | NULL |
        | bool * null | true | CAST(NULL AS INT) |
        | bool * bool | true | true |
        | bool * tinyint | true | CAST(2 AS TINYINT) |
        | bool * smallint | true | CAST(2 AS SMALLINT) |
        | bool * int | true | CAST(2 AS INT) |
        | bool * bigint | true | CAST(2 AS BIGINT) |
        | bool * float | true | CAST(2 AS FLOAT) |
        | bool * double | true | CAST(2 AS DOUBLE) |
        | bool * dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool * str | true | '2' |
        | bool * bin | true | CAST('2' AS BINARY) |
        | bool * date | true | DATE'2024-01-15' |
        | bool * ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool * ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool * ival_d | true | INTERVAL '2' DAY |
        | bool * ival_dt | true | INTERVAL '25' HOUR |
        | bool * ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool * ival_m | true | INTERVAL '2' MONTH |
        | bool * ival_y | true | INTERVAL '2' YEAR |
        | bool * ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool * calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool * array | true | array(1,2) |
        | bool * map | true | map('a',1) |
        | bool * struct | true | named_struct('a',1) |
        | tinyint * bool | CAST(2 AS TINYINT) | true |
        | tinyint * bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint * date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint * ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint * ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint * array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint * map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint * struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint * bool | CAST(2 AS SMALLINT) | true |
        | smallint * bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint * date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint * ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint * ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint * array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint * map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint * struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int * bool | CAST(2 AS INT) | true |
        | int * bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int * date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int * ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int * ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int * array | CAST(2 AS INT) | array(1,2) |
        | int * map | CAST(2 AS INT) | map('a',1) |
        | int * struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint * bool | CAST(2 AS BIGINT) | true |
        | bigint * bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint * date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint * ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint * ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint * array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint * map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint * struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float * bool | CAST(2 AS FLOAT) | true |
        | float * bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float * date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float * ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float * ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float * array | CAST(2 AS FLOAT) | array(1,2) |
        | float * map | CAST(2 AS FLOAT) | map('a',1) |
        | float * struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double * bool | CAST(2 AS DOUBLE) | true |
        | double * bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double * date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double * ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double * ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double * array | CAST(2 AS DOUBLE) | array(1,2) |
        | double * map | CAST(2 AS DOUBLE) | map('a',1) |
        | double * struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec * bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec * bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec * date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec * ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec * ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec * array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec * map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec * struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str * bool | '2' | true |
        | str * bin | '2' | CAST('2' AS BINARY) |
        | str * date | '2' | DATE'2024-01-15' |
        | str * ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str * ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str * array | '2' | array(1,2) |
        | str * map | '2' | map('a',1) |
        | str * struct | '2' | named_struct('a',1) |
        | bin * unull | CAST('2' AS BINARY) | NULL |
        | bin * null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin * bool | CAST('2' AS BINARY) | true |
        | bin * tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin * smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin * int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin * bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin * float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin * double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin * dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin * str | CAST('2' AS BINARY) | '2' |
        | bin * bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin * date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin * ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin * ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin * ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin * ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin * ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin * ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin * ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin * ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin * calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin * array | CAST('2' AS BINARY) | array(1,2) |
        | bin * map | CAST('2' AS BINARY) | map('a',1) |
        | bin * struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date * unull | DATE'2024-01-15' | NULL |
        | date * null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date * bool | DATE'2024-01-15' | true |
        | date * tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date * smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date * int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date * bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date * float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date * double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date * dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date * str | DATE'2024-01-15' | '2' |
        | date * bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date * date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date * ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date * ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date * ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date * ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date * ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date * ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date * ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date * ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date * calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date * array | DATE'2024-01-15' | array(1,2) |
        | date * map | DATE'2024-01-15' | map('a',1) |
        | date * struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts * unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts * null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts * bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts * tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts * smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts * int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts * bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts * float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts * double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts * dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts * str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts * bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts * date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts * ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts * ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts * ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts * ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts * ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts * ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts * ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts * ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts * calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts * array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts * map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts * struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz * unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz * null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz * bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz * tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz * smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz * int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz * bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz * float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz * double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz * dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz * str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz * bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz * date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz * ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz * ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz * ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz * ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz * ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz * ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz * ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz * ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz * calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz * array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz * map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz * struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d * bool | INTERVAL '2' DAY | true |
        | ival_d * bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d * date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d * ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d * ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d * ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d * ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d * ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d * ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d * ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d * ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d * calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d * array | INTERVAL '2' DAY | array(1,2) |
        | ival_d * map | INTERVAL '2' DAY | map('a',1) |
        | ival_d * struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt * bool | INTERVAL '25' HOUR | true |
        | ival_dt * bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt * date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt * ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt * ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt * ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt * ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt * ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt * ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt * ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt * ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt * calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt * array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt * map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt * struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds * bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds * bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds * date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds * ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds * ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds * ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds * ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds * ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds * ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds * ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds * ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds * calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds * array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds * map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds * struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m * bool | INTERVAL '2' MONTH | true |
        | ival_m * bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m * date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m * ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m * ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m * ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m * ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m * ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m * ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m * ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m * ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m * array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m * map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m * struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y * bool | INTERVAL '2' YEAR | true |
        | ival_y * bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y * date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y * ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y * ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y * ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y * ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y * ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y * ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y * ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y * ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y * calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y * array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y * map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y * struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym * bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym * bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym * date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym * ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym * ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym * ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym * ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym * ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym * ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym * ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym * ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym * calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym * array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym * map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym * struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar * bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar * bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar * date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar * ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar * ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar * ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar * ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar * ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar * ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar * ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar * ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar * calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar * array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar * map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar * struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array * unull | array(1,2) | NULL |
        | array * null | array(1,2) | CAST(NULL AS INT) |
        | array * bool | array(1,2) | true |
        | array * tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array * smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array * int | array(1,2) | CAST(2 AS INT) |
        | array * bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array * float | array(1,2) | CAST(2 AS FLOAT) |
        | array * double | array(1,2) | CAST(2 AS DOUBLE) |
        | array * dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array * str | array(1,2) | '2' |
        | array * bin | array(1,2) | CAST('2' AS BINARY) |
        | array * date | array(1,2) | DATE'2024-01-15' |
        | array * ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array * ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array * ival_d | array(1,2) | INTERVAL '2' DAY |
        | array * ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array * ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array * ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array * ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array * ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array * calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array * array | array(1,2) | array(1,2) |
        | array * map | array(1,2) | map('a',1) |
        | array * struct | array(1,2) | named_struct('a',1) |
        | map * unull | map('a',1) | NULL |
        | map * null | map('a',1) | CAST(NULL AS INT) |
        | map * bool | map('a',1) | true |
        | map * tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map * smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map * int | map('a',1) | CAST(2 AS INT) |
        | map * bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map * float | map('a',1) | CAST(2 AS FLOAT) |
        | map * double | map('a',1) | CAST(2 AS DOUBLE) |
        | map * dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map * str | map('a',1) | '2' |
        | map * bin | map('a',1) | CAST('2' AS BINARY) |
        | map * date | map('a',1) | DATE'2024-01-15' |
        | map * ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map * ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map * ival_d | map('a',1) | INTERVAL '2' DAY |
        | map * ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map * ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map * ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map * ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map * ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map * calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map * array | map('a',1) | array(1,2) |
        | map * map | map('a',1) | map('a',1) |
        | map * struct | map('a',1) | named_struct('a',1) |
        | struct * unull | named_struct('a',1) | NULL |
        | struct * null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct * bool | named_struct('a',1) | true |
        | struct * tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct * smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct * int | named_struct('a',1) | CAST(2 AS INT) |
        | struct * bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct * float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct * double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct * dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct * str | named_struct('a',1) | '2' |
        | struct * bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct * date | named_struct('a',1) | DATE'2024-01-15' |
        | struct * ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct * ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct * ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct * ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct * ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct * ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct * ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct * ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct * calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct * array | named_struct('a',1) | array(1,2) |
        | struct * map | named_struct('a',1) | map('a',1) |
        | struct * struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: times ansi-off: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * variant | NULL | parse_json('{"a":1}') |
        | null * variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool * variant | true | parse_json('{"a":1}') |
        | tinyint * variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint * variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int * variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint * variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float * variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double * variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec * variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str * variant | '2' | parse_json('{"a":1}') |
        | bin * variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date * variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts * variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz * variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d * variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt * variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds * variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m * variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y * variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym * variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar * variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array * variant | array(1,2) | parse_json('{"a":1}') |
        | map * variant | map('a',1) | parse_json('{"a":1}') |
        | struct * variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant * unull | parse_json('{"a":1}') | NULL |
        | variant * null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant * bool | parse_json('{"a":1}') | true |
        | variant * tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant * smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant * int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant * bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant * float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant * double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant * dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant * str | parse_json('{"a":1}') | '2' |
        | variant * bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant * date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant * ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant * ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant * ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant * ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant * ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant * ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant * ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant * ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant * calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant * array | parse_json('{"a":1}') | array(1,2) |
        | variant * map | parse_json('{"a":1}') | map('a',1) |
        | variant * struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant * variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: times ansi-off: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * time | NULL | TIME '12:00:00' |
        | null * time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool * time | true | TIME '12:00:00' |
        | tinyint * time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint * time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int * time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint * time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float * time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double * time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec * time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str * time | '2' | TIME '12:00:00' |
        | bin * time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date * time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts * time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz * time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time * unull | TIME '12:00:00' | NULL |
        | time * null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time * bool | TIME '12:00:00' | true |
        | time * tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time * smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time * int | TIME '12:00:00' | CAST(2 AS INT) |
        | time * bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time * float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time * double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time * dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time * str | TIME '12:00:00' | '2' |
        | time * bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time * date | TIME '12:00:00' | DATE'2024-01-15' |
        | time * ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time * ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time * time | TIME '12:00:00' | TIME '12:00:00' |
        | time * ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time * ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time * ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time * ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time * ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time * ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time * calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time * array | TIME '12:00:00' | array(1,2) |
        | time * map | TIME '12:00:00' | map('a',1) |
        | time * struct | TIME '12:00:00' | named_struct('a',1) |
        | time * variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d * time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt * time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds * time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m * time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y * time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym * time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar * time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array * time | array(1,2) | TIME '12:00:00' |
        | map * time | map('a',1) | TIME '12:00:00' |
        | struct * time | named_struct('a',1) | TIME '12:00:00' |
        | variant * time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: times ansi-off: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null * geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool * geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint * geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint * geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int * geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint * geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float * geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double * geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec * geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str * geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin * geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date * geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts * geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz * geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time * geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d * geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt * geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds * geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m * geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y * geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym * geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar * geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array * geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map * geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct * geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant * geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom * unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom * null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom * bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom * tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom * smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom * int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom * bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom * float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom * double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom * dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom * str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom * bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom * date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom * ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom * ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom * time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom * ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom * ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom * ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom * ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom * ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom * ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom * calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom * array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom * map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom * struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom * variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom * geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `*` operand-type rejection (ANSI on)

    Scenario Outline: times ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * bool | NULL | true |
        | unull * str | NULL | '2' |
        | unull * bin | NULL | CAST('2' AS BINARY) |
        | unull * date | NULL | DATE'2024-01-15' |
        | unull * ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull * ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull * array | NULL | array(1,2) |
        | unull * map | NULL | map('a',1) |
        | unull * struct | NULL | named_struct('a',1) |
        | null * bool | CAST(NULL AS INT) | true |
        | null * bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null * date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null * ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null * ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null * array | CAST(NULL AS INT) | array(1,2) |
        | null * map | CAST(NULL AS INT) | map('a',1) |
        | null * struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool * unull | true | NULL |
        | bool * null | true | CAST(NULL AS INT) |
        | bool * bool | true | true |
        | bool * tinyint | true | CAST(2 AS TINYINT) |
        | bool * smallint | true | CAST(2 AS SMALLINT) |
        | bool * int | true | CAST(2 AS INT) |
        | bool * bigint | true | CAST(2 AS BIGINT) |
        | bool * float | true | CAST(2 AS FLOAT) |
        | bool * double | true | CAST(2 AS DOUBLE) |
        | bool * dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool * str | true | '2' |
        | bool * bin | true | CAST('2' AS BINARY) |
        | bool * date | true | DATE'2024-01-15' |
        | bool * ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool * ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool * ival_d | true | INTERVAL '2' DAY |
        | bool * ival_dt | true | INTERVAL '25' HOUR |
        | bool * ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool * ival_m | true | INTERVAL '2' MONTH |
        | bool * ival_y | true | INTERVAL '2' YEAR |
        | bool * ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool * calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool * array | true | array(1,2) |
        | bool * map | true | map('a',1) |
        | bool * struct | true | named_struct('a',1) |
        | tinyint * bool | CAST(2 AS TINYINT) | true |
        | tinyint * bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint * date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint * ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint * ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint * array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint * map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint * struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint * bool | CAST(2 AS SMALLINT) | true |
        | smallint * bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint * date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint * ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint * ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint * array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint * map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint * struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int * bool | CAST(2 AS INT) | true |
        | int * bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int * date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int * ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int * ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int * array | CAST(2 AS INT) | array(1,2) |
        | int * map | CAST(2 AS INT) | map('a',1) |
        | int * struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint * bool | CAST(2 AS BIGINT) | true |
        | bigint * bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint * date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint * ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint * ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint * array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint * map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint * struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float * bool | CAST(2 AS FLOAT) | true |
        | float * bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float * date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float * ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float * ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float * array | CAST(2 AS FLOAT) | array(1,2) |
        | float * map | CAST(2 AS FLOAT) | map('a',1) |
        | float * struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double * bool | CAST(2 AS DOUBLE) | true |
        | double * bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double * date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double * ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double * ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double * array | CAST(2 AS DOUBLE) | array(1,2) |
        | double * map | CAST(2 AS DOUBLE) | map('a',1) |
        | double * struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec * bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec * bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec * date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec * ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec * ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec * array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec * map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec * struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str * unull | '2' | NULL |
        | str * bool | '2' | true |
        | str * str | '2' | '2' |
        | str * bin | '2' | CAST('2' AS BINARY) |
        | str * date | '2' | DATE'2024-01-15' |
        | str * ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str * ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str * array | '2' | array(1,2) |
        | str * map | '2' | map('a',1) |
        | str * struct | '2' | named_struct('a',1) |
        | bin * unull | CAST('2' AS BINARY) | NULL |
        | bin * null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin * bool | CAST('2' AS BINARY) | true |
        | bin * tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin * smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin * int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin * bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin * float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin * double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin * dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin * str | CAST('2' AS BINARY) | '2' |
        | bin * bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin * date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin * ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin * ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin * ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin * ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin * ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin * ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin * ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin * ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin * calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin * array | CAST('2' AS BINARY) | array(1,2) |
        | bin * map | CAST('2' AS BINARY) | map('a',1) |
        | bin * struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date * unull | DATE'2024-01-15' | NULL |
        | date * null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date * bool | DATE'2024-01-15' | true |
        | date * tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date * smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date * int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date * bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date * float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date * double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date * dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date * str | DATE'2024-01-15' | '2' |
        | date * bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date * date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date * ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date * ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date * ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date * ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date * ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date * ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date * ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date * ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date * calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date * array | DATE'2024-01-15' | array(1,2) |
        | date * map | DATE'2024-01-15' | map('a',1) |
        | date * struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts * unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts * null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts * bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts * tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts * smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts * int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts * bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts * float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts * double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts * dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts * str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts * bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts * date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts * ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts * ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts * ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts * ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts * ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts * ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts * ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts * ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts * calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts * array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts * map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts * struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz * unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz * null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz * bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz * tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz * smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz * int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz * bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz * float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz * double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz * dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz * str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz * bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz * date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz * ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz * ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz * ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz * ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz * ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz * ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz * ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz * ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz * calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz * array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz * map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz * struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d * bool | INTERVAL '2' DAY | true |
        | ival_d * bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d * date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d * ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d * ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d * ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d * ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d * ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d * ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d * ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d * ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d * calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d * array | INTERVAL '2' DAY | array(1,2) |
        | ival_d * map | INTERVAL '2' DAY | map('a',1) |
        | ival_d * struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt * bool | INTERVAL '25' HOUR | true |
        | ival_dt * bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt * date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt * ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt * ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt * ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt * ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt * ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt * ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt * ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt * ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt * calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt * array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt * map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt * struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds * bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds * bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds * date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds * ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds * ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds * ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds * ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds * ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds * ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds * ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds * ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds * calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds * array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds * map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds * struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m * bool | INTERVAL '2' MONTH | true |
        | ival_m * bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m * date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m * ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m * ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m * ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m * ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m * ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m * ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m * ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m * ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m * calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m * array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m * map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m * struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y * bool | INTERVAL '2' YEAR | true |
        | ival_y * bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y * date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y * ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y * ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y * ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y * ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y * ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y * ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y * ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y * ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y * calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y * array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y * map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y * struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym * bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym * bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym * date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym * ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym * ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym * ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym * ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym * ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym * ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym * ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym * ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym * calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym * array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym * map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym * struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar * bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar * bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar * date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar * ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar * ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar * ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar * ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar * ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar * ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar * ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar * ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar * calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar * array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar * map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar * struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array * unull | array(1,2) | NULL |
        | array * null | array(1,2) | CAST(NULL AS INT) |
        | array * bool | array(1,2) | true |
        | array * tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array * smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array * int | array(1,2) | CAST(2 AS INT) |
        | array * bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array * float | array(1,2) | CAST(2 AS FLOAT) |
        | array * double | array(1,2) | CAST(2 AS DOUBLE) |
        | array * dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array * str | array(1,2) | '2' |
        | array * bin | array(1,2) | CAST('2' AS BINARY) |
        | array * date | array(1,2) | DATE'2024-01-15' |
        | array * ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array * ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array * ival_d | array(1,2) | INTERVAL '2' DAY |
        | array * ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array * ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array * ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array * ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array * ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array * calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array * array | array(1,2) | array(1,2) |
        | array * map | array(1,2) | map('a',1) |
        | array * struct | array(1,2) | named_struct('a',1) |
        | map * unull | map('a',1) | NULL |
        | map * null | map('a',1) | CAST(NULL AS INT) |
        | map * bool | map('a',1) | true |
        | map * tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map * smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map * int | map('a',1) | CAST(2 AS INT) |
        | map * bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map * float | map('a',1) | CAST(2 AS FLOAT) |
        | map * double | map('a',1) | CAST(2 AS DOUBLE) |
        | map * dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map * str | map('a',1) | '2' |
        | map * bin | map('a',1) | CAST('2' AS BINARY) |
        | map * date | map('a',1) | DATE'2024-01-15' |
        | map * ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map * ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map * ival_d | map('a',1) | INTERVAL '2' DAY |
        | map * ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map * ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map * ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map * ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map * ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map * calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map * array | map('a',1) | array(1,2) |
        | map * map | map('a',1) | map('a',1) |
        | map * struct | map('a',1) | named_struct('a',1) |
        | struct * unull | named_struct('a',1) | NULL |
        | struct * null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct * bool | named_struct('a',1) | true |
        | struct * tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct * smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct * int | named_struct('a',1) | CAST(2 AS INT) |
        | struct * bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct * float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct * double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct * dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct * str | named_struct('a',1) | '2' |
        | struct * bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct * date | named_struct('a',1) | DATE'2024-01-15' |
        | struct * ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct * ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct * ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct * ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct * ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct * ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct * ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct * ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct * calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct * array | named_struct('a',1) | array(1,2) |
        | struct * map | named_struct('a',1) | map('a',1) |
        | struct * struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: times ansi-on: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * variant | NULL | parse_json('{"a":1}') |
        | null * variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool * variant | true | parse_json('{"a":1}') |
        | tinyint * variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint * variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int * variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint * variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float * variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double * variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec * variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str * variant | '2' | parse_json('{"a":1}') |
        | bin * variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date * variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts * variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz * variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d * variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt * variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds * variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m * variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y * variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym * variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar * variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array * variant | array(1,2) | parse_json('{"a":1}') |
        | map * variant | map('a',1) | parse_json('{"a":1}') |
        | struct * variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant * unull | parse_json('{"a":1}') | NULL |
        | variant * null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant * bool | parse_json('{"a":1}') | true |
        | variant * tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant * smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant * int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant * bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant * float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant * double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant * dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant * str | parse_json('{"a":1}') | '2' |
        | variant * bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant * date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant * ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant * ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant * ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant * ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant * ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant * ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant * ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant * ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant * calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant * array | parse_json('{"a":1}') | array(1,2) |
        | variant * map | parse_json('{"a":1}') | map('a',1) |
        | variant * struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant * variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: times ansi-on: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * time | NULL | TIME '12:00:00' |
        | null * time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool * time | true | TIME '12:00:00' |
        | tinyint * time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint * time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int * time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint * time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float * time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double * time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec * time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str * time | '2' | TIME '12:00:00' |
        | bin * time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date * time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts * time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz * time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time * unull | TIME '12:00:00' | NULL |
        | time * null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time * bool | TIME '12:00:00' | true |
        | time * tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time * smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time * int | TIME '12:00:00' | CAST(2 AS INT) |
        | time * bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time * float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time * double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time * dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time * str | TIME '12:00:00' | '2' |
        | time * bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time * date | TIME '12:00:00' | DATE'2024-01-15' |
        | time * ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time * ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time * time | TIME '12:00:00' | TIME '12:00:00' |
        | time * ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time * ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time * ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time * ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time * ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time * ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time * calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time * array | TIME '12:00:00' | array(1,2) |
        | time * map | TIME '12:00:00' | map('a',1) |
        | time * struct | TIME '12:00:00' | named_struct('a',1) |
        | time * variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d * time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt * time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds * time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m * time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y * time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym * time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar * time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array * time | array(1,2) | TIME '12:00:00' |
        | map * time | map('a',1) | TIME '12:00:00' |
        | struct * time | named_struct('a',1) | TIME '12:00:00' |
        | variant * time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: times ansi-on: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) * (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull * geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null * geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool * geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint * geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint * geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int * geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint * geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float * geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double * geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec * geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str * geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin * geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date * geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts * geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz * geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time * geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d * geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt * geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds * geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m * geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y * geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym * geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar * geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array * geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map * geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct * geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant * geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom * unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom * null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom * bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom * tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom * smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom * int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom * bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom * float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom * double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom * dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom * str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom * bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom * date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom * ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom * ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom * time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom * ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom * ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom * ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom * ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom * ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom * ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom * calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom * array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom * map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom * struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom * variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom * geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `/` operand-type rejection (ANSI off)

    Scenario Outline: divide ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / bool | NULL | true |
        | unull / bin | NULL | CAST('2' AS BINARY) |
        | unull / date | NULL | DATE'2024-01-15' |
        | unull / ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull / ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull / ival_d | NULL | INTERVAL '2' DAY |
        | unull / ival_dt | NULL | INTERVAL '25' HOUR |
        | unull / ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull / ival_m | NULL | INTERVAL '2' MONTH |
        | unull / ival_y | NULL | INTERVAL '2' YEAR |
        | unull / ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull / calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | unull / array | NULL | array(1,2) |
        | unull / map | NULL | map('a',1) |
        | unull / struct | NULL | named_struct('a',1) |
        | null / bool | CAST(NULL AS INT) | true |
        | null / bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null / date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null / ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null / ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null / ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null / ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null / ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null / ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null / ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null / ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null / calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null / array | CAST(NULL AS INT) | array(1,2) |
        | null / map | CAST(NULL AS INT) | map('a',1) |
        | null / struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool / unull | true | NULL |
        | bool / null | true | CAST(NULL AS INT) |
        | bool / bool | true | true |
        | bool / tinyint | true | CAST(2 AS TINYINT) |
        | bool / smallint | true | CAST(2 AS SMALLINT) |
        | bool / int | true | CAST(2 AS INT) |
        | bool / bigint | true | CAST(2 AS BIGINT) |
        | bool / float | true | CAST(2 AS FLOAT) |
        | bool / double | true | CAST(2 AS DOUBLE) |
        | bool / dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool / str | true | '2' |
        | bool / bin | true | CAST('2' AS BINARY) |
        | bool / date | true | DATE'2024-01-15' |
        | bool / ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool / ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool / ival_d | true | INTERVAL '2' DAY |
        | bool / ival_dt | true | INTERVAL '25' HOUR |
        | bool / ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool / ival_m | true | INTERVAL '2' MONTH |
        | bool / ival_y | true | INTERVAL '2' YEAR |
        | bool / ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool / calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool / array | true | array(1,2) |
        | bool / map | true | map('a',1) |
        | bool / struct | true | named_struct('a',1) |
        | tinyint / bool | CAST(2 AS TINYINT) | true |
        | tinyint / bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint / date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint / ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint / ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint / ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint / ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint / ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint / ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint / ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint / ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint / calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint / array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint / map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint / struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint / bool | CAST(2 AS SMALLINT) | true |
        | smallint / bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint / date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint / ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint / ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint / ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint / ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint / ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint / ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint / ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint / ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint / calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint / array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint / map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint / struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int / bool | CAST(2 AS INT) | true |
        | int / bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int / date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int / ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int / ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int / ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int / ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int / ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int / ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int / ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int / ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int / calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int / array | CAST(2 AS INT) | array(1,2) |
        | int / map | CAST(2 AS INT) | map('a',1) |
        | int / struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint / bool | CAST(2 AS BIGINT) | true |
        | bigint / bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint / date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint / ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint / ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint / ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint / ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint / ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint / ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint / ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint / ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint / calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint / array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint / map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint / struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float / bool | CAST(2 AS FLOAT) | true |
        | float / bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float / date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float / ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float / ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float / ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float / ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float / ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float / ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float / ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float / ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float / calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float / array | CAST(2 AS FLOAT) | array(1,2) |
        | float / map | CAST(2 AS FLOAT) | map('a',1) |
        | float / struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double / bool | CAST(2 AS DOUBLE) | true |
        | double / bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double / date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double / ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double / ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double / ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double / ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double / ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double / ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double / ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double / ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double / calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double / array | CAST(2 AS DOUBLE) | array(1,2) |
        | double / map | CAST(2 AS DOUBLE) | map('a',1) |
        | double / struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec / bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec / bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec / date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec / ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec / ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec / ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec / ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec / ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec / ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec / ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec / ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec / calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec / array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec / map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec / struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str / bool | '2' | true |
        | str / bin | '2' | CAST('2' AS BINARY) |
        | str / date | '2' | DATE'2024-01-15' |
        | str / ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str / ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str / ival_d | '2' | INTERVAL '2' DAY |
        | str / ival_dt | '2' | INTERVAL '25' HOUR |
        | str / ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str / ival_m | '2' | INTERVAL '2' MONTH |
        | str / ival_y | '2' | INTERVAL '2' YEAR |
        | str / ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str / calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | str / array | '2' | array(1,2) |
        | str / map | '2' | map('a',1) |
        | str / struct | '2' | named_struct('a',1) |
        | bin / unull | CAST('2' AS BINARY) | NULL |
        | bin / null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin / bool | CAST('2' AS BINARY) | true |
        | bin / tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin / smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin / int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin / bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin / float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin / double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin / dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin / str | CAST('2' AS BINARY) | '2' |
        | bin / bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin / date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin / ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin / ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin / ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin / ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin / ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin / ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin / ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin / ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin / calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin / array | CAST('2' AS BINARY) | array(1,2) |
        | bin / map | CAST('2' AS BINARY) | map('a',1) |
        | bin / struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date / unull | DATE'2024-01-15' | NULL |
        | date / null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date / bool | DATE'2024-01-15' | true |
        | date / tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date / smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date / int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date / bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date / float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date / double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date / dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date / str | DATE'2024-01-15' | '2' |
        | date / bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date / date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date / ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date / ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date / ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date / ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date / ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date / ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date / ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date / ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date / calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date / array | DATE'2024-01-15' | array(1,2) |
        | date / map | DATE'2024-01-15' | map('a',1) |
        | date / struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts / unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts / null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts / bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts / tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts / smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts / int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts / bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts / float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts / double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts / dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts / str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts / bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts / date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts / ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts / ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts / ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts / ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts / ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts / ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts / ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts / ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts / calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts / array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts / map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts / struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz / unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz / null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz / bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz / tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz / smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz / int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz / bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz / float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz / double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz / dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz / str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz / bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz / date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz / ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz / ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz / ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz / ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz / ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz / ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz / ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz / ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz / calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz / array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz / map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz / struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d / bool | INTERVAL '2' DAY | true |
        | ival_d / bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d / date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d / ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d / ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d / ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d / ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d / ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d / ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d / ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d / ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d / calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d / array | INTERVAL '2' DAY | array(1,2) |
        | ival_d / map | INTERVAL '2' DAY | map('a',1) |
        | ival_d / struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt / bool | INTERVAL '25' HOUR | true |
        | ival_dt / bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt / date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt / ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt / ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt / ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt / ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt / ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt / ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt / ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt / ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt / calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt / array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt / map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt / struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds / bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds / bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds / date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds / ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds / ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds / ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds / ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds / ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds / ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds / ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds / ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds / calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds / array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds / map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds / struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m / bool | INTERVAL '2' MONTH | true |
        | ival_m / bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m / date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m / ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m / ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m / ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m / ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m / ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m / ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m / ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m / ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m / calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m / array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m / map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m / struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y / bool | INTERVAL '2' YEAR | true |
        | ival_y / bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y / date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y / ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y / ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y / ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y / ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y / ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y / ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y / ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y / ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y / calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y / array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y / map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y / struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym / bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym / bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym / date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym / ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym / ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym / ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym / ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym / ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym / ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym / ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym / ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym / calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym / array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym / map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym / struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar / bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar / bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar / date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar / ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar / ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar / ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar / ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar / ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar / ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar / ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar / ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar / calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar / array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar / map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar / struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array / unull | array(1,2) | NULL |
        | array / null | array(1,2) | CAST(NULL AS INT) |
        | array / bool | array(1,2) | true |
        | array / tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array / smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array / int | array(1,2) | CAST(2 AS INT) |
        | array / bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array / float | array(1,2) | CAST(2 AS FLOAT) |
        | array / double | array(1,2) | CAST(2 AS DOUBLE) |
        | array / dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array / str | array(1,2) | '2' |
        | array / bin | array(1,2) | CAST('2' AS BINARY) |
        | array / date | array(1,2) | DATE'2024-01-15' |
        | array / ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array / ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array / ival_d | array(1,2) | INTERVAL '2' DAY |
        | array / ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array / ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array / ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array / ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array / ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array / calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array / array | array(1,2) | array(1,2) |
        | array / map | array(1,2) | map('a',1) |
        | array / struct | array(1,2) | named_struct('a',1) |
        | map / unull | map('a',1) | NULL |
        | map / null | map('a',1) | CAST(NULL AS INT) |
        | map / bool | map('a',1) | true |
        | map / tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map / smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map / int | map('a',1) | CAST(2 AS INT) |
        | map / bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map / float | map('a',1) | CAST(2 AS FLOAT) |
        | map / double | map('a',1) | CAST(2 AS DOUBLE) |
        | map / dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map / str | map('a',1) | '2' |
        | map / bin | map('a',1) | CAST('2' AS BINARY) |
        | map / date | map('a',1) | DATE'2024-01-15' |
        | map / ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map / ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map / ival_d | map('a',1) | INTERVAL '2' DAY |
        | map / ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map / ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map / ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map / ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map / ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map / calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map / array | map('a',1) | array(1,2) |
        | map / map | map('a',1) | map('a',1) |
        | map / struct | map('a',1) | named_struct('a',1) |
        | struct / unull | named_struct('a',1) | NULL |
        | struct / null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct / bool | named_struct('a',1) | true |
        | struct / tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct / smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct / int | named_struct('a',1) | CAST(2 AS INT) |
        | struct / bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct / float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct / double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct / dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct / str | named_struct('a',1) | '2' |
        | struct / bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct / date | named_struct('a',1) | DATE'2024-01-15' |
        | struct / ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct / ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct / ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct / ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct / ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct / ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct / ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct / ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct / calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct / array | named_struct('a',1) | array(1,2) |
        | struct / map | named_struct('a',1) | map('a',1) |
        | struct / struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: divide ansi-off: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / variant | NULL | parse_json('{"a":1}') |
        | null / variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool / variant | true | parse_json('{"a":1}') |
        | tinyint / variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint / variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int / variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint / variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float / variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double / variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec / variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str / variant | '2' | parse_json('{"a":1}') |
        | bin / variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date / variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts / variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz / variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d / variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt / variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds / variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m / variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y / variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym / variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar / variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array / variant | array(1,2) | parse_json('{"a":1}') |
        | map / variant | map('a',1) | parse_json('{"a":1}') |
        | struct / variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant / unull | parse_json('{"a":1}') | NULL |
        | variant / null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant / bool | parse_json('{"a":1}') | true |
        | variant / tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant / smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant / int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant / bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant / float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant / double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant / dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant / str | parse_json('{"a":1}') | '2' |
        | variant / bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant / date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant / ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant / ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant / ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant / ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant / ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant / ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant / ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant / ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant / calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant / array | parse_json('{"a":1}') | array(1,2) |
        | variant / map | parse_json('{"a":1}') | map('a',1) |
        | variant / struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant / variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: divide ansi-off: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / time | NULL | TIME '12:00:00' |
        | null / time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool / time | true | TIME '12:00:00' |
        | tinyint / time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint / time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int / time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint / time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float / time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double / time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec / time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str / time | '2' | TIME '12:00:00' |
        | bin / time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date / time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts / time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz / time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time / unull | TIME '12:00:00' | NULL |
        | time / null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time / bool | TIME '12:00:00' | true |
        | time / tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time / smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time / int | TIME '12:00:00' | CAST(2 AS INT) |
        | time / bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time / float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time / double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time / dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time / str | TIME '12:00:00' | '2' |
        | time / bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time / date | TIME '12:00:00' | DATE'2024-01-15' |
        | time / ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time / ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time / time | TIME '12:00:00' | TIME '12:00:00' |
        | time / ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time / ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time / ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time / ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time / ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time / ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time / calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time / array | TIME '12:00:00' | array(1,2) |
        | time / map | TIME '12:00:00' | map('a',1) |
        | time / struct | TIME '12:00:00' | named_struct('a',1) |
        | time / variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d / time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt / time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds / time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m / time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y / time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym / time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar / time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array / time | array(1,2) | TIME '12:00:00' |
        | map / time | map('a',1) | TIME '12:00:00' |
        | struct / time | named_struct('a',1) | TIME '12:00:00' |
        | variant / time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: divide ansi-off: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null / geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool / geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint / geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint / geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int / geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint / geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float / geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double / geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec / geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str / geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin / geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date / geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts / geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz / geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time / geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d / geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt / geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds / geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m / geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y / geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym / geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar / geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array / geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map / geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct / geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant / geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom / unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom / null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom / bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom / tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom / smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom / int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom / bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom / float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom / double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom / dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom / str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom / bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom / date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom / ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom / ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom / time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom / ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom / ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom / ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom / ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom / ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom / ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom / calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom / array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom / map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom / struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom / variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom / geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `/` operand-type rejection (ANSI on)

    Scenario Outline: divide ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / bool | NULL | true |
        | unull / str | NULL | '2' |
        | unull / bin | NULL | CAST('2' AS BINARY) |
        | unull / date | NULL | DATE'2024-01-15' |
        | unull / ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull / ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull / ival_d | NULL | INTERVAL '2' DAY |
        | unull / ival_dt | NULL | INTERVAL '25' HOUR |
        | unull / ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull / ival_m | NULL | INTERVAL '2' MONTH |
        | unull / ival_y | NULL | INTERVAL '2' YEAR |
        | unull / ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull / calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | unull / array | NULL | array(1,2) |
        | unull / map | NULL | map('a',1) |
        | unull / struct | NULL | named_struct('a',1) |
        | null / bool | CAST(NULL AS INT) | true |
        | null / bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null / date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null / ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null / ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null / ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null / ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null / ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null / ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null / ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null / ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null / calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null / array | CAST(NULL AS INT) | array(1,2) |
        | null / map | CAST(NULL AS INT) | map('a',1) |
        | null / struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool / unull | true | NULL |
        | bool / null | true | CAST(NULL AS INT) |
        | bool / bool | true | true |
        | bool / tinyint | true | CAST(2 AS TINYINT) |
        | bool / smallint | true | CAST(2 AS SMALLINT) |
        | bool / int | true | CAST(2 AS INT) |
        | bool / bigint | true | CAST(2 AS BIGINT) |
        | bool / float | true | CAST(2 AS FLOAT) |
        | bool / double | true | CAST(2 AS DOUBLE) |
        | bool / dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool / str | true | '2' |
        | bool / bin | true | CAST('2' AS BINARY) |
        | bool / date | true | DATE'2024-01-15' |
        | bool / ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool / ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool / ival_d | true | INTERVAL '2' DAY |
        | bool / ival_dt | true | INTERVAL '25' HOUR |
        | bool / ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool / ival_m | true | INTERVAL '2' MONTH |
        | bool / ival_y | true | INTERVAL '2' YEAR |
        | bool / ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool / calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool / array | true | array(1,2) |
        | bool / map | true | map('a',1) |
        | bool / struct | true | named_struct('a',1) |
        | tinyint / bool | CAST(2 AS TINYINT) | true |
        | tinyint / bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint / date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint / ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint / ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint / ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint / ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint / ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint / ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint / ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint / ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint / calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint / array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint / map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint / struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint / bool | CAST(2 AS SMALLINT) | true |
        | smallint / bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint / date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint / ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint / ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint / ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint / ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint / ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint / ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint / ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint / ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint / calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint / array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint / map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint / struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int / bool | CAST(2 AS INT) | true |
        | int / bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int / date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int / ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int / ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int / ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int / ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int / ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int / ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int / ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int / ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int / calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int / array | CAST(2 AS INT) | array(1,2) |
        | int / map | CAST(2 AS INT) | map('a',1) |
        | int / struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint / bool | CAST(2 AS BIGINT) | true |
        | bigint / bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint / date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint / ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint / ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint / ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint / ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint / ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint / ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint / ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint / ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint / calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint / array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint / map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint / struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float / bool | CAST(2 AS FLOAT) | true |
        | float / bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float / date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float / ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float / ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float / ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float / ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float / ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float / ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float / ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float / ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float / calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float / array | CAST(2 AS FLOAT) | array(1,2) |
        | float / map | CAST(2 AS FLOAT) | map('a',1) |
        | float / struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double / bool | CAST(2 AS DOUBLE) | true |
        | double / bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double / date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double / ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double / ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double / ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double / ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double / ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double / ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double / ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double / ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double / calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double / array | CAST(2 AS DOUBLE) | array(1,2) |
        | double / map | CAST(2 AS DOUBLE) | map('a',1) |
        | double / struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec / bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec / bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec / date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec / ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec / ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec / ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec / ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec / ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec / ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec / ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec / ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec / calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec / array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec / map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec / struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str / unull | '2' | NULL |
        | str / bool | '2' | true |
        | str / str | '2' | '2' |
        | str / bin | '2' | CAST('2' AS BINARY) |
        | str / date | '2' | DATE'2024-01-15' |
        | str / ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str / ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str / ival_d | '2' | INTERVAL '2' DAY |
        | str / ival_dt | '2' | INTERVAL '25' HOUR |
        | str / ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str / ival_m | '2' | INTERVAL '2' MONTH |
        | str / ival_y | '2' | INTERVAL '2' YEAR |
        | str / ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str / calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | str / array | '2' | array(1,2) |
        | str / map | '2' | map('a',1) |
        | str / struct | '2' | named_struct('a',1) |
        | bin / unull | CAST('2' AS BINARY) | NULL |
        | bin / null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin / bool | CAST('2' AS BINARY) | true |
        | bin / tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin / smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin / int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin / bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin / float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin / double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin / dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin / str | CAST('2' AS BINARY) | '2' |
        | bin / bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin / date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin / ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin / ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin / ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin / ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin / ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin / ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin / ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin / ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin / calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin / array | CAST('2' AS BINARY) | array(1,2) |
        | bin / map | CAST('2' AS BINARY) | map('a',1) |
        | bin / struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date / unull | DATE'2024-01-15' | NULL |
        | date / null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date / bool | DATE'2024-01-15' | true |
        | date / tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date / smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date / int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date / bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date / float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date / double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date / dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date / str | DATE'2024-01-15' | '2' |
        | date / bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date / date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date / ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date / ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date / ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date / ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date / ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date / ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date / ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date / ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date / calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date / array | DATE'2024-01-15' | array(1,2) |
        | date / map | DATE'2024-01-15' | map('a',1) |
        | date / struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts / unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts / null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts / bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts / tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts / smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts / int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts / bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts / float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts / double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts / dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts / str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts / bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts / date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts / ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts / ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts / ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts / ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts / ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts / ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts / ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts / ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts / calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts / array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts / map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts / struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz / unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz / null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz / bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz / tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz / smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz / int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz / bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz / float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz / double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz / dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz / str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz / bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz / date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz / ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz / ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz / ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz / ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz / ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz / ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz / ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz / ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz / calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz / array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz / map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz / struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d / bool | INTERVAL '2' DAY | true |
        | ival_d / bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d / date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d / ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d / ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d / ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d / ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d / ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d / ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d / ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d / ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d / calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d / array | INTERVAL '2' DAY | array(1,2) |
        | ival_d / map | INTERVAL '2' DAY | map('a',1) |
        | ival_d / struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt / bool | INTERVAL '25' HOUR | true |
        | ival_dt / bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt / date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt / ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt / ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt / ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt / ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt / ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt / ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt / ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt / ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt / calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt / array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt / map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt / struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds / bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds / bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds / date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds / ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds / ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds / ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds / ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds / ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds / ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds / ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds / ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds / calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds / array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds / map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds / struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m / bool | INTERVAL '2' MONTH | true |
        | ival_m / bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m / date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m / ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m / ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m / ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m / ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m / ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m / ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m / ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m / ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m / calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m / array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m / map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m / struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y / bool | INTERVAL '2' YEAR | true |
        | ival_y / bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y / date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y / ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y / ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y / ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y / ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y / ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y / ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y / ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y / ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y / calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y / array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y / map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y / struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym / bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym / bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym / date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym / ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym / ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym / ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym / ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym / ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym / ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym / ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym / ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym / calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym / array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym / map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym / struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar / bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar / bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar / date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar / ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar / ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar / ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar / ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar / ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar / ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar / ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar / ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar / calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar / array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar / map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar / struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array / unull | array(1,2) | NULL |
        | array / null | array(1,2) | CAST(NULL AS INT) |
        | array / bool | array(1,2) | true |
        | array / tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array / smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array / int | array(1,2) | CAST(2 AS INT) |
        | array / bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array / float | array(1,2) | CAST(2 AS FLOAT) |
        | array / double | array(1,2) | CAST(2 AS DOUBLE) |
        | array / dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array / str | array(1,2) | '2' |
        | array / bin | array(1,2) | CAST('2' AS BINARY) |
        | array / date | array(1,2) | DATE'2024-01-15' |
        | array / ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array / ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array / ival_d | array(1,2) | INTERVAL '2' DAY |
        | array / ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array / ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array / ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array / ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array / ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array / calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array / array | array(1,2) | array(1,2) |
        | array / map | array(1,2) | map('a',1) |
        | array / struct | array(1,2) | named_struct('a',1) |
        | map / unull | map('a',1) | NULL |
        | map / null | map('a',1) | CAST(NULL AS INT) |
        | map / bool | map('a',1) | true |
        | map / tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map / smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map / int | map('a',1) | CAST(2 AS INT) |
        | map / bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map / float | map('a',1) | CAST(2 AS FLOAT) |
        | map / double | map('a',1) | CAST(2 AS DOUBLE) |
        | map / dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map / str | map('a',1) | '2' |
        | map / bin | map('a',1) | CAST('2' AS BINARY) |
        | map / date | map('a',1) | DATE'2024-01-15' |
        | map / ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map / ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map / ival_d | map('a',1) | INTERVAL '2' DAY |
        | map / ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map / ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map / ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map / ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map / ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map / calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map / array | map('a',1) | array(1,2) |
        | map / map | map('a',1) | map('a',1) |
        | map / struct | map('a',1) | named_struct('a',1) |
        | struct / unull | named_struct('a',1) | NULL |
        | struct / null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct / bool | named_struct('a',1) | true |
        | struct / tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct / smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct / int | named_struct('a',1) | CAST(2 AS INT) |
        | struct / bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct / float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct / double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct / dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct / str | named_struct('a',1) | '2' |
        | struct / bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct / date | named_struct('a',1) | DATE'2024-01-15' |
        | struct / ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct / ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct / ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct / ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct / ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct / ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct / ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct / ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct / calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct / array | named_struct('a',1) | array(1,2) |
        | struct / map | named_struct('a',1) | map('a',1) |
        | struct / struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: divide ansi-on: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / variant | NULL | parse_json('{"a":1}') |
        | null / variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool / variant | true | parse_json('{"a":1}') |
        | tinyint / variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint / variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int / variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint / variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float / variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double / variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec / variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str / variant | '2' | parse_json('{"a":1}') |
        | bin / variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date / variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts / variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz / variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d / variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt / variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds / variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m / variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y / variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym / variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar / variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array / variant | array(1,2) | parse_json('{"a":1}') |
        | map / variant | map('a',1) | parse_json('{"a":1}') |
        | struct / variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant / unull | parse_json('{"a":1}') | NULL |
        | variant / null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant / bool | parse_json('{"a":1}') | true |
        | variant / tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant / smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant / int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant / bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant / float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant / double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant / dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant / str | parse_json('{"a":1}') | '2' |
        | variant / bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant / date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant / ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant / ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant / ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant / ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant / ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant / ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant / ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant / ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant / calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant / array | parse_json('{"a":1}') | array(1,2) |
        | variant / map | parse_json('{"a":1}') | map('a',1) |
        | variant / struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant / variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: divide ansi-on: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / time | NULL | TIME '12:00:00' |
        | null / time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool / time | true | TIME '12:00:00' |
        | tinyint / time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint / time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int / time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint / time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float / time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double / time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec / time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str / time | '2' | TIME '12:00:00' |
        | bin / time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date / time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts / time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz / time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time / unull | TIME '12:00:00' | NULL |
        | time / null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time / bool | TIME '12:00:00' | true |
        | time / tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time / smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time / int | TIME '12:00:00' | CAST(2 AS INT) |
        | time / bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time / float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time / double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time / dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time / str | TIME '12:00:00' | '2' |
        | time / bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time / date | TIME '12:00:00' | DATE'2024-01-15' |
        | time / ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time / ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time / time | TIME '12:00:00' | TIME '12:00:00' |
        | time / ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time / ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time / ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time / ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time / ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time / ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time / calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time / array | TIME '12:00:00' | array(1,2) |
        | time / map | TIME '12:00:00' | map('a',1) |
        | time / struct | TIME '12:00:00' | named_struct('a',1) |
        | time / variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d / time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt / time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds / time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m / time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y / time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym / time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar / time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array / time | array(1,2) | TIME '12:00:00' |
        | map / time | map('a',1) | TIME '12:00:00' |
        | struct / time | named_struct('a',1) | TIME '12:00:00' |
        | variant / time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: divide ansi-on: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) / (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull / geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null / geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool / geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint / geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint / geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int / geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint / geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float / geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double / geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec / geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str / geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin / geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date / geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts / geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz / geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time / geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d / geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt / geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds / geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m / geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y / geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym / geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar / geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array / geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map / geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct / geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant / geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom / unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom / null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom / bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom / tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom / smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom / int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom / bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom / float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom / double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom / dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom / str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom / bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom / date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom / ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom / ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom / time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom / ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom / ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom / ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom / ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom / ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom / ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom / calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom / array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom / map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom / struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom / variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom / geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `%` operand-type rejection (ANSI off)

    Scenario Outline: modulo ansi-off: rejected pair: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % bool | NULL | true |
        | unull % bin | NULL | CAST('2' AS BINARY) |
        | unull % date | NULL | DATE'2024-01-15' |
        | unull % ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull % ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull % ival_d | NULL | INTERVAL '2' DAY |
        | unull % ival_dt | NULL | INTERVAL '25' HOUR |
        | unull % ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull % ival_m | NULL | INTERVAL '2' MONTH |
        | unull % ival_y | NULL | INTERVAL '2' YEAR |
        | unull % ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull % calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | unull % array | NULL | array(1,2) |
        | unull % map | NULL | map('a',1) |
        | unull % struct | NULL | named_struct('a',1) |
        | null % bool | CAST(NULL AS INT) | true |
        | null % bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null % date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null % ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null % ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null % ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null % ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null % ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null % ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null % ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null % ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null % calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null % array | CAST(NULL AS INT) | array(1,2) |
        | null % map | CAST(NULL AS INT) | map('a',1) |
        | null % struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool % unull | true | NULL |
        | bool % null | true | CAST(NULL AS INT) |
        | bool % bool | true | true |
        | bool % tinyint | true | CAST(2 AS TINYINT) |
        | bool % smallint | true | CAST(2 AS SMALLINT) |
        | bool % int | true | CAST(2 AS INT) |
        | bool % bigint | true | CAST(2 AS BIGINT) |
        | bool % float | true | CAST(2 AS FLOAT) |
        | bool % double | true | CAST(2 AS DOUBLE) |
        | bool % dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool % str | true | '2' |
        | bool % bin | true | CAST('2' AS BINARY) |
        | bool % date | true | DATE'2024-01-15' |
        | bool % ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool % ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool % ival_d | true | INTERVAL '2' DAY |
        | bool % ival_dt | true | INTERVAL '25' HOUR |
        | bool % ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool % ival_m | true | INTERVAL '2' MONTH |
        | bool % ival_y | true | INTERVAL '2' YEAR |
        | bool % ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool % calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool % array | true | array(1,2) |
        | bool % map | true | map('a',1) |
        | bool % struct | true | named_struct('a',1) |
        | tinyint % bool | CAST(2 AS TINYINT) | true |
        | tinyint % bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint % date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint % ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint % ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint % ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint % ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint % ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint % ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint % ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint % ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint % calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint % array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint % map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint % struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint % bool | CAST(2 AS SMALLINT) | true |
        | smallint % bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint % date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint % ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint % ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint % ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint % ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint % ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint % ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint % ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint % ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint % calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint % array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint % map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint % struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int % bool | CAST(2 AS INT) | true |
        | int % bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int % date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int % ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int % ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int % ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int % ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int % ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int % ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int % ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int % ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int % calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int % array | CAST(2 AS INT) | array(1,2) |
        | int % map | CAST(2 AS INT) | map('a',1) |
        | int % struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint % bool | CAST(2 AS BIGINT) | true |
        | bigint % bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint % date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint % ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint % ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint % ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint % ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint % ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint % ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint % ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint % ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint % calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint % array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint % map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint % struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float % bool | CAST(2 AS FLOAT) | true |
        | float % bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float % date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float % ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float % ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float % ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float % ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float % ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float % ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float % ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float % ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float % calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float % array | CAST(2 AS FLOAT) | array(1,2) |
        | float % map | CAST(2 AS FLOAT) | map('a',1) |
        | float % struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double % bool | CAST(2 AS DOUBLE) | true |
        | double % bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double % date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double % ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double % ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double % ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double % ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double % ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double % ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double % ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double % ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double % calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double % array | CAST(2 AS DOUBLE) | array(1,2) |
        | double % map | CAST(2 AS DOUBLE) | map('a',1) |
        | double % struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec % bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec % bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec % date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec % ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec % ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec % ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec % ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec % ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec % ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec % ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec % ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec % calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec % array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec % map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec % struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str % bool | '2' | true |
        | str % bin | '2' | CAST('2' AS BINARY) |
        | str % date | '2' | DATE'2024-01-15' |
        | str % ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str % ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str % ival_d | '2' | INTERVAL '2' DAY |
        | str % ival_dt | '2' | INTERVAL '25' HOUR |
        | str % ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str % ival_m | '2' | INTERVAL '2' MONTH |
        | str % ival_y | '2' | INTERVAL '2' YEAR |
        | str % ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str % calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | str % array | '2' | array(1,2) |
        | str % map | '2' | map('a',1) |
        | str % struct | '2' | named_struct('a',1) |
        | bin % unull | CAST('2' AS BINARY) | NULL |
        | bin % null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin % bool | CAST('2' AS BINARY) | true |
        | bin % tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin % smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin % int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin % bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin % float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin % double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin % dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin % str | CAST('2' AS BINARY) | '2' |
        | bin % bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin % date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin % ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin % ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin % ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin % ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin % ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin % ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin % ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin % ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin % calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin % array | CAST('2' AS BINARY) | array(1,2) |
        | bin % map | CAST('2' AS BINARY) | map('a',1) |
        | bin % struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date % unull | DATE'2024-01-15' | NULL |
        | date % null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date % bool | DATE'2024-01-15' | true |
        | date % tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date % smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date % int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date % bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date % float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date % double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date % dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date % str | DATE'2024-01-15' | '2' |
        | date % bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date % date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date % ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date % ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date % ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date % ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date % ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date % ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date % ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date % ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date % calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date % array | DATE'2024-01-15' | array(1,2) |
        | date % map | DATE'2024-01-15' | map('a',1) |
        | date % struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts % unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts % null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts % bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts % tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts % smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts % int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts % bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts % float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts % double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts % dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts % str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts % bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts % date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts % ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts % ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts % ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts % ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts % ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts % ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts % ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts % ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts % calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts % array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts % map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts % struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz % unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz % null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz % bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz % tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz % smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz % int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz % bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz % float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz % double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz % dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz % str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz % bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz % date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz % ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz % ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz % ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz % ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz % ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz % ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz % ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz % ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz % calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz % array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz % map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz % struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d % unull | INTERVAL '2' DAY | NULL |
        | ival_d % null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d % bool | INTERVAL '2' DAY | true |
        | ival_d % tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d % smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d % int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d % bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d % float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d % double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d % dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d % str | INTERVAL '2' DAY | '2' |
        | ival_d % bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d % date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d % ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d % ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d % ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d % ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d % ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d % ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d % ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d % ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d % calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d % array | INTERVAL '2' DAY | array(1,2) |
        | ival_d % map | INTERVAL '2' DAY | map('a',1) |
        | ival_d % struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt % unull | INTERVAL '25' HOUR | NULL |
        | ival_dt % null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt % bool | INTERVAL '25' HOUR | true |
        | ival_dt % tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt % smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt % int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt % bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt % float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt % double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt % dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt % str | INTERVAL '25' HOUR | '2' |
        | ival_dt % bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt % date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt % ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt % ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt % ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt % ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt % ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt % ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt % ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt % ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt % calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt % array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt % map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt % struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds % unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds % null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds % bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds % tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds % smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds % int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds % bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds % float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds % double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds % dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds % str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds % bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds % date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds % ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds % ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds % ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds % ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds % ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds % ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds % ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds % ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds % calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds % array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds % map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds % struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m % unull | INTERVAL '2' MONTH | NULL |
        | ival_m % null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m % bool | INTERVAL '2' MONTH | true |
        | ival_m % tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m % smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m % int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m % bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m % float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m % double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m % dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m % str | INTERVAL '2' MONTH | '2' |
        | ival_m % bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m % date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m % ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m % ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m % ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m % ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m % ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m % ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m % ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m % ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m % calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m % array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m % map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m % struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y % unull | INTERVAL '2' YEAR | NULL |
        | ival_y % null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y % bool | INTERVAL '2' YEAR | true |
        | ival_y % tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y % smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y % int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y % bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y % float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y % double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y % dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y % str | INTERVAL '2' YEAR | '2' |
        | ival_y % bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y % date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y % ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y % ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y % ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y % ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y % ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y % ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y % ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y % ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y % calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y % array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y % map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y % struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym % unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym % null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym % bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym % tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym % smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym % int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym % bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym % float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym % double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym % dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym % str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym % bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym % date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym % ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym % ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym % ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym % ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym % ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym % ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym % ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym % ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym % calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym % array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym % map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym % struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar % unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar % null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar % bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar % tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar % smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar % int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar % bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar % float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar % double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar % dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar % str | make_interval(0,1,0,1,0,0,0) | '2' |
        | calendar % bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar % date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar % ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar % ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar % ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar % ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar % ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar % ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar % ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar % ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar % calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar % array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar % map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar % struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array % unull | array(1,2) | NULL |
        | array % null | array(1,2) | CAST(NULL AS INT) |
        | array % bool | array(1,2) | true |
        | array % tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array % smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array % int | array(1,2) | CAST(2 AS INT) |
        | array % bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array % float | array(1,2) | CAST(2 AS FLOAT) |
        | array % double | array(1,2) | CAST(2 AS DOUBLE) |
        | array % dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array % str | array(1,2) | '2' |
        | array % bin | array(1,2) | CAST('2' AS BINARY) |
        | array % date | array(1,2) | DATE'2024-01-15' |
        | array % ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array % ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array % ival_d | array(1,2) | INTERVAL '2' DAY |
        | array % ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array % ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array % ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array % ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array % ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array % calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array % array | array(1,2) | array(1,2) |
        | array % map | array(1,2) | map('a',1) |
        | array % struct | array(1,2) | named_struct('a',1) |
        | map % unull | map('a',1) | NULL |
        | map % null | map('a',1) | CAST(NULL AS INT) |
        | map % bool | map('a',1) | true |
        | map % tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map % smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map % int | map('a',1) | CAST(2 AS INT) |
        | map % bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map % float | map('a',1) | CAST(2 AS FLOAT) |
        | map % double | map('a',1) | CAST(2 AS DOUBLE) |
        | map % dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map % str | map('a',1) | '2' |
        | map % bin | map('a',1) | CAST('2' AS BINARY) |
        | map % date | map('a',1) | DATE'2024-01-15' |
        | map % ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map % ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map % ival_d | map('a',1) | INTERVAL '2' DAY |
        | map % ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map % ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map % ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map % ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map % ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map % calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map % array | map('a',1) | array(1,2) |
        | map % map | map('a',1) | map('a',1) |
        | map % struct | map('a',1) | named_struct('a',1) |
        | struct % unull | named_struct('a',1) | NULL |
        | struct % null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct % bool | named_struct('a',1) | true |
        | struct % tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct % smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct % int | named_struct('a',1) | CAST(2 AS INT) |
        | struct % bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct % float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct % double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct % dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct % str | named_struct('a',1) | '2' |
        | struct % bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct % date | named_struct('a',1) | DATE'2024-01-15' |
        | struct % ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct % ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct % ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct % ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct % ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct % ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct % ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct % ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct % calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct % array | named_struct('a',1) | array(1,2) |
        | struct % map | named_struct('a',1) | map('a',1) |
        | struct % struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: modulo ansi-off: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % variant | NULL | parse_json('{"a":1}') |
        | null % variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool % variant | true | parse_json('{"a":1}') |
        | tinyint % variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint % variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int % variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint % variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float % variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double % variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec % variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str % variant | '2' | parse_json('{"a":1}') |
        | bin % variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date % variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts % variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz % variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d % variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt % variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds % variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m % variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y % variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym % variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar % variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array % variant | array(1,2) | parse_json('{"a":1}') |
        | map % variant | map('a',1) | parse_json('{"a":1}') |
        | struct % variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant % unull | parse_json('{"a":1}') | NULL |
        | variant % null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant % bool | parse_json('{"a":1}') | true |
        | variant % tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant % smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant % int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant % bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant % float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant % double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant % dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant % str | parse_json('{"a":1}') | '2' |
        | variant % bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant % date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant % ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant % ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant % ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant % ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant % ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant % ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant % ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant % ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant % calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant % array | parse_json('{"a":1}') | array(1,2) |
        | variant % map | parse_json('{"a":1}') | map('a',1) |
        | variant % struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant % variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: modulo ansi-off: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % time | NULL | TIME '12:00:00' |
        | null % time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool % time | true | TIME '12:00:00' |
        | tinyint % time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint % time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int % time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint % time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float % time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double % time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec % time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str % time | '2' | TIME '12:00:00' |
        | bin % time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date % time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts % time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz % time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time % unull | TIME '12:00:00' | NULL |
        | time % null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time % bool | TIME '12:00:00' | true |
        | time % tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time % smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time % int | TIME '12:00:00' | CAST(2 AS INT) |
        | time % bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time % float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time % double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time % dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time % str | TIME '12:00:00' | '2' |
        | time % bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time % date | TIME '12:00:00' | DATE'2024-01-15' |
        | time % ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time % ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time % time | TIME '12:00:00' | TIME '12:00:00' |
        | time % ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time % ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time % ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time % ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time % ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time % ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time % calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time % array | TIME '12:00:00' | array(1,2) |
        | time % map | TIME '12:00:00' | map('a',1) |
        | time % struct | TIME '12:00:00' | named_struct('a',1) |
        | time % variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d % time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt % time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds % time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m % time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y % time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym % time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar % time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array % time | array(1,2) | TIME '12:00:00' |
        | map % time | map('a',1) | TIME '12:00:00' |
        | struct % time | named_struct('a',1) | TIME '12:00:00' |
        | variant % time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: modulo ansi-off: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null % geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool % geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint % geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint % geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int % geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint % geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float % geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double % geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec % geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str % geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin % geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date % geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts % geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz % geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time % geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d % geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt % geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds % geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m % geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y % geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym % geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar % geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array % geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map % geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct % geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant % geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom % unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom % null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom % bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom % tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom % smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom % int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom % bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom % float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom % double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom % dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom % str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom % bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom % date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom % ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom % ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom % time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom % ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom % ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom % ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom % ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom % ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom % ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom % calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom % array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom % map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom % struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom % variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom % geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  Rule: `%` operand-type rejection (ANSI on)

    Scenario Outline: modulo ansi-on: rejected pair: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % bool | NULL | true |
        | unull % str | NULL | '2' |
        | unull % bin | NULL | CAST('2' AS BINARY) |
        | unull % date | NULL | DATE'2024-01-15' |
        | unull % ts | NULL | TIMESTAMP'2024-01-15 12:00:00' |
        | unull % ts_ntz | NULL | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | unull % ival_d | NULL | INTERVAL '2' DAY |
        | unull % ival_dt | NULL | INTERVAL '25' HOUR |
        | unull % ival_ds | NULL | INTERVAL '1 02:03:04' DAY TO SECOND |
        | unull % ival_m | NULL | INTERVAL '2' MONTH |
        | unull % ival_y | NULL | INTERVAL '2' YEAR |
        | unull % ival_ym | NULL | INTERVAL '1-2' YEAR TO MONTH |
        | unull % calendar | NULL | make_interval(0,1,0,1,0,0,0) |
        | unull % array | NULL | array(1,2) |
        | unull % map | NULL | map('a',1) |
        | unull % struct | NULL | named_struct('a',1) |
        | null % bool | CAST(NULL AS INT) | true |
        | null % bin | CAST(NULL AS INT) | CAST('2' AS BINARY) |
        | null % date | CAST(NULL AS INT) | DATE'2024-01-15' |
        | null % ts | CAST(NULL AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | null % ts_ntz | CAST(NULL AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | null % ival_d | CAST(NULL AS INT) | INTERVAL '2' DAY |
        | null % ival_dt | CAST(NULL AS INT) | INTERVAL '25' HOUR |
        | null % ival_ds | CAST(NULL AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | null % ival_m | CAST(NULL AS INT) | INTERVAL '2' MONTH |
        | null % ival_y | CAST(NULL AS INT) | INTERVAL '2' YEAR |
        | null % ival_ym | CAST(NULL AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | null % calendar | CAST(NULL AS INT) | make_interval(0,1,0,1,0,0,0) |
        | null % array | CAST(NULL AS INT) | array(1,2) |
        | null % map | CAST(NULL AS INT) | map('a',1) |
        | null % struct | CAST(NULL AS INT) | named_struct('a',1) |
        | bool % unull | true | NULL |
        | bool % null | true | CAST(NULL AS INT) |
        | bool % bool | true | true |
        | bool % tinyint | true | CAST(2 AS TINYINT) |
        | bool % smallint | true | CAST(2 AS SMALLINT) |
        | bool % int | true | CAST(2 AS INT) |
        | bool % bigint | true | CAST(2 AS BIGINT) |
        | bool % float | true | CAST(2 AS FLOAT) |
        | bool % double | true | CAST(2 AS DOUBLE) |
        | bool % dec | true | CAST(2 AS DECIMAL(10,2)) |
        | bool % str | true | '2' |
        | bool % bin | true | CAST('2' AS BINARY) |
        | bool % date | true | DATE'2024-01-15' |
        | bool % ts | true | TIMESTAMP'2024-01-15 12:00:00' |
        | bool % ts_ntz | true | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bool % ival_d | true | INTERVAL '2' DAY |
        | bool % ival_dt | true | INTERVAL '25' HOUR |
        | bool % ival_ds | true | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bool % ival_m | true | INTERVAL '2' MONTH |
        | bool % ival_y | true | INTERVAL '2' YEAR |
        | bool % ival_ym | true | INTERVAL '1-2' YEAR TO MONTH |
        | bool % calendar | true | make_interval(0,1,0,1,0,0,0) |
        | bool % array | true | array(1,2) |
        | bool % map | true | map('a',1) |
        | bool % struct | true | named_struct('a',1) |
        | tinyint % bool | CAST(2 AS TINYINT) | true |
        | tinyint % bin | CAST(2 AS TINYINT) | CAST('2' AS BINARY) |
        | tinyint % date | CAST(2 AS TINYINT) | DATE'2024-01-15' |
        | tinyint % ts | CAST(2 AS TINYINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | tinyint % ts_ntz | CAST(2 AS TINYINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | tinyint % ival_d | CAST(2 AS TINYINT) | INTERVAL '2' DAY |
        | tinyint % ival_dt | CAST(2 AS TINYINT) | INTERVAL '25' HOUR |
        | tinyint % ival_ds | CAST(2 AS TINYINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | tinyint % ival_m | CAST(2 AS TINYINT) | INTERVAL '2' MONTH |
        | tinyint % ival_y | CAST(2 AS TINYINT) | INTERVAL '2' YEAR |
        | tinyint % ival_ym | CAST(2 AS TINYINT) | INTERVAL '1-2' YEAR TO MONTH |
        | tinyint % calendar | CAST(2 AS TINYINT) | make_interval(0,1,0,1,0,0,0) |
        | tinyint % array | CAST(2 AS TINYINT) | array(1,2) |
        | tinyint % map | CAST(2 AS TINYINT) | map('a',1) |
        | tinyint % struct | CAST(2 AS TINYINT) | named_struct('a',1) |
        | smallint % bool | CAST(2 AS SMALLINT) | true |
        | smallint % bin | CAST(2 AS SMALLINT) | CAST('2' AS BINARY) |
        | smallint % date | CAST(2 AS SMALLINT) | DATE'2024-01-15' |
        | smallint % ts | CAST(2 AS SMALLINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | smallint % ts_ntz | CAST(2 AS SMALLINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | smallint % ival_d | CAST(2 AS SMALLINT) | INTERVAL '2' DAY |
        | smallint % ival_dt | CAST(2 AS SMALLINT) | INTERVAL '25' HOUR |
        | smallint % ival_ds | CAST(2 AS SMALLINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | smallint % ival_m | CAST(2 AS SMALLINT) | INTERVAL '2' MONTH |
        | smallint % ival_y | CAST(2 AS SMALLINT) | INTERVAL '2' YEAR |
        | smallint % ival_ym | CAST(2 AS SMALLINT) | INTERVAL '1-2' YEAR TO MONTH |
        | smallint % calendar | CAST(2 AS SMALLINT) | make_interval(0,1,0,1,0,0,0) |
        | smallint % array | CAST(2 AS SMALLINT) | array(1,2) |
        | smallint % map | CAST(2 AS SMALLINT) | map('a',1) |
        | smallint % struct | CAST(2 AS SMALLINT) | named_struct('a',1) |
        | int % bool | CAST(2 AS INT) | true |
        | int % bin | CAST(2 AS INT) | CAST('2' AS BINARY) |
        | int % date | CAST(2 AS INT) | DATE'2024-01-15' |
        | int % ts | CAST(2 AS INT) | TIMESTAMP'2024-01-15 12:00:00' |
        | int % ts_ntz | CAST(2 AS INT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | int % ival_d | CAST(2 AS INT) | INTERVAL '2' DAY |
        | int % ival_dt | CAST(2 AS INT) | INTERVAL '25' HOUR |
        | int % ival_ds | CAST(2 AS INT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | int % ival_m | CAST(2 AS INT) | INTERVAL '2' MONTH |
        | int % ival_y | CAST(2 AS INT) | INTERVAL '2' YEAR |
        | int % ival_ym | CAST(2 AS INT) | INTERVAL '1-2' YEAR TO MONTH |
        | int % calendar | CAST(2 AS INT) | make_interval(0,1,0,1,0,0,0) |
        | int % array | CAST(2 AS INT) | array(1,2) |
        | int % map | CAST(2 AS INT) | map('a',1) |
        | int % struct | CAST(2 AS INT) | named_struct('a',1) |
        | bigint % bool | CAST(2 AS BIGINT) | true |
        | bigint % bin | CAST(2 AS BIGINT) | CAST('2' AS BINARY) |
        | bigint % date | CAST(2 AS BIGINT) | DATE'2024-01-15' |
        | bigint % ts | CAST(2 AS BIGINT) | TIMESTAMP'2024-01-15 12:00:00' |
        | bigint % ts_ntz | CAST(2 AS BIGINT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bigint % ival_d | CAST(2 AS BIGINT) | INTERVAL '2' DAY |
        | bigint % ival_dt | CAST(2 AS BIGINT) | INTERVAL '25' HOUR |
        | bigint % ival_ds | CAST(2 AS BIGINT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bigint % ival_m | CAST(2 AS BIGINT) | INTERVAL '2' MONTH |
        | bigint % ival_y | CAST(2 AS BIGINT) | INTERVAL '2' YEAR |
        | bigint % ival_ym | CAST(2 AS BIGINT) | INTERVAL '1-2' YEAR TO MONTH |
        | bigint % calendar | CAST(2 AS BIGINT) | make_interval(0,1,0,1,0,0,0) |
        | bigint % array | CAST(2 AS BIGINT) | array(1,2) |
        | bigint % map | CAST(2 AS BIGINT) | map('a',1) |
        | bigint % struct | CAST(2 AS BIGINT) | named_struct('a',1) |
        | float % bool | CAST(2 AS FLOAT) | true |
        | float % bin | CAST(2 AS FLOAT) | CAST('2' AS BINARY) |
        | float % date | CAST(2 AS FLOAT) | DATE'2024-01-15' |
        | float % ts | CAST(2 AS FLOAT) | TIMESTAMP'2024-01-15 12:00:00' |
        | float % ts_ntz | CAST(2 AS FLOAT) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | float % ival_d | CAST(2 AS FLOAT) | INTERVAL '2' DAY |
        | float % ival_dt | CAST(2 AS FLOAT) | INTERVAL '25' HOUR |
        | float % ival_ds | CAST(2 AS FLOAT) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | float % ival_m | CAST(2 AS FLOAT) | INTERVAL '2' MONTH |
        | float % ival_y | CAST(2 AS FLOAT) | INTERVAL '2' YEAR |
        | float % ival_ym | CAST(2 AS FLOAT) | INTERVAL '1-2' YEAR TO MONTH |
        | float % calendar | CAST(2 AS FLOAT) | make_interval(0,1,0,1,0,0,0) |
        | float % array | CAST(2 AS FLOAT) | array(1,2) |
        | float % map | CAST(2 AS FLOAT) | map('a',1) |
        | float % struct | CAST(2 AS FLOAT) | named_struct('a',1) |
        | double % bool | CAST(2 AS DOUBLE) | true |
        | double % bin | CAST(2 AS DOUBLE) | CAST('2' AS BINARY) |
        | double % date | CAST(2 AS DOUBLE) | DATE'2024-01-15' |
        | double % ts | CAST(2 AS DOUBLE) | TIMESTAMP'2024-01-15 12:00:00' |
        | double % ts_ntz | CAST(2 AS DOUBLE) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | double % ival_d | CAST(2 AS DOUBLE) | INTERVAL '2' DAY |
        | double % ival_dt | CAST(2 AS DOUBLE) | INTERVAL '25' HOUR |
        | double % ival_ds | CAST(2 AS DOUBLE) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | double % ival_m | CAST(2 AS DOUBLE) | INTERVAL '2' MONTH |
        | double % ival_y | CAST(2 AS DOUBLE) | INTERVAL '2' YEAR |
        | double % ival_ym | CAST(2 AS DOUBLE) | INTERVAL '1-2' YEAR TO MONTH |
        | double % calendar | CAST(2 AS DOUBLE) | make_interval(0,1,0,1,0,0,0) |
        | double % array | CAST(2 AS DOUBLE) | array(1,2) |
        | double % map | CAST(2 AS DOUBLE) | map('a',1) |
        | double % struct | CAST(2 AS DOUBLE) | named_struct('a',1) |
        | dec % bool | CAST(2 AS DECIMAL(10,2)) | true |
        | dec % bin | CAST(2 AS DECIMAL(10,2)) | CAST('2' AS BINARY) |
        | dec % date | CAST(2 AS DECIMAL(10,2)) | DATE'2024-01-15' |
        | dec % ts | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP'2024-01-15 12:00:00' |
        | dec % ts_ntz | CAST(2 AS DECIMAL(10,2)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | dec % ival_d | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' DAY |
        | dec % ival_dt | CAST(2 AS DECIMAL(10,2)) | INTERVAL '25' HOUR |
        | dec % ival_ds | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | dec % ival_m | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' MONTH |
        | dec % ival_y | CAST(2 AS DECIMAL(10,2)) | INTERVAL '2' YEAR |
        | dec % ival_ym | CAST(2 AS DECIMAL(10,2)) | INTERVAL '1-2' YEAR TO MONTH |
        | dec % calendar | CAST(2 AS DECIMAL(10,2)) | make_interval(0,1,0,1,0,0,0) |
        | dec % array | CAST(2 AS DECIMAL(10,2)) | array(1,2) |
        | dec % map | CAST(2 AS DECIMAL(10,2)) | map('a',1) |
        | dec % struct | CAST(2 AS DECIMAL(10,2)) | named_struct('a',1) |
        | str % unull | '2' | NULL |
        | str % bool | '2' | true |
        | str % str | '2' | '2' |
        | str % bin | '2' | CAST('2' AS BINARY) |
        | str % date | '2' | DATE'2024-01-15' |
        | str % ts | '2' | TIMESTAMP'2024-01-15 12:00:00' |
        | str % ts_ntz | '2' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | str % ival_d | '2' | INTERVAL '2' DAY |
        | str % ival_dt | '2' | INTERVAL '25' HOUR |
        | str % ival_ds | '2' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | str % ival_m | '2' | INTERVAL '2' MONTH |
        | str % ival_y | '2' | INTERVAL '2' YEAR |
        | str % ival_ym | '2' | INTERVAL '1-2' YEAR TO MONTH |
        | str % calendar | '2' | make_interval(0,1,0,1,0,0,0) |
        | str % array | '2' | array(1,2) |
        | str % map | '2' | map('a',1) |
        | str % struct | '2' | named_struct('a',1) |
        | bin % unull | CAST('2' AS BINARY) | NULL |
        | bin % null | CAST('2' AS BINARY) | CAST(NULL AS INT) |
        | bin % bool | CAST('2' AS BINARY) | true |
        | bin % tinyint | CAST('2' AS BINARY) | CAST(2 AS TINYINT) |
        | bin % smallint | CAST('2' AS BINARY) | CAST(2 AS SMALLINT) |
        | bin % int | CAST('2' AS BINARY) | CAST(2 AS INT) |
        | bin % bigint | CAST('2' AS BINARY) | CAST(2 AS BIGINT) |
        | bin % float | CAST('2' AS BINARY) | CAST(2 AS FLOAT) |
        | bin % double | CAST('2' AS BINARY) | CAST(2 AS DOUBLE) |
        | bin % dec | CAST('2' AS BINARY) | CAST(2 AS DECIMAL(10,2)) |
        | bin % str | CAST('2' AS BINARY) | '2' |
        | bin % bin | CAST('2' AS BINARY) | CAST('2' AS BINARY) |
        | bin % date | CAST('2' AS BINARY) | DATE'2024-01-15' |
        | bin % ts | CAST('2' AS BINARY) | TIMESTAMP'2024-01-15 12:00:00' |
        | bin % ts_ntz | CAST('2' AS BINARY) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | bin % ival_d | CAST('2' AS BINARY) | INTERVAL '2' DAY |
        | bin % ival_dt | CAST('2' AS BINARY) | INTERVAL '25' HOUR |
        | bin % ival_ds | CAST('2' AS BINARY) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | bin % ival_m | CAST('2' AS BINARY) | INTERVAL '2' MONTH |
        | bin % ival_y | CAST('2' AS BINARY) | INTERVAL '2' YEAR |
        | bin % ival_ym | CAST('2' AS BINARY) | INTERVAL '1-2' YEAR TO MONTH |
        | bin % calendar | CAST('2' AS BINARY) | make_interval(0,1,0,1,0,0,0) |
        | bin % array | CAST('2' AS BINARY) | array(1,2) |
        | bin % map | CAST('2' AS BINARY) | map('a',1) |
        | bin % struct | CAST('2' AS BINARY) | named_struct('a',1) |
        | date % unull | DATE'2024-01-15' | NULL |
        | date % null | DATE'2024-01-15' | CAST(NULL AS INT) |
        | date % bool | DATE'2024-01-15' | true |
        | date % tinyint | DATE'2024-01-15' | CAST(2 AS TINYINT) |
        | date % smallint | DATE'2024-01-15' | CAST(2 AS SMALLINT) |
        | date % int | DATE'2024-01-15' | CAST(2 AS INT) |
        | date % bigint | DATE'2024-01-15' | CAST(2 AS BIGINT) |
        | date % float | DATE'2024-01-15' | CAST(2 AS FLOAT) |
        | date % double | DATE'2024-01-15' | CAST(2 AS DOUBLE) |
        | date % dec | DATE'2024-01-15' | CAST(2 AS DECIMAL(10,2)) |
        | date % str | DATE'2024-01-15' | '2' |
        | date % bin | DATE'2024-01-15' | CAST('2' AS BINARY) |
        | date % date | DATE'2024-01-15' | DATE'2024-01-15' |
        | date % ts | DATE'2024-01-15' | TIMESTAMP'2024-01-15 12:00:00' |
        | date % ts_ntz | DATE'2024-01-15' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | date % ival_d | DATE'2024-01-15' | INTERVAL '2' DAY |
        | date % ival_dt | DATE'2024-01-15' | INTERVAL '25' HOUR |
        | date % ival_ds | DATE'2024-01-15' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | date % ival_m | DATE'2024-01-15' | INTERVAL '2' MONTH |
        | date % ival_y | DATE'2024-01-15' | INTERVAL '2' YEAR |
        | date % ival_ym | DATE'2024-01-15' | INTERVAL '1-2' YEAR TO MONTH |
        | date % calendar | DATE'2024-01-15' | make_interval(0,1,0,1,0,0,0) |
        | date % array | DATE'2024-01-15' | array(1,2) |
        | date % map | DATE'2024-01-15' | map('a',1) |
        | date % struct | DATE'2024-01-15' | named_struct('a',1) |
        | ts % unull | TIMESTAMP'2024-01-15 12:00:00' | NULL |
        | ts % null | TIMESTAMP'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts % bool | TIMESTAMP'2024-01-15 12:00:00' | true |
        | ts % tinyint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts % smallint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts % int | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts % bigint | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts % float | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts % double | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts % dec | TIMESTAMP'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts % str | TIMESTAMP'2024-01-15 12:00:00' | '2' |
        | ts % bin | TIMESTAMP'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts % date | TIMESTAMP'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts % ts | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts % ts_ntz | TIMESTAMP'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts % ival_d | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts % ival_dt | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts % ival_ds | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts % ival_m | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts % ival_y | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts % ival_ym | TIMESTAMP'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts % calendar | TIMESTAMP'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts % array | TIMESTAMP'2024-01-15 12:00:00' | array(1,2) |
        | ts % map | TIMESTAMP'2024-01-15 12:00:00' | map('a',1) |
        | ts % struct | TIMESTAMP'2024-01-15 12:00:00' | named_struct('a',1) |
        | ts_ntz % unull | TIMESTAMP_NTZ'2024-01-15 12:00:00' | NULL |
        | ts_ntz % null | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(NULL AS INT) |
        | ts_ntz % bool | TIMESTAMP_NTZ'2024-01-15 12:00:00' | true |
        | ts_ntz % tinyint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS TINYINT) |
        | ts_ntz % smallint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS SMALLINT) |
        | ts_ntz % int | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS INT) |
        | ts_ntz % bigint | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS BIGINT) |
        | ts_ntz % float | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS FLOAT) |
        | ts_ntz % double | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DOUBLE) |
        | ts_ntz % dec | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | ts_ntz % str | TIMESTAMP_NTZ'2024-01-15 12:00:00' | '2' |
        | ts_ntz % bin | TIMESTAMP_NTZ'2024-01-15 12:00:00' | CAST('2' AS BINARY) |
        | ts_ntz % date | TIMESTAMP_NTZ'2024-01-15 12:00:00' | DATE'2024-01-15' |
        | ts_ntz % ts | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | ts_ntz % ts_ntz | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ts_ntz % ival_d | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' DAY |
        | ts_ntz % ival_dt | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '25' HOUR |
        | ts_ntz % ival_ds | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ts_ntz % ival_m | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' MONTH |
        | ts_ntz % ival_y | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '2' YEAR |
        | ts_ntz % ival_ym | TIMESTAMP_NTZ'2024-01-15 12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | ts_ntz % calendar | TIMESTAMP_NTZ'2024-01-15 12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | ts_ntz % array | TIMESTAMP_NTZ'2024-01-15 12:00:00' | array(1,2) |
        | ts_ntz % map | TIMESTAMP_NTZ'2024-01-15 12:00:00' | map('a',1) |
        | ts_ntz % struct | TIMESTAMP_NTZ'2024-01-15 12:00:00' | named_struct('a',1) |
        | ival_d % unull | INTERVAL '2' DAY | NULL |
        | ival_d % null | INTERVAL '2' DAY | CAST(NULL AS INT) |
        | ival_d % bool | INTERVAL '2' DAY | true |
        | ival_d % tinyint | INTERVAL '2' DAY | CAST(2 AS TINYINT) |
        | ival_d % smallint | INTERVAL '2' DAY | CAST(2 AS SMALLINT) |
        | ival_d % int | INTERVAL '2' DAY | CAST(2 AS INT) |
        | ival_d % bigint | INTERVAL '2' DAY | CAST(2 AS BIGINT) |
        | ival_d % float | INTERVAL '2' DAY | CAST(2 AS FLOAT) |
        | ival_d % double | INTERVAL '2' DAY | CAST(2 AS DOUBLE) |
        | ival_d % dec | INTERVAL '2' DAY | CAST(2 AS DECIMAL(10,2)) |
        | ival_d % str | INTERVAL '2' DAY | '2' |
        | ival_d % bin | INTERVAL '2' DAY | CAST('2' AS BINARY) |
        | ival_d % date | INTERVAL '2' DAY | DATE'2024-01-15' |
        | ival_d % ts | INTERVAL '2' DAY | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_d % ts_ntz | INTERVAL '2' DAY | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_d % ival_d | INTERVAL '2' DAY | INTERVAL '2' DAY |
        | ival_d % ival_dt | INTERVAL '2' DAY | INTERVAL '25' HOUR |
        | ival_d % ival_ds | INTERVAL '2' DAY | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_d % ival_m | INTERVAL '2' DAY | INTERVAL '2' MONTH |
        | ival_d % ival_y | INTERVAL '2' DAY | INTERVAL '2' YEAR |
        | ival_d % ival_ym | INTERVAL '2' DAY | INTERVAL '1-2' YEAR TO MONTH |
        | ival_d % calendar | INTERVAL '2' DAY | make_interval(0,1,0,1,0,0,0) |
        | ival_d % array | INTERVAL '2' DAY | array(1,2) |
        | ival_d % map | INTERVAL '2' DAY | map('a',1) |
        | ival_d % struct | INTERVAL '2' DAY | named_struct('a',1) |
        | ival_dt % unull | INTERVAL '25' HOUR | NULL |
        | ival_dt % null | INTERVAL '25' HOUR | CAST(NULL AS INT) |
        | ival_dt % bool | INTERVAL '25' HOUR | true |
        | ival_dt % tinyint | INTERVAL '25' HOUR | CAST(2 AS TINYINT) |
        | ival_dt % smallint | INTERVAL '25' HOUR | CAST(2 AS SMALLINT) |
        | ival_dt % int | INTERVAL '25' HOUR | CAST(2 AS INT) |
        | ival_dt % bigint | INTERVAL '25' HOUR | CAST(2 AS BIGINT) |
        | ival_dt % float | INTERVAL '25' HOUR | CAST(2 AS FLOAT) |
        | ival_dt % double | INTERVAL '25' HOUR | CAST(2 AS DOUBLE) |
        | ival_dt % dec | INTERVAL '25' HOUR | CAST(2 AS DECIMAL(10,2)) |
        | ival_dt % str | INTERVAL '25' HOUR | '2' |
        | ival_dt % bin | INTERVAL '25' HOUR | CAST('2' AS BINARY) |
        | ival_dt % date | INTERVAL '25' HOUR | DATE'2024-01-15' |
        | ival_dt % ts | INTERVAL '25' HOUR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_dt % ts_ntz | INTERVAL '25' HOUR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_dt % ival_d | INTERVAL '25' HOUR | INTERVAL '2' DAY |
        | ival_dt % ival_dt | INTERVAL '25' HOUR | INTERVAL '25' HOUR |
        | ival_dt % ival_ds | INTERVAL '25' HOUR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_dt % ival_m | INTERVAL '25' HOUR | INTERVAL '2' MONTH |
        | ival_dt % ival_y | INTERVAL '25' HOUR | INTERVAL '2' YEAR |
        | ival_dt % ival_ym | INTERVAL '25' HOUR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_dt % calendar | INTERVAL '25' HOUR | make_interval(0,1,0,1,0,0,0) |
        | ival_dt % array | INTERVAL '25' HOUR | array(1,2) |
        | ival_dt % map | INTERVAL '25' HOUR | map('a',1) |
        | ival_dt % struct | INTERVAL '25' HOUR | named_struct('a',1) |
        | ival_ds % unull | INTERVAL '1 02:03:04' DAY TO SECOND | NULL |
        | ival_ds % null | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(NULL AS INT) |
        | ival_ds % bool | INTERVAL '1 02:03:04' DAY TO SECOND | true |
        | ival_ds % tinyint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS TINYINT) |
        | ival_ds % smallint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS SMALLINT) |
        | ival_ds % int | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS INT) |
        | ival_ds % bigint | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS BIGINT) |
        | ival_ds % float | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS FLOAT) |
        | ival_ds % double | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DOUBLE) |
        | ival_ds % dec | INTERVAL '1 02:03:04' DAY TO SECOND | CAST(2 AS DECIMAL(10,2)) |
        | ival_ds % str | INTERVAL '1 02:03:04' DAY TO SECOND | '2' |
        | ival_ds % bin | INTERVAL '1 02:03:04' DAY TO SECOND | CAST('2' AS BINARY) |
        | ival_ds % date | INTERVAL '1 02:03:04' DAY TO SECOND | DATE'2024-01-15' |
        | ival_ds % ts | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ds % ts_ntz | INTERVAL '1 02:03:04' DAY TO SECOND | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ds % ival_d | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' DAY |
        | ival_ds % ival_dt | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '25' HOUR |
        | ival_ds % ival_ds | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ds % ival_m | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' MONTH |
        | ival_ds % ival_y | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '2' YEAR |
        | ival_ds % ival_ym | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ds % calendar | INTERVAL '1 02:03:04' DAY TO SECOND | make_interval(0,1,0,1,0,0,0) |
        | ival_ds % array | INTERVAL '1 02:03:04' DAY TO SECOND | array(1,2) |
        | ival_ds % map | INTERVAL '1 02:03:04' DAY TO SECOND | map('a',1) |
        | ival_ds % struct | INTERVAL '1 02:03:04' DAY TO SECOND | named_struct('a',1) |
        | ival_m % unull | INTERVAL '2' MONTH | NULL |
        | ival_m % null | INTERVAL '2' MONTH | CAST(NULL AS INT) |
        | ival_m % bool | INTERVAL '2' MONTH | true |
        | ival_m % tinyint | INTERVAL '2' MONTH | CAST(2 AS TINYINT) |
        | ival_m % smallint | INTERVAL '2' MONTH | CAST(2 AS SMALLINT) |
        | ival_m % int | INTERVAL '2' MONTH | CAST(2 AS INT) |
        | ival_m % bigint | INTERVAL '2' MONTH | CAST(2 AS BIGINT) |
        | ival_m % float | INTERVAL '2' MONTH | CAST(2 AS FLOAT) |
        | ival_m % double | INTERVAL '2' MONTH | CAST(2 AS DOUBLE) |
        | ival_m % dec | INTERVAL '2' MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_m % str | INTERVAL '2' MONTH | '2' |
        | ival_m % bin | INTERVAL '2' MONTH | CAST('2' AS BINARY) |
        | ival_m % date | INTERVAL '2' MONTH | DATE'2024-01-15' |
        | ival_m % ts | INTERVAL '2' MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_m % ts_ntz | INTERVAL '2' MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_m % ival_d | INTERVAL '2' MONTH | INTERVAL '2' DAY |
        | ival_m % ival_dt | INTERVAL '2' MONTH | INTERVAL '25' HOUR |
        | ival_m % ival_ds | INTERVAL '2' MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_m % ival_m | INTERVAL '2' MONTH | INTERVAL '2' MONTH |
        | ival_m % ival_y | INTERVAL '2' MONTH | INTERVAL '2' YEAR |
        | ival_m % ival_ym | INTERVAL '2' MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_m % calendar | INTERVAL '2' MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_m % array | INTERVAL '2' MONTH | array(1,2) |
        | ival_m % map | INTERVAL '2' MONTH | map('a',1) |
        | ival_m % struct | INTERVAL '2' MONTH | named_struct('a',1) |
        | ival_y % unull | INTERVAL '2' YEAR | NULL |
        | ival_y % null | INTERVAL '2' YEAR | CAST(NULL AS INT) |
        | ival_y % bool | INTERVAL '2' YEAR | true |
        | ival_y % tinyint | INTERVAL '2' YEAR | CAST(2 AS TINYINT) |
        | ival_y % smallint | INTERVAL '2' YEAR | CAST(2 AS SMALLINT) |
        | ival_y % int | INTERVAL '2' YEAR | CAST(2 AS INT) |
        | ival_y % bigint | INTERVAL '2' YEAR | CAST(2 AS BIGINT) |
        | ival_y % float | INTERVAL '2' YEAR | CAST(2 AS FLOAT) |
        | ival_y % double | INTERVAL '2' YEAR | CAST(2 AS DOUBLE) |
        | ival_y % dec | INTERVAL '2' YEAR | CAST(2 AS DECIMAL(10,2)) |
        | ival_y % str | INTERVAL '2' YEAR | '2' |
        | ival_y % bin | INTERVAL '2' YEAR | CAST('2' AS BINARY) |
        | ival_y % date | INTERVAL '2' YEAR | DATE'2024-01-15' |
        | ival_y % ts | INTERVAL '2' YEAR | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_y % ts_ntz | INTERVAL '2' YEAR | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_y % ival_d | INTERVAL '2' YEAR | INTERVAL '2' DAY |
        | ival_y % ival_dt | INTERVAL '2' YEAR | INTERVAL '25' HOUR |
        | ival_y % ival_ds | INTERVAL '2' YEAR | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_y % ival_m | INTERVAL '2' YEAR | INTERVAL '2' MONTH |
        | ival_y % ival_y | INTERVAL '2' YEAR | INTERVAL '2' YEAR |
        | ival_y % ival_ym | INTERVAL '2' YEAR | INTERVAL '1-2' YEAR TO MONTH |
        | ival_y % calendar | INTERVAL '2' YEAR | make_interval(0,1,0,1,0,0,0) |
        | ival_y % array | INTERVAL '2' YEAR | array(1,2) |
        | ival_y % map | INTERVAL '2' YEAR | map('a',1) |
        | ival_y % struct | INTERVAL '2' YEAR | named_struct('a',1) |
        | ival_ym % unull | INTERVAL '1-2' YEAR TO MONTH | NULL |
        | ival_ym % null | INTERVAL '1-2' YEAR TO MONTH | CAST(NULL AS INT) |
        | ival_ym % bool | INTERVAL '1-2' YEAR TO MONTH | true |
        | ival_ym % tinyint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS TINYINT) |
        | ival_ym % smallint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS SMALLINT) |
        | ival_ym % int | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS INT) |
        | ival_ym % bigint | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS BIGINT) |
        | ival_ym % float | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS FLOAT) |
        | ival_ym % double | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DOUBLE) |
        | ival_ym % dec | INTERVAL '1-2' YEAR TO MONTH | CAST(2 AS DECIMAL(10,2)) |
        | ival_ym % str | INTERVAL '1-2' YEAR TO MONTH | '2' |
        | ival_ym % bin | INTERVAL '1-2' YEAR TO MONTH | CAST('2' AS BINARY) |
        | ival_ym % date | INTERVAL '1-2' YEAR TO MONTH | DATE'2024-01-15' |
        | ival_ym % ts | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP'2024-01-15 12:00:00' |
        | ival_ym % ts_ntz | INTERVAL '1-2' YEAR TO MONTH | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | ival_ym % ival_d | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' DAY |
        | ival_ym % ival_dt | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '25' HOUR |
        | ival_ym % ival_ds | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1 02:03:04' DAY TO SECOND |
        | ival_ym % ival_m | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' MONTH |
        | ival_ym % ival_y | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '2' YEAR |
        | ival_ym % ival_ym | INTERVAL '1-2' YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH |
        | ival_ym % calendar | INTERVAL '1-2' YEAR TO MONTH | make_interval(0,1,0,1,0,0,0) |
        | ival_ym % array | INTERVAL '1-2' YEAR TO MONTH | array(1,2) |
        | ival_ym % map | INTERVAL '1-2' YEAR TO MONTH | map('a',1) |
        | ival_ym % struct | INTERVAL '1-2' YEAR TO MONTH | named_struct('a',1) |
        | calendar % unull | make_interval(0,1,0,1,0,0,0) | NULL |
        | calendar % null | make_interval(0,1,0,1,0,0,0) | CAST(NULL AS INT) |
        | calendar % bool | make_interval(0,1,0,1,0,0,0) | true |
        | calendar % tinyint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS TINYINT) |
        | calendar % smallint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS SMALLINT) |
        | calendar % int | make_interval(0,1,0,1,0,0,0) | CAST(2 AS INT) |
        | calendar % bigint | make_interval(0,1,0,1,0,0,0) | CAST(2 AS BIGINT) |
        | calendar % float | make_interval(0,1,0,1,0,0,0) | CAST(2 AS FLOAT) |
        | calendar % double | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DOUBLE) |
        | calendar % dec | make_interval(0,1,0,1,0,0,0) | CAST(2 AS DECIMAL(10,2)) |
        | calendar % str | make_interval(0,1,0,1,0,0,0) | '2' |
        | calendar % bin | make_interval(0,1,0,1,0,0,0) | CAST('2' AS BINARY) |
        | calendar % date | make_interval(0,1,0,1,0,0,0) | DATE'2024-01-15' |
        | calendar % ts | make_interval(0,1,0,1,0,0,0) | TIMESTAMP'2024-01-15 12:00:00' |
        | calendar % ts_ntz | make_interval(0,1,0,1,0,0,0) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | calendar % ival_d | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' DAY |
        | calendar % ival_dt | make_interval(0,1,0,1,0,0,0) | INTERVAL '25' HOUR |
        | calendar % ival_ds | make_interval(0,1,0,1,0,0,0) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | calendar % ival_m | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' MONTH |
        | calendar % ival_y | make_interval(0,1,0,1,0,0,0) | INTERVAL '2' YEAR |
        | calendar % ival_ym | make_interval(0,1,0,1,0,0,0) | INTERVAL '1-2' YEAR TO MONTH |
        | calendar % calendar | make_interval(0,1,0,1,0,0,0) | make_interval(0,1,0,1,0,0,0) |
        | calendar % array | make_interval(0,1,0,1,0,0,0) | array(1,2) |
        | calendar % map | make_interval(0,1,0,1,0,0,0) | map('a',1) |
        | calendar % struct | make_interval(0,1,0,1,0,0,0) | named_struct('a',1) |
        | array % unull | array(1,2) | NULL |
        | array % null | array(1,2) | CAST(NULL AS INT) |
        | array % bool | array(1,2) | true |
        | array % tinyint | array(1,2) | CAST(2 AS TINYINT) |
        | array % smallint | array(1,2) | CAST(2 AS SMALLINT) |
        | array % int | array(1,2) | CAST(2 AS INT) |
        | array % bigint | array(1,2) | CAST(2 AS BIGINT) |
        | array % float | array(1,2) | CAST(2 AS FLOAT) |
        | array % double | array(1,2) | CAST(2 AS DOUBLE) |
        | array % dec | array(1,2) | CAST(2 AS DECIMAL(10,2)) |
        | array % str | array(1,2) | '2' |
        | array % bin | array(1,2) | CAST('2' AS BINARY) |
        | array % date | array(1,2) | DATE'2024-01-15' |
        | array % ts | array(1,2) | TIMESTAMP'2024-01-15 12:00:00' |
        | array % ts_ntz | array(1,2) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | array % ival_d | array(1,2) | INTERVAL '2' DAY |
        | array % ival_dt | array(1,2) | INTERVAL '25' HOUR |
        | array % ival_ds | array(1,2) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | array % ival_m | array(1,2) | INTERVAL '2' MONTH |
        | array % ival_y | array(1,2) | INTERVAL '2' YEAR |
        | array % ival_ym | array(1,2) | INTERVAL '1-2' YEAR TO MONTH |
        | array % calendar | array(1,2) | make_interval(0,1,0,1,0,0,0) |
        | array % array | array(1,2) | array(1,2) |
        | array % map | array(1,2) | map('a',1) |
        | array % struct | array(1,2) | named_struct('a',1) |
        | map % unull | map('a',1) | NULL |
        | map % null | map('a',1) | CAST(NULL AS INT) |
        | map % bool | map('a',1) | true |
        | map % tinyint | map('a',1) | CAST(2 AS TINYINT) |
        | map % smallint | map('a',1) | CAST(2 AS SMALLINT) |
        | map % int | map('a',1) | CAST(2 AS INT) |
        | map % bigint | map('a',1) | CAST(2 AS BIGINT) |
        | map % float | map('a',1) | CAST(2 AS FLOAT) |
        | map % double | map('a',1) | CAST(2 AS DOUBLE) |
        | map % dec | map('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | map % str | map('a',1) | '2' |
        | map % bin | map('a',1) | CAST('2' AS BINARY) |
        | map % date | map('a',1) | DATE'2024-01-15' |
        | map % ts | map('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | map % ts_ntz | map('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | map % ival_d | map('a',1) | INTERVAL '2' DAY |
        | map % ival_dt | map('a',1) | INTERVAL '25' HOUR |
        | map % ival_ds | map('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | map % ival_m | map('a',1) | INTERVAL '2' MONTH |
        | map % ival_y | map('a',1) | INTERVAL '2' YEAR |
        | map % ival_ym | map('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | map % calendar | map('a',1) | make_interval(0,1,0,1,0,0,0) |
        | map % array | map('a',1) | array(1,2) |
        | map % map | map('a',1) | map('a',1) |
        | map % struct | map('a',1) | named_struct('a',1) |
        | struct % unull | named_struct('a',1) | NULL |
        | struct % null | named_struct('a',1) | CAST(NULL AS INT) |
        | struct % bool | named_struct('a',1) | true |
        | struct % tinyint | named_struct('a',1) | CAST(2 AS TINYINT) |
        | struct % smallint | named_struct('a',1) | CAST(2 AS SMALLINT) |
        | struct % int | named_struct('a',1) | CAST(2 AS INT) |
        | struct % bigint | named_struct('a',1) | CAST(2 AS BIGINT) |
        | struct % float | named_struct('a',1) | CAST(2 AS FLOAT) |
        | struct % double | named_struct('a',1) | CAST(2 AS DOUBLE) |
        | struct % dec | named_struct('a',1) | CAST(2 AS DECIMAL(10,2)) |
        | struct % str | named_struct('a',1) | '2' |
        | struct % bin | named_struct('a',1) | CAST('2' AS BINARY) |
        | struct % date | named_struct('a',1) | DATE'2024-01-15' |
        | struct % ts | named_struct('a',1) | TIMESTAMP'2024-01-15 12:00:00' |
        | struct % ts_ntz | named_struct('a',1) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | struct % ival_d | named_struct('a',1) | INTERVAL '2' DAY |
        | struct % ival_dt | named_struct('a',1) | INTERVAL '25' HOUR |
        | struct % ival_ds | named_struct('a',1) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | struct % ival_m | named_struct('a',1) | INTERVAL '2' MONTH |
        | struct % ival_y | named_struct('a',1) | INTERVAL '2' YEAR |
        | struct % ival_ym | named_struct('a',1) | INTERVAL '1-2' YEAR TO MONTH |
        | struct % calendar | named_struct('a',1) | make_interval(0,1,0,1,0,0,0) |
        | struct % array | named_struct('a',1) | array(1,2) |
        | struct % map | named_struct('a',1) | map('a',1) |
        | struct % struct | named_struct('a',1) | named_struct('a',1) |

    @spark-4
    Scenario Outline: modulo ansi-on: rejected pair, VARIANT operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % variant | NULL | parse_json('{"a":1}') |
        | null % variant | CAST(NULL AS INT) | parse_json('{"a":1}') |
        | bool % variant | true | parse_json('{"a":1}') |
        | tinyint % variant | CAST(2 AS TINYINT) | parse_json('{"a":1}') |
        | smallint % variant | CAST(2 AS SMALLINT) | parse_json('{"a":1}') |
        | int % variant | CAST(2 AS INT) | parse_json('{"a":1}') |
        | bigint % variant | CAST(2 AS BIGINT) | parse_json('{"a":1}') |
        | float % variant | CAST(2 AS FLOAT) | parse_json('{"a":1}') |
        | double % variant | CAST(2 AS DOUBLE) | parse_json('{"a":1}') |
        | dec % variant | CAST(2 AS DECIMAL(10,2)) | parse_json('{"a":1}') |
        | str % variant | '2' | parse_json('{"a":1}') |
        | bin % variant | CAST('2' AS BINARY) | parse_json('{"a":1}') |
        | date % variant | DATE'2024-01-15' | parse_json('{"a":1}') |
        | ts % variant | TIMESTAMP'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ts_ntz % variant | TIMESTAMP_NTZ'2024-01-15 12:00:00' | parse_json('{"a":1}') |
        | ival_d % variant | INTERVAL '2' DAY | parse_json('{"a":1}') |
        | ival_dt % variant | INTERVAL '25' HOUR | parse_json('{"a":1}') |
        | ival_ds % variant | INTERVAL '1 02:03:04' DAY TO SECOND | parse_json('{"a":1}') |
        | ival_m % variant | INTERVAL '2' MONTH | parse_json('{"a":1}') |
        | ival_y % variant | INTERVAL '2' YEAR | parse_json('{"a":1}') |
        | ival_ym % variant | INTERVAL '1-2' YEAR TO MONTH | parse_json('{"a":1}') |
        | calendar % variant | make_interval(0,1,0,1,0,0,0) | parse_json('{"a":1}') |
        | array % variant | array(1,2) | parse_json('{"a":1}') |
        | map % variant | map('a',1) | parse_json('{"a":1}') |
        | struct % variant | named_struct('a',1) | parse_json('{"a":1}') |
        | variant % unull | parse_json('{"a":1}') | NULL |
        | variant % null | parse_json('{"a":1}') | CAST(NULL AS INT) |
        | variant % bool | parse_json('{"a":1}') | true |
        | variant % tinyint | parse_json('{"a":1}') | CAST(2 AS TINYINT) |
        | variant % smallint | parse_json('{"a":1}') | CAST(2 AS SMALLINT) |
        | variant % int | parse_json('{"a":1}') | CAST(2 AS INT) |
        | variant % bigint | parse_json('{"a":1}') | CAST(2 AS BIGINT) |
        | variant % float | parse_json('{"a":1}') | CAST(2 AS FLOAT) |
        | variant % double | parse_json('{"a":1}') | CAST(2 AS DOUBLE) |
        | variant % dec | parse_json('{"a":1}') | CAST(2 AS DECIMAL(10,2)) |
        | variant % str | parse_json('{"a":1}') | '2' |
        | variant % bin | parse_json('{"a":1}') | CAST('2' AS BINARY) |
        | variant % date | parse_json('{"a":1}') | DATE'2024-01-15' |
        | variant % ts | parse_json('{"a":1}') | TIMESTAMP'2024-01-15 12:00:00' |
        | variant % ts_ntz | parse_json('{"a":1}') | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | variant % ival_d | parse_json('{"a":1}') | INTERVAL '2' DAY |
        | variant % ival_dt | parse_json('{"a":1}') | INTERVAL '25' HOUR |
        | variant % ival_ds | parse_json('{"a":1}') | INTERVAL '1 02:03:04' DAY TO SECOND |
        | variant % ival_m | parse_json('{"a":1}') | INTERVAL '2' MONTH |
        | variant % ival_y | parse_json('{"a":1}') | INTERVAL '2' YEAR |
        | variant % ival_ym | parse_json('{"a":1}') | INTERVAL '1-2' YEAR TO MONTH |
        | variant % calendar | parse_json('{"a":1}') | make_interval(0,1,0,1,0,0,0) |
        | variant % array | parse_json('{"a":1}') | array(1,2) |
        | variant % map | parse_json('{"a":1}') | map('a',1) |
        | variant % struct | parse_json('{"a":1}') | named_struct('a',1) |
        | variant % variant | parse_json('{"a":1}') | parse_json('{"a":1}') |

    @spark-4.1
    Scenario Outline: modulo ansi-on: rejected pair, TIME operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % time | NULL | TIME '12:00:00' |
        | null % time | CAST(NULL AS INT) | TIME '12:00:00' |
        | bool % time | true | TIME '12:00:00' |
        | tinyint % time | CAST(2 AS TINYINT) | TIME '12:00:00' |
        | smallint % time | CAST(2 AS SMALLINT) | TIME '12:00:00' |
        | int % time | CAST(2 AS INT) | TIME '12:00:00' |
        | bigint % time | CAST(2 AS BIGINT) | TIME '12:00:00' |
        | float % time | CAST(2 AS FLOAT) | TIME '12:00:00' |
        | double % time | CAST(2 AS DOUBLE) | TIME '12:00:00' |
        | dec % time | CAST(2 AS DECIMAL(10,2)) | TIME '12:00:00' |
        | str % time | '2' | TIME '12:00:00' |
        | bin % time | CAST('2' AS BINARY) | TIME '12:00:00' |
        | date % time | DATE'2024-01-15' | TIME '12:00:00' |
        | ts % time | TIMESTAMP'2024-01-15 12:00:00' | TIME '12:00:00' |
        | ts_ntz % time | TIMESTAMP_NTZ'2024-01-15 12:00:00' | TIME '12:00:00' |
        | time % unull | TIME '12:00:00' | NULL |
        | time % null | TIME '12:00:00' | CAST(NULL AS INT) |
        | time % bool | TIME '12:00:00' | true |
        | time % tinyint | TIME '12:00:00' | CAST(2 AS TINYINT) |
        | time % smallint | TIME '12:00:00' | CAST(2 AS SMALLINT) |
        | time % int | TIME '12:00:00' | CAST(2 AS INT) |
        | time % bigint | TIME '12:00:00' | CAST(2 AS BIGINT) |
        | time % float | TIME '12:00:00' | CAST(2 AS FLOAT) |
        | time % double | TIME '12:00:00' | CAST(2 AS DOUBLE) |
        | time % dec | TIME '12:00:00' | CAST(2 AS DECIMAL(10,2)) |
        | time % str | TIME '12:00:00' | '2' |
        | time % bin | TIME '12:00:00' | CAST('2' AS BINARY) |
        | time % date | TIME '12:00:00' | DATE'2024-01-15' |
        | time % ts | TIME '12:00:00' | TIMESTAMP'2024-01-15 12:00:00' |
        | time % ts_ntz | TIME '12:00:00' | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | time % time | TIME '12:00:00' | TIME '12:00:00' |
        | time % ival_d | TIME '12:00:00' | INTERVAL '2' DAY |
        | time % ival_dt | TIME '12:00:00' | INTERVAL '25' HOUR |
        | time % ival_ds | TIME '12:00:00' | INTERVAL '1 02:03:04' DAY TO SECOND |
        | time % ival_m | TIME '12:00:00' | INTERVAL '2' MONTH |
        | time % ival_y | TIME '12:00:00' | INTERVAL '2' YEAR |
        | time % ival_ym | TIME '12:00:00' | INTERVAL '1-2' YEAR TO MONTH |
        | time % calendar | TIME '12:00:00' | make_interval(0,1,0,1,0,0,0) |
        | time % array | TIME '12:00:00' | array(1,2) |
        | time % map | TIME '12:00:00' | map('a',1) |
        | time % struct | TIME '12:00:00' | named_struct('a',1) |
        | time % variant | TIME '12:00:00' | parse_json('{"a":1}') |
        | ival_d % time | INTERVAL '2' DAY | TIME '12:00:00' |
        | ival_dt % time | INTERVAL '25' HOUR | TIME '12:00:00' |
        | ival_ds % time | INTERVAL '1 02:03:04' DAY TO SECOND | TIME '12:00:00' |
        | ival_m % time | INTERVAL '2' MONTH | TIME '12:00:00' |
        | ival_y % time | INTERVAL '2' YEAR | TIME '12:00:00' |
        | ival_ym % time | INTERVAL '1-2' YEAR TO MONTH | TIME '12:00:00' |
        | calendar % time | make_interval(0,1,0,1,0,0,0) | TIME '12:00:00' |
        | array % time | array(1,2) | TIME '12:00:00' |
        | map % time | map('a',1) | TIME '12:00:00' |
        | struct % time | named_struct('a',1) | TIME '12:00:00' |
        | variant % time | parse_json('{"a":1}') | TIME '12:00:00' |

    @spark-4.2
    Scenario Outline: modulo ansi-on: rejected pair, GEOMETRY operand: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof((<l>) % (<r>)) AS t
        """
      Then query error (?i)cannot resolve

      Examples:
        | case | l | r |
        | unull % geom | NULL | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | null % geom | CAST(NULL AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bool % geom | true | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | tinyint % geom | CAST(2 AS TINYINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | smallint % geom | CAST(2 AS SMALLINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | int % geom | CAST(2 AS INT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bigint % geom | CAST(2 AS BIGINT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | float % geom | CAST(2 AS FLOAT) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | double % geom | CAST(2 AS DOUBLE) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | dec % geom | CAST(2 AS DECIMAL(10,2)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | str % geom | '2' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | bin % geom | CAST('2' AS BINARY) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | date % geom | DATE'2024-01-15' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts % geom | TIMESTAMP'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ts_ntz % geom | TIMESTAMP_NTZ'2024-01-15 12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | time % geom | TIME '12:00:00' | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_d % geom | INTERVAL '2' DAY | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_dt % geom | INTERVAL '25' HOUR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ds % geom | INTERVAL '1 02:03:04' DAY TO SECOND | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_m % geom | INTERVAL '2' MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_y % geom | INTERVAL '2' YEAR | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | ival_ym % geom | INTERVAL '1-2' YEAR TO MONTH | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | calendar % geom | make_interval(0,1,0,1,0,0,0) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | array % geom | array(1,2) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | map % geom | map('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | struct % geom | named_struct('a',1) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | variant % geom | parse_json('{"a":1}') | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |
        | geom % unull | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | NULL |
        | geom % null | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(NULL AS INT) |
        | geom % bool | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | true |
        | geom % tinyint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS TINYINT) |
        | geom % smallint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS SMALLINT) |
        | geom % int | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS INT) |
        | geom % bigint | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS BIGINT) |
        | geom % float | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS FLOAT) |
        | geom % double | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DOUBLE) |
        | geom % dec | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST(2 AS DECIMAL(10,2)) |
        | geom % str | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | '2' |
        | geom % bin | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | CAST('2' AS BINARY) |
        | geom % date | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | DATE'2024-01-15' |
        | geom % ts | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP'2024-01-15 12:00:00' |
        | geom % ts_ntz | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIMESTAMP_NTZ'2024-01-15 12:00:00' |
        | geom % time | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | TIME '12:00:00' |
        | geom % ival_d | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' DAY |
        | geom % ival_dt | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '25' HOUR |
        | geom % ival_ds | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1 02:03:04' DAY TO SECOND |
        | geom % ival_m | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' MONTH |
        | geom % ival_y | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '2' YEAR |
        | geom % ival_ym | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | INTERVAL '1-2' YEAR TO MONTH |
        | geom % calendar | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | make_interval(0,1,0,1,0,0,0) |
        | geom % array | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | array(1,2) |
        | geom % map | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | map('a',1) |
        | geom % struct | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | named_struct('a',1) |
        | geom % variant | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | parse_json('{"a":1}') |
        | geom % geom | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) | st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) |

  # The operand TYPE NAME the message carries is its own parity axis, and the rows above are
  # blind to it: they assert that a rejection happened, not what it called the operand. Naming
  # the Arrow storage type instead of the Spark type is the exact leak this guard exists to
  # remove, so it needs its own assertions.
  # Plain `Scenario`, not `Scenario Outline`: an `ARRAY<INT>` inside an Outline would be read as
  # an Examples placeholder. The assertion is a bare substring so it holds on BOTH engines --
  # Sail writes `... with operand types ARRAY<INT> and INT`, Spark writes
  # `... incompatible types ("ARRAY<INT>" and "INT")`.
  Rule: the reject message names the Spark type, not the Arrow storage type

    Scenario: an ARRAY operand is named with its element type
      When query
        """
        SELECT array(1, 2) + 1
        """
      Then query error (?i)cannot resolve.*ARRAY<INT>

    Scenario: a MAP operand is named with its key and value types
      When query
        """
        SELECT map('a', 1) + 1
        """
      Then query error (?i)cannot resolve.*MAP<STRING, INT>

    # VARIANT is stored as a struct of two binary fields in Sail, so without the dedicated
    # arm the message would leak `STRUCT<value: BINARY, metadata: BINARY>`.
    @spark-4
    Scenario: a VARIANT operand is named VARIANT, not its storage struct
      When query
        """
        SELECT parse_json('{"a": 1}') * 2
        """
      Then query error (?i)cannot resolve.*VARIANT

    Scenario: a BOOLEAN operand is named BOOLEAN
      When query
        """
        SELECT true / 2
        """
      Then query error (?i)cannot resolve.*BOOLEAN

    Scenario: a BINARY operand is named BINARY
      When query
        """
        SELECT CAST('6' AS BINARY) % 2
        """
      Then query error (?i)cannot resolve.*BINARY

    @spark-4.1
    Scenario: a TIME operand carries its precision
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '12:00:00' * 2
        """
      Then query error (?i)cannot resolve.*TIME\(6\)

    # Spark's legacy CalendarIntervalType is plain `INTERVAL` (`CalendarIntervalType.scala:40`),
    # NOT `INTERVAL DAY TO SECOND` -- Spark says `the binary operator requires the input type
    # "NUMERIC", not "INTERVAL"`. The negative lookahead is what makes this discriminating: it
    # fails if the name widens back to `INTERVAL DAY TO SECOND` or `INTERVAL YEAR TO MONTH`.
    Scenario: a calendar INTERVAL operand is named INTERVAL, not the day-time spelling
      When query
        """
        SELECT make_interval(0, 1, 0, 1, 0, 0, 0) % make_interval(0, 1, 0, 1, 0, 0, 0)
        """
      Then query error (?i)cannot resolve.*\bINTERVAL\b(?!\s+(DAY|YEAR))


    # Sail stores GEOMETRY/GEOGRAPHY as Arrow `Binary` and keeps the extension metadata on
    # the `Field`, while `spark_type_name` only receives a `DataType` -- the semantic name is
    # not recoverable at that signature. Spark names the type and its SRID.
    @sail-bug
    @spark-4.2
    Scenario: a GEOMETRY operand is named GEOMETRY with its SRID
      When query
        """
        SELECT st_geomfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) + 1
        """
      Then query error (?i)cannot resolve.*GEOMETRY\(0\)

    @sail-bug
    @spark-4.2
    Scenario: a GEOGRAPHY operand is named GEOGRAPHY with its SRID
      When query
        """
        SELECT st_geogfromwkb(CAST('0101000000000000000000F03F000000000000F03F' AS BINARY)) + 1
        """
      Then query error (?i)cannot resolve.*GEOGRAPHY\(4326\)

    # Spark reports the field as NOT NULL because `named_struct` gives it a non-nullable
    # field; Sail declares it nullable. The divergence is in `named_struct`, not in the namer.
    @sail-bug
    Scenario: a STRUCT operand carries its field nullability
      When query
        """
        SELECT named_struct('a', 1) + 1
        """
      Then query error (?i)cannot resolve.*STRUCT<a: INT NOT NULL>
