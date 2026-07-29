@cast
@cast_nullability
Feature: nullability of an explicit CAST

  Cast.nullable = child.nullable || Cast.forceNullable(from, to) (Cast.scala:656).
  Every input below is a non-null literal, so `nullable = true` means one thing only:
  that coercion is force-nullable (Cast.scala:452-473).

  VARIANT and INTERVAL cells are split into `@spark-4` outlines: PySpark 3.5 has no
  VARIANT type and renders both interval types as a bare `interval` in `treeString`.

  @spark_null
  Rule: scalar casts, non-nullable

    Scenario Outline: a scalar CAST that keeps the input non-nullable: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = false)
        """

      Examples:
        | case                           | input                               | type          | result        |
        | bigint -> bigint               | CAST(1 AS BIGINT)                   | BIGINT        | long          |
        | bigint -> boolean              | CAST(1 AS BIGINT)                   | BOOLEAN       | boolean       |
        | bigint -> double               | CAST(1 AS BIGINT)                   | DOUBLE        | double        |
        | bigint -> float                | CAST(1 AS BIGINT)                   | FLOAT         | float         |
        | bigint -> int                  | CAST(1 AS BIGINT)                   | INT           | integer       |
        | bigint -> smallint             | CAST(1 AS BIGINT)                   | SMALLINT      | short         |
        | bigint -> string               | CAST(1 AS BIGINT)                   | STRING        | string        |
        | bigint -> timestamp            | CAST(1 AS BIGINT)                   | TIMESTAMP     | timestamp     |
        | bigint -> tinyint              | CAST(1 AS BIGINT)                   | TINYINT       | byte          |
        | binary -> binary               | X'01'                               | BINARY        | binary        |
        | binary -> string               | X'01'                               | STRING        | string        |
        | boolean -> bigint              | true                                | BIGINT        | long          |
        | boolean -> boolean             | true                                | BOOLEAN       | boolean       |
        | boolean -> decimal(10,2)       | true                                | DECIMAL(10,2) | decimal(10,2) |
        | boolean -> double              | true                                | DOUBLE        | double        |
        | boolean -> float               | true                                | FLOAT         | float         |
        | boolean -> int                 | true                                | INT           | integer       |
        | boolean -> smallint            | true                                | SMALLINT      | short         |
        | boolean -> string              | true                                | STRING        | string        |
        | boolean -> tinyint             | true                                | TINYINT       | byte          |
        | date -> date                   | DATE '2024-01-15'                   | DATE          | date          |
        | date -> timestamp              | DATE '2024-01-15'                   | TIMESTAMP     | timestamp     |
        | decimal(10,2) -> boolean       | CAST(1.5 AS DECIMAL(10,2))          | BOOLEAN       | boolean       |
        | decimal(10,2) -> decimal(10,2) | CAST(1.5 AS DECIMAL(10,2))          | DECIMAL(10,2) | decimal(10,2) |
        | decimal(10,2) -> double        | CAST(1.5 AS DECIMAL(10,2))          | DOUBLE        | double        |
        | decimal(10,2) -> float         | CAST(1.5 AS DECIMAL(10,2))          | FLOAT         | float         |
        | decimal(10,2) -> string        | CAST(1.5 AS DECIMAL(10,2))          | STRING        | string        |
        | decimal(10,2) -> timestamp     | CAST(1.5 AS DECIMAL(10,2))          | TIMESTAMP     | timestamp     |
        | double -> boolean              | CAST(1.5 AS DOUBLE)                 | BOOLEAN       | boolean       |
        | double -> double               | CAST(1.5 AS DOUBLE)                 | DOUBLE        | double        |
        | double -> float                | CAST(1.5 AS DOUBLE)                 | FLOAT         | float         |
        | double -> string               | CAST(1.5 AS DOUBLE)                 | STRING        | string        |
        | float -> boolean               | CAST(1.5 AS FLOAT)                  | BOOLEAN       | boolean       |
        | float -> double                | CAST(1.5 AS FLOAT)                  | DOUBLE        | double        |
        | float -> float                 | CAST(1.5 AS FLOAT)                  | FLOAT         | float         |
        | float -> string                | CAST(1.5 AS FLOAT)                  | STRING        | string        |
        | int -> bigint                  | 1                                   | BIGINT        | long          |
        | int -> boolean                 | 1                                   | BOOLEAN       | boolean       |
        | int -> double                  | 1                                   | DOUBLE        | double        |
        | int -> float                   | 1                                   | FLOAT         | float         |
        | int -> int                     | 1                                   | INT           | integer       |
        | int -> smallint                | 1                                   | SMALLINT      | short         |
        | int -> string                  | 1                                   | STRING        | string        |
        | int -> timestamp               | 1                                   | TIMESTAMP     | timestamp     |
        | int -> tinyint                 | 1                                   | TINYINT       | byte          |
        | smallint -> bigint             | CAST(1 AS SMALLINT)                 | BIGINT        | long          |
        | smallint -> boolean            | CAST(1 AS SMALLINT)                 | BOOLEAN       | boolean       |
        | smallint -> decimal(10,2)      | CAST(1 AS SMALLINT)                 | DECIMAL(10,2) | decimal(10,2) |
        | smallint -> double             | CAST(1 AS SMALLINT)                 | DOUBLE        | double        |
        | smallint -> float              | CAST(1 AS SMALLINT)                 | FLOAT         | float         |
        | smallint -> int                | CAST(1 AS SMALLINT)                 | INT           | integer       |
        | smallint -> smallint           | CAST(1 AS SMALLINT)                 | SMALLINT      | short         |
        | smallint -> string             | CAST(1 AS SMALLINT)                 | STRING        | string        |
        | smallint -> timestamp          | CAST(1 AS SMALLINT)                 | TIMESTAMP     | timestamp     |
        | smallint -> tinyint            | CAST(1 AS SMALLINT)                 | TINYINT       | byte          |
        | string -> binary               | '1'                                 | BINARY        | binary        |
        | string -> string               | '1'                                 | STRING        | string        |
        | timestamp -> bigint            | TIMESTAMP '2024-01-15 10:00:00'     | BIGINT        | long          |
        | timestamp -> date              | TIMESTAMP '2024-01-15 10:00:00'     | DATE          | date          |
        | timestamp -> double            | TIMESTAMP '2024-01-15 10:00:00'     | DOUBLE        | double        |
        | timestamp -> float             | TIMESTAMP '2024-01-15 10:00:00'     | FLOAT         | float         |
        | timestamp -> timestamp         | TIMESTAMP '2024-01-15 10:00:00'     | TIMESTAMP     | timestamp     |
        | timestamp -> timestamp_ntz     | TIMESTAMP '2024-01-15 10:00:00'     | TIMESTAMP_NTZ | timestamp_ntz |
        | timestamp_ntz -> timestamp     | TIMESTAMP_NTZ '2024-01-15 10:00:00' | TIMESTAMP     | timestamp     |
        | timestamp_ntz -> timestamp_ntz | TIMESTAMP_NTZ '2024-01-15 10:00:00' | TIMESTAMP_NTZ | timestamp_ntz |
        | tinyint -> bigint              | CAST(1 AS TINYINT)                  | BIGINT        | long          |
        | tinyint -> boolean             | CAST(1 AS TINYINT)                  | BOOLEAN       | boolean       |
        | tinyint -> decimal(10,2)       | CAST(1 AS TINYINT)                  | DECIMAL(10,2) | decimal(10,2) |
        | tinyint -> double              | CAST(1 AS TINYINT)                  | DOUBLE        | double        |
        | tinyint -> float               | CAST(1 AS TINYINT)                  | FLOAT         | float         |
        | tinyint -> int                 | CAST(1 AS TINYINT)                  | INT           | integer       |
        | tinyint -> smallint            | CAST(1 AS TINYINT)                  | SMALLINT      | short         |
        | tinyint -> string              | CAST(1 AS TINYINT)                  | STRING        | string        |
        | tinyint -> timestamp           | CAST(1 AS TINYINT)                  | TIMESTAMP     | timestamp     |
        | tinyint -> tinyint             | CAST(1 AS TINYINT)                  | TINYINT       | byte          |
        | array<int> -> string           | array(1, 2)                         | STRING        | string        |
        | date -> string                 | DATE '2024-01-15'                   | STRING        | string        |
        | timestamp -> string            | TIMESTAMP '2024-01-15 10:00:00'     | STRING        | string        |
        | timestamp_ntz -> string        | TIMESTAMP_NTZ '2024-01-15 10:00:00' | STRING        | string        |

  @spark_null
  Rule: scalar casts, non-nullable, VARIANT and INTERVAL types

    @spark-4
    Scenario Outline: a scalar CAST that keeps the input non-nullable: <case>, on Spark 4+
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = false)
        """

      Examples:
        | case                          | input                               | type                   | result                 |
        | bigint -> interval d-s        | CAST(1 AS BIGINT)                   | INTERVAL DAY TO SECOND | interval day to second |
        | bigint -> interval y-m        | CAST(1 AS BIGINT)                   | INTERVAL YEAR TO MONTH | interval year to month |
        | decimal(10,2) -> interval d-s | CAST(1.5 AS DECIMAL(10,2))          | INTERVAL DAY TO SECOND | interval day to second |
        | decimal(10,2) -> interval y-m | CAST(1.5 AS DECIMAL(10,2))          | INTERVAL YEAR TO MONTH | interval year to month |
        | int -> interval d-s           | 1                                   | INTERVAL DAY TO SECOND | interval day to second |
        | int -> interval y-m           | 1                                   | INTERVAL YEAR TO MONTH | interval year to month |
        | interval d-s -> bigint        | INTERVAL '1 12:30:01' DAY TO SECOND | BIGINT                 | long                   |
        | interval d-s -> int           | INTERVAL '1 12:30:01' DAY TO SECOND | INT                    | integer                |
        | interval d-s -> interval d-s  | INTERVAL '1 12:30:01' DAY TO SECOND | INTERVAL DAY TO SECOND | interval day to second |
        | interval d-s -> smallint      | INTERVAL '1 12:30:01' DAY TO SECOND | SMALLINT               | short                  |
        | interval d-s -> tinyint       | INTERVAL '1 12:30:01' DAY TO SECOND | TINYINT                | byte                   |
        | interval y-m -> bigint        | INTERVAL '1-2' YEAR TO MONTH        | BIGINT                 | long                   |
        | interval y-m -> int           | INTERVAL '1-2' YEAR TO MONTH        | INT                    | integer                |
        | interval y-m -> interval y-m  | INTERVAL '1-2' YEAR TO MONTH        | INTERVAL YEAR TO MONTH | interval year to month |
        | interval y-m -> smallint      | INTERVAL '1-2' YEAR TO MONTH        | SMALLINT               | short                  |
        | interval y-m -> tinyint       | INTERVAL '1-2' YEAR TO MONTH        | TINYINT                | byte                   |
        | smallint -> interval d-s      | CAST(1 AS SMALLINT)                 | INTERVAL DAY TO SECOND | interval day to second |
        | smallint -> interval y-m      | CAST(1 AS SMALLINT)                 | INTERVAL YEAR TO MONTH | interval year to month |
        | tinyint -> interval d-s       | CAST(1 AS TINYINT)                  | INTERVAL DAY TO SECOND | interval day to second |
        | tinyint -> interval y-m       | CAST(1 AS TINYINT)                  | INTERVAL YEAR TO MONTH | interval year to month |
        | array<int> -> variant         | array(1, 2)                         | VARIANT                | variant                |
        | bigint -> variant             | CAST(1 AS BIGINT)                   | VARIANT                | variant                |
        | binary -> variant             | X'01'                               | VARIANT                | variant                |
        | boolean -> variant            | true                                | VARIANT                | variant                |
        | decimal(10,2) -> variant      | CAST(1.5 AS DECIMAL(10,2))          | VARIANT                | variant                |
        | double -> variant             | CAST(1.5 AS DOUBLE)                 | VARIANT                | variant                |
        | float -> variant              | CAST(1.5 AS FLOAT)                  | VARIANT                | variant                |
        | int -> variant                | 1                                   | VARIANT                | variant                |
        | interval d-s -> string        | INTERVAL '1 12:30:01' DAY TO SECOND | STRING                 | string                 |
        | interval y-m -> string        | INTERVAL '1-2' YEAR TO MONTH        | STRING                 | string                 |
        | smallint -> variant           | CAST(1 AS SMALLINT)                 | VARIANT                | variant                |
        | timestamp -> variant          | TIMESTAMP '2024-01-15 10:00:00'     | VARIANT                | variant                |
        | timestamp_ntz -> variant      | TIMESTAMP_NTZ '2024-01-15 10:00:00' | VARIANT                | variant                |
        | tinyint -> variant            | CAST(1 AS TINYINT)                  | VARIANT                | variant                |

  @spark_null
  Rule: scalar casts, non-nullable (Sail diverges)

    @sail-bug
    Scenario Outline: a scalar CAST that keeps the input non-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = false)
        """

      Examples:
        | case                      | input                | type   | result |
        | map<string,int> -> string | map('a', 1)          | STRING | string |
        | struct<a:int> -> string   | named_struct('a', 1) | STRING | string |

  @spark_null
  Rule: scalar casts, non-nullable (Sail diverges), VARIANT and INTERVAL types

    @sail-bug
    @spark-4
    Scenario Outline: a scalar CAST that keeps the input non-nullable but Sail does not: <case>, on Spark 4+
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = false)
        """

      Examples:
        | case               | input           | type    | result  |
        | variant -> variant | PARSE_JSON('1') | VARIANT | variant |

  @spark_null
  Rule: scalar casts, force-nullable

    Scenario Outline: a scalar CAST that is force-nullable: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = true)
        """

      Examples:
        | case                       | input                               | type          | result        | rule |
        | string -> date             | '1'                                 | DATE          | date          | :458 |
        | string -> timestamp        | '1'                                 | TIMESTAMP     | timestamp     | :458 |
        | string -> timestamp_ntz    | '1'                                 | TIMESTAMP_NTZ | timestamp_ntz | :458 |
        | bigint -> decimal(10,2)    | CAST(1 AS BIGINT)                   | DECIMAL(10,2) | decimal(10,2) | :470 |
        | date -> timestamp_ntz      | DATE '2024-01-15'                   | TIMESTAMP_NTZ | timestamp_ntz | :467 |
        | decimal(10,2) -> bigint    | CAST(1.5 AS DECIMAL(10,2))          | BIGINT        | long          | :471 |
        | decimal(10,2) -> int       | CAST(1.5 AS DECIMAL(10,2))          | INT           | integer       | :471 |
        | decimal(10,2) -> smallint  | CAST(1.5 AS DECIMAL(10,2))          | SMALLINT      | short         | :471 |
        | decimal(10,2) -> tinyint   | CAST(1.5 AS DECIMAL(10,2))          | TINYINT       | byte          | :471 |
        | double -> bigint           | CAST(1.5 AS DOUBLE)                 | BIGINT        | long          | :471 |
        | double -> decimal(10,2)    | CAST(1.5 AS DOUBLE)                 | DECIMAL(10,2) | decimal(10,2) | :470 |
        | double -> int              | CAST(1.5 AS DOUBLE)                 | INT           | integer       | :471 |
        | double -> smallint         | CAST(1.5 AS DOUBLE)                 | SMALLINT      | short         | :471 |
        | double -> timestamp        | CAST(1.5 AS DOUBLE)                 | TIMESTAMP     | timestamp     | :463 |
        | double -> tinyint          | CAST(1.5 AS DOUBLE)                 | TINYINT       | byte          | :471 |
        | float -> bigint            | CAST(1.5 AS FLOAT)                  | BIGINT        | long          | :471 |
        | float -> decimal(10,2)     | CAST(1.5 AS FLOAT)                  | DECIMAL(10,2) | decimal(10,2) | :470 |
        | float -> int               | CAST(1.5 AS FLOAT)                  | INT           | integer       | :471 |
        | float -> smallint          | CAST(1.5 AS FLOAT)                  | SMALLINT      | short         | :471 |
        | float -> timestamp         | CAST(1.5 AS FLOAT)                  | TIMESTAMP     | timestamp     | :463 |
        | float -> tinyint           | CAST(1.5 AS FLOAT)                  | TINYINT       | byte          | :471 |
        | int -> decimal(10,2)       | 1                                   | DECIMAL(10,2) | decimal(10,2) | :470 |
        | string -> bigint           | '1'                                 | BIGINT        | long          | :458 |
        | string -> boolean          | '1'                                 | BOOLEAN       | boolean       | :458 |
        | string -> decimal(10,2)    | '1'                                 | DECIMAL(10,2) | decimal(10,2) | :458 |
        | string -> double           | '1'                                 | DOUBLE        | double        | :458 |
        | string -> float            | '1'                                 | FLOAT         | float         | :458 |
        | string -> int              | '1'                                 | INT           | integer       | :458 |
        | string -> smallint         | '1'                                 | SMALLINT      | short         | :458 |
        | string -> tinyint          | '1'                                 | TINYINT       | byte          | :458 |
        | timestamp -> decimal(10,2) | TIMESTAMP '2024-01-15 10:00:00'     | DECIMAL(10,2) | decimal(10,2) | :470 |
        | timestamp -> int           | TIMESTAMP '2024-01-15 10:00:00'     | INT           | integer       | :461 |
        | timestamp -> smallint      | TIMESTAMP '2024-01-15 10:00:00'     | SMALLINT      | short         | :461 |
        | timestamp -> tinyint       | TIMESTAMP '2024-01-15 10:00:00'     | TINYINT       | byte          | :461 |
        | timestamp_ntz -> date      | TIMESTAMP_NTZ '2024-01-15 10:00:00' | DATE          | date          | :465 |

  @spark_null
  Rule: scalar casts, force-nullable, VARIANT and INTERVAL types

    @spark-4
    Scenario Outline: a scalar CAST that is force-nullable: <case>, on Spark 4+
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = true)
        """

      Examples:
        | case                          | input                               | type                   | result                 | rule |
        | date -> variant               | DATE '2024-01-15'                   | VARIANT                | variant                | :467 |
        | string -> interval d-s        | '1'                                 | INTERVAL DAY TO SECOND | interval day to second | :458 |
        | string -> interval y-m        | '1'                                 | INTERVAL YEAR TO MONTH | interval year to month | :458 |
        | string -> variant             | '1'                                 | VARIANT                | variant                | :458 |
        | variant -> bigint             | PARSE_JSON('1')                     | BIGINT                 | long                   | :455 |
        | variant -> binary             | PARSE_JSON('1')                     | BINARY                 | binary                 | :455 |
        | variant -> boolean            | PARSE_JSON('1')                     | BOOLEAN                | boolean                | :455 |
        | variant -> date               | PARSE_JSON('1')                     | DATE                   | date                   | :455 |
        | variant -> decimal(10,2)      | PARSE_JSON('1')                     | DECIMAL(10,2)          | decimal(10,2)          | :455 |
        | variant -> double             | PARSE_JSON('1')                     | DOUBLE                 | double                 | :455 |
        | variant -> float              | PARSE_JSON('1')                     | FLOAT                  | float                  | :455 |
        | variant -> int                | PARSE_JSON('1')                     | INT                    | integer                | :455 |
        | variant -> smallint           | PARSE_JSON('1')                     | SMALLINT               | short                  | :455 |
        | variant -> string             | PARSE_JSON('1')                     | STRING                 | string                 | :455 |
        | variant -> timestamp_ntz      | PARSE_JSON('1')                     | TIMESTAMP_NTZ          | timestamp_ntz          | :455 |
        | variant -> tinyint            | PARSE_JSON('1')                     | TINYINT                | byte                   | :455 |
        | interval d-s -> decimal(10,2) | INTERVAL '1 12:30:01' DAY TO SECOND | DECIMAL(10,2)          | decimal(10,2)          | :470 |
        | interval y-m -> decimal(10,2) | INTERVAL '1-2' YEAR TO MONTH        | DECIMAL(10,2)          | decimal(10,2)          | :470 |

  @spark_null
  Rule: scalar casts, force-nullable (Sail diverges)

    @sail-bug
    @spark-4
    Scenario Outline: a scalar CAST that is force-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: <result> (nullable = true)
        """

      Examples:
        | case                          | input                               | type          | result        | rule |
        | variant -> timestamp          | PARSE_JSON('1')                     | TIMESTAMP     | timestamp     | :455 |

  @spark_null
  Rule: array casts, non-nullable

    Scenario Outline: an array CAST that keeps the input non-nullable: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

      Examples:
        | case                     | input       | type       |
        | array<int> -> array<int> | array(1, 2) | ARRAY<INT> |

  @spark_null
  Rule: array casts, force-nullable (Sail diverges)

    @sail-bug
    @spark-4
    Scenario Outline: an array CAST that is force-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """

      Examples:
        | case                  | input           | type       | rule |
        | variant -> array<int> | PARSE_JSON('1') | ARRAY<INT> | :455 |

  @spark_null
  Rule: map casts, non-nullable (Sail diverges)

    @sail-bug
    Scenario Outline: a map CAST that keeps the input non-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: integer (valueContainsNull = true)
        """

      Examples:
        | case                               | input       | type            |
        | map<string,int> -> map<string,int> | map('a', 1) | MAP<STRING,INT> |

  @spark_null
  Rule: map casts, force-nullable (Sail diverges)

    @sail-bug
    @spark-4
    Scenario Outline: a map CAST that is force-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: string
         |    |-- value: integer (valueContainsNull = true)
        """

      Examples:
        | case                       | input           | type            | rule |
        | variant -> map<string,int> | PARSE_JSON('1') | MAP<STRING,INT> | :455 |

  @spark_null
  Rule: struct casts, non-nullable (Sail diverges)

    @sail-bug
    Scenario Outline: a struct CAST that keeps the input non-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: integer (nullable = true)
        """

      Examples:
        | case                           | input                | type          |
        | struct<a:int> -> struct<a:int> | named_struct('a', 1) | STRUCT<a:INT> |

  @spark_null
  Rule: struct casts, force-nullable (Sail diverges)

    @sail-bug
    @spark-4
    Scenario Outline: a struct CAST that is force-nullable but Sail does not: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = true)
         |    |-- a: integer (nullable = true)
        """

      Examples:
        | case                     | input           | type          | rule |
        | variant -> struct<a:int> | PARSE_JSON('1') | STRUCT<a:INT> | :455 |

  @spark_null
  Rule: casts beyond bare literals

    Scenario Outline: a force-nullable CAST of a non-null column is nullable: <case>
      When query
        """
        SELECT CAST(c AS INT) AS result FROM VALUES <rows> AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

      Examples:
        | case          | rows         |
        | string -> INT | ('1'), ('2') |

    # root cause: Sail declares VALUES columns nullable even without NULL rows (source, not cast)
    @sail-bug
    Scenario Outline: a non-force CAST of a non-null column keeps non-nullability but Sail does not: <case>
      When query
        """
        SELECT CAST(c AS INT) AS result FROM VALUES <rows> AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case          | rows                                     |
        | bigint -> INT | (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)) |

    Scenario Outline: a chained CAST propagates the inner force-nullable cast: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | case                      | input            | type   |
        | STRING -> INT then DOUBLE | CAST('1' AS INT) | DOUBLE |

    Scenario Outline: a chained CAST with no force-nullable step stays non-nullable: <case>
      When query
        """
        SELECT CAST(<input> AS <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case                    | input             | type |
        | BIGINT -> INT, no force | CAST(1 AS BIGINT) | INT  |

    # Spark folds WHEN true before schema derivation; DataFusion's CASE-without-ELSE reports nullable.
    # This is also the shape the resolver uses internally to force cast nullability: if Sail ever
    # starts folding like Spark, this strict xfail turns XPASS and flags the coupling.
    @sail-bug
    Scenario Outline: a CASE WHEN true wrapping a CAST is non-nullable in Spark but not in Sail: <case>
      When query
        """
        SELECT CASE WHEN true THEN CAST(5 AS INT) END AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case                  |
        | folded WHEN true CASE |

  @spark_null
  Rule: the cast(...) function spelling

    Scenario: a non-null literal input to cast yields the schema Spark declares
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
