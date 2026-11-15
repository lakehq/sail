Feature: Approximate percentiles select observations within the requested rank error
  Both aliases select observed values and preserve native numeric observations.
  A single partition with 100000 rows crosses the 50000-value buffering boundary;
  thirteen partitions exercise partial-state serialization and merging.

  Rule: Scalar and array results retain the numeric input type
    Scenario Outline: approx_percentile preserves <type> values and types with <function>
      When query
        """
        SELECT <function>(CAST(value AS <type>), 0.5) AS p,
               <function>(CAST(value AS <type>), array(1D, 0.5D, 0D), 1000000) AS ps
        FROM VALUES (0), (1), (2), (10), (NULL) AS t(value)
        """
      Then query result collected
        | p   | ps   |
        | <p> | <ps> |
      Then query column types
        | column | type         |
        | p      | <type>       |
        | ps     | <array_type> |

      Examples:
        | function          | type     | p   | ps               | array_type      |
        | approx_percentile | tinyint  | 1   | [10, 1, 0]       | array<tinyint>  |
        | approx_percentile | smallint | 1   | [10, 1, 0]       | array<smallint> |
        | approx_percentile | int      | 1   | [10, 1, 0]       | array<int>      |
        | approx_percentile | bigint   | 1   | [10, 1, 0]       | array<bigint>   |
        | approx_percentile | float    | 1.0 | [10.0, 1.0, 0.0] | array<float>    |
        | approx_percentile | double   | 1.0 | [10.0, 1.0, 0.0] | array<double>   |
        | percentile_approx | tinyint  | 1   | [10, 1, 0]       | array<tinyint>  |
        | percentile_approx | smallint | 1   | [10, 1, 0]       | array<smallint> |
        | percentile_approx | int      | 1   | [10, 1, 0]       | array<int>      |
        | percentile_approx | bigint   | 1   | [10, 1, 0]       | array<bigint>   |
        | percentile_approx | float    | 1.0 | [10.0, 1.0, 0.0] | array<float>    |
        | percentile_approx | double   | 1.0 | [10.0, 1.0, 0.0] | array<double>   |

    Scenario Outline: approx_percentile array elements are nonnullable with <function>
      When query
        """
        SELECT <function>(value, 0.5) AS p,
               <function>(value, array(0D, 0.5D, 1D)) AS ps
        FROM VALUES (0), (1), (2), (10) AS t(value)
        """
      Then query schema
        """
        root
         |-- p: integer (nullable = true)
         |-- ps: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

      Examples:
        | function          |
        | approx_percentile |
        | percentile_approx |

    Scenario Outline: approx_percentile growing window preserves values and nullability with <function>
      When query
        """
        SELECT id,
               <function>(id, 0.5) OVER (
                 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS p,
               <function>(id, array(0D, 0.5D, 1D)) OVER (
                 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ps,
               <function>(id, array()) OVER (
                 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS empty_ps
        FROM range(3)
        ORDER BY id
        """
      Then query result ordered
        | id | p | ps        | empty_ps |
        | 0  | 0 | [0, 0, 0] | NULL     |
        | 1  | 0 | [0, 0, 1] | NULL     |
        | 2  | 1 | [0, 1, 2] | NULL     |
      Then query schema
        """
        root
         |-- id: long (nullable = false)
         |-- p: long (nullable = true)
         |-- ps: array (nullable = true)
         |    |-- element: long (containsNull = false)
         |-- empty_ps: array (nullable = true)
         |    |-- element: long (containsNull = false)
        """

      Examples:
        | function          |
        | approx_percentile |
        | percentile_approx |

  Rule: Every native numeric Arrow input is aggregated and merged without narrowing
    # variant_get's existing Arrow type syntax creates input representations that
    # Spark SQL cannot name. Output is projected to supported types after aggregation
    # because Spark Connect cannot describe Decimal32/64 schemas.
    @sail-only
    Scenario Outline: approx_percentile aggregates native <type> across partitions with <function>
      When query
        """
        WITH input AS (
          SELECT variant_get(parse_json(CASE id % 5
                     WHEN 0 THEN '10' WHEN 1 THEN '0' WHEN 2 THEN NULL
                     WHEN 3 THEN '2' ELSE '1' END), '$', '<type>') AS value
          FROM range(0, 65, 1, 13)
        ), result AS (
          SELECT <function>(value, 0.5) AS p,
                 <function>(value, array(1D, 0.5D, 0.4D, 0.1D, 0D, 0.5D), 1000000) AS ps
          FROM input
        )
        SELECT CAST(p AS STRING) AS p,
               ps[0] = 10 AND ps[1] = 1 AND ps[2] = 1
                 AND ps[3] = 0 AND ps[4] = 0 AND ps[5] = 1 AS percentiles
        FROM result
        """
      Then query result
        | p   | percentiles |
        | <p> | true        |

      Examples:
        | function          | type               | p                      |
        | approx_percentile | Int8               | 1                      |
        | approx_percentile | Int16              | 1                      |
        | approx_percentile | Int32              | 1                      |
        | approx_percentile | Int64              | 1                      |
        | approx_percentile | UInt8              | 1                      |
        | approx_percentile | UInt16             | 1                      |
        | approx_percentile | UInt32             | 1                      |
        | approx_percentile | UInt64             | 1                      |
        | approx_percentile | Float16            | 1                      |
        | approx_percentile | Float32            | 1.0                    |
        | approx_percentile | Float64            | 1.0                    |
        | approx_percentile | Decimal32(9, 2)    | 1.00                   |
        | approx_percentile | Decimal64(18, 4)   | 1.0000                 |
        | approx_percentile | Decimal128(38, 10) | 1.0000000000           |
        | approx_percentile | Decimal256(76, 20) | 1.00000000000000000000 |
        | percentile_approx | Int8               | 1                      |
        | percentile_approx | Int16              | 1                      |
        | percentile_approx | Int32              | 1                      |
        | percentile_approx | Int64              | 1                      |
        | percentile_approx | UInt8              | 1                      |
        | percentile_approx | UInt16             | 1                      |
        | percentile_approx | UInt32             | 1                      |
        | percentile_approx | UInt64             | 1                      |
        | percentile_approx | Float16            | 1                      |
        | percentile_approx | Float32            | 1.0                    |
        | percentile_approx | Float64            | 1.0                    |
        | percentile_approx | Decimal32(9, 2)    | 1.00                   |
        | percentile_approx | Decimal64(18, 4)   | 1.0000                 |
        | percentile_approx | Decimal128(38, 10) | 1.0000000000           |
        | percentile_approx | Decimal256(76, 20) | 1.00000000000000000000 |

    # Decimal32/64 cannot be formatted by typeof; their values and scales
    # are checked above without extending the production type formatter.
    @sail-only
    Scenario Outline: approx_percentile retains native output type <type> with <function>
      When query
        """
        WITH result AS (
          SELECT <function>(variant_get(parse_json(CAST(id AS STRING)), '$', '<type>'), 0.5) AS p,
                 <function>(variant_get(parse_json(CAST(id AS STRING)), '$', '<type>'),
                   array(0D, 0.5D, 1D)) AS ps
          FROM range(0, 4, 1, 2)
        )
        SELECT typeof(p) AS scalar_type, typeof(ps) AS array_type,
               p = 1 AND ps[0] = 0 AND ps[1] = 1 AND ps[2] = 3 AS values_match
        FROM result
        """
      Then query result
        | scalar_type   | array_type   | values_match |
        | <scalar_type> | <array_type> | true         |

      Examples:
        | function          | type               | scalar_type       | array_type               |
        | approx_percentile | UInt8              | unsigned tinyint  | array<unsigned tinyint>  |
        | approx_percentile | UInt16             | unsigned smallint | array<unsigned smallint> |
        | approx_percentile | UInt32             | unsigned int      | array<unsigned int>      |
        | approx_percentile | UInt64             | unsigned bigint   | array<unsigned bigint>   |
        | approx_percentile | Float16            | half float        | array<half float>        |
        | approx_percentile | Decimal128(38, 10) | decimal(38,10)    | array<decimal(38,10)>    |
        | approx_percentile | Decimal256(76, 20) | decimal(76,20)    | array<decimal(76,20)>    |
        | percentile_approx | UInt8              | unsigned tinyint  | array<unsigned tinyint>  |
        | percentile_approx | UInt16             | unsigned smallint | array<unsigned smallint> |
        | percentile_approx | UInt32             | unsigned int      | array<unsigned int>      |
        | percentile_approx | UInt64             | unsigned bigint   | array<unsigned bigint>   |
        | percentile_approx | Float16            | half float        | array<half float>        |
        | percentile_approx | Decimal128(38, 10) | decimal(38,10)    | array<decimal(38,10)>    |
        | percentile_approx | Decimal256(76, 20) | decimal(76,20)    | array<decimal(76,20)>    |

  Rule: Dictionary encoded inputs and constant parameters are decoded
    @sail-only
    Scenario Outline: approx_percentile decodes <key> dictionaries with <function>
      When query
        """
        SELECT <function>(
          variant_get(parse_json(CAST(id AS STRING)), '$', 'Dictionary(<key>, Int64)'),
          variant_get(parse_json('0.5'), '$', 'Dictionary(<key>, Decimal64(10, 2))'),
          variant_get(parse_json('1000000'), '$', 'Dictionary(<key>, UInt32)')) AS p
        FROM range(0, 4, 1, 2)
        """
      Then query result
        | p |
        | 1 |

      Examples:
        | function          | key    |
        | approx_percentile | Int8   |
        | approx_percentile | Int16  |
        | approx_percentile | Int32  |
        | approx_percentile | Int64  |
        | approx_percentile | UInt8  |
        | approx_percentile | UInt16 |
        | approx_percentile | UInt32 |
        | approx_percentile | UInt64 |
        | percentile_approx | Int8   |
        | percentile_approx | Int16  |
        | percentile_approx | Int32  |
        | percentile_approx | Int64  |
        | percentile_approx | UInt8  |
        | percentile_approx | UInt16 |
        | percentile_approx | UInt32 |
        | percentile_approx | UInt64 |

    @sail-only
    Scenario Outline: approx_percentile rejects zero dictionary accuracy for <key> with <function>
      When query
        """
        SELECT <function>(id, 0.5,
          variant_get(parse_json('0'), '$', 'Dictionary(<key>, UInt32)'))
        FROM range(4)
        """
      Then query error accuracy must be between 1 and 2147483647

      Examples:
        | function          | key    |
        | approx_percentile | Int8   |
        | approx_percentile | Int16  |
        | approx_percentile | Int32  |
        | approx_percentile | Int64  |
        | approx_percentile | UInt8  |
        | approx_percentile | UInt16 |
        | approx_percentile | UInt32 |
        | approx_percentile | UInt64 |
        | percentile_approx | Int8   |
        | percentile_approx | Int16  |
        | percentile_approx | Int32  |
        | percentile_approx | Int64  |
        | percentile_approx | UInt8  |
        | percentile_approx | UInt16 |
        | percentile_approx | UInt32 |
        | percentile_approx | UInt64 |

  Rule: All list representations of constant percentages preserve order and duplicates
    @sail-only
    Scenario Outline: approx_percentile accepts <type> percentages with <function>
      When query
        """
        SELECT <function>(id,
          variant_get(parse_json('[1, 0.5, 0, 0.5]'), '$', '<type>'), 1000000) AS ps
        FROM range(4)
        """
      Then query result
        | ps           |
        | [3, 1, 0, 1] |

      Examples:
        | function          | type                                         |
        | approx_percentile | List(Float32)                                |
        | approx_percentile | LargeList(Float32)                           |
        | approx_percentile | FixedSizeList(4, Float32)                    |
        | approx_percentile | ListView(Float32)                            |
        | approx_percentile | LargeListView(Float32)                       |
        | approx_percentile | List(Dictionary(Int16, Float32))             |
        | approx_percentile | LargeList(Dictionary(Int16, Float32))        |
        | approx_percentile | FixedSizeList(4, Dictionary(Int16, Float32)) |
        | approx_percentile | ListView(Dictionary(Int16, Float32))         |
        | approx_percentile | LargeListView(Dictionary(Int16, Float32))    |
        | percentile_approx | List(Float32)                                |
        | percentile_approx | LargeList(Float32)                           |
        | percentile_approx | FixedSizeList(4, Float32)                    |
        | percentile_approx | ListView(Float32)                            |
        | percentile_approx | LargeListView(Float32)                       |
        | percentile_approx | List(Dictionary(Int16, Float32))             |
        | percentile_approx | LargeList(Dictionary(Int16, Float32))        |
        | percentile_approx | FixedSizeList(4, Dictionary(Int16, Float32)) |
        | percentile_approx | ListView(Dictionary(Int16, Float32))         |
        | percentile_approx | LargeListView(Dictionary(Int16, Float32))    |

  Rule: Selected native observations retain large integer and decimal precision
    @sail-only
    Scenario Outline: approx_percentile preserves exact <type> observations with <function>
      When query
        """
        SELECT CAST(ps[0] AS STRING) AS minimum,
               CAST(ps[1] AS STRING) AS median,
               CAST(ps[2] AS STRING) AS maximum
        FROM (
          SELECT <function>(value, array(0D, 0.5D, 1D)) AS ps
          FROM (SELECT CAST(CASE id WHEN 0 THEN '<maximum>' WHEN 1 THEN '<minimum>'
                                  ELSE '<median>' END AS <type>) AS value
                FROM range(0, 3, 1, 3)) input
        ) result
        """
      Then query result
        | minimum   | median   | maximum   |
        | <minimum> | <median> | <maximum> |

      Examples:
        | function          | type           | minimum                                                                       | median                                                                        | maximum                                                                       |
        | approx_percentile | bigint         | -9223372036854775807                                                          | 9007199254740993                                                              | 9223372036854775807                                                           |
        | approx_percentile | UINT64         | 9007199254740993                                                              | 18446744073709551614                                                          | 18446744073709551615                                                          |
        | approx_percentile | DECIMAL(38,18) | 12345678901234567890.123456789012345678                                       | 12345678901234567890.123456789012345679                                       | 12345678901234567890.123456789012345680                                       |
        | approx_percentile | DECIMAL(76,18) | 1234567890123456789012345678901234567890123456789012345678.123456789012345678 | 1234567890123456789012345678901234567890123456789012345678.123456789012345679 | 1234567890123456789012345678901234567890123456789012345678.123456789012345680 |
        | percentile_approx | bigint         | -9223372036854775807                                                          | 9007199254740993                                                              | 9223372036854775807                                                           |
        | percentile_approx | UINT64         | 9007199254740993                                                              | 18446744073709551614                                                          | 18446744073709551615                                                          |
        | percentile_approx | DECIMAL(38,18) | 12345678901234567890.123456789012345678                                       | 12345678901234567890.123456789012345679                                       | 12345678901234567890.123456789012345680                                       |
        | percentile_approx | DECIMAL(76,18) | 1234567890123456789012345678901234567890123456789012345678.123456789012345678 | 1234567890123456789012345678901234567890123456789012345678.123456789012345679 | 1234567890123456789012345678901234567890123456789012345678.123456789012345680 |

  Rule: Empty populations and percentage arrays return typed nulls
    Scenario Outline: approx_percentile handles <case> with <function>
      When query
        """
        SELECT <function>(value, <percentage>) AS p FROM (<input>) input
        """
      Then query result
        | p    |
        | NULL |
      Then query column types
        | column | type   |
        | p      | <type> |

      Examples:
        | function          | case                | percentage          | input                                           | type          |
        | approx_percentile | empty scalar        | 0.5                 | SELECT CAST(NULL AS INT) AS value FROM range(0) | int           |
        | approx_percentile | empty array         | array(0D, 0.5D, 1D) | SELECT CAST(NULL AS INT) AS value FROM range(0) | array<int>    |
        | approx_percentile | all-null scalar     | 0.5                 | SELECT CAST(NULL AS INT) AS value FROM range(2) | int           |
        | approx_percentile | all-null array      | array(0D, 0.5D, 1D) | SELECT CAST(NULL AS INT) AS value FROM range(2) | array<int>    |
        | approx_percentile | empty percentages   | array()             | SELECT id AS value FROM range(4)                | array<bigint> |
        | approx_percentile | untyped null scalar | 0.5                 | SELECT NULL AS value FROM range(2)              | double        |
        | approx_percentile | untyped null array  | array(0D, 0.5D, 1D) | SELECT NULL AS value FROM range(2)              | array<double> |
        | percentile_approx | empty scalar        | 0.5                 | SELECT CAST(NULL AS INT) AS value FROM range(0) | int           |
        | percentile_approx | empty array         | array(0D, 0.5D, 1D) | SELECT CAST(NULL AS INT) AS value FROM range(0) | array<int>    |
        | percentile_approx | all-null scalar     | 0.5                 | SELECT CAST(NULL AS INT) AS value FROM range(2) | int           |
        | percentile_approx | all-null array      | array(0D, 0.5D, 1D) | SELECT CAST(NULL AS INT) AS value FROM range(2) | array<int>    |
        | percentile_approx | empty percentages   | array()             | SELECT id AS value FROM range(4)                | array<bigint> |
        | percentile_approx | untyped null scalar | 0.5                 | SELECT NULL AS value FROM range(2)              | double        |
        | percentile_approx | untyped null array  | array(0D, 0.5D, 1D) | SELECT NULL AS value FROM range(2)              | array<double> |

    Scenario Outline: approx_percentile merges empty partial partitions with <function>
      When query
        """
        SELECT <function>(id, 0.5) AS p,
               <function>(id, array(0D, 0.5D, 1D)) AS ps
        FROM range(0, 1, 1, 13)
        """
      Then query result
        | p | ps |
        | 0 | [0, 0, 0] |

      Examples:
        | function          |
        | approx_percentile |
        | percentile_approx |

  Rule: Accuracy bounds are inclusive and control ranked selection
    Scenario Outline: approx_percentile accepts accuracy <accuracy> with <function>
      When query
        """
        SELECT <function>(id, array(0D, 0.5D, 1D), <accuracy>) AS ps FROM range(4)
        """
      Then query result
        | ps   |
        | <ps> |

      Examples:
        | function          | accuracy   | ps        |
        | approx_percentile | 1          | [0, 0, 0] |
        | approx_percentile | 10000      | [0, 1, 3] |
        | approx_percentile | 2147483647 | [0, 1, 3] |
        | percentile_approx | 1          | [0, 0, 0] |
        | percentile_approx | 10000      | [0, 1, 3] |
        | percentile_approx | 2147483647 | [0, 1, 3] |

  Rule: Invalid parameters and nonnumeric populations fail consistently
    @sail-only
    Scenario Outline: approx_percentile rejects <case> with <function>
      When query
        """
        SELECT <function>(<arguments>) FROM range(4)
        """
      Then query error <error>

      Examples:
        | function          | case                         | arguments                                       | error                                        |
        | approx_percentile | missing percentage           | id                                              | requires 2 or 3 arguments                    |
        | approx_percentile | extra argument               | id, 0.5, 10000, 1                               | requires 2 or 3 arguments                    |
        | approx_percentile | boolean input                | true, 0.5                                       | requires numeric input                       |
        | approx_percentile | string input                 | '1', 0.5                                        | requires numeric input                       |
        | approx_percentile | date input                   | DATE '2000-01-01', 0.5                          | requires numeric input                       |
        | approx_percentile | array input                  | array(1), 0.5                                   | requires numeric input                       |
        | approx_percentile | negative percentage          | id, -0.1                                        | percentages must be between 0 and 1          |
        | approx_percentile | large percentage             | id, 1.1                                         | percentages must be between 0 and 1          |
        | approx_percentile | NaN percentage               | id, CAST('NaN' AS DOUBLE)                       | percentages must be between 0 and 1          |
        | approx_percentile | infinite percentage          | id, CAST('Infinity' AS DOUBLE)                  | percentages must be between 0 and 1          |
        | approx_percentile | null percentage              | id, NULL                                        | percentage must not be null                  |
        | approx_percentile | null list member             | id, array(0.5D, NULL)                           | percentages must not contain nulls           |
        | approx_percentile | invalid list member          | id, array(0.5D, 1.1D)                           | percentages must be between 0 and 1          |
        | approx_percentile | boolean percentage           | id, true                                        | percentage must be numeric                   |
        | approx_percentile | nonnumeric list              | id, array(true)                                 | percentage must be numeric                   |
        | approx_percentile | column percentage            | id, id / 10D                                    | must be constant expressions                 |
        | approx_percentile | volatile percentage          | id, rand()                                      | must be constant expressions                 |
        | approx_percentile | zero accuracy                | id, 0.5, 0                                      | accuracy must be a positive integer constant |
        | approx_percentile | negative accuracy            | id, 0.5, -1                                     | accuracy must be a positive integer constant |
        | approx_percentile | null accuracy                | id, 0.5, NULL                                   | accuracy must be an integer                  |
        | approx_percentile | fractional accuracy          | id, 0.5, 1.5                                    | accuracy must be an integer                  |
        | approx_percentile | excess accuracy              | id, 0.5, 2147483648L                            | accuracy must be between 1 and 2147483647    |
        | approx_percentile | column accuracy              | id, 0.5, id + 1                                 | must be constant expressions                 |
        | approx_percentile | volatile accuracy            | id, 0.5, CAST(rand() * 10 + 1 AS BIGINT)        | must be constant expressions                 |
        | percentile_approx | missing percentage           | id                                              | requires 2 or 3 arguments                    |
        | percentile_approx | extra argument               | id, 0.5, 10000, 1                               | requires 2 or 3 arguments                    |
        | percentile_approx | boolean input                | true, 0.5                                       | requires numeric input                       |
        | percentile_approx | string input                 | '1', 0.5                                        | requires numeric input                       |
        | percentile_approx | date input                   | DATE '2000-01-01', 0.5                          | requires numeric input                       |
        | percentile_approx | array input                  | array(1), 0.5                                   | requires numeric input                       |
        | percentile_approx | negative percentage          | id, -0.1                                        | percentages must be between 0 and 1          |
        | percentile_approx | large percentage             | id, 1.1                                         | percentages must be between 0 and 1          |
        | percentile_approx | NaN percentage               | id, CAST('NaN' AS DOUBLE)                       | percentages must be between 0 and 1          |
        | percentile_approx | infinite percentage          | id, CAST('Infinity' AS DOUBLE)                  | percentages must be between 0 and 1          |
        | percentile_approx | null percentage              | id, NULL                                        | percentage must not be null                  |
        | percentile_approx | null list member             | id, array(0.5D, NULL)                           | percentages must not contain nulls           |
        | percentile_approx | invalid list member          | id, array(0.5D, 1.1D)                           | percentages must be between 0 and 1          |
        | percentile_approx | boolean percentage           | id, true                                        | percentage must be numeric                   |
        | percentile_approx | nonnumeric list              | id, array(true)                                 | percentage must be numeric                   |
        | percentile_approx | column percentage            | id, id / 10D                                    | must be constant expressions                 |
        | percentile_approx | volatile percentage          | id, rand()                                      | must be constant expressions                 |
        | percentile_approx | zero accuracy                | id, 0.5, 0                                      | accuracy must be a positive integer constant |
        | percentile_approx | negative accuracy            | id, 0.5, -1                                     | accuracy must be a positive integer constant |
        | percentile_approx | null accuracy                | id, 0.5, NULL                                   | accuracy must be an integer                  |
        | percentile_approx | fractional accuracy          | id, 0.5, 1.5                                    | accuracy must be an integer                  |
        | percentile_approx | excess accuracy              | id, 0.5, 2147483648L                            | accuracy must be between 1 and 2147483647    |
        | percentile_approx | column accuracy              | id, 0.5, id + 1                                 | must be constant expressions                 |
        | percentile_approx | volatile accuracy            | id, 0.5, CAST(rand() * 10 + 1 AS BIGINT)        | must be constant expressions                 |
        | approx_percentile | binary input                 | X'01', 0.5                                      | requires numeric input                       |
        | approx_percentile | timestamp input              | TIMESTAMP '2000-01-01 00:00:00', 0.5            | requires numeric input                       |
        | approx_percentile | typed null accuracy          | id, 0.5, CAST(NULL AS BIGINT)                   | accuracy must be a positive integer constant |
        | approx_percentile | unsigned maximum accuracy    | id, 0.5, CAST('18446744073709551615' AS UINT64) | accuracy must be between 1 and 2147483647    |
        | approx_percentile | negative infinite percentage | id, CAST('-Infinity' AS DOUBLE)                 | percentages must be between 0 and 1          |
        | percentile_approx | binary input                 | X'01', 0.5                                      | requires numeric input                       |
        | percentile_approx | timestamp input              | TIMESTAMP '2000-01-01 00:00:00', 0.5            | requires numeric input                       |
        | percentile_approx | typed null accuracy          | id, 0.5, CAST(NULL AS BIGINT)                   | accuracy must be a positive integer constant |
        | percentile_approx | unsigned maximum accuracy    | id, 0.5, CAST('18446744073709551615' AS UINT64) | accuracy must be between 1 and 2147483647    |
        | percentile_approx | negative infinite percentage | id, CAST('-Infinity' AS DOUBLE)                 | percentages must be between 0 and 1          |

  Rule: NaN observations sort after infinities consistently during merges
    @sail-only
    Scenario Outline: approx_percentile orders <type> nonfinite observations with <function>
      When query
        """
        WITH values AS (
          SELECT variant_get(to_variant_object(array(
                   CAST(CASE id WHEN 0 THEN 'NaN' WHEN 1 THEN '-Infinity'
                         WHEN 2 THEN '-1' WHEN 3 THEN '-0.0' WHEN 4 THEN '0.0'
                         WHEN 5 THEN '1' WHEN 6 THEN 'Infinity' ELSE 'NaN' END AS DOUBLE))),
                   '$[0]', '<type>') AS value
          FROM range(0, 8, 1, 4)
        ), result AS (
          SELECT <function>(value, array(0D, 0.25D, 0.625D, 0.75D, 1D), 1000000) AS ps FROM values
        )
        SELECT ps[0] = CAST('-Infinity' AS DOUBLE) AS minimum,
               ps[1] = -1 AND ps[2] = 1 AS finite,
               ps[3] = CAST('Infinity' AS DOUBLE) AS infinity,
               isnan(ps[4]) AS nan
        FROM result
        """
      Then query result
        | minimum | finite | infinity | nan  |
        | true    | true   | true     | true |

      Examples:
        | function          | type    |
        | approx_percentile | Float16 |
        | approx_percentile | Float32 |
        | approx_percentile | Float64 |
        | percentile_approx | Float16 |
        | percentile_approx | Float32 |
        | percentile_approx | Float64 |

  Rule: Compression and partition merges respect independent exact rank bounds
    Scenario Outline: approx_percentile rank bounds for <distribution> accuracy <accuracy> partitions <partitions> with <function>
      When query
        """
        WITH quantiles AS (
          SELECT <function>(<expression>,
                   array(0.00D, 0.01D, 0.02D, 0.03D, 0.04D, 0.05D, 0.06D, 0.07D, 0.08D, 0.09D,
                         0.10D, 0.11D, 0.12D, 0.13D, 0.14D, 0.15D, 0.16D, 0.17D, 0.18D, 0.19D,
                         0.20D, 0.21D, 0.22D, 0.23D, 0.24D, 0.25D, 0.26D, 0.27D, 0.28D, 0.29D,
                         0.30D, 0.31D, 0.32D, 0.33D, 0.34D, 0.35D, 0.36D, 0.37D, 0.38D, 0.39D,
                         0.40D, 0.41D, 0.42D, 0.43D, 0.44D, 0.45D, 0.46D, 0.47D, 0.48D, 0.49D,
                         0.50D, 0.51D, 0.52D, 0.53D, 0.54D, 0.55D, 0.56D, 0.57D, 0.58D, 0.59D,
                         0.60D, 0.61D, 0.62D, 0.63D, 0.64D, 0.65D, 0.66D, 0.67D, 0.68D, 0.69D,
                         0.70D, 0.71D, 0.72D, 0.73D, 0.74D, 0.75D, 0.76D, 0.77D, 0.78D, 0.79D,
                         0.80D, 0.81D, 0.82D, 0.83D, 0.84D, 0.85D, 0.86D, 0.87D, 0.88D, 0.89D,
                         0.90D, 0.91D, 0.92D, 0.93D, 0.94D, 0.95D, 0.96D, 0.97D, 0.98D, 0.99D,
                         1.00D), <accuracy>) AS ps
          FROM range(0, 100000, 1, <partitions>)
        ), ranked AS (
          SELECT pos, value FROM quantiles LATERAL VIEW posexplode(ps) AS pos, value
        )
        SELECT count(*) AS checked,
               bool_and(value IS NOT NULL AND value BETWEEN 0 AND 99999 AND value + 1 BETWEEN
                 greatest(1L, floor((pos / 100.0D - 1.0D / <accuracy>) * 100000)) AND
                 least(100000L, ceil((pos / 100.0D + 1.0D / <accuracy>) * 100000))) AS valid
        FROM ranked
        """
      Then query result
        | checked | valid |
        | 101     | true  |

      Examples:
        | function          | distribution | expression           | accuracy | partitions |
        | approx_percentile | ascending    | id                   | 10       | 1          |
        | approx_percentile | ascending    | id                   | 10       | 13         |
        | approx_percentile | ascending    | id                   | 100      | 1          |
        | approx_percentile | ascending    | id                   | 100      | 13         |
        | approx_percentile | ascending    | id                   | 1000     | 1          |
        | approx_percentile | ascending    | id                   | 1000     | 13         |
        | approx_percentile | descending   | 99999 - id           | 10       | 1          |
        | approx_percentile | descending   | 99999 - id           | 10       | 13         |
        | approx_percentile | descending   | 99999 - id           | 100      | 1          |
        | approx_percentile | descending   | 99999 - id           | 100      | 13         |
        | approx_percentile | descending   | 99999 - id           | 1000     | 1          |
        | approx_percentile | descending   | 99999 - id           | 1000     | 13         |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 10       | 1          |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 10       | 13         |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 100      | 1          |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 100      | 13         |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 1000     | 1          |
        | approx_percentile | permuted     | (id * 7919) % 100000 | 1000     | 13         |
        | percentile_approx | ascending    | id                   | 10       | 1          |
        | percentile_approx | ascending    | id                   | 10       | 13         |
        | percentile_approx | ascending    | id                   | 100      | 1          |
        | percentile_approx | ascending    | id                   | 100      | 13         |
        | percentile_approx | ascending    | id                   | 1000     | 1          |
        | percentile_approx | ascending    | id                   | 1000     | 13         |
        | percentile_approx | descending   | 99999 - id           | 10       | 1          |
        | percentile_approx | descending   | 99999 - id           | 10       | 13         |
        | percentile_approx | descending   | 99999 - id           | 100      | 1          |
        | percentile_approx | descending   | 99999 - id           | 100      | 13         |
        | percentile_approx | descending   | 99999 - id           | 1000     | 1          |
        | percentile_approx | descending   | 99999 - id           | 1000     | 13         |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 10       | 1          |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 10       | 13         |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 100      | 1          |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 100      | 13         |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 1000     | 1          |
        | percentile_approx | permuted     | (id * 7919) % 100000 | 1000     | 13         |
