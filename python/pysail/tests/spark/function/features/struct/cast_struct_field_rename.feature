Feature: CAST between structs whose field names differ pairs the fields by position at every depth

  # Spark resolves the fields of a struct cast by position and takes the names of the target
  # (`Cast.canCast` over `StructType`, `Cast.scala`). DataFusion matches struct fields by name and
  # case-sensitively, so Sail renames the source fields positionally first
  # (`needs_struct_field_rename`); that rename has to reach the structs nested inside other
  # structs, arrays and maps, and it must not change what the cast reports about nullability.
  #
  # Expected values captured from Spark Connect 4.2.0 (JVM, session timezone UTC).

  Rule: A nested struct keeps its values when only the case of an inner name differs

    Scenario Outline: CAST renames the struct fields: <case>
      When query
        """
        SELECT <expr> AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                              | expr                                                                                                        | result |
        | two levels                        | CAST(named_struct('x', named_struct('A', 1)) AS STRUCT<x: STRUCT<a: BIGINT>>).x.a                           | 1      |
        | two levels, a second field        | CAST(named_struct('x', named_struct('A', 1, 'b', 2)) AS STRUCT<x: STRUCT<a: BIGINT, b: BIGINT>>).x.a        | 1      |
        | two levels, the other field       | CAST(named_struct('x', named_struct('A', 1, 'b', 2)) AS STRUCT<x: STRUCT<a: BIGINT, b: BIGINT>>).x.b        | 2      |
        | three levels                      | CAST(named_struct('x', named_struct('y', named_struct('A', 1))) AS STRUCT<x: STRUCT<y: STRUCT<a: BIGINT>>>).x.y.a | 1      |
        | TRY_CAST                          | TRY_CAST(named_struct('x', named_struct('A', 1)) AS STRUCT<x: STRUCT<a: BIGINT>>).x.a                       | 1      |
        | struct inside an array            | CAST(array(named_struct('A', 1)) AS ARRAY<STRUCT<a: BIGINT>>)[0].a                                          | 1      |
        | struct inside a map value         | CAST(map('k', named_struct('A', 1)) AS MAP<STRING, STRUCT<a: BIGINT>>)['k'].a                               | 1      |
        | names that already agree          | CAST(named_struct('x', named_struct('a', 1)) AS STRUCT<x: STRUCT<a: BIGINT>>).x.a                           | 1      |
        | only the top level renamed        | CAST(named_struct('A', 1) AS STRUCT<a: BIGINT>).a                                                           | 1      |

  Rule: The rename keeps the nullability of its input

    # The renamed value is as nullable as the value it came from: a map literal stays non-null at
    # the top, and a nullable input stays nullable.
    Scenario: a map of renamed structs built from literals is not nullable
      When query
        """
        SELECT CAST(map('k', named_struct('A', 1)) AS MAP<STRING, STRUCT<a: BIGINT>>) AS r
        """
      Then query schema
        """
        root
         |-- r: map (nullable = false)
         |    |-- key: string
         |    |-- value: struct (valueContainsNull = true)
         |    |    |-- a: long (nullable = true)
        """

    Scenario: a renamed struct built from a nullable column stays nullable
      When query
        """
        SELECT CAST(s AS STRUCT<a: BIGINT>) AS r
        FROM VALUES (named_struct('A', 1)), (CAST(NULL AS STRUCT<A: INT>)) AS t(s)
        """
      Then query schema
        """
        root
         |-- r: struct (nullable = true)
         |    |-- a: long (nullable = true)
        """
