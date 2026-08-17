Feature: from_json function parses JSON strings into structured types

  Rule: Basic struct parsing
    Scenario Outline: Basic struct: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                                                 | result      |
        | Parse simple struct from JSON   | '{"a":1, "b":0.8}', 'a INT, b DOUBLE'                | {1, 0.8}    |
        | Parse struct with string fields | '{"name":"Alice", "age":30}', 'name STRING, age INT' | {Alice, 30} |
        | Parse nested struct from JSON   | '{"a":1, "b":{"c":3}}', 'a INT, b STRUCT<c: INT>'    | {1, {3}}    |

  Rule: Struct with STRUCT<> schema syntax
    Scenario: Parse struct using explicit STRUCT syntax
      When query
        """
        SELECT from_json('{"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS result
        """
      Then query result
        | result                            |
        | {Alice, [{Bob, 1}, {Charlie, 2}]} |

  Rule: Null and error handling (PERMISSIVE mode)
    Scenario Outline: PERMISSIVE: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | args                         | result    |
        | Null input returns null struct               | NULL, 'a INT, b STRING'      | NULL      |
        | Invalid JSON returns struct with null fields | 'not valid json', 'a INT'    | {NULL}    |
        | Missing fields return null values            | '{"a":1}', 'a INT, b STRING' | {1, NULL} |

  Rule: Timestamp formatting with options
    Scenario: Parse struct with timestamp using custom format
      When query
        """
        SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result                |
        | {2015-08-26 00:00:00} |

  Rule: Boolean and numeric types
    Scenario Outline: Boolean and numeric: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | args                                                        | result      |
        | Parse boolean values                   | '{"flag":true}', 'flag BOOLEAN'                             | {true}      |
        | Parse various numeric types            | '{"a":1, "b":2.5}', 'a BIGINT, b DOUBLE'                    | {1, 2.5}    |
        | Parse tinyint smallint and float types | '{"a":1, "b":2, "c":3.5}', 'a TINYINT, b SMALLINT, c FLOAT' | {1, 2, 3.5} |

  Rule: Array parsing
    Scenario Outline: Array: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | args                               | result    |
        | Parse top-level array of integers   | '[1, 2, 3]', 'ARRAY<INT>'          | [1, 2, 3] |
        | Parse empty array                   | '[]', 'ARRAY<INT>'                 | []        |
        | Null input returns null for array   | CAST(NULL AS STRING), 'ARRAY<INT>' | NULL      |
        | Invalid JSON returns null for array | 'not json', 'ARRAY<INT>'           | NULL      |

  Rule: Map parsing
    Scenario Outline: Map: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                                     | result           |
        | Parse top-level map             | '{"a":1, "b":2}', 'MAP<STRING, INT>'     | {a -> 1, b -> 2} |
        | Null input returns null for map | CAST(NULL AS STRING), 'MAP<STRING, INT>' | NULL             |

  Rule: Date parsing
    Scenario Outline: Date: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |

      Examples:
        | case                          | args                                                            |
        | Parse date field              | '{"d":"2024-01-15"}', 'd DATE'                                  |
        | Parse date with custom format | '{"d":"15/01/2024"}', 'd DATE', map('dateFormat', 'dd/MM/yyyy') |

  Rule: Decimal parsing
    Scenario Outline: Decimal: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {3.14} |

      Examples:
        | case                      | args                              |
        | Parse decimal from number | '{"v":3.14}', 'v DECIMAL(10,2)'   |
        | Parse decimal from string | '{"v":"3.14"}', 'v DECIMAL(10,2)' |

  Rule: Nested collections in struct
    Scenario Outline: Nested collection: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | args                                                 | result             |
        | Parse struct with nested array | '{"items":[1,2,3]}', 'STRUCT<items: ARRAY<INT>>'     | {[1, 2, 3]}        |
        | Parse struct with nested map   | '{"m":{"x":1,"y":2}}', 'STRUCT<m: MAP<STRING, INT>>' | {{x -> 1, y -> 2}} |

  Rule: String coercion
    Scenario Outline: String coercion: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | args                     | result |
        | Parse number as string type        | '{"a":123}', 'a STRING'  | {123}  |
        | Parse boolean value as string type | '{"a":true}', 'a STRING' | {true} |
        | Parse null value as string type    | '{"a":null}', 'a STRING' | {NULL} |
        | Parse float value as string type   | '{"a":1.5}', 'a STRING'  | {1.5}  |

  Rule: Batch processing
    Scenario: Parse multiple valid JSON rows as struct
      When query
        """
        SELECT from_json(json_str, 'a INT, b STRING') AS result
        FROM VALUES
          ('{"a":1,"b":"x"}'),
          ('{"a":2,"b":"y"}'),
          ('{"a":3,"b":"z"}')
        AS t(json_str)
        ORDER BY result.a
        """
      Then query result ordered
        | result |
        | {1, x} |
        | {2, y} |
        | {3, z} |

    Scenario: Parse batch with mixed valid invalid and null rows
      When query
        """
        SELECT from_json(json_str, 'a INT') AS result
        FROM VALUES
          ('{"a":10}'),
          (NULL),
          ('not json'),
          ('{"a":20}')
        AS t(json_str)
        ORDER BY result.a NULLS FIRST
        """
      Then query result ordered
        | result |
        | NULL   |
        | {NULL} |
        | {10}   |
        | {20}   |

    Scenario: Parse multiple rows returning arrays
      When query
        """
        SELECT from_json(json_str, 'ARRAY<INT>') AS result
        FROM VALUES
          ('[1, 2]'),
          ('[3, 4, 5]'),
          ('[]')
        AS t(json_str)
        ORDER BY size(result)
        """
      Then query result ordered
        | result    |
        | []        |
        | [1, 2]    |
        | [3, 4, 5] |

    Scenario: Parse multiple rows returning maps
      When query
        """
        SELECT from_json(json_str, 'MAP<STRING, INT>') AS result
        FROM VALUES
          ('{"x":1}'),
          ('{"y":2}')
        AS t(json_str)
        ORDER BY to_json(result)
        """
      Then query result ordered
        | result   |
        | {x -> 1} |
        | {y -> 2} |

  Rule: Type mismatch returns null
    Scenario Outline: Type mismatch: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {NULL} |

      Examples:
        | case                                                   | args                                              |
        | Boolean field with numeric JSON value returns null     | '{"flag":1}', 'flag BOOLEAN'                      |
        | Int field with string JSON value returns null          | '{"n":"not_a_number"}', 'n INT'                   |
        | Float field with string JSON value returns null        | '{"f":"text"}', 'f DOUBLE'                        |
        | Date field with numeric JSON value returns null        | '{"d":20240115}', 'd DATE'                        |
        | Nested struct field with non-object value returns null | '{"s":"not_object"}', 'STRUCT<s: STRUCT<x: INT>>' |
        | Nested array field with non-array value returns null   | '{"arr":"not_array"}', 'STRUCT<arr: ARRAY<INT>>'  |
        | Nested map field with non-object value returns null    | '{"m":"not_map"}', 'STRUCT<m: MAP<STRING, INT>>'  |

  Rule: Decimal edge cases
    Scenario Outline: Decimal edge: <case>
      When query
        """
        SELECT from_json(<json>, 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                           | json            | result     |
        | Parse negative decimal from number                             | '{"v":-3.14}'   | {-3.14}    |
        | Parse negative decimal from string                             | '{"v":"-1.50"}' | {-1.50}    |
        | Parse decimal with more fractional digits than scale truncates | '{"v":3.141}'   | {3.14}     |
        | Parse large integer as decimal                                 | '{"v":12345}'   | {12345.00} |

  Rule: Array element type coverage
    Scenario Outline: Array element type: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | args                                         | result              |
        | Parse array of booleans        | '[true, false, true]', 'ARRAY<BOOLEAN>'      | [true, false, true] |
        | Parse array of strings         | '["hello", "world"]', 'ARRAY<STRING>'        | [hello, world]      |
        | Parse array of doubles         | '[1.1, 2.2, 3.3]', 'ARRAY<DOUBLE>'           | [1.1, 2.2, 3.3]     |
        | Parse array of structs         | '[{"x":1},{"x":2}]', 'ARRAY<STRUCT<x: INT>>' | [{1}, {2}]          |
        | Parse array of arrays          | '[[1,2],[3,4]]', 'ARRAY<ARRAY<INT>>'         | [[1, 2], [3, 4]]    |
        | Parse array with null elements | '[1, null, 3]', 'ARRAY<INT>'                 | [1, NULL, 3]        |

  Rule: Struct edge cases
    Scenario Outline: Struct edge: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                    | args                                              | result    |
        | Explicit JSON null value for a typed field returns null | '{"a":1,"b":null}', 'a INT, b STRING'             | {1, NULL} |
        | Extra JSON fields not in schema are ignored             | '{"a":1,"extra":"ignored","b":2}', 'a INT, b INT' | {1, 2}    |

  Rule: Map edge cases
    Scenario Outline: Map edge: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | args                                                   | result                       |
        | Invalid JSON for map returns null       | 'not json', 'MAP<STRING, INT>'                         | NULL                         |
        | Array input for map schema returns null | '[1,2,3]', 'MAP<STRING, INT>'                          | NULL                         |
        | Map with string values                  | '{"key1":"val1","key2":"val2"}', 'MAP<STRING, STRING>' | {key1 -> val1, key2 -> val2} |

  Rule: Timestamp without timezone
    Scenario Outline: Timestamp NTZ: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result                |
        | {2024-06-15 10:30:00} |

      Examples:
        | case                                                        | args                                               |
        | Parse timestamp without timezone using TIMESTAMP_NTZ schema | '{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_NTZ' |

    # A TIMESTAMP_NTZ field is driven by the `timestampNTZFormat` option, not `timestampFormat`,
    # so Spark ignores the custom pattern here and fails the parse. Sail applies `timestampFormat`
    # to TIMESTAMP_NTZ as well and parses the value.
    @sail-bug
    Scenario: Parse timestamp without timezone with a custom timestampFormat is not applied
      When query
        """
        SELECT from_json('{"ts":"15/06/2024 10:30"}', 'ts TIMESTAMP_NTZ', map('timestampFormat', 'dd/MM/yyyy HH:mm')) AS result
        """
      Then query result
        | result |
        | {NULL} |

  Rule: Null value handling
    Scenario Outline: JSON null: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {NULL} |

      Examples:
        | case                                       | args                            |
        | JSON null for boolean field returns null   | '{"flag":null}', 'flag BOOLEAN' |
        | JSON null for int field returns null       | '{"n":null}', 'n INT'           |
        | JSON null for decimal field returns null   | '{"v":null}', 'v DECIMAL(10,2)' |
        | JSON null for date field returns null      | '{"d":null}', 'd DATE'          |
        | JSON null for timestamp field returns null | '{"ts":null}', 'ts TIMESTAMP'   |

  Rule: Decimal advanced parsing
    Scenario Outline: Decimal advanced: <case>
      When query
        """
        SELECT from_json(<json>, 'v DECIMAL(10,<scale>)') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                            | json             | scale | result   |
        | Parse decimal with scientific notation          | '{"v":"1.5e2"}'  | 2     | {150.00} |
        | Parse decimal with positive sign prefix         | '{"v":"+3.14"}'  | 2     | {3.14}   |
        | Parse decimal with zero value                   | '{"v":"0.00"}'   | 2     | {0.00}   |
        | Parse decimal from integer JSON number          | '{"v":42}'       | 2     | {42.00}  |
        | Parse decimal with rounding (half-up)           | '{"v":"3.145"}'  | 2     | {3.15}   |
        | Decimal type mismatch with boolean returns null | '{"v":true}'     | 2     | {NULL}   |
        | Parse decimal with negative scientific notation | '{"v":"1.5e-1"}' | 4     | {0.1500} |
        | Parse decimal integer without fraction          | '{"v":"100"}'    | 2     | {100.00} |

  Rule: Timestamp error and edge cases
    Scenario Outline: Timestamp edge: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {NULL} |

      Examples:
        | case                                                   | args                              |
        | Timestamp NTZ field with non-string value returns null | '{"ts":true}', 'ts TIMESTAMP_NTZ' |

    # Spark reads a JSON number into a TIMESTAMP field as epoch seconds
    # (`JacksonParser`: `VALUE_NUMBER_INT` -> `longToTimestamp`); Sail returns NULL.
    @sail-bug
    Scenario: Timestamp field with a numeric value is read as epoch seconds
      When query
        """
        SELECT from_json('{"ts":12345}', 'ts TIMESTAMP') AS result
        """
      Then query result
        | result                |
        | {1970-01-01 03:25:45} |

    Scenario: Parse date-only string as timestamp
      When query
        """
        SELECT from_json('{"ts":"2024-06-15"}', 'ts TIMESTAMP_NTZ', map('timestampFormat', 'yyyy-MM-dd')) AS result
        """
      Then query result
        | result                |
        | {2024-06-15 00:00:00} |

  Rule: Array of maps and nested collections
    Scenario Outline: Nested collections: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | args                                                                             | result               |
        | Parse array of maps                            | '[{"a":1},{"b":2}]', 'ARRAY<MAP<STRING, INT>>'                                   | [{a -> 1}, {b -> 2}] |
        | Parse struct with nested array and map         | '{"arr":[1,2],"m":{"k":"v"}}', 'STRUCT<arr: ARRAY<INT>, m: MAP<STRING, STRING>>' | {[1, 2], {k -> v}}   |
        | Parse map with double values                   | '{"x":1.5,"y":2.5}', 'MAP<STRING, DOUBLE>'                                       | {x -> 1.5, y -> 2.5} |
        | Parse array of nested structs with mixed types | '[{"a":1,"b":"x"},{"a":2,"b":"y"}]', 'ARRAY<STRUCT<a: INT, b: STRING>>'          | [{1, x}, {2, y}]     |

  Rule: Non-string JSON values as string type
    Scenario Outline: Non-string as string: <case>
      When query
        """
        SELECT from_json(<json>, 'a STRING') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | json                     | result             |
        | Parse object value as string type | '{"a":{"nested":"obj"}}' | {{"nested":"obj"}} |
        | Parse array value as string type  | '{"a":[1,2,3]}'          | {[1,2,3]}          |

  Rule: Decimal from nested struct
    Scenario: Parse struct with decimal field
      When query
        """
        SELECT from_json('{"price":19.99,"qty":5}', 'price DECIMAL(10,2), qty INT') AS result
        """
      Then query result
        | result     |
        | {19.99, 5} |

  Rule: Multiple fields with various types
    Scenario: Parse struct with boolean int float string and date
      When query
        """
        SELECT from_json('{"flag":true,"count":42,"ratio":0.5,"name":"test","dt":"2024-01-01"}', 'flag BOOLEAN, count INT, ratio DOUBLE, name STRING, dt DATE') AS result
        """
      Then query result
        | result                            |
        | {true, 42, 0.5, test, 2024-01-01} |

  Rule: Empty and edge case collections
    Scenario Outline: Empty and edge collection: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | args                                        | result       |
        | Parse empty map                      | '{}', 'MAP<STRING, INT>'                    | {}           |
        | Parse struct from empty JSON object  | '{}', 'a INT, b STRING'                     | {NULL, NULL} |
        | Parse struct with nested null struct | '{"s":null}', 'STRUCT<s: STRUCT<x: INT>>'   | {NULL}       |
        | Parse struct with nested null array  | '{"arr":null}', 'STRUCT<arr: ARRAY<INT>>'   | {NULL}       |
        | Parse struct with nested null map    | '{"m":null}', 'STRUCT<m: MAP<STRING, INT>>' | {NULL}       |

  Rule: Map with nested struct values
    Scenario: Parse map with struct values
      When query
        """
        SELECT from_json('{"k1":{"a":1},"k2":{"a":2}}', 'MAP<STRING, STRUCT<a: INT>>') AS result
        """
      Then query result
        | result                 |
        | {k1 -> {1}, k2 -> {2}} |

  Rule: Deeply nested structures
    Scenario Outline: Deeply nested: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                                                            | result             |
        | Parse triple nested struct      | '{"a":{"b":{"c":42}}}', 'STRUCT<a: STRUCT<b: STRUCT<c: INT>>>'  | {{{42}}}           |
        | Parse struct with list of lists | '{"matrix":[[1,2],[3,4]]}', 'STRUCT<matrix: ARRAY<ARRAY<INT>>>' | {[[1, 2], [3, 4]]} |

  Rule: Struct field ordering
    Scenario: JSON field order does not matter
      When query
        """
        SELECT from_json('{"b":2,"a":1}', 'a INT, b INT') AS result
        """
      Then query result
        | result |
        | {1, 2} |

  Rule: LargeUtf8 schema support
    Scenario: Parse struct with string value to verify string handling
      When query
        """
        SELECT from_json('{"a":"hello","b":"world"}', 'a STRING, b STRING') AS result
        """
      Then query result
        | result         |
        | {hello, world} |

  Rule: Map with nested array values
    Scenario: Parse map with array values
      When query
        """
        SELECT from_json('{"nums":[1,2,3]}', 'MAP<STRING, ARRAY<INT>>') AS result
        """
      Then query result
        | result              |
        | {nums -> [1, 2, 3]} |

  # `TEXT` is an Arrow type name accepted by Sail's schema parser; Spark's DDL parser rejects
  # it with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'TEXT'`.
  Rule: TEXT (LargeUtf8) schema type
    @sail-only
    Scenario Outline: TEXT field: <case>
      When query
        """
        SELECT from_json(<json>, 'a TEXT') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | json            | result  |
        | Parse string value to TEXT field                    | '{"a":"hello"}' | {hello} |
        | Parse number value to TEXT field coerces to string  | '{"a":123}'     | {123}   |
        | Parse boolean value to TEXT field coerces to string | '{"a":true}'    | {true}  |

  Rule: Schema types with no native json_value_to_scalar handling return null
    Scenario Outline: Unsupported scalar type: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {NULL} |

      Examples:
        | case                                    | args                        |
        | Parse JSON to BINARY field returns null | '{"b":"hello"}', 'b BINARY' |

    # `DATE64` is an Arrow type name, and Spark caps DECIMAL precision at 38. Both are rejected
    # by Spark's DDL parser with `[PARSE_SYNTAX_ERROR]`, so these are Sail schema extensions.
    @sail-only
    Scenario Outline: Unsupported scalar type, Sail-only schema type: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result |
        | {NULL} |

      Examples:
        | case                                                              | args                             |
        | Parse JSON to DATE64 field returns null                           | '{"d":"2024-01-15"}', 'd DATE64' |
        | Parse JSON to DECIMAL with precision greater than 38 returns null | '{"v":3.14}', 'v DECIMAL(40,2)'  |

    # Spark parses TIME fields in `from_json`: the `spark.sql.timeType.enabled` gate lives in
    # `TimeExpression`, not in the type itself, so no configuration is needed here.
    @sail-bug
    Scenario Outline: TIME schema type: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result     |
        | {12:00:00} |

      Examples:
        | case                                     | args                            |
        | Parse JSON to TIME field                 | '{"t":"12:00:00"}', 't TIME'    |
        | Parse JSON to TIME(0) field (Time32)     | '{"t":"12:00:00"}', 't TIME(0)' |

  Rule: Timestamp schema precision variants
    Scenario Outline: Timestamp precision: <case>
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts <type>') AS result
        """
      Then query result
        | result                |
        | {2024-06-15 10:30:00} |

      Examples:
        | case                                      | type          |
        | Parse timestamp with TIMESTAMP_LTZ schema | TIMESTAMP_LTZ |

    # Spark has no parameterized timestamp type: `TIMESTAMP_NTZ(<p>)` is rejected by its DDL
    # parser with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'TIMESTAMP_NTZ'`.
    @sail-only
    Scenario Outline: Timestamp precision, Sail-only schema type: <case>
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts <type>') AS result
        """
      Then query result
        | result                |
        | {2024-06-15 10:30:00} |

      Examples:
        | case                                                          | type             |
        | Parse timestamp with second precision (TIMESTAMP_NTZ(0))      | TIMESTAMP_NTZ(0) |
        | Parse timestamp with millisecond precision (TIMESTAMP_NTZ(3)) | TIMESTAMP_NTZ(3) |
        | Parse timestamp with nanosecond precision (TIMESTAMP_NTZ(9))  | TIMESTAMP_NTZ(9) |

  Rule: Schema parsing errors
    Scenario Outline: Schema error: <case>
      When query
        """
        SELECT from_json('{"a":1}', <schema>) AS result
        """
      Then query error .*

      Examples:
        | case                                         | schema       |
        | Schema with unsupported type produces error  | 'a GEOMETRY' |
        | Empty schema string produces error           | ''           |
        | Whitespace-only schema string produces error | '   '        |

  Rule: Spark JSON schema format
    Scenario: Parse struct using Spark JSON schema
      When query
        """
        SELECT from_json('{"a": 1}', '{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: Parse struct with multiple fields using JSON schema
      When query
        """
        SELECT from_json('{"a": 1, "b": "hello"}', '{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}},{"name":"b","type":"string","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result     |
        | {1, hello} |

    Scenario: Parse array using Spark JSON schema
      When query
        """
        SELECT from_json('[1, 2, 3]', '{"type":"array","elementType":"integer","containsNull":true}') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Parse map using Spark JSON schema
      When query
        """
        SELECT from_json('{"a":1, "b":2}', '{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}') AS result
        """
      Then query result
        | result           |
        | {a -> 1, b -> 2} |

    Scenario: Parse decimal type using JSON schema
      When query
        """
        SELECT from_json('{"v":3.14}', '{"type":"struct","fields":[{"name":"v","type":"decimal(10,2)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result |
        | {3.14} |

    Scenario: Parse nested struct using JSON schema
      When query
        """
        SELECT from_json('{"a":{"b":42}}', '{"type":"struct","fields":[{"name":"a","type":{"type":"struct","fields":[{"name":"b","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result |
        | {{42}} |

    Scenario: Invalid JSON schema string returns error
      When query
        """
        SELECT from_json('{"a":1}', '{"type":"struct"') AS result
        """
      Then query error .*

    # Spark's JSON schema type names for TIME always carry a precision (`time(6)`), so the
    # bare `time` name is not recognized and the string falls through to the DDL parser,
    # which fails with `[PARSE_SYNTAX_ERROR] Syntax error at or near '{'`.
    @sail-only
    Scenario: Parse struct with time field using Spark JSON schema
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', '{"type":"struct","fields":[{"name":"t","type":"time","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result |
        | {NULL} |

    @sail-bug
    Scenario: Parse struct with time(0) field using Spark JSON schema
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', '{"type":"struct","fields":[{"name":"t","type":"time(0)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result     |
        | {12:00:00} |

    # Spark rejects CHAR/VARCHAR in a `from_json` schema with
    # `[UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING] The char/varchar type can't be used in the table
    # schema.` Sail accepts them and reads the values as strings.
    @sail-bug
    Scenario: Parse struct with char and varchar fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"c":"abc","v":"hello"}', '{"type":"struct","fields":[{"name":"c","type":"char(3)","nullable":true,"metadata":{}},{"name":"v","type":"varchar(5)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query error char/varchar type can't be used in the table schema

    @sail-bug
    Scenario: Parse nested char and varchar fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"items":["a","b"],"m":{"k":"value"}}', '{"type":"struct","fields":[{"name":"items","type":{"type":"array","elementType":"char(1)","containsNull":true},"nullable":true,"metadata":{}},{"name":"m","type":{"type":"map","keyType":"string","valueType":"varchar(5)","valueContainsNull":true},"nullable":true,"metadata":{}}]}') AS result
        """
      Then query error char/varchar type can't be used in the table schema

    Scenario: Parse struct with interval fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"cal":null,"ym":null,"dt":null}', '{"type":"struct","fields":[{"name":"cal","type":"interval","nullable":true,"metadata":{}},{"name":"ym","type":"interval year to month","nullable":true,"metadata":{}},{"name":"dt","type":"interval day to second","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result             |
        | {NULL, NULL, NULL} |

    # Spark's JSON schema parser does not know the `geometry(...)` / `geography(...)` type
    # names, so the whole schema string falls through to the DDL parser and fails with
    # `[PARSE_SYNTAX_ERROR] Syntax error at or near '{'`.
    @sail-only
    Scenario: Parse struct with variant and geospatial fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"v":null,"g":null,"p":null}', '{"type":"struct","fields":[{"name":"v","type":"variant","nullable":true,"metadata":{}},{"name":"g","type":"geometry(ANY)","nullable":true,"metadata":{}},{"name":"p","type":"geography(ANY, spherical)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result             |
        | {NULL, NULL, NULL} |

    Scenario: Parse UDT field using its Spark JSON sqlType
      When query
        """
        SELECT from_json('{"point":{"x":1.5,"y":2.5}}', '{"type":"struct","fields":[{"name":"point","type":{"type":"udt","pyClass":"example.PointUDT","serializedClass":"abc","sqlType":{"type":"struct","fields":[{"name":"x","type":"double","nullable":false,"metadata":{}},{"name":"y","type":"double","nullable":false,"metadata":{}}]}},"nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result       |
        | {{1.5, 2.5}} |

  Rule: Column display names
    Scenario Outline: Display name from_json(value): <case>
      When query
        """
        SELECT from_json(value, <schema>)
        FROM VALUES (<value>) AS t(value)
        """
      Then query result
        | from_json(value) |
        | <result>         |

      Examples:
        | case                                                                  | schema                        | value           | result     |
        | from_json column name shows only input column for struct              | 'a INT'                       | '{"a":1}'       | {1}        |
        | from_json column name for struct with nested map does not use entries | 'STRUCT<m: MAP<STRING, INT>>' | '{"m":{"x":1}}' | {{x -> 1}} |

    Scenario Outline: Display name entries: <case>
      When query
        """
        SELECT from_json(value, <schema>)
        FROM VALUES ('{"a":1}') AS t(value)
        """
      Then query result
        | entries  |
        | {a -> 1} |

      Examples:
        | case                                                    | schema                                                                             |
        | from_json column name shows entries for MAP schema      | 'MAP<STRING, INT>'                                                                 |
        | from_json column name shows entries for MAP JSON schema | '{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}' |

  Rule: DDL schema with column reference
    Scenario: Parse struct with DDL schema from column values
      When query
        """
        SELECT from_json(value, 'a INT') AS json
        FROM VALUES ('{"a": 1}') AS t(value)
        """
      Then query result
        | json |
        | {1}  |

  Rule: Constant-fold schema expression at planning time
    Scenario Outline: Constant-fold schema: <case>
      When query
        """
        SELECT from_json(value, schema_of_json(<schema_args>)) AS result
        FROM VALUES (<value>) AS t(value)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                     | schema_args                          | value                  | result      |
        | from_json with schema_of_json as the schema argument                     | '{"a":1,"b":"hello"}'                | '{"a":42,"b":"world"}' | {42, world} |
        | from_json with schema_of_json options as the schema argument             | '{"a":1}', map('mode', 'PERMISSIVE') | '{"a":42}'             | {42}        |
        | from_json with schema_of_json non-default options as the schema argument | '{"a":1}', map('mode', 'FAILFAST')   | '{"a":42}'             | {42}        |

    Scenario: from_json with schema_of_json handles multiple rows
      When query
        """
        SELECT from_json(value, schema_of_json('{"x":1}')) AS result
        FROM VALUES ('{"x":10}'), ('{"x":20}'), ('{"x":30}') AS t(value)
        ORDER BY result.x
        """
      Then query result ordered
        | result |
        | {10}   |
        | {20}   |
        | {30}   |

  Rule: Single value wrapping for array schema
    Scenario: Single JSON object with array schema wraps into singleton array
      When query
        """
        SELECT from_json('{"a":1}', 'ARRAY<STRUCT<a: INT>>') AS result
        """
      Then query result
        | result |
        | [{1}]  |

  Rule: Binary field type
    # Spark base64-decodes JSON strings into BINARY fields (`JacksonParser` uses
    # `getBinaryValue`), so a valid base64 payload round-trips to its bytes. Sail returns NULL.
    # The sibling case above with `"hello"` (not valid base64) yields NULL on both engines.
    @sail-bug
    Scenario: Parse binary field base64-decodes the value
      When query
        """
        SELECT from_json('{"b":"aGVsbG8="}', 'b BINARY') AS result
        """
      Then query result
        | result             |
        | {[68 65 6C 6C 6F]} |

  Rule: Null propagation through nested structures
    Scenario Outline: Null propagation: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | args                                                | result              |
        | Null in array of structs | '[{"a":1}, null, {"a":3}]', 'ARRAY<STRUCT<a: INT>>' | [{1}, NULL, {3}]    |
        | Null values in map       | '{"a":1, "b":null}', 'MAP<STRING, INT>'             | {a -> 1, b -> NULL} |

  Rule: Unicode handling
    Scenario: Parse JSON with unicode characters
      When query
        """
        SELECT from_json('{"name":"héllo wörld"}', 'name STRING') AS result
        """
      Then query result
        | result        |
        | {héllo wörld} |

  Rule: Escaped strings in JSON
    Scenario: Parse JSON with escaped quotes
      When query
        """
        SELECT from_json('{"a":"he said \\"hello\\""}', 'a STRING') AS result
        """
      Then query result
        | result            |
        | {he said "hello"} |

  Rule: Large batch processing
    Scenario: Parse many rows with mixed valid and null inputs
      When query
        """
        SELECT from_json(json_str, 'x INT') AS result
        FROM VALUES
          ('{"x":1}'),
          (NULL),
          ('{"x":2}'),
          ('invalid'),
          ('{"x":3}'),
          (NULL),
          ('{}'),
          ('{"x":4}')
        AS t(json_str)
        ORDER BY result.x
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | {NULL} |
        | {NULL} |
        | {1}    |
        | {2}    |
        | {3}    |
        | {4}    |

  Rule: Valid but non-matching JSON value at top level (PERMISSIVE)
    Scenario Outline: Non-matching top level: <case>
      When query
        """
        SELECT from_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                    | args                    | result |
        | Parseable JSON number as struct target returns struct with null fields  | '42', 'a INT'           | {NULL} |
        | Parseable JSON string as struct target returns struct with null fields  | '"hello"', 'a INT'      | {NULL} |
        | Parseable JSON array as struct target returns struct with null fields   | '[1,2,3]', 'a INT'      | {NULL} |
        | Parseable JSON boolean as struct target returns struct with null fields | 'true', 'a INT'         | {NULL} |
        | Parseable JSON number as array target returns null                      | '42', 'ARRAY<INT>'      | NULL   |
        | Parseable JSON number as map target returns null                        | '42', 'MAP<STRING,INT>' | NULL   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null json literal yields a struct
      When query
        """
        SELECT from_json('{"a":1}', 'a INT') AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = true)
         |    |-- a: integer (nullable = true)
        """

    Scenario: a non-null json column yields a struct
      When query
        """
        SELECT from_json(CONCAT('{"n":', CAST(id AS STRING), '}'), 'n INT') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = true)
         |    |-- n: integer (nullable = true)
        """

    Scenario: a nullable json column stays nullable
      When query
        """
        SELECT from_json(c, 'a INT') AS result FROM VALUES ('{"a":1}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = true)
         |    |-- a: integer (nullable = true)
        """

  Rule: Result values (migrated from test_from_json.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT from_json(value, <schema>) AS json FROM VALUES (1, <value>) AS t(key, value)
        """
      Then query result
        | json   |
        | <json> |

      Examples:
        | case                          | schema                  | value                    | json       |
        | from_json doctest #1 (result) | 'a INT'                 | '{"a": 1}'               | {1}        |
        | from_json doctest #2 (result) | 'MAP<STRING,INT>'       | '{"a": 1}'               | {a -> 1}   |
        | from_json doctest #3 (result) | 'ARRAY<STRUCT<a: INT>>' | '{"a": 1}'               | [{1}]      |
        | from_json doctest #5 (result) | 'a INT, b STRING'       | '{"a": 1, "b": "hello"}' | {1, hello} |
        | from_json doctest #6 (result) | 'price DECIMAL(10,2)'   | '{"price": 19.99}'       | {19.99}    |

    Scenario: from_json doctest #4 (result)
      When query
        """
        SELECT from_json(value, 'a INT') AS json FROM VALUES (1, '{"a": 1}'), (2, '{"a": 2}'), (3, CAST(NULL AS STRING)) AS t(key, value) ORDER BY key
        """
      Then query result ordered
        | json |
        | {1}  |
        | {2}  |
        | NULL |
