Feature: from_json function parses JSON strings into structured types

  Rule: Basic struct parsing
    Scenario: Parse simple struct from JSON
      When query
        """
        SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE') AS result
        """
      Then query result
        | result       |
        | {1, 0.8}     |

    Scenario: Parse struct with string fields
      When query
        """
        SELECT from_json('{"name":"Alice", "age":30}', 'name STRING, age INT') AS result
        """
      Then query result
        | result          |
        | {Alice, 30}     |

    Scenario: Parse nested struct from JSON
      When query
        """
        SELECT from_json('{"a":1, "b":{"c":3}}', 'a INT, b STRUCT<c: INT>') AS result
        """
      Then query result
        | result          |
        | {1, {3}}        |

  Rule: Struct with STRUCT<> schema syntax
    Scenario: Parse struct using explicit STRUCT syntax
      When query
        """
        SELECT from_json('{"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS result
        """
      Then query result
        | result                                         |
        | {Alice, [{Bob, 1}, {Charlie, 2}]}              |

  Rule: Null and error handling (PERMISSIVE mode)
    Scenario: Null input returns null struct
      When query
        """
        SELECT from_json(NULL, 'a INT, b STRING') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid JSON returns struct with null fields
      When query
        """
        SELECT from_json('not valid json', 'a INT') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Missing fields return null values
      When query
        """
        SELECT from_json('{"a":1}', 'a INT, b STRING') AS result
        """
      Then query result
        | result      |
        | {1, NULL}   |

  Rule: Timestamp formatting with options
    Scenario: Parse struct with timestamp using custom format
      When query
        """
        SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result                       |
        | {2015-08-26 00:00:00}        |

  Rule: Boolean and numeric types
    Scenario: Parse boolean values
      When query
        """
        SELECT from_json('{"flag":true}', 'flag BOOLEAN') AS result
        """
      Then query result
        | result  |
        | {true}  |

    Scenario: Parse various numeric types
      When query
        """
        SELECT from_json('{"a":1, "b":2.5}', 'a BIGINT, b DOUBLE') AS result
        """
      Then query result
        | result    |
        | {1, 2.5}  |

    Scenario: Parse tinyint smallint and float types
      When query
        """
        SELECT from_json('{"a":1, "b":2, "c":3.5}', 'a TINYINT, b SMALLINT, c FLOAT') AS result
        """
      Then query result
        | result      |
        | {1, 2, 3.5} |

  Rule: Array parsing
    Scenario: Parse top-level array of integers
      When query
        """
        SELECT from_json('[1, 2, 3]', 'ARRAY<INT>') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Parse empty array
      When query
        """
        SELECT from_json('[]', 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Null input returns null for array
      When query
        """
        SELECT from_json(CAST(NULL AS STRING), 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid JSON returns null for array
      When query
        """
        SELECT from_json('not json', 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Map parsing
    Scenario: Parse top-level map
      When query
        """
        SELECT from_json('{"a":1, "b":2}', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result          |
        | {a -> 1, b -> 2} |

    Scenario: Null input returns null for map
      When query
        """
        SELECT from_json(CAST(NULL AS STRING), 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Date parsing
    Scenario: Parse date field
      When query
        """
        SELECT from_json('{"d":"2024-01-15"}', 'd DATE') AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |

    Scenario: Parse date with custom format
      When query
        """
        SELECT from_json('{"d":"15/01/2024"}', 'd DATE', map('dateFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |

  Rule: Decimal parsing
    Scenario: Parse decimal from number
      When query
        """
        SELECT from_json('{"v":3.14}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

    Scenario: Parse decimal from string
      When query
        """
        SELECT from_json('{"v":"3.14"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

  Rule: Nested collections in struct
    Scenario: Parse struct with nested array
      When query
        """
        SELECT from_json('{"items":[1,2,3]}', 'STRUCT<items: ARRAY<INT>>') AS result
        """
      Then query result
        | result        |
        | {[1, 2, 3]}   |

    Scenario: Parse struct with nested map
      When query
        """
        SELECT from_json('{"m":{"x":1,"y":2}}', 'STRUCT<m: MAP<STRING, INT>>') AS result
        """
      Then query result
        | result               |
        | {{x -> 1, y -> 2}}   |

  Rule: String coercion
    Scenario: Parse number as string type
      When query
        """
        SELECT from_json('{"a":123}', 'a STRING') AS result
        """
      Then query result
        | result |
        | {123}  |

    Scenario: Parse boolean value as string type
      When query
        """
        SELECT from_json('{"a":true}', 'a STRING') AS result
        """
      Then query result
        | result |
        | {true} |

    Scenario: Parse null value as string type
      When query
        """
        SELECT from_json('{"a":null}', 'a STRING') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

    Scenario: Parse float value as string type
      When query
        """
        SELECT from_json('{"a":1.5}', 'a STRING') AS result
        """
      Then query result
        | result  |
        | {1.5}   |

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
        | result   |
        | {1, x}   |
        | {2, y}   |
        | {3, z}   |

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
        | result  |
        | NULL    |
        | {NULL}  |
        | {10}    |
        | {20}    |

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
        | result      |
        | []          |
        | [1, 2]      |
        | [3, 4, 5]   |

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
        | result    |
        | {x -> 1}  |
        | {y -> 2}  |

  Rule: Type mismatch returns null
    Scenario: Boolean field with numeric JSON value returns null
      When query
        """
        SELECT from_json('{"flag":1}', 'flag BOOLEAN') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Int field with string JSON value returns null
      When query
        """
        SELECT from_json('{"n":"not_a_number"}', 'n INT') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Float field with string JSON value returns null
      When query
        """
        SELECT from_json('{"f":"text"}', 'f DOUBLE') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Date field with numeric JSON value returns null
      When query
        """
        SELECT from_json('{"d":20240115}', 'd DATE') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Nested struct field with non-object value returns null
      When query
        """
        SELECT from_json('{"s":"not_object"}', 'STRUCT<s: STRUCT<x: INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

    Scenario: Nested array field with non-array value returns null
      When query
        """
        SELECT from_json('{"arr":"not_array"}', 'STRUCT<arr: ARRAY<INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

    Scenario: Nested map field with non-object value returns null
      When query
        """
        SELECT from_json('{"m":"not_map"}', 'STRUCT<m: MAP<STRING, INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

  Rule: Decimal edge cases
    Scenario: Parse negative decimal from number
      When query
        """
        SELECT from_json('{"v":-3.14}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result  |
        | {-3.14} |

    Scenario: Parse negative decimal from string
      When query
        """
        SELECT from_json('{"v":"-1.50"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result  |
        | {-1.50} |

    Scenario: Parse decimal with more fractional digits than scale truncates
      When query
        """
        SELECT from_json('{"v":3.141}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

    Scenario: Parse large integer as decimal
      When query
        """
        SELECT from_json('{"v":12345}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result     |
        | {12345.00} |

  Rule: Array element type coverage
    Scenario: Parse array of booleans
      When query
        """
        SELECT from_json('[true, false, true]', 'ARRAY<BOOLEAN>') AS result
        """
      Then query result
        | result               |
        | [true, false, true]  |

    Scenario: Parse array of strings
      When query
        """
        SELECT from_json('["hello", "world"]', 'ARRAY<STRING>') AS result
        """
      Then query result
        | result           |
        | [hello, world]   |

    Scenario: Parse array of doubles
      When query
        """
        SELECT from_json('[1.1, 2.2, 3.3]', 'ARRAY<DOUBLE>') AS result
        """
      Then query result
        | result          |
        | [1.1, 2.2, 3.3] |

    Scenario: Parse array of structs
      When query
        """
        SELECT from_json('[{"x":1},{"x":2}]', 'ARRAY<STRUCT<x: INT>>') AS result
        """
      Then query result
        | result          |
        | [{1}, {2}]      |

    Scenario: Parse array of arrays
      When query
        """
        SELECT from_json('[[1,2],[3,4]]', 'ARRAY<ARRAY<INT>>') AS result
        """
      Then query result
        | result              |
        | [[1, 2], [3, 4]]    |

    Scenario: Parse array with null elements
      When query
        """
        SELECT from_json('[1, null, 3]', 'ARRAY<INT>') AS result
        """
      Then query result
        | result          |
        | [1, NULL, 3]    |

  Rule: Struct edge cases
    Scenario: Explicit JSON null value for a typed field returns null
      When query
        """
        SELECT from_json('{"a":1,"b":null}', 'a INT, b STRING') AS result
        """
      Then query result
        | result      |
        | {1, NULL}   |

    Scenario: Extra JSON fields not in schema are ignored
      When query
        """
        SELECT from_json('{"a":1,"extra":"ignored","b":2}', 'a INT, b INT') AS result
        """
      Then query result
        | result   |
        | {1, 2}   |

  Rule: Map edge cases
    Scenario: Invalid JSON for map returns null
      When query
        """
        SELECT from_json('not json', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Array input for map schema returns null
      When query
        """
        SELECT from_json('[1,2,3]', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Map with string values
      When query
        """
        SELECT from_json('{"key1":"val1","key2":"val2"}', 'MAP<STRING, STRING>') AS result
        """
      Then query result
        | result                       |
        | {key1 -> val1, key2 -> val2} |

  Rule: Timestamp without timezone
    Scenario: Parse timestamp without timezone using TIMESTAMP_NTZ schema
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_NTZ') AS result
        """
      Then query result
        | result                    |
        | {2024-06-15 10:30:00}     |

    Scenario: Parse timestamp without timezone with custom format
      When query
        """
        SELECT from_json('{"ts":"15/06/2024 10:30"}', 'ts TIMESTAMP_NTZ', map('timestampFormat', 'dd/MM/yyyy HH:mm')) AS result
        """
      Then query result
        | result                    |
        | {2024-06-15 10:30:00}     |

  Rule: Null value handling
    Scenario: JSON null for boolean field returns null
      When query
        """
        SELECT from_json('{"flag":null}', 'flag BOOLEAN') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: JSON null for int field returns null
      When query
        """
        SELECT from_json('{"n":null}', 'n INT') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: JSON null for decimal field returns null
      When query
        """
        SELECT from_json('{"v":null}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: JSON null for date field returns null
      When query
        """
        SELECT from_json('{"d":null}', 'd DATE') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: JSON null for timestamp field returns null
      When query
        """
        SELECT from_json('{"ts":null}', 'ts TIMESTAMP') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

  Rule: Decimal advanced parsing
    Scenario: Parse decimal with scientific notation
      When query
        """
        SELECT from_json('{"v":"1.5e2"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result   |
        | {150.00} |

    Scenario: Parse decimal with positive sign prefix
      When query
        """
        SELECT from_json('{"v":"+3.14"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

    Scenario: Parse decimal with zero value
      When query
        """
        SELECT from_json('{"v":"0.00"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {0.00} |

    Scenario: Parse decimal from integer JSON number
      When query
        """
        SELECT from_json('{"v":42}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result  |
        | {42.00} |

    Scenario: Parse decimal with rounding (half-up)
      When query
        """
        SELECT from_json('{"v":"3.145"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.15} |

    Scenario: Decimal type mismatch with boolean returns null
      When query
        """
        SELECT from_json('{"v":true}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse decimal with negative scientific notation
      When query
        """
        SELECT from_json('{"v":"1.5e-1"}', 'v DECIMAL(10,4)') AS result
        """
      Then query result
        | result   |
        | {0.1500} |

    Scenario: Parse decimal integer without fraction
      When query
        """
        SELECT from_json('{"v":"100"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result    |
        | {100.00}  |

  Rule: Timestamp error and edge cases
    Scenario: Timestamp field with non-string value returns null
      When query
        """
        SELECT from_json('{"ts":12345}', 'ts TIMESTAMP') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Timestamp NTZ field with non-string value returns null
      When query
        """
        SELECT from_json('{"ts":true}', 'ts TIMESTAMP_NTZ') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse date-only string as timestamp
      When query
        """
        SELECT from_json('{"ts":"2024-06-15"}', 'ts TIMESTAMP_NTZ', map('timestampFormat', 'yyyy-MM-dd')) AS result
        """
      Then query result
        | result                    |
        | {2024-06-15 00:00:00}     |

  Rule: Array of maps and nested collections
    Scenario: Parse array of maps
      When query
        """
        SELECT from_json('[{"a":1},{"b":2}]', 'ARRAY<MAP<STRING, INT>>') AS result
        """
      Then query result
        | result                    |
        | [{a -> 1}, {b -> 2}]     |

    Scenario: Parse struct with nested array and map
      When query
        """
        SELECT from_json('{"arr":[1,2],"m":{"k":"v"}}', 'STRUCT<arr: ARRAY<INT>, m: MAP<STRING, STRING>>') AS result
        """
      Then query result
        | result                  |
        | {[1, 2], {k -> v}}     |

    Scenario: Parse map with double values
      When query
        """
        SELECT from_json('{"x":1.5,"y":2.5}', 'MAP<STRING, DOUBLE>') AS result
        """
      Then query result
        | result                  |
        | {x -> 1.5, y -> 2.5}   |

    Scenario: Parse array of nested structs with mixed types
      When query
        """
        SELECT from_json('[{"a":1,"b":"x"},{"a":2,"b":"y"}]', 'ARRAY<STRUCT<a: INT, b: STRING>>') AS result
        """
      Then query result
        | result                  |
        | [{1, x}, {2, y}]       |

  Rule: Non-string JSON values as string type
    Scenario: Parse object value as string type
      When query
        """
        SELECT from_json('{"a":{"nested":"obj"}}', 'a STRING') AS result
        """
      Then query result
        | result                      |
        | {{"nested":"obj"}}          |

    Scenario: Parse array value as string type
      When query
        """
        SELECT from_json('{"a":[1,2,3]}', 'a STRING') AS result
        """
      Then query result
        | result        |
        | {[1,2,3]}     |

  Rule: Decimal from nested struct
    Scenario: Parse struct with decimal field
      When query
        """
        SELECT from_json('{"price":19.99,"qty":5}', 'price DECIMAL(10,2), qty INT') AS result
        """
      Then query result
        | result       |
        | {19.99, 5}   |

  Rule: Multiple fields with various types
    Scenario: Parse struct with boolean int float string and date
      When query
        """
        SELECT from_json('{"flag":true,"count":42,"ratio":0.5,"name":"test","dt":"2024-01-01"}', 'flag BOOLEAN, count INT, ratio DOUBLE, name STRING, dt DATE') AS result
        """
      Then query result
        | result                           |
        | {true, 42, 0.5, test, 2024-01-01} |

  Rule: Empty and edge case collections
    Scenario: Parse empty map
      When query
        """
        SELECT from_json('{}', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result  |
        | {}      |

    Scenario: Parse struct from empty JSON object
      When query
        """
        SELECT from_json('{}', 'a INT, b STRING') AS result
        """
      Then query result
        | result       |
        | {NULL, NULL} |

    Scenario: Parse struct with nested null struct
      When query
        """
        SELECT from_json('{"s":null}', 'STRUCT<s: STRUCT<x: INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

    Scenario: Parse struct with nested null array
      When query
        """
        SELECT from_json('{"arr":null}', 'STRUCT<arr: ARRAY<INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

    Scenario: Parse struct with nested null map
      When query
        """
        SELECT from_json('{"m":null}', 'STRUCT<m: MAP<STRING, INT>>') AS result
        """
      Then query result
        | result   |
        | {NULL}   |

  Rule: Map with nested struct values
    Scenario: Parse map with struct values
      When query
        """
        SELECT from_json('{"k1":{"a":1},"k2":{"a":2}}', 'MAP<STRING, STRUCT<a: INT>>') AS result
        """
      Then query result
        | result                        |
        | {k1 -> {1}, k2 -> {2}}       |

  Rule: Deeply nested structures
    Scenario: Parse triple nested struct
      When query
        """
        SELECT from_json('{"a":{"b":{"c":42}}}', 'STRUCT<a: STRUCT<b: STRUCT<c: INT>>>') AS result
        """
      Then query result
        | result      |
        | {{{42}}}    |

    Scenario: Parse struct with list of lists
      When query
        """
        SELECT from_json('{"matrix":[[1,2],[3,4]]}', 'STRUCT<matrix: ARRAY<ARRAY<INT>>>') AS result
        """
      Then query result
        | result                    |
        | {[[1, 2], [3, 4]]}       |

  Rule: Struct field ordering
    Scenario: JSON field order does not matter
      When query
        """
        SELECT from_json('{"b":2,"a":1}', 'a INT, b INT') AS result
        """
      Then query result
        | result   |
        | {1, 2}   |

  Rule: LargeUtf8 schema support
    Scenario: Parse struct with string value to verify string handling
      When query
        """
        SELECT from_json('{"a":"hello","b":"world"}', 'a STRING, b STRING') AS result
        """
      Then query result
        | result           |
        | {hello, world}   |

  Rule: Map with nested array values
    Scenario: Parse map with array values
      When query
        """
        SELECT from_json('{"nums":[1,2,3]}', 'MAP<STRING, ARRAY<INT>>') AS result
        """
      Then query result
        | result                |
        | {nums -> [1, 2, 3]}  |

  Rule: TEXT (LargeUtf8) schema type
    Scenario: Parse string value to TEXT field
      When query
        """
        SELECT from_json('{"a":"hello"}', 'a TEXT') AS result
        """
      Then query result
        | result  |
        | {hello} |

    Scenario: Parse number value to TEXT field coerces to string
      When query
        """
        SELECT from_json('{"a":123}', 'a TEXT') AS result
        """
      Then query result
        | result |
        | {123}  |

    Scenario: Parse boolean value to TEXT field coerces to string
      When query
        """
        SELECT from_json('{"a":true}', 'a TEXT') AS result
        """
      Then query result
        | result |
        | {true} |

  Rule: Schema types with no native json_value_to_scalar handling return null
    Scenario: Parse JSON to BINARY field returns null
      When query
        """
        SELECT from_json('{"b":"hello"}', 'b BINARY') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse JSON to DATE64 field returns null
      When query
        """
        SELECT from_json('{"d":"2024-01-15"}', 'd DATE64') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse JSON to DECIMAL with precision greater than 38 returns null
      When query
        """
        SELECT from_json('{"v":3.14}', 'v DECIMAL(40,2)') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse JSON to TIME field returns null
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', 't TIME') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse JSON to TIME(0) field (Time32) returns null
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', 't TIME(0)') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

  Rule: Timestamp schema precision variants
    Scenario: Parse timestamp with second precision (TIMESTAMP_NTZ(0))
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_NTZ(0)') AS result
        """
      Then query result
        | result                  |
        | {2024-06-15 10:30:00}   |

    Scenario: Parse timestamp with millisecond precision (TIMESTAMP_NTZ(3))
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_NTZ(3)') AS result
        """
      Then query result
        | result                  |
        | {2024-06-15 10:30:00}   |

    Scenario: Parse timestamp with nanosecond precision (TIMESTAMP_NTZ(9))
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_NTZ(9)') AS result
        """
      Then query result
        | result                  |
        | {2024-06-15 10:30:00}   |

    Scenario: Parse timestamp with TIMESTAMP_LTZ schema
      When query
        """
        SELECT from_json('{"ts":"2024-06-15 10:30:00"}', 'ts TIMESTAMP_LTZ') AS result
        """
      Then query result
        | result                  |
        | {2024-06-15 10:30:00}   |

  Rule: Schema parsing errors
    Scenario: Schema with unsupported type produces error
      When query
        """
        SELECT from_json('{"a":1}', 'a GEOMETRY') AS result
        """
      Then query error .*

    Scenario: Empty schema string produces error
      When query
        """
        SELECT from_json('{"a":1}', '') AS result
        """
      Then query error .*

    Scenario: Whitespace-only schema string produces error
      When query
        """
        SELECT from_json('{"a":1}', '   ') AS result
        """
      Then query error .*

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
        | result      |
        | {1, hello}  |

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
        | result            |
        | {a -> 1, b -> 2}  |

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
        | result   |
        | {{42}}   |

    Scenario: Invalid JSON schema string returns error
      When query
        """
        SELECT from_json('{"a":1}', '{"type":"struct"') AS result
        """
      Then query error .*

    Scenario: Parse struct with time field using Spark JSON schema
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', '{"type":"struct","fields":[{"name":"t","type":"time","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse struct with time(0) field using Spark JSON schema
      When query
        """
        SELECT from_json('{"t":"12:00:00"}', '{"type":"struct","fields":[{"name":"t","type":"time(0)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Parse struct with char and varchar fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"c":"abc","v":"hello"}', '{"type":"struct","fields":[{"name":"c","type":"char(3)","nullable":true,"metadata":{}},{"name":"v","type":"varchar(5)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result       |
        | {abc, hello} |

    Scenario: Parse nested char and varchar fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"items":["a","b"],"m":{"k":"value"}}', '{"type":"struct","fields":[{"name":"items","type":{"type":"array","elementType":"char(1)","containsNull":true},"nullable":true,"metadata":{}},{"name":"m","type":{"type":"map","keyType":"string","valueType":"varchar(5)","valueContainsNull":true},"nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result                      |
        | {[a, b], {k -> value}}      |

    Scenario: Parse struct with interval fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"cal":null,"ym":null,"dt":null}', '{"type":"struct","fields":[{"name":"cal","type":"interval","nullable":true,"metadata":{}},{"name":"ym","type":"interval year to month","nullable":true,"metadata":{}},{"name":"dt","type":"interval day to second","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result              |
        | {NULL, NULL, NULL}  |

    Scenario: Parse struct with variant and geospatial fields using Spark JSON schema
      When query
        """
        SELECT from_json('{"v":null,"g":null,"p":null}', '{"type":"struct","fields":[{"name":"v","type":"variant","nullable":true,"metadata":{}},{"name":"g","type":"geometry(ANY)","nullable":true,"metadata":{}},{"name":"p","type":"geography(ANY, spherical)","nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result              |
        | {NULL, NULL, NULL}  |

    Scenario: Parse UDT field using its Spark JSON sqlType
      When query
        """
        SELECT from_json('{"point":{"x":1.5,"y":2.5}}', '{"type":"struct","fields":[{"name":"point","type":{"type":"udt","pyClass":"example.PointUDT","serializedClass":"abc","sqlType":{"type":"struct","fields":[{"name":"x","type":"double","nullable":false,"metadata":{}},{"name":"y","type":"double","nullable":false,"metadata":{}}]}},"nullable":true,"metadata":{}}]}') AS result
        """
      Then query result
        | result       |
        | {{1.5, 2.5}} |

  Rule: Column display names
    Scenario: from_json column name shows only input column for struct
      When query
        """
        SELECT from_json(value, 'a INT')
        FROM VALUES ('{"a":1}') AS t(value)
        """
      Then query result
        | from_json(value) |
        | {1}              |

    Scenario: from_json column name shows entries for MAP schema
      When query
        """
        SELECT from_json(value, 'MAP<STRING, INT>')
        FROM VALUES ('{"a":1}') AS t(value)
        """
      Then query result
        | entries   |
        | {a -> 1}  |

    Scenario: from_json column name shows entries for MAP JSON schema
      When query
        """
        SELECT from_json(value, '{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}')
        FROM VALUES ('{"a":1}') AS t(value)
        """
      Then query result
        | entries   |
        | {a -> 1}  |

    Scenario: from_json column name for struct with nested map does not use entries
      When query
        """
        SELECT from_json(value, 'STRUCT<m: MAP<STRING, INT>>')
        FROM VALUES ('{"m":{"x":1}}') AS t(value)
        """
      Then query result
        | from_json(value) |
        | {{x -> 1}}       |

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
    Scenario: from_json with schema_of_json as the schema argument
      When query
        """
        SELECT from_json(value, schema_of_json('{"a":1,"b":"hello"}')) AS result
        FROM VALUES ('{"a":42,"b":"world"}') AS t(value)
        """
      Then query result
        | result        |
        | {42, world}   |

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

    Scenario: from_json with schema_of_json options as the schema argument
      When query
        """
        SELECT from_json(value, schema_of_json('{"a":1}', map('mode', 'PERMISSIVE'))) AS result
        FROM VALUES ('{"a":42}') AS t(value)
        """
      Then query result
        | result |
        | {42}   |

    Scenario: from_json with schema_of_json non-default options as the schema argument
      When query
        """
        SELECT from_json(value, schema_of_json('{"a":1}', map('mode', 'FAILFAST'))) AS result
        FROM VALUES ('{"a":42}') AS t(value)
        """
      Then query result
        | result |
        | {42}   |

  Rule: Single value wrapping for array schema
    Scenario: Single JSON object with array schema wraps into singleton array
      When query
        """
        SELECT from_json('{"a":1}', 'ARRAY<STRUCT<a: INT>>') AS result
        """
      Then query result
        | result   |
        | [{1}]    |

  Rule: Binary field type
    Scenario: Parse binary field returns null value
      When query
        """
        SELECT from_json('{"b":"aGVsbG8="}', 'b BINARY') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

  Rule: Null propagation through nested structures
    Scenario: Null in array of structs
      When query
        """
        SELECT from_json('[{"a":1}, null, {"a":3}]', 'ARRAY<STRUCT<a: INT>>') AS result
        """
      Then query result
        | result              |
        | [{1}, NULL, {3}]    |

    Scenario: Null values in map
      When query
        """
        SELECT from_json('{"a":1, "b":null}', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result                |
        | {a -> 1, b -> NULL}   |

  Rule: Unicode handling
    Scenario: Parse JSON with unicode characters
      When query
        """
        SELECT from_json('{"name":"héllo wörld"}', 'name STRING') AS result
        """
      Then query result
        | result           |
        | {héllo wörld}    |

  Rule: Escaped strings in JSON
    Scenario: Parse JSON with escaped quotes
      When query
        """
        SELECT from_json('{"a":"he said \\"hello\\""}', 'a STRING') AS result
        """
      Then query result
        | result              |
        | {he said "hello"}   |

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
