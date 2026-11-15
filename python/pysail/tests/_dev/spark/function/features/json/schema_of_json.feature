Feature: schema_of_json() returns the schema of a JSON string as DDL

  Rule: Argument count validation

    Scenario Outline: Argument count: <case>
      When query
        """
        SELECT schema_of_json(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                   | args                                       |
        | zero arguments errors  |                                            |
        | three arguments errors | '{"a":1}', map('mode','FAILFAST'), 'extra' |

    Scenario: two arguments with options
      When query
        """
        SELECT schema_of_json('{"a":1}', map('mode','FAILFAST')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  Rule: NULL handling

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT schema_of_json(<arg>) AS result
        """
      Then query error .*

      Examples:
        | case                    | arg                  |
        | NULL input errors       | NULL                 |
        | typed NULL input errors | CAST(NULL AS STRING) |

  Rule: Basic struct inference

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | json                                      | result                                             |
        | simple types                     | '{"name":"Alice","age":30,"active":true}' | STRUCT<active: BOOLEAN, age: BIGINT, name: STRING> |
        | numeric types integer and double | '{"id":100,"price":29.99,"count":5}'      | STRUCT<count: BIGINT, id: BIGINT, price: DOUBLE>   |
        | negative integer                 | '{"v":-42}'                               | STRUCT<v: BIGINT>                                  |
        | negative float                   | '{"v":-3.14}'                             | STRUCT<v: DOUBLE>                                  |
        | scientific notation              | '{"v":1.5e10}'                            | STRUCT<v: DOUBLE>                                  |
        | zero                             | '{"v":0}'                                 | STRUCT<v: BIGINT>                                  |
        | large integer                    | '{"v":9999999999999}'                     | STRUCT<v: BIGINT>                                  |
        | string containing numbers        | '{"id":"123","value":"456.78"}'           | STRUCT<id: STRING, value: STRING>                  |
        | boolean true and false           | '{"a":true,"b":false}'                    | STRUCT<a: BOOLEAN, b: BOOLEAN>                     |

  Rule: Nested structures

    Scenario Outline: Nested: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | json                                             | result                                                           |
        | nested object           | '{"user":{"name":"Bob","age":25},"active":true}' | STRUCT<active: BOOLEAN, user: STRUCT<age: BIGINT, name: STRING>> |
        | deeply nested structure | '{"a":{"b":{"c":{"d":1}}}}'                      | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: BIGINT>>>>               |
        | array and nested object | '{"data":[1,2,3],"meta":{"count":3}}'            | STRUCT<data: ARRAY<BIGINT>, meta: STRUCT<count: BIGINT>>         |

  Rule: Array type inference

    Scenario Outline: Array type: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | json                                                  | result                                                 |
        | array of primitives               | '{"tags":["a","b","c"],"count":3}'                    | STRUCT<count: BIGINT, tags: ARRAY<STRING>>             |
        | array of objects with same schema | '{"items":[{"id":1,"name":"x"},{"id":2,"name":"y"}]}' | STRUCT<items: ARRAY<STRUCT<id: BIGINT, name: STRING>>> |
        | single element array              | '{"v":[42]}'                                          | STRUCT<v: ARRAY<BIGINT>>                               |
        | array of arrays                   | '{"v":[[1,2],[3,4]]}'                                 | STRUCT<v: ARRAY<ARRAY<BIGINT>>>                        |

  Rule: Array supertype inference (mixed types)

    Scenario Outline: Array supertype: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | json                        | result                              |
        | int and string and bool in array                      | '{"v":[1, "two", true]}'    | STRUCT<v: ARRAY<STRING>>            |
        | int and double in array                               | '{"v":[1, 2.5]}'            | STRUCT<v: ARRAY<DOUBLE>>            |
        | int and null in array                                 | '{"v":[1, null]}'           | STRUCT<v: ARRAY<BIGINT>>            |
        | bool and null in array                                | '{"v":[true, null]}'        | STRUCT<v: ARRAY<BOOLEAN>>           |
        | all null array                                        | '{"v":[null, null]}'        | STRUCT<v: ARRAY<STRING>>            |
        | double and string in array                            | '{"v":[1.5, "hi"]}'         | STRUCT<v: ARRAY<STRING>>            |
        | nested arrays with mixed types                        | '{"v":[[1],["a"]]}'         | STRUCT<v: ARRAY<ARRAY<STRING>>>     |
        | object and null in array                              | '{"v":[{"a":1}, null]}'     | STRUCT<v: ARRAY<STRUCT<a: BIGINT>>> |
        | array of objects with different fields merges schemas | '[{"a":1},{"a":2,"b":"x"}]' | ARRAY<STRUCT<a: BIGINT, b: STRING>> |
        | array of objects with mixed field types               | '{"v":[{"a":1},{"a":"x"}]}' | STRUCT<v: ARRAY<STRUCT<a: STRING>>> |

  Rule: Null handling

    Scenario Outline: Null field: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | json                          | result                            |
        | null field in struct | '{"name":"Alice","age":null}' | STRUCT<age: STRING, name: STRING> |
        | all null fields      | '{"a":null,"b":null}'         | STRUCT<a: STRING, b: STRING>      |
        | top-level null       | 'null'                        | STRING                            |

  Rule: Empty structures

    Scenario Outline: Empty: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | json           | result                       |
        | empty object          | '{}'           | STRUCT<>                     |
        | empty array           | '{"items":[]}' | STRUCT<items: ARRAY<STRING>> |
        | top-level empty array | '[]'           | ARRAY<STRING>                |

  Rule: Top-level types

    Scenario Outline: Top-level: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | json      | result        |
        | top-level array of ints | '[1,2,3]' | ARRAY<BIGINT> |
        | top-level string        | '"hello"' | STRING        |
        | top-level integer       | '42'      | BIGINT        |
        | top-level boolean       | 'true'    | BOOLEAN       |
        | top-level double        | '3.14'    | DOUBLE        |

  Rule: Multiple rows

    Scenario: multiple rows via UNION ALL
      When query
        """
        SELECT result FROM (
          SELECT schema_of_json('{"name":"Alice","age":30}') AS result
          UNION ALL
          SELECT schema_of_json('{"x":1.5,"y":true}') AS result
        ) ORDER BY result
        """
      Then query result ordered
        | result                            |
        | STRUCT<age: BIGINT, name: STRING> |
        | STRUCT<x: DOUBLE, y: BOOLEAN>     |

  Rule: Numeric edge cases

    Scenario Outline: Numeric edge: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | json        | result            |
        | float zero point zero | '{"v":0.0}' | STRUCT<v: DOUBLE> |
        | negative zero         | '{"v":-0}'  | STRUCT<v: BIGINT> |

  Rule: String edge cases

    Scenario Outline: String edge: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | json       | result            |
        | empty string value | '{"v":""}' | STRUCT<v: STRING> |
        | empty string input | ''         | STRING            |

  Rule: Structure edge cases

    Scenario Outline: Structure edge case: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | json                                | result                                             |
        | empty object in array merges with non-empty              | '{"v":[{},{"a":1}]}'                | STRUCT<v: ARRAY<STRUCT<a: BIGINT>>>                |
        | mixed array element and nested array                     | '{"v":[1,[2]]}'                     | STRUCT<v: ARRAY<STRING>>                           |
        | nested struct with same-named fields at different levels | '{"a":1,"b":{"a":"hello","b":2.5}}' | STRUCT<a: BIGINT, b: STRUCT<a: STRING, b: DOUBLE>> |
        | duplicate keys in object                                 | '{"a":1,"a":"x"}'                   | STRUCT<a: BIGINT, a: STRING>                       |

  Rule: Error cases

    Scenario: rejects non-foldable column input
      When query
        """
        SELECT schema_of_json(json_col) AS result
        FROM VALUES ('{"name":"Alice"}') AS t(json_col)
        """
      Then query error .*(foldable|literal value).*

    Scenario: allowNumericLeadingZeros option is accepted
      When query
        """
        SELECT schema_of_json('{"a": 01}', map('allowNumericLeadingZeros', 'true')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  Rule: Numeric boundary types

    Scenario Outline: Numeric boundary: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | json                         | result                   |
        | very large integer becomes DECIMAL | '{"v":99999999999999999999}' | STRUCT<v: DECIMAL(20,0)> |
        | BIGINT max stays BIGINT            | '{"v":9223372036854775807}'  | STRUCT<v: BIGINT>        |
        | BIGINT overflow becomes DECIMAL    | '{"v":9223372036854775808}'  | STRUCT<v: DECIMAL(19,0)> |
        | INT max stays BIGINT               | '{"v":2147483647}'           | STRUCT<v: BIGINT>        |
        | negative zero float is DOUBLE      | '{"v":-0.0}'                 | STRUCT<v: DOUBLE>        |
        | negative scientific notation       | '{"v":1.5e-3}'               | STRUCT<v: DOUBLE>        |

  Rule: Deep nesting

    Scenario: five levels deep
      When query
        """
        SELECT schema_of_json('{"a":{"b":{"c":{"d":{"e":1}}}}}') AS result
        """
      Then query result
        | result                                                        |
        | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: STRUCT<e: BIGINT>>>>> |

  Rule: Array of objects with missing fields

    Scenario Outline: Missing fields: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | json                                 | result                                          |
        | array of objects merges all fields | '[{"a":1,"b":"x"},{"a":2,"c":true}]' | ARRAY<STRUCT<a: BIGINT, b: STRING, c: BOOLEAN>> |
        | top-level array of objects         | '[{"id":1},{"id":2}]'                | ARRAY<STRUCT<id: BIGINT>>                       |

  Rule: Special key names

    Scenario Outline: Special key: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | json        | result                |
        | dot in key name   | '{"a.b":1}' | STRUCT<`a.b`: BIGINT> |
        | space in key name | '{"a b":1}' | STRUCT<`a b`: BIGINT> |

  Rule: Nested null values

    Scenario Outline: Nested null: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | json               | result                       |
        | null in nested object | '{"a":{"b":null}}' | STRUCT<a: STRUCT<b: STRING>> |
        | null in nested array  | '{"a":[null]}'     | STRUCT<a: ARRAY<STRING>>     |

  Rule: Invalid JSON errors

    Scenario Outline: Invalid JSON: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query error .*

      Examples:
        | case                  | json       |
        | invalid JSON errors   | 'not json' |
        | unclosed brace errors | '{"a":1'   |

  Rule: Error conditions

    Scenario Outline: Bad input type: <case>
      When query
        """
        SELECT schema_of_json(<arg>) AS result
        """
      Then query error .*

      Examples:
        | case                 | arg  |
        | integer input errors | 42   |
        | boolean input errors | true |

  Rule: Decimal boundary precision

    Scenario Outline: Decimal boundary: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | json                                            | result                   |
        | 18 digit integer stays BIGINT                         | '{"v":123456789012345678}'                      | STRUCT<v: BIGINT>        |
        | 21 digit integer becomes DECIMAL(21,0)                | '{"v":999999999999999999999}'                   | STRUCT<v: DECIMAL(21,0)> |
        | 38 digit integer becomes DECIMAL(38,0)                | '{"v":99999999999999999999999999999999999999}'  | STRUCT<v: DECIMAL(38,0)> |
        | 39 digit integer overflows to DOUBLE                  | '{"v":999999999999999999999999999999999999999}' | STRUCT<v: DOUBLE>        |
        | top-level DECIMAL(19,0) for number just above i64 max | '9223372036854775808'                           | DECIMAL(19,0)            |
        | top-level 38 digit integer is DECIMAL(38,0)           | '99999999999999999999999999999999999999'        | DECIMAL(38,0)            |

  Rule: Array with DECIMAL element promotion

    Scenario Outline: Decimal promotion: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                 | json                                         | result                          |
        | BIGINT and DECIMAL in array promotes to DECIMAL with wider precision | '[1, 9223372036854775808]'                   | ARRAY<DECIMAL(20,0)>            |
        | two DECIMAL values in array uses narrower precision                  | '[9223372036854775808, 9999999999999999999]' | ARRAY<DECIMAL(19,0)>            |
        | DECIMAL and DOUBLE in array promotes to DOUBLE                       | '[9223372036854775808, 1.5]'                 | ARRAY<DOUBLE>                   |
        | struct field contains array with DECIMAL promotion                   | '{"v":[1,9223372036854775808]}'              | STRUCT<v: ARRAY<DECIMAL(20,0)>> |
        | three integers triggering DECIMAL promotion in array                 | '[1, 2, 9223372036854775808]'                | ARRAY<DECIMAL(20,0)>            |

  Rule: Top-level array supertype inference

    Scenario Outline: Top-level array: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                            | json                          | result                              |
        | top-level array of booleans                                     | '[true, false]'               | ARRAY<BOOLEAN>                      |
        | top-level array of bool and null                                | '[true, null]'                | ARRAY<BOOLEAN>                      |
        | top-level array of bool and string                              | '[true, "hello"]'             | ARRAY<STRING>                       |
        | top-level array of bool and int promotes to STRING              | '[true, 1]'                   | ARRAY<STRING>                       |
        | top-level array of bool and double promotes to STRING           | '[true, 1.5]'                 | ARRAY<STRING>                       |
        | top-level array of int and object promotes to STRING            | '[1, {"a":2}]'                | ARRAY<STRING>                       |
        | top-level array of DECIMAL and string promotes to STRING        | '[9223372036854775808, "hi"]' | ARRAY<STRING>                       |
        | top-level array of objects with null between them merges fields | '[{"a":1}, null, {"b":2}]'    | ARRAY<STRUCT<a: BIGINT, b: BIGINT>> |
        | top-level array of single null                                  | '[null]'                      | ARRAY<STRING>                       |

  Rule: Field ordering is always alphabetical

    Scenario: JSON object fields output in alphabetical order not insertion order
      When query
        """
        SELECT schema_of_json('{"z": 1, "a": 2}') AS result
        """
      Then query result
        | result                       |
        | STRUCT<a: BIGINT, z: BIGINT> |

    Scenario: three fields sorted alphabetically regardless of insertion order
      When query
        """
        SELECT schema_of_json('{"b": 1, "a": 2, "c": 3}') AS result
        """
      Then query result
        | result                                  |
        | STRUCT<a: BIGINT, b: BIGINT, c: BIGINT> |

    Scenario: merged array struct fields are sorted alphabetically
      When query
        """
        SELECT schema_of_json('[{"z":1},{"a":2}]') AS result
        """
      Then query result
        | result                              |
        | ARRAY<STRUCT<a: BIGINT, z: BIGINT>> |

  Rule: Additional special key names

    Scenario Outline: Special key name: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                               | json        | result                 |
        | hyphen in key name requires backtick quoting       | '{"a-b":1}' | STRUCT<`a-b`: BIGINT>  |
        | slash in key name requires backtick quoting        | '{"a/b":1}' | STRUCT<`a/b`: BIGINT>  |
        | colon in key name requires backtick quoting        | '{"a:b":1}' | STRUCT<`a:b`: BIGINT>  |
        | backtick in key name is escaped as double backtick | '{"a`b":1}' | STRUCT<`a``b`: BIGINT> |
        | empty string key produces empty struct             | '{"":1}'    | STRUCT<>               |

  Rule: primitivesAsString option

    Scenario: primitivesAsString converts integers to STRING
      When query
        """
        SELECT schema_of_json('{"a": 1, "b": 1.5, "c": true}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                                  |
        | STRUCT<a: STRING, b: STRING, c: STRING> |

    Scenario: primitivesAsString keeps arrays of primitives as ARRAY<STRING>
      When query
        """
        SELECT schema_of_json('{"a": [1, 2]}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                   |
        | STRUCT<a: ARRAY<STRING>> |

    Scenario: primitivesAsString keeps nested structs intact with STRING leaf values
      When query
        """
        SELECT schema_of_json('{"a": {"b": 1}}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                       |
        | STRUCT<a: STRUCT<b: STRING>> |

  Rule: inferTimestamp option

    Scenario Outline: inferTimestamp: <case>
      When query
        """
        SELECT schema_of_json(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                       | args                                                          | result               |
        | inferTimestamp true infers TIMESTAMP from datetime string  | '{"a": "2021-01-01 00:00:00"}', map('inferTimestamp', 'true') | STRUCT<a: TIMESTAMP> |
        | inferTimestamp true infers TIMESTAMP from date-only string | '{"a": "2021-01-01"}', map('inferTimestamp', 'true')          | STRUCT<a: TIMESTAMP> |
        | inferTimestamp false keeps datetime string as STRING       | '{"a": "2021-01-01 00:00:00"}'                                | STRUCT<a: STRING>    |
        | inferTimestamp with non-timestamp string keeps STRING      | '{"a": "hello"}', map('inferTimestamp', 'true')               | STRUCT<a: STRING>    |

    # Spark's `inferTimestamp` uses a lenient timestamp parser that also accepts
    # fractional seconds, a trailing `Z`, timezone offsets, partial time
    # (no seconds), and time-only values. JVM-verified: all expect TIMESTAMP.
    Scenario Outline: inferTimestamp lenient parser: <case>
      When query
        """
        SELECT schema_of_json(<json>, map('inferTimestamp', 'true')) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                  | json                                 | result               |
        | inferTimestamp infers TIMESTAMP from datetime with fractional seconds | '{"a": "2021-01-01 00:00:00.123"}'   | STRUCT<a: TIMESTAMP> |
        | inferTimestamp infers TIMESTAMP from ISO datetime with trailing Z     | '{"a": "2021-01-01T00:00:00Z"}'      | STRUCT<a: TIMESTAMP> |
        | inferTimestamp infers TIMESTAMP from datetime with timezone offset    | '{"a": "2021-01-01 00:00:00+02:00"}' | STRUCT<a: TIMESTAMP> |
        | inferTimestamp infers TIMESTAMP from datetime without seconds         | '{"a": "2021-01-01 00:00"}'          | STRUCT<a: TIMESTAMP> |
        | inferTimestamp infers TIMESTAMP from a time-only string               | '{"a": "03:04:05"}'                  | STRUCT<a: TIMESTAMP> |
        | inferTimestamp keeps slash-separated date as STRING                   | '{"a": "2024/01/02"}'                | STRUCT<a: STRING>    |
        | inferTimestamp keeps invalid calendar date as STRING                  | '{"a": "2024-13-02"}'                | STRUCT<a: STRING>    |

  Rule: allowNonNumericNumbers option

    Scenario: allowNonNumericNumbers true allows NaN as DOUBLE
      When query
        """
        SELECT schema_of_json('{"a": NaN}', map('allowNonNumericNumbers', 'true')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: DOUBLE> |

    Scenario: allowNonNumericNumbers true allows Infinity as DOUBLE
      When query
        """
        SELECT schema_of_json('{"a": Infinity}', map('allowNonNumericNumbers', 'true')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: DOUBLE> |

  Rule: Nested struct merging in arrays

    Scenario: nested struct fields from different objects are merged and sorted
      When query
        """
        SELECT schema_of_json('[{"a":{"z":1}},{"a":{"m":2}}]') AS result
        """
      Then query result
        | result                                         |
        | ARRAY<STRUCT<a: STRUCT<m: BIGINT, z: BIGINT>>> |

    Scenario: struct field conflicting with primitive becomes STRING
      When query
        """
        SELECT schema_of_json('[{"a":{"x":1}},{"a":1}]') AS result
        """
      Then query result
        | result                   |
        | ARRAY<STRUCT<a: STRING>> |

    Scenario: array field types conflicting in different objects merge to supertype
      When query
        """
        SELECT schema_of_json('[{"a":[1]},{"a":["x"]}]') AS result
        """
      Then query result
        | result                          |
        | ARRAY<STRUCT<a: ARRAY<STRING>>> |

  Rule: Whitespace and invalid JSON

    Scenario: whitespace-only string returns STRING
      When query
        """
        SELECT schema_of_json('   ') AS result
        """
      Then query result
        | result |
        | STRING |

  Rule: struct field merge ordering and type promotion across many fields

    Scenario: merging structs with many disjoint fields preserves sorted order
      When query
        """
        SELECT schema_of_json('[{"z":1,"a":2,"m":3},{"b":4,"n":5,"c":6}]') AS result
        """
      Then query result
        | result                                                                          |
        | ARRAY<STRUCT<a: BIGINT, b: BIGINT, c: BIGINT, m: BIGINT, n: BIGINT, z: BIGINT>> |

    Scenario: merging structs with shared and disjoint fields promotes types correctly
      When query
        """
        SELECT schema_of_json('[{"a":1,"b":"x","c":1.5},{"a":"text","b":2,"d":true}]') AS result
        """
      Then query result
        | result                                                     |
        | ARRAY<STRUCT<a: STRING, b: STRING, c: DOUBLE, d: BOOLEAN>> |

  Rule: Array with mixed null, normal, and unusual values

    Scenario: array with null, integer, and string values promotes to STRING
      When query
        """
        SELECT schema_of_json('[{"a": null}, {"a": 1}, {"a": "weird"}]') AS result
        """
      Then query result
        | result                   |
        | ARRAY<STRUCT<a: STRING>> |

  Rule: Multiple schema_of_json calls in one query

    Scenario: null JSON bare value, normal struct, and wide integer in same SELECT
      When query
        """
        SELECT
          schema_of_json('null')                       AS from_null,
          schema_of_json('{"id": 1, "name": "alice"}') AS normal_struct,
          schema_of_json('{"n": 99999999999999999999}') AS big_num
        """
      Then query result
        | from_null | normal_struct                    | big_num                  |
        | STRING    | STRUCT<id: BIGINT, name: STRING> | STRUCT<n: DECIMAL(20,0)> |

  Rule: Result nullability

    Scenario: foldable successful call produces a non-nullable result
      When query
        """
        SELECT schema_of_json('{"a":1}') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: foldable scalar call produces a non-nullable result
      When query
        """
        SELECT schema_of_json('[1,2,3]') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

  Rule: allowSingleQuotes option

    Scenario: single-quoted field name is allowed by default
      When query
        """
        SELECT schema_of_json('{\'a\': 1}') AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

    Scenario: single-quoted string value is allowed by default
      When query
        """
        SELECT schema_of_json('{"a": \'hi\'}') AS result
        """
      Then query result
        | result            |
        | STRUCT<a: STRING> |

    Scenario: allowSingleQuotes false rejects single quotes
      When query
        """
        SELECT schema_of_json('{\'a\': 1}', map('allowSingleQuotes', 'false')) AS result
        """
      Then query error .*

  Rule: allowUnquotedFieldNames option

    Scenario: unquoted field name is rejected by default
      When query
        """
        SELECT schema_of_json('{a: 1}') AS result
        """
      Then query error .*

    Scenario: allowUnquotedFieldNames true accepts unquoted field names
      When query
        """
        SELECT schema_of_json('{a: 1}', map('allowUnquotedFieldNames', 'true')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  Rule: prefersDecimal option

    Scenario: prefersDecimal true infers DECIMAL for a fractional number
      When query
        """
        SELECT schema_of_json('{"a": 1.5}', map('prefersDecimal', 'true')) AS result
        """
      Then query result
        | result                  |
        | STRUCT<a: DECIMAL(2,1)> |

    Scenario: prefersDecimal false keeps fractional number as DOUBLE
      When query
        """
        SELECT schema_of_json('{"a": 1.5}', map('prefersDecimal', 'false')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: DOUBLE> |

    Scenario: prefersDecimal with a negative-scale exponent errors
      When query
        """
        SELECT schema_of_json('{"a": 1.5e10}', map('prefersDecimal', 'true')) AS result
        """
      Then query error .*

  Rule: Unicode and non-ASCII

    Scenario Outline: Unicode: <case>
      When query
        """
        SELECT schema_of_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | json                      | result                 |
        | non-ASCII field name requires backtick quoting | '{"café": 1}'             | STRUCT<`café`: BIGINT> |
        | unicode escape in field name is decoded        | '{"\\u00e9": 1}'          | STRUCT<`é`: BIGINT>    |
        | unicode escape in string value stays STRING    | '{"a": "\\u00e9"}'        | STRUCT<a: STRING>      |
        | surrogate-pair escape stays STRING             | '{"a": "\\ud83d\\ude00"}' | STRUCT<a: STRING>      |

  Rule: Option value validation

    Scenario: invalid boolean option value errors
      When query
        """
        SELECT schema_of_json('{"a":1}', map('allowSingleQuotes', 'yes')) AS result
        """
      Then query error .*

    Scenario: unrecognized mode falls back to PERMISSIVE
      When query
        """
        SELECT schema_of_json('{"a":1}', map('mode', 'BOGUS')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

    Scenario: mode value is case-insensitive
      When query
        """
        SELECT schema_of_json('{"a":1}', map('mode', 'permissive')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to schema_of_json yields the schema Spark declares
      When query
        """
        SELECT schema_of_json('[{"col":0}]') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

  Rule: Result values (migrated from test_schema_of_json.txt doctests)

    # Doctests #5 and #9-#13 stay separate: #5 asserts the auto-derived column
    # name instead of `AS r`, and #9-#13 contain backslashes, which a data-table
    # cell would re-escape.
    Scenario Outline: Doctest result: <case>
      When query
        """
        SELECT schema_of_json(<args>) AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                                | args                                                                                        | result                                         |
        | schema_of_json doctest #1 (result)  | '{"a": 0}'                                                                                  | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #2 (result)  | '{a: 1}', map('allowUnquotedFieldNames', 'true')                                            | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #3 (result)  | '{a: 1}', map('ALLOWUNQUOTEDFIELDNAMES', 'True')                                            | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #4 (result)  | '{a: {"b": [1.5], c: "x: {y: 1}"}}', map('allowUnquotedFieldNames', 'true')                 | STRUCT<a: STRUCT<b: ARRAY<DOUBLE>, c: STRING>> |
        | schema_of_json doctest #6 (result)  | '{"a": 2}', map('allowUnquotedFieldNames', 'false')                                         | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #8 (result)  | '{"a": 1}', map('myImaginaryOption', 'banana')                                              | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #15 (result) | '{"x y": 1}'                                                                                | STRUCT<`x y`: BIGINT>                          |
        | schema_of_json doctest #16 (result) | '{"x-y": 1}'                                                                                | STRUCT<`x-y`: BIGINT>                          |
        | schema_of_json doctest #17 (result) | '{"x.y": 1}'                                                                                | STRUCT<`x.y`: BIGINT>                          |
        | schema_of_json doctest #18 (result) | '{"1a": 1}'                                                                                 | STRUCT<`1a`: BIGINT>                           |
        | schema_of_json doctest #19 (result) | '{"_a": 1}'                                                                                 | STRUCT<_a: BIGINT>                             |
        | schema_of_json doctest #20 (result) | '{"ab1_": 1}'                                                                               | STRUCT<ab1_: BIGINT>                           |
        | schema_of_json doctest #21 (result) | '{"x`y": 1}'                                                                                | STRUCT<`x``y`: BIGINT>                         |
        | schema_of_json doctest #22 (result) | '{"café": 1}'                                                                               | STRUCT<`café`: BIGINT>                         |
        | schema_of_json doctest #23 (result) | '{"": 1}'                                                                                   | STRUCT<>                                       |
        | schema_of_json doctest #24 (result) | '{"a": true}'                                                                               | STRUCT<a: BOOLEAN>                             |
        | schema_of_json doctest #25 (result) | '{"a": false}'                                                                              | STRUCT<a: BOOLEAN>                             |
        | schema_of_json doctest #26 (result) | '{"a": null}'                                                                               | STRUCT<a: STRING>                              |
        | schema_of_json doctest #27 (result) | 'null'                                                                                      | STRING                                         |
        | schema_of_json doctest #28 (result) | '{"a": {"b": null}}'                                                                        | STRUCT<a: STRUCT<b: STRING>>                   |
        | schema_of_json doctest #29 (result) | '{"a": [null]}'                                                                             | STRUCT<a: ARRAY<STRING>>                       |
        | schema_of_json doctest #30 (result) | '{"a": [null, 1]}'                                                                          | STRUCT<a: ARRAY<BIGINT>>                       |
        | schema_of_json doctest #31 (result) | '{"a": [1, "x"]}'                                                                           | STRUCT<a: ARRAY<STRING>>                       |
        | schema_of_json doctest #32 (result) | '{"a": [1, 2.5]}'                                                                           | STRUCT<a: ARRAY<DOUBLE>>                       |
        | schema_of_json doctest #33 (result) | '{"a": [1, 2.5, "x"]}'                                                                      | STRUCT<a: ARRAY<STRING>>                       |
        | schema_of_json doctest #34 (result) | '{"a": [1, true]}'                                                                          | STRUCT<a: ARRAY<STRING>>                       |
        | schema_of_json doctest #35 (result) | '{"a": [{"b": 1}, {"c": 2}]}'                                                               | STRUCT<a: ARRAY<STRUCT<b: BIGINT, c: BIGINT>>> |
        | schema_of_json doctest #36 (result) | '{"a": [{"b": 1}, {"b": "x"}]}'                                                             | STRUCT<a: ARRAY<STRUCT<b: STRING>>>            |
        | schema_of_json doctest #37 (result) | '{"a": [{"b": 1}, 2]}'                                                                      | STRUCT<a: ARRAY<STRING>>                       |
        | schema_of_json doctest #38 (result) | '{"a": [[1], [2.5]]}'                                                                       | STRUCT<a: ARRAY<ARRAY<DOUBLE>>>                |
        | schema_of_json doctest #39 (result) | '{"a": {}}'                                                                                 | STRUCT<>                                       |
        | schema_of_json doctest #40 (result) | '{}'                                                                                        | STRUCT<>                                       |
        | schema_of_json doctest #41 (result) | '{"a": 9223372036854775807}'                                                                | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #42 (result) | '{"a": 9223372036854775808}'                                                                | STRUCT<a: DECIMAL(19,0)>                       |
        | schema_of_json doctest #43 (result) | '{"a": 99999999999999999999999999}'                                                         | STRUCT<a: DECIMAL(26,0)>                       |
        | schema_of_json doctest #44 (result) | '{"a": -99999999999999999999999999}'                                                        | STRUCT<a: DECIMAL(26,0)>                       |
        | schema_of_json doctest #45 (result) | '{"a": 999999999999999999999999999999999999999}'                                            | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #46 (result) | '{"a": 1.7976931348623157E308}'                                                             | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #47 (result) | '{"a": 1E309}'                                                                              | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #48 (result) | '{"a": 12.345678901234567890123}'                                                           | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #49 (result) | '{"a": 1.5}', map('prefersDecimal', 'true')                                                 | STRUCT<a: DECIMAL(2,1)>                        |
        | schema_of_json doctest #50 (result) | '{"a": 99999999999999999999999999}', map('prefersDecimal', 'true')                          | STRUCT<a: DECIMAL(26,0)>                       |
        | schema_of_json doctest #51 (result) | '{"a": 1.55E1}', map('prefersDecimal', 'true')                                              | STRUCT<a: DECIMAL(3,1)>                        |
        | schema_of_json doctest #52 (result) | '{"a": 1.5E-2}', map('prefersDecimal', 'true')                                              | STRUCT<a: DECIMAL(3,3)>                        |
        | schema_of_json doctest #53 (result) | '{"a": 1E-2}', map('prefersDecimal', 'true')                                                | STRUCT<a: DECIMAL(2,2)>                        |
        | schema_of_json doctest #54 (result) | '{"a": 15E-1}', map('prefersDecimal', 'true')                                               | STRUCT<a: DECIMAL(2,1)>                        |
        | schema_of_json doctest #55 (result) | '{"a": 1.500E1}', map('prefersDecimal', 'true')                                             | STRUCT<a: DECIMAL(4,2)>                        |
        | schema_of_json doctest #56 (result) | '{"a": 1.5E0}', map('prefersDecimal', 'true')                                               | STRUCT<a: DECIMAL(2,1)>                        |
        | schema_of_json doctest #60 (result) | '{"a": 1.5E2}'                                                                              | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #61 (result) | '{"a": NaN}'                                                                                | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #62 (result) | '{"a": Infinity}'                                                                           | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #63 (result) | '{"a": 01}', map('allowNumericLeadingZeros', 'true')                                        | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #64 (result) | '{"a": 0001}', map('allowNumericLeadingZeros', 'true')                                      | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #65 (result) | '{"a": -01}', map('allowNumericLeadingZeros', 'true')                                       | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #66 (result) | '{"a": 007}', map('allowNumericLeadingZeros', 'true')                                       | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #67 (result) | '{"a": 0}', map('allowNumericLeadingZeros', 'true')                                         | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #68 (result) | '{"a": 00}', map('allowNumericLeadingZeros', 'true')                                        | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #69 (result) | '{"a": -00}', map('allowNumericLeadingZeros', 'true')                                       | STRUCT<a: BIGINT>                              |
        | schema_of_json doctest #70 (result) | '{"a": 01.5}', map('allowNumericLeadingZeros', 'true')                                      | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #71 (result) | '{"a": 00.5}', map('allowNumericLeadingZeros', 'true')                                      | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #72 (result) | '{"a": 01e2}', map('allowNumericLeadingZeros', 'true')                                      | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #73 (result) | '{"a": 009223372036854775808}', map('allowNumericLeadingZeros', 'true')                     | STRUCT<a: DECIMAL(19,0)>                       |
        | schema_of_json doctest #74 (result) | '{"a": 0099999999999999999999999999999999999999}', map('allowNumericLeadingZeros', 'true')  | STRUCT<a: DECIMAL(38,0)>                       |
        | schema_of_json doctest #75 (result) | '{"a": 00999999999999999999999999999999999999999}', map('allowNumericLeadingZeros', 'true') | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #76 (result) | '{"01b": 01, "b": "01"}', map('allowNumericLeadingZeros', 'true')                           | STRUCT<`01b`: BIGINT, b: STRING>               |
        | schema_of_json doctest #77 (result) | '{"a": 1e02}', map('allowNumericLeadingZeros', 'true')                                      | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #78 (result) | '{"a": 1e02}'                                                                               | STRUCT<a: DOUBLE>                              |
        | schema_of_json doctest #79 (result) | '{"a": 00.50}', map('allowNumericLeadingZeros', 'true', 'prefersDecimal', 'true')           | STRUCT<a: DECIMAL(2,2)>                        |
        | schema_of_json doctest #80 (result) | '{"a": 01.5}', map('allowNumericLeadingZeros', 'true', 'prefersDecimal', 'true')            | STRUCT<a: DECIMAL(2,1)>                        |
        | schema_of_json doctest #83 (result) | '{+a: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<`+a`: BIGINT>                           |
        | schema_of_json doctest #84 (result) | '{a@b: 1}', map('allowUnquotedFieldNames', 'true')                                          | STRUCT<`a@b`: BIGINT>                          |
        | schema_of_json doctest #85 (result) | '{a#b: 1}', map('allowUnquotedFieldNames', 'true')                                          | STRUCT<`a#b`: BIGINT>                          |
        | schema_of_json doctest #86 (result) | '{a*b: 1}', map('allowUnquotedFieldNames', 'true')                                          | STRUCT<`a*b`: BIGINT>                          |
        | schema_of_json doctest #87 (result) | '{a$b: 1}', map('allowUnquotedFieldNames', 'true')                                          | STRUCT<`a$b`: BIGINT>                          |
        | schema_of_json doctest #88 (result) | '{$a: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<`$a`: BIGINT>                           |
        | schema_of_json doctest #89 (result) | '{_a: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<_a: BIGINT>                             |
        | schema_of_json doctest #90 (result) | '{9a: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<`9a`: BIGINT>                           |
        | schema_of_json doctest #91 (result) | '{a9: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<a9: BIGINT>                             |
        | schema_of_json doctest #92 (result) | '{ключ: 1}', map('allowUnquotedFieldNames', 'true')                                         | STRUCT<`ключ`: BIGINT>                         |
        | schema_of_json doctest #93 (result) | '{aé: 1}', map('allowUnquotedFieldNames', 'true')                                           | STRUCT<`aé`: BIGINT>                           |

    Scenario: schema_of_json doctest #5 (result)
      When query
        """
        SELECT schema_of_json('{a: 1}', map('allowUnquotedFieldNames', 'true'))
        """
      Then query result
        | schema_of_json({a: 1}) |
        | STRUCT<a: BIGINT>      |

    Scenario: schema_of_json doctest #9 (result)
      When query
        """
        SELECT schema_of_json('{"a": \'x\'}') AS r
        """
      Then query result
        | r                 |
        | STRUCT<a: STRING> |

    Scenario: schema_of_json doctest #10 (result)
      When query
        """
        SELECT schema_of_json('{\'a\': 1}') AS r
        """
      Then query result
        | r                 |
        | STRUCT<a: BIGINT> |

    Scenario: schema_of_json doctest #11 (result)
      When query
        """
        SELECT schema_of_json('{\'a\': \'it\\\'s\'}') AS r
        """
      Then query result
        | r                 |
        | STRUCT<a: STRING> |

    Scenario: schema_of_json doctest #12 (result)
      When query
        """
        SELECT schema_of_json('{\'a\': \'say "hi"\'}') AS r
        """
      Then query result
        | r                 |
        | STRUCT<a: STRING> |

    Scenario: schema_of_json doctest #13 (result)
      When query
        """
        SELECT schema_of_json('{"a": "it\'s"}') AS r
        """
      Then query result
        | r                 |
        | STRUCT<a: STRING> |

    # Doctests #14 and #100 stay separate: their SQL contains backslashes, which
    # a data-table cell would re-escape.
    Scenario Outline: Doctest error: <case>
      When query
        """
        SELECT schema_of_json(<args>) AS r
        """
      Then query error (?i).*

      Examples:
        | case                               | args                                                  |
        | schema_of_json doctest #7 (error)  | '{"a": 1}', map('allowUnquotedFieldNames', 'yes')     |
        | schema_of_json doctest #57 (error) | '{"a": 1.5E2}', map('prefersDecimal', 'true')         |
        | schema_of_json doctest #58 (error) | '{"a": 1E2}', map('prefersDecimal', 'true')           |
        | schema_of_json doctest #59 (error) | '{"a": 15E2}', map('prefersDecimal', 'true')          |
        | schema_of_json doctest #81 (error) | '{"a": 01}'                                           |
        | schema_of_json doctest #82 (error) | '{"a": 01}', map('allowNumericLeadingZeros', 'false') |
        | schema_of_json doctest #94 (error) | '{.a: 1}', map('allowUnquotedFieldNames', 'true')     |
        | schema_of_json doctest #95 (error) | '{`a: 1}', map('allowUnquotedFieldNames', 'true')     |
        | schema_of_json doctest #96 (error) | '{a`b: 1}', map('allowUnquotedFieldNames', 'true')    |
        | schema_of_json doctest #97 (error) | '{<a: 1}', map('allowUnquotedFieldNames', 'true')     |
        | schema_of_json doctest #98 (error) | '{a<b: 1}', map('allowUnquotedFieldNames', 'true')    |
        | schema_of_json doctest #99 (error) | '{>a: 1}', map('allowUnquotedFieldNames', 'true')     |

    Scenario: schema_of_json doctest #14 (error)
      When query
        """
        SELECT schema_of_json('{\'a\': 1}', map('allowSingleQuotes', 'false')) AS r
        """
      Then query error (?i).*

    Scenario: schema_of_json doctest #100 (error)
      When query
        """
        SELECT schema_of_json('{\\a: 1}', map('allowUnquotedFieldNames', 'true')) AS r
        """
      Then query error (?i).*
