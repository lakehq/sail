@schema_of_json
Feature: schema_of_json() returns the schema of a JSON string as DDL

  Rule: Argument count validation

    Scenario: zero arguments errors
      When query
        """
        SELECT schema_of_json() AS result
        """
      Then query error .*

    Scenario: three arguments errors
      When query
        """
        SELECT schema_of_json('{"a":1}', map('mode','FAILFAST'), 'extra') AS result
        """
      Then query error .*

    Scenario: two arguments with options
      When query
        """
        SELECT schema_of_json('{"a":1}', map('mode','FAILFAST')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  Rule: NULL handling

    Scenario: NULL input errors
      When query
        """
        SELECT schema_of_json(NULL) AS result
        """
      Then query error .*

    Scenario: typed NULL input errors
      When query
        """
        SELECT schema_of_json(CAST(NULL AS STRING)) AS result
        """
      Then query error .*

  Rule: Basic struct inference

    Scenario: simple types
      When query
        """
        SELECT schema_of_json('{"name":"Alice","age":30,"active":true}') AS result
        """
      Then query result
        | result                                            |
        | STRUCT<active: BOOLEAN, age: BIGINT, name: STRING> |

    Scenario: numeric types integer and double
      When query
        """
        SELECT schema_of_json('{"id":100,"price":29.99,"count":5}') AS result
        """
      Then query result
        | result                                           |
        | STRUCT<count: BIGINT, id: BIGINT, price: DOUBLE> |

    Scenario: negative integer
      When query
        """
        SELECT schema_of_json('{"v":-42}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: negative float
      When query
        """
        SELECT schema_of_json('{"v":-3.14}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: DOUBLE>    |

    Scenario: scientific notation
      When query
        """
        SELECT schema_of_json('{"v":1.5e10}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: DOUBLE>    |

    Scenario: zero
      When query
        """
        SELECT schema_of_json('{"v":0}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: large integer
      When query
        """
        SELECT schema_of_json('{"v":9999999999999}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: string containing numbers
      When query
        """
        SELECT schema_of_json('{"id":"123","value":"456.78"}') AS result
        """
      Then query result
        | result                               |
        | STRUCT<id: STRING, value: STRING>    |

    Scenario: boolean true and false
      When query
        """
        SELECT schema_of_json('{"a":true,"b":false}') AS result
        """
      Then query result
        | result                              |
        | STRUCT<a: BOOLEAN, b: BOOLEAN>      |

  Rule: Nested structures

    Scenario: nested object
      When query
        """
        SELECT schema_of_json('{"user":{"name":"Bob","age":25},"active":true}') AS result
        """
      Then query result
        | result                                                        |
        | STRUCT<active: BOOLEAN, user: STRUCT<age: BIGINT, name: STRING>> |

    Scenario: deeply nested structure
      When query
        """
        SELECT schema_of_json('{"a":{"b":{"c":{"d":1}}}}') AS result
        """
      Then query result
        | result                                               |
        | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: BIGINT>>>>  |

    Scenario: array and nested object
      When query
        """
        SELECT schema_of_json('{"data":[1,2,3],"meta":{"count":3}}') AS result
        """
      Then query result
        | result                                                  |
        | STRUCT<data: ARRAY<BIGINT>, meta: STRUCT<count: BIGINT>> |

  Rule: Array type inference

    Scenario: array of primitives
      When query
        """
        SELECT schema_of_json('{"tags":["a","b","c"],"count":3}') AS result
        """
      Then query result
        | result                                      |
        | STRUCT<count: BIGINT, tags: ARRAY<STRING>>  |

    Scenario: array of objects with same schema
      When query
        """
        SELECT schema_of_json('{"items":[{"id":1,"name":"x"},{"id":2,"name":"y"}]}') AS result
        """
      Then query result
        | result                                                    |
        | STRUCT<items: ARRAY<STRUCT<id: BIGINT, name: STRING>>>   |

    Scenario: single element array
      When query
        """
        SELECT schema_of_json('{"v":[42]}') AS result
        """
      Then query result
        | result                    |
        | STRUCT<v: ARRAY<BIGINT>>  |

    Scenario: array of arrays
      When query
        """
        SELECT schema_of_json('{"v":[[1,2],[3,4]]}') AS result
        """
      Then query result
        | result                            |
        | STRUCT<v: ARRAY<ARRAY<BIGINT>>>   |

  Rule: Array supertype inference (mixed types)

    Scenario: int and string and bool in array
      When query
        """
        SELECT schema_of_json('{"v":[1, "two", true]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: int and double in array
      When query
        """
        SELECT schema_of_json('{"v":[1, 2.5]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<DOUBLE>>   |

    Scenario: int and null in array
      When query
        """
        SELECT schema_of_json('{"v":[1, null]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<BIGINT>>   |

    Scenario: bool and null in array
      When query
        """
        SELECT schema_of_json('{"v":[true, null]}') AS result
        """
      Then query result
        | result                      |
        | STRUCT<v: ARRAY<BOOLEAN>>   |

    Scenario: all null array
      When query
        """
        SELECT schema_of_json('{"v":[null, null]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: double and string in array
      When query
        """
        SELECT schema_of_json('{"v":[1.5, "hi"]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: nested arrays with mixed types
      When query
        """
        SELECT schema_of_json('{"v":[[1],["a"]]}') AS result
        """
      Then query result
        | result                              |
        | STRUCT<v: ARRAY<ARRAY<STRING>>>     |

    Scenario: object and null in array
      When query
        """
        SELECT schema_of_json('{"v":[{"a":1}, null]}') AS result
        """
      Then query result
        | result                                    |
        | STRUCT<v: ARRAY<STRUCT<a: BIGINT>>>       |

    Scenario: array of objects with different fields merges schemas
      When query
        """
        SELECT schema_of_json('[{"a":1},{"a":2,"b":"x"}]') AS result
        """
      Then query result
        | result                                |
        | ARRAY<STRUCT<a: BIGINT, b: STRING>>   |

    Scenario: array of objects with mixed field types
      When query
        """
        SELECT schema_of_json('{"v":[{"a":1},{"a":"x"}]}') AS result
        """
      Then query result
        | result                                |
        | STRUCT<v: ARRAY<STRUCT<a: STRING>>>   |

  Rule: Null handling

    Scenario: null field in struct
      When query
        """
        SELECT schema_of_json('{"name":"Alice","age":null}') AS result
        """
      Then query result
        | result                                  |
        | STRUCT<age: STRING, name: STRING>       |

    Scenario: all null fields
      When query
        """
        SELECT schema_of_json('{"a":null,"b":null}') AS result
        """
      Then query result
        | result                          |
        | STRUCT<a: STRING, b: STRING>    |

    Scenario: top-level null
      When query
        """
        SELECT schema_of_json('null') AS result
        """
      Then query result
        | result |
        | STRING |

  Rule: Empty structures

    Scenario: empty object
      When query
        """
        SELECT schema_of_json('{}') AS result
        """
      Then query result
        | result     |
        | STRUCT<>   |

    Scenario: empty array
      When query
        """
        SELECT schema_of_json('{"items":[]}') AS result
        """
      Then query result
        | result                       |
        | STRUCT<items: ARRAY<STRING>> |

    Scenario: top-level empty array
      When query
        """
        SELECT schema_of_json('[]') AS result
        """
      Then query result
        | result          |
        | ARRAY<STRING>   |

  Rule: Top-level types

    Scenario: top-level array of ints
      When query
        """
        SELECT schema_of_json('[1,2,3]') AS result
        """
      Then query result
        | result          |
        | ARRAY<BIGINT>   |

    Scenario: top-level string
      When query
        """
        SELECT schema_of_json('"hello"') AS result
        """
      Then query result
        | result |
        | STRING |

    Scenario: top-level integer
      When query
        """
        SELECT schema_of_json('42') AS result
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: top-level boolean
      When query
        """
        SELECT schema_of_json('true') AS result
        """
      Then query result
        | result  |
        | BOOLEAN |

    Scenario: top-level double
      When query
        """
        SELECT schema_of_json('3.14') AS result
        """
      Then query result
        | result |
        | DOUBLE |

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
        | result                                |
        | STRUCT<age: BIGINT, name: STRING>     |
        | STRUCT<x: DOUBLE, y: BOOLEAN>         |

  Rule: Numeric edge cases

    Scenario: float zero point zero
      When query
        """
        SELECT schema_of_json('{"v":0.0}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: DOUBLE>    |

    Scenario: negative zero
      When query
        """
        SELECT schema_of_json('{"v":-0}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

  Rule: String edge cases

    Scenario: empty string value
      When query
        """
        SELECT schema_of_json('{"v":""}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: STRING>    |

    Scenario: empty string input
      When query
        """
        SELECT schema_of_json('') AS result
        """
      Then query result
        | result |
        | STRING |

  Rule: Structure edge cases

    Scenario: empty object in array merges with non-empty
      When query
        """
        SELECT schema_of_json('{"v":[{},{"a":1}]}') AS result
        """
      Then query result
        | result                                    |
        | STRUCT<v: ARRAY<STRUCT<a: BIGINT>>>       |

    Scenario: mixed array element and nested array
      When query
        """
        SELECT schema_of_json('{"v":[1,[2]]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: nested struct with same-named fields at different levels
      When query
        """
        SELECT schema_of_json('{"a":1,"b":{"a":"hello","b":2.5}}') AS result
        """
      Then query result
        | result                                                  |
        | STRUCT<a: BIGINT, b: STRUCT<a: STRING, b: DOUBLE>>     |

    Scenario: duplicate keys in object
      When query
        """
        SELECT schema_of_json('{"a":1,"a":"x"}') AS result
        """
      Then query result
        | result                          |
        | STRUCT<a: BIGINT, a: STRING>    |

  Rule: Error cases

    Scenario: rejects non-foldable column input
      When query
        """
        SELECT schema_of_json(json_col) AS result
        FROM VALUES ('{"name":"Alice"}') AS t(json_col)
        """
      Then query error .*foldable.*

    @sail-bug
    # Sail currently rejects this option as NotImplemented: jiter does not
    # accept leading-zero numbers, and silently accepting the option would
    # produce the wrong schema on JSON with leading zeros. TODO: preprocess
    # the JSON (or switch to a parser that honours the option) to plumb it
    # through, then drop the tag.
    Scenario: allowNumericLeadingZeros option is accepted
      When query
        """
        SELECT schema_of_json('{"a": 01}', map('allowNumericLeadingZeros', 'true')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

  Rule: Numeric boundary types

    Scenario: very large integer becomes DECIMAL
      When query
        """
        SELECT schema_of_json('{"v":99999999999999999999}') AS result
        """
      Then query result
        | result                  |
        | STRUCT<v: DECIMAL(20,0)> |

    Scenario: BIGINT max stays BIGINT
      When query
        """
        SELECT schema_of_json('{"v":9223372036854775807}') AS result
        """
      Then query result
        | result            |
        | STRUCT<v: BIGINT> |

    Scenario: BIGINT overflow becomes DECIMAL
      When query
        """
        SELECT schema_of_json('{"v":9223372036854775808}') AS result
        """
      Then query result
        | result                  |
        | STRUCT<v: DECIMAL(19,0)> |

    Scenario: INT max stays BIGINT
      When query
        """
        SELECT schema_of_json('{"v":2147483647}') AS result
        """
      Then query result
        | result            |
        | STRUCT<v: BIGINT> |

    Scenario: negative zero float is DOUBLE
      When query
        """
        SELECT schema_of_json('{"v":-0.0}') AS result
        """
      Then query result
        | result             |
        | STRUCT<v: DOUBLE>  |

    Scenario: negative scientific notation
      When query
        """
        SELECT schema_of_json('{"v":1.5e-3}') AS result
        """
      Then query result
        | result             |
        | STRUCT<v: DOUBLE>  |

  Rule: Deep nesting

    Scenario: five levels deep
      When query
        """
        SELECT schema_of_json('{"a":{"b":{"c":{"d":{"e":1}}}}}') AS result
        """
      Then query result
        | result                                                       |
        | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: STRUCT<e: BIGINT>>>>> |

  Rule: Array of objects with missing fields

    Scenario: array of objects merges all fields
      When query
        """
        SELECT schema_of_json('[{"a":1,"b":"x"},{"a":2,"c":true}]') AS result
        """
      Then query result
        | result                                          |
        | ARRAY<STRUCT<a: BIGINT, b: STRING, c: BOOLEAN>> |

    Scenario: top-level array of objects
      When query
        """
        SELECT schema_of_json('[{"id":1},{"id":2}]') AS result
        """
      Then query result
        | result                    |
        | ARRAY<STRUCT<id: BIGINT>> |

  Rule: Special key names

    Scenario: dot in key name
      When query
        """
        SELECT schema_of_json('{"a.b":1}') AS result
        """
      Then query result
        | result               |
        | STRUCT<`a.b`: BIGINT> |

    Scenario: space in key name
      When query
        """
        SELECT schema_of_json('{"a b":1}') AS result
        """
      Then query result
        | result               |
        | STRUCT<`a b`: BIGINT> |

  Rule: Nested null values

    Scenario: null in nested object
      When query
        """
        SELECT schema_of_json('{"a":{"b":null}}') AS result
        """
      Then query result
        | result                       |
        | STRUCT<a: STRUCT<b: STRING>> |

    Scenario: null in nested array
      When query
        """
        SELECT schema_of_json('{"a":[null]}') AS result
        """
      Then query result
        | result                    |
        | STRUCT<a: ARRAY<STRING>>  |

  Rule: Invalid JSON errors

    Scenario: invalid JSON errors
      When query
        """
        SELECT schema_of_json('not json') AS result
        """
      Then query error .*

    Scenario: unclosed brace errors
      When query
        """
        SELECT schema_of_json('{"a":1') AS result
        """
      Then query error .*

  Rule: Error conditions

    Scenario: integer input errors
      When query
        """
        SELECT schema_of_json(42) AS result
        """
      Then query error .*

    Scenario: boolean input errors
      When query
        """
        SELECT schema_of_json(true) AS result
        """
      Then query error .*

  Rule: Decimal boundary precision

    Scenario: 18 digit integer stays BIGINT
      When query
        """
        SELECT schema_of_json('{"v":123456789012345678}') AS result
        """
      Then query result
        | result            |
        | STRUCT<v: BIGINT> |

    Scenario: 21 digit integer becomes DECIMAL(21,0)
      When query
        """
        SELECT schema_of_json('{"v":999999999999999999999}') AS result
        """
      Then query result
        | result                   |
        | STRUCT<v: DECIMAL(21,0)> |

    Scenario: 38 digit integer becomes DECIMAL(38,0)
      When query
        """
        SELECT schema_of_json('{"v":99999999999999999999999999999999999999}') AS result
        """
      Then query result
        | result                   |
        | STRUCT<v: DECIMAL(38,0)> |

    Scenario: 39 digit integer overflows to DOUBLE
      When query
        """
        SELECT schema_of_json('{"v":999999999999999999999999999999999999999}') AS result
        """
      Then query result
        | result             |
        | STRUCT<v: DOUBLE>  |

    Scenario: top-level DECIMAL(19,0) for number just above i64 max
      When query
        """
        SELECT schema_of_json('9223372036854775808') AS result
        """
      Then query result
        | result          |
        | DECIMAL(19,0)   |

    Scenario: top-level 38 digit integer is DECIMAL(38,0)
      When query
        """
        SELECT schema_of_json('99999999999999999999999999999999999999') AS result
        """
      Then query result
        | result        |
        | DECIMAL(38,0) |

  Rule: Array with DECIMAL element promotion

    Scenario: BIGINT and DECIMAL in array promotes to DECIMAL with wider precision
      When query
        """
        SELECT schema_of_json('[1, 9223372036854775808]') AS result
        """
      Then query result
        | result               |
        | ARRAY<DECIMAL(20,0)> |

    Scenario: two DECIMAL values in array uses narrower precision
      When query
        """
        SELECT schema_of_json('[9223372036854775808, 9999999999999999999]') AS result
        """
      Then query result
        | result               |
        | ARRAY<DECIMAL(19,0)> |

    Scenario: DECIMAL and DOUBLE in array promotes to DOUBLE
      When query
        """
        SELECT schema_of_json('[9223372036854775808, 1.5]') AS result
        """
      Then query result
        | result        |
        | ARRAY<DOUBLE> |

    Scenario: struct field contains array with DECIMAL promotion
      When query
        """
        SELECT schema_of_json('{"v":[1,9223372036854775808]}') AS result
        """
      Then query result
        | result                        |
        | STRUCT<v: ARRAY<DECIMAL(20,0)>> |

    Scenario: three integers triggering DECIMAL promotion in array
      When query
        """
        SELECT schema_of_json('[1, 2, 9223372036854775808]') AS result
        """
      Then query result
        | result               |
        | ARRAY<DECIMAL(20,0)> |

  Rule: Top-level array supertype inference

    Scenario: top-level array of booleans
      When query
        """
        SELECT schema_of_json('[true, false]') AS result
        """
      Then query result
        | result           |
        | ARRAY<BOOLEAN>   |

    Scenario: top-level array of bool and null
      When query
        """
        SELECT schema_of_json('[true, null]') AS result
        """
      Then query result
        | result           |
        | ARRAY<BOOLEAN>   |

    Scenario: top-level array of bool and string
      When query
        """
        SELECT schema_of_json('[true, "hello"]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

    Scenario: top-level array of bool and int promotes to STRING
      When query
        """
        SELECT schema_of_json('[true, 1]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

    Scenario: top-level array of bool and double promotes to STRING
      When query
        """
        SELECT schema_of_json('[true, 1.5]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

    Scenario: top-level array of int and object promotes to STRING
      When query
        """
        SELECT schema_of_json('[1, {"a":2}]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

    Scenario: top-level array of DECIMAL and string promotes to STRING
      When query
        """
        SELECT schema_of_json('[9223372036854775808, "hi"]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

    Scenario: top-level array of objects with null between them merges fields
      When query
        """
        SELECT schema_of_json('[{"a":1}, null, {"b":2}]') AS result
        """
      Then query result
        | result                            |
        | ARRAY<STRUCT<a: BIGINT, b: BIGINT>> |

    Scenario: top-level array of single null
      When query
        """
        SELECT schema_of_json('[null]') AS result
        """
      Then query result
        | result        |
        | ARRAY<STRING> |

  Rule: Field ordering is always alphabetical

    Scenario: JSON object fields output in alphabetical order not insertion order
      When query
        """
        SELECT schema_of_json('{"z": 1, "a": 2}') AS result
        """
      Then query result
        | result                     |
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
        | result                            |
        | ARRAY<STRUCT<a: BIGINT, z: BIGINT>> |

  Rule: Additional special key names

    Scenario: hyphen in key name requires backtick quoting
      When query
        """
        SELECT schema_of_json('{"a-b":1}') AS result
        """
      Then query result
        | result                 |
        | STRUCT<`a-b`: BIGINT>  |

    Scenario: slash in key name requires backtick quoting
      When query
        """
        SELECT schema_of_json('{"a/b":1}') AS result
        """
      Then query result
        | result                 |
        | STRUCT<`a/b`: BIGINT>  |

    Scenario: colon in key name requires backtick quoting
      When query
        """
        SELECT schema_of_json('{"a:b":1}') AS result
        """
      Then query result
        | result                 |
        | STRUCT<`a:b`: BIGINT>  |

    Scenario: backtick in key name is escaped as double backtick
      When query
        """
        SELECT schema_of_json('{"a`b":1}') AS result
        """
      Then query result
        | result                   |
        | STRUCT<`a``b`: BIGINT>   |

    Scenario: empty string key produces empty struct
      When query
        """
        SELECT schema_of_json('{"":1}') AS result
        """
      Then query result
        | result   |
        | STRUCT<> |

  Rule: primitivesAsString option

    Scenario: primitivesAsString converts integers to STRING
      When query
        """
        SELECT schema_of_json('{"a": 1, "b": 1.5, "c": true}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                                      |
        | STRUCT<a: STRING, b: STRING, c: STRING>     |

    Scenario: primitivesAsString keeps arrays of primitives as ARRAY<STRING>
      When query
        """
        SELECT schema_of_json('{"a": [1, 2]}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                        |
        | STRUCT<a: ARRAY<STRING>>      |

    Scenario: primitivesAsString keeps nested structs intact with STRING leaf values
      When query
        """
        SELECT schema_of_json('{"a": {"b": 1}}', map('primitivesAsString', 'true')) AS result
        """
      Then query result
        | result                        |
        | STRUCT<a: STRUCT<b: STRING>>  |

  Rule: inferTimestamp option

    Scenario: inferTimestamp true infers TIMESTAMP from datetime string
      When query
        """
        SELECT schema_of_json('{"a": "2021-01-01 00:00:00"}', map('inferTimestamp', 'true')) AS result
        """
      Then query result
        | result                   |
        | STRUCT<a: TIMESTAMP>     |

    Scenario: inferTimestamp true infers TIMESTAMP from date-only string
      When query
        """
        SELECT schema_of_json('{"a": "2021-01-01"}', map('inferTimestamp', 'true')) AS result
        """
      Then query result
        | result               |
        | STRUCT<a: TIMESTAMP> |

    Scenario: inferTimestamp false keeps datetime string as STRING
      When query
        """
        SELECT schema_of_json('{"a": "2021-01-01 00:00:00"}') AS result
        """
      Then query result
        | result             |
        | STRUCT<a: STRING>  |

    Scenario: inferTimestamp with non-timestamp string keeps STRING
      When query
        """
        SELECT schema_of_json('{"a": "hello"}', map('inferTimestamp', 'true')) AS result
        """
      Then query result
        | result             |
        | STRUCT<a: STRING>  |

  Rule: allowNonNumericNumbers option

    Scenario: allowNonNumericNumbers true allows NaN as DOUBLE
      When query
        """
        SELECT schema_of_json('{"a": NaN}', map('allowNonNumericNumbers', 'true')) AS result
        """
      Then query result
        | result             |
        | STRUCT<a: DOUBLE>  |

    Scenario: allowNonNumericNumbers true allows Infinity as DOUBLE
      When query
        """
        SELECT schema_of_json('{"a": Infinity}', map('allowNonNumericNumbers', 'true')) AS result
        """
      Then query result
        | result             |
        | STRUCT<a: DOUBLE>  |

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
        | result                                                              |
        | ARRAY<STRUCT<a: BIGINT, b: BIGINT, c: BIGINT, m: BIGINT, n: BIGINT, z: BIGINT>> |

    Scenario: merging structs with shared and disjoint fields promotes types correctly
      When query
        """
        SELECT schema_of_json('[{"a":1,"b":"x","c":1.5},{"a":"text","b":2,"d":true}]') AS result
        """
      Then query result
        | result                                                          |
        | ARRAY<STRUCT<a: STRING, b: STRING, c: DOUBLE, d: BOOLEAN>>     |

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
        | from_null | normal_struct                    | big_num               |
        | STRING    | STRUCT<id: BIGINT, name: STRING> | STRUCT<n: DECIMAL(20,0)> |
