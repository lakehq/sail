@spark-4
Feature: Variant type functions (parse_json, is_variant_null, variant_get)

  Rule: parse_json + variant_get roundtrip

    Scenario Outline: Roundtrip: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | json                   | path      | type      | result |
        | Parse and extract integer             | '42'                   | '$'       | 'int'     | 42     |
        | Parse and extract string              | '"hello"'              | '$'       | 'string'  | hello  |
        | Parse and extract boolean true        | 'true'                 | '$'       | 'boolean' | true   |
        | Parse and extract boolean false       | 'false'                | '$'       | 'boolean' | false  |
        | Parse and extract double              | '3.14'                 | '$'       | 'double'  | 3.14   |
        | Parse and extract nested field        | '{"a":1}'              | '$.a'     | 'int'     | 1      |
        | Parse and extract deeply nested field | '{"a":{"b":{"c":99}}}' | '$.a.b.c' | 'int'     | 99     |

    Scenario: Parse and extract integer
      When query
        """
        SELECT variant_get(parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Parse and extract string
      When query
        """
        SELECT variant_get(parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: Parse and extract boolean true
      When query
        """
        SELECT variant_get(parse_json('true'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Parse and extract boolean false
      When query
        """
        SELECT variant_get(parse_json('false'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Parse and extract double
      When query
        """
        SELECT variant_get(parse_json('3.14'), '$', 'double') AS result
        """
      Then query result
        | result |
        | 3.14   |

    Scenario: Parse and extract nested field
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Parse and extract deeply nested field
      When query
        """
        SELECT variant_get(parse_json('{"a":{"b":{"c":99}}}'), '$.a.b.c', 'int') AS result
        """
      Then query result
        | result |
        | 99     |

  Rule: parse_json NULL handling

    Scenario: Parse NULL input returns NULL
      When query
        """
        SELECT parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: is_variant_null

    Scenario Outline: is_variant_null: <case>
      When query
        """
        SELECT is_variant_null(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                            | json      | result |
        | JSON null is variant null                       | 'null'    | true   |
        | Integer is not variant null                     | '42'      | false  |
        | String is not variant null                      | '"hello"' | false  |
        | Object is not variant null                      | '{"a":1}' | false  |
        | SQL NULL input to is_variant_null returns false | NULL      | false  |
        | String "null" (quoted) is NOT variant null      | '"null"'  | false  |
        | Boolean false is not variant null               | 'false'   | false  |
        | Empty array is not variant null                 | '[]'      | false  |
        | Empty object is not variant null                | '{}'      | false  |

    Scenario: Integer is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('42')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: String is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('"hello"')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Object is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('{"a":1}')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: SQL NULL input to is_variant_null returns false
      When query
        """
        SELECT is_variant_null(parse_json(NULL)) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: String "null" (quoted) is NOT variant null
      When query
        """
        SELECT is_variant_null(parse_json('"null"')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Boolean false is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('false')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Empty array is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('[]')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Empty object is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('{}')) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: parse_json roundtrip with complex types

    Scenario Outline: Complex type: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | json                       | path     | type      | result |
        | Parse object with float field                  | '{"a":1,"b":0.8}'          | '$.b'    | 'double'  | 0.8    |
        | Parse object with boolean and integer          | '{"flag":true,"count":42}' | '$.flag' | 'boolean' | true   |
        | Extract string field from object               | '{"a":null,"b":"spark"}'   | '$.b'    | 'string'  | spark  |
        | Extract null field from object returns NULL    | '{"a":null,"b":"spark"}'   | '$.a'    | 'string'  | NULL   |
        | Extract missing field from object returns NULL | '{"a":null,"b":"spark"}'   | '$.c'    | 'string'  | NULL   |

    Scenario: Parse object with float field
      When query
        """
        SELECT variant_get(parse_json('{"a":1,"b":0.8}'), '$.b', 'double') AS result
        """
      Then query result
        | result |
        | 0.8    |

    Scenario: Parse object with boolean and integer
      When query
        """
        SELECT variant_get(parse_json('{"flag":true,"count":42}'), '$.flag', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Extract string field from object
      When query
        """
        SELECT variant_get(parse_json('{"a":null,"b":"spark"}'), '$.b', 'string') AS result
        """
      Then query result
        | result |
        | spark  |

    Scenario: Extract null field from object returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":null,"b":"spark"}'), '$.a', 'string') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Extract missing field from object returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":null,"b":"spark"}'), '$.c', 'string') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Array access

    Scenario Outline: Array access: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, 'int') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | json            | path      | result |
        | Array index 0                    | '[10,20,30]'    | '$[0]'    | 10     |
        | Array index 2                    | '[10,20,30]'    | '$[2]'    | 30     |
        | Array out of bounds returns NULL | '[10,20,30]'    | '$[5]'    | NULL   |
        | Nested array access              | '[[1,2],[3,4]]' | '$[1][0]' | 3      |

    Scenario: Array index 0
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[0]', 'int') AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: Array index 2
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[2]', 'int') AS result
        """
      Then query result
        | result |
        | 30     |

    Scenario: Array out of bounds returns NULL
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[5]', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Nested array access
      When query
        """
        SELECT variant_get(parse_json('[[1,2],[3,4]]'), '$[1][0]', 'int') AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: Edge cases

    Scenario Outline: Edge case: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | json                               | path          | type     | result |
        | Empty string value    | '""'                               | '$'           | 'string' |        |
        | Negative double       | '-3.14'                            | '$'           | 'double' | -3.14  |
        | Zero integer          | '0'                                | '$'           | 'int'    | 0      |
        | Deep nesting 5 levels | '{"a":{"b":{"c":{"d":{"e":42}}}}}' | '$.a.b.c.d.e' | 'int'    | 42     |
        | Mixed types in array  | '[1, "two", true]'                 | '$[1]'        | 'string' | two    |

    Scenario: try_variant_get returns NULL for wrong type
      When query
        """
        SELECT try_variant_get(parse_json('"hello"'), '$', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Multiple rows with variant
      When query
        """
        SELECT variant_get(parse_json(v), '$', 'int') AS result
        FROM VALUES ('1'), ('2'), ('3') AS t(v)
        ORDER BY result
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | 3      |

    Scenario: Empty string value
      When query
        """
        SELECT variant_get(parse_json('""'), '$', 'string') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Negative double
      When query
        """
        SELECT variant_get(parse_json('-3.14'), '$', 'double') AS result
        """
      Then query result
        | result |
        | -3.14  |

    Scenario: Zero integer
      When query
        """
        SELECT variant_get(parse_json('0'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: Deep nesting 5 levels
      When query
        """
        SELECT variant_get(parse_json('{"a":{"b":{"c":{"d":{"e":42}}}}}'), '$.a.b.c.d.e', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Mixed types in array
      When query
        """
        SELECT variant_get(parse_json('[1, "two", true]'), '$[1]', 'string') AS result
        """
      Then query result
        | result |
        | two    |

  Rule: is_variant_null additional cases from doctest

    Scenario Outline: is_variant_null doctest: <case>
      When query
        """
        SELECT is_variant_null(parse_json(<json>)) AS result
        """
      Then query result
        | result |
        | false  |

      Examples:
        | case                                       | json          |
        | Array containing null is not variant null  | '[null]'      |
        | Object with null field is not variant null | '{"a": null}' |
        | Empty string value is not variant null     | '""'          |
        | Zero is not variant null                   | '0'           |

    Scenario: Array containing null is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('[null]')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Object with null field is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('{"a": null}')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Empty string value is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('""')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Zero is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('0')) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: parse_json display and multi-row

    Scenario Outline: parse_json display: <case>
      When query
        """
        SELECT parse_json(<json>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | json              | result          |
        | Parse JSON object displays correctly | '{"name":"sail"}' | {"name":"sail"} |
        | Parse empty object                   | '{}'              | {}              |
        | Parse empty string value             | '""'              | ""              |

    Scenario: Multi-row parse_json with is_variant_null
      When query
        """
        SELECT is_variant_null(parse_json(col)) AS result
        FROM VALUES ('null'), ('{"a":1}'), (null), ('0') AS t(col)
        """
      Then query result
        | result |
        | true   |
        | false  |
        | false  |
        | false  |

    Scenario: Parse JSON object displays correctly
      When query
        """
        SELECT parse_json('{"name":"sail"}') AS result
        """
      Then query result
        | result          |
        | {"name":"sail"} |

    Scenario: Parse empty object
      When query
        """
        SELECT parse_json('{}') AS result
        """
      Then query result
        | result |
        | {}     |

    Scenario: Parse empty string value
      When query
        """
        SELECT parse_json('""') AS result
        """
      Then query result
        | result |
        | ""     |

  Rule: Decimal type extraction

    Scenario Outline: Decimal extraction: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | json              | path      | type            | result   |
        | Extract as decimal with default precision | '3.14'            | '$'       | 'decimal'       | 3        |
        | Extract as decimal(10,2)                  | '3.14'            | '$'       | 'decimal(10,2)' | 3.14     |
        | Extract negative decimal                  | '-123.456'        | '$'       | 'decimal(10,3)' | -123.456 |
        | Extract nested decimal field              | '{"price":19.99}' | '$.price' | 'decimal(10,2)' | 19.99    |

    Scenario: Extract as decimal with default precision
      When query
        """
        SELECT variant_get(parse_json('3.14'), '$', 'decimal') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Extract as decimal(10,2)
      When query
        """
        SELECT variant_get(parse_json('3.14'), '$', 'decimal(10,2)') AS result
        """
      Then query result
        | result |
        | 3.14   |

    Scenario: Extract negative decimal
      When query
        """
        SELECT variant_get(parse_json('-123.456'), '$', 'decimal(10,3)') AS result
        """
      Then query result
        | result   |
        | -123.456 |

    Scenario: Extract nested decimal field
      When query
        """
        SELECT variant_get(parse_json('{"price":19.99}'), '$.price', 'decimal(10,2)') AS result
        """
      Then query result
        | result |
        | 19.99  |

  Rule: Timestamp type extraction

    Scenario: Extract as timestamp
      When query
        """
        SELECT CAST(variant_get(parse_json('"2024-01-15T10:30:00"'), '$', 'timestamp') AS STRING) AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |

  Rule: Byte and short types

    Scenario Outline: Byte and short: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), '$', <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | json    | type    | result |
        | Extract as byte  | '127'   | 'byte'  | 127    |
        | Extract as short | '32767' | 'short' | 32767  |

    Scenario: Extract as byte
      When query
        """
        SELECT variant_get(parse_json('127'), '$', 'byte') AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: Extract as short
      When query
        """
        SELECT variant_get(parse_json('32767'), '$', 'short') AS result
        """
      Then query result
        | result |
        | 32767  |

  Rule: try_variant_get with wrong types

    Scenario Outline: try_variant_get: <case>
      When query
        """
        SELECT try_variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | json                 | path    | type            | result     |
        | try_variant_get string as decimal returns NULL | '"hello"'            | '$'     | 'decimal(10,2)' | NULL       |
        | try_variant_get object as int returns NULL     | '{"a":1}'            | '$'     | 'int'           | NULL       |
        | try_variant_get missing path returns NULL      | '{"a":1}'            | '$.b'   | 'int'           | NULL       |
        | try_variant_get NULL input returns NULL        | CAST(NULL AS STRING) | '$'     | 'int'           | NULL       |
        | try_variant_get valid int extraction           | '42'                 | '$'     | 'int'           | 42         |
        | try_variant_get valid string extraction        | '"hello"'            | '$'     | 'string'        | hello      |
        | try_variant_get valid boolean extraction       | 'true'               | '$'     | 'boolean'       | true       |
        | try_variant_get array index                    | '[10,20,30]'         | '$[1]'  | 'int'           | 20         |
        | try_variant_get nested field                   | '{"a":{"b":99}}'     | '$.a.b' | 'int'           | 99         |
        | try_variant_get array as int returns NULL      | '[1,2,3]'            | '$'     | 'int'           | NULL       |
        | try_variant_get bool as int returns 1          | 'true'               | '$'     | 'int'           | 1          |
        | try_variant_get false as int returns 0         | 'false'              | '$'     | 'int'           | 0          |
        | try_variant_get bool as bigint                 | 'true'               | '$'     | 'bigint'        | 1          |
        | try_variant_get bool as short                  | 'false'              | '$'     | 'short'         | 0          |
        | try_variant_get null JSON value returns NULL   | '{"a":null}'         | '$.a'   | 'string'        | NULL       |
        | try_variant_get out of bounds returns NULL     | '[1,2]'              | '$[5]'  | 'int'           | NULL       |
        | try_variant_get bigint extraction              | '9999999999'         | '$'     | 'bigint'        | 9999999999 |
        | try_variant_get decimal extraction             | '19.99'              | '$'     | 'decimal(10,2)' | 19.99      |

    Scenario Outline: variant_get bool as int: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), '$', 'int') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | json    | result |
        | variant_get bool as int returns 1  | 'true'  | 1      |
        | variant_get false as int returns 0 | 'false' | 0      |

    Scenario: try_variant_get multi-row bool as int
      When query
        """
        SELECT try_variant_get(parse_json(v), '$', 'int') AS result
        FROM VALUES ('true'), ('false'), ('null'), ('"text"') AS t(v)
        """
      Then query result
        | result |
        | 1      |
        | 0      |
        | NULL   |
        | NULL   |

    Scenario: try_variant_get multi-row with mixed types
      When query
        """
        SELECT try_variant_get(parse_json(v), '$', 'int') AS result
        FROM VALUES ('1'), ('"text"'), ('null'), ('3') AS t(v)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |
        | NULL   |
        | 3      |

    Scenario: try_variant_get string as decimal returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('"hello"'), '$', 'decimal(10,2)') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get object as int returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('{"a":1}'), '$', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get missing path returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('{"a":1}'), '$.b', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get NULL input returns NULL
      When query
        """
        SELECT try_variant_get(parse_json(CAST(NULL AS STRING)), '$', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get valid int extraction
      When query
        """
        SELECT try_variant_get(parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_variant_get valid string extraction
      When query
        """
        SELECT try_variant_get(parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: try_variant_get valid boolean extraction
      When query
        """
        SELECT try_variant_get(parse_json('true'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: try_variant_get array index
      When query
        """
        SELECT try_variant_get(parse_json('[10,20,30]'), '$[1]', 'int') AS result
        """
      Then query result
        | result |
        | 20     |

    Scenario: try_variant_get nested field
      When query
        """
        SELECT try_variant_get(parse_json('{"a":{"b":99}}'), '$.a.b', 'int') AS result
        """
      Then query result
        | result |
        | 99     |

    Scenario: try_variant_get array as int returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('[1,2,3]'), '$', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get bool as int returns 1
      When query
        """
        SELECT try_variant_get(parse_json('true'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: try_variant_get false as int returns 0
      When query
        """
        SELECT try_variant_get(parse_json('false'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: try_variant_get bool as bigint
      When query
        """
        SELECT try_variant_get(parse_json('true'), '$', 'bigint') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: try_variant_get bool as short
      When query
        """
        SELECT try_variant_get(parse_json('false'), '$', 'short') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: variant_get bool as int returns 1
      When query
        """
        SELECT variant_get(parse_json('true'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: variant_get false as int returns 0
      When query
        """
        SELECT variant_get(parse_json('false'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: try_variant_get null JSON value returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('{"a":null}'), '$.a', 'string') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get out of bounds returns NULL
      When query
        """
        SELECT try_variant_get(parse_json('[1,2]'), '$[5]', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get bigint extraction
      When query
        """
        SELECT try_variant_get(parse_json('9999999999'), '$', 'bigint') AS result
        """
      Then query result
        | result     |
        | 9999999999 |

    Scenario: try_variant_get decimal extraction
      When query
        """
        SELECT try_variant_get(parse_json('19.99'), '$', 'decimal(10,2)') AS result
        """
      Then query result
        | result |
        | 19.99  |

  Rule: Error cases

    Scenario: Invalid JSON raises error
      When query
        """
        SELECT parse_json('not json') AS result
        """
      Then query error (MALFORMED_RECORD_IN_PARSING|JSON format error)

    Scenario: Empty string raises error
      When query
        """
        SELECT parse_json('') AS result
        """
      Then query error (MALFORMED_RECORD_IN_PARSING|JSON format error|empty)

  Rule: variant_to_json with options (ignores options for Variant input)

    Scenario Outline: variant_to_json options: <case>
      When query
        """
        SELECT to_json(parse_json(<json>), map(<options>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | json      | options                         | result  |
        | variant_to_json ignores options for Variant input     | '{"a":1}' | 'timestampFormat', 'yyyy-MM-dd' | {"a":1} |
        | variant_to_json ignores options with different format | '[1,2,3]' | 'pretty', 'true'                | [1,2,3] |

    Scenario: variant_to_json ignores options for Variant input
      When query
        """
        SELECT to_json(parse_json('{"a":1}'), map('timestampFormat', 'yyyy-MM-dd')) AS result
        """
      Then query result
        | result  |
        | {"a":1} |

    Scenario: variant_to_json ignores options with different format
      When query
        """
        SELECT to_json(parse_json('[1,2,3]'), map('pretty', 'true')) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

  Rule: Additional type extractions

    Scenario Outline: Additional type: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), '$', <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | json                  | type     | result              |
        | Extract as bigint         | '9999999999'          | 'bigint' | 9999999999          |
        | Extract as long (max i64) | '9223372036854775807' | 'long'   | 9223372036854775807 |
        | Extract as float          | '3.14'                | 'float'  | 3.14                |

    Scenario: Extract as bigint
      When query
        """
        SELECT variant_get(parse_json('9999999999'), '$', 'bigint') AS result
        """
      Then query result
        | result     |
        | 9999999999 |

    Scenario: Extract as long (max i64)
      When query
        """
        SELECT variant_get(parse_json('9223372036854775807'), '$', 'long') AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: Extract as float
      When query
        """
        SELECT variant_get(parse_json('3.14'), '$', 'float') AS result
        """
      Then query result
        | result |
        | 3.14   |

  Rule: variant_get error cases (non-try)

    Scenario: Negative array index raises error
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[-1]', 'int') AS result
        """
      Then query error (INVALID_VARIANT_GET_PATH|not a valid variant extraction path|path|Invalid token)

    Scenario: Invalid path dollar-dot raises error
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.', 'int') AS result
        """
      Then query error (INVALID_VARIANT_GET_PATH|not a valid variant extraction path|path)

    Scenario: Invalid path double-dot raises error
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$..a', 'int') AS result
        """
      Then query error (INVALID_VARIANT_GET_PATH|not a valid|Unexpected leading)

  Rule: NULL handling edge cases

    Scenario Outline: NULL scalar: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                         | expr                                      |
        | parse_json of NULL scalar returns NULL       | parse_json(CAST(NULL AS STRING))          |
        | variant_to_json of NULL variant returns NULL | to_json(parse_json(CAST(NULL AS STRING))) |

    Scenario Outline: NULL column: <case>
      When query
        """
        SELECT <expr> AS result FROM VALUES (CAST(NULL AS STRING)) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                     | expr                   |
        | parse_json NULL column returns NULL      | parse_json(x)          |
        | variant_to_json NULL column returns NULL | to_json(parse_json(x)) |

    Scenario: mixed NULL and non-NULL variant_to_json
      When query
        """
        SELECT to_json(parse_json(x)) AS result FROM VALUES ('42'), (CAST(NULL AS STRING)), ('{"a":1}') AS t(x)
        """
      Then query result
        | result  |
        | 42      |
        | NULL    |
        | {"a":1} |

    Scenario: variant_to_json of NULL variant returns NULL
      When query
        """
        SELECT to_json(parse_json(CAST(NULL AS STRING))) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_json NULL column returns NULL
      When query
        """
        SELECT parse_json(x) AS result FROM VALUES (CAST(NULL AS STRING)) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: variant_to_json NULL column returns NULL
      When query
        """
        SELECT to_json(parse_json(x)) AS result FROM VALUES (CAST(NULL AS STRING)) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

  Rule: Variant storage detection

    Scenario: ordinary struct with Variant-shaped field names is not treated as Variant
      When query
        """
        SELECT CAST(named_struct('metadata', X'01', 'value', X'02') AS STRING) IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

  Rule: CAST to VARIANT

    Scenario Outline: CAST to VARIANT: <case>
      When query
        """
        SELECT CAST(<value> AS VARIANT) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | value                        | result  |
        | CAST string to variant  | 'hello'                      | "hello" |
        | CAST integer to variant | 42                           | 42      |
        | CAST null to variant    | NULL                         | NULL    |
        | CAST boolean to variant | true                         | true    |
        | CAST decimal to variant | CAST(99.99 AS DECIMAL(10,2)) | 99.99   |
        | CAST array to variant   | array(1,2,3)                 | [1,2,3] |

    Scenario: CAST string to variant
      When query
        """
        SELECT CAST('hello' AS VARIANT) AS result
        """
      Then query result
        | result  |
        | "hello" |

    Scenario: CAST integer to variant
      When query
        """
        SELECT CAST(42 AS VARIANT) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: CAST null to variant
      When query
        """
        SELECT CAST(NULL AS VARIANT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: CAST boolean to variant
      When query
        """
        SELECT CAST(true AS VARIANT) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: CAST decimal to variant
      When query
        """
        SELECT CAST(CAST(99.99 AS DECIMAL(10,2)) AS VARIANT) AS result
        """
      Then query result
        | result |
        | 99.99  |

    Scenario: CAST array to variant
      When query
        """
        SELECT CAST(array(1,2,3) AS VARIANT) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

  Rule: Variant NULL handling

    Scenario Outline: Variant NULL: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | expr                                   | result |
        | parse_json NULL returns SQL NULL                  | parse_json(NULL)                       | NULL   |
        | parse_json null string returns variant null       | parse_json('null')                     | null   |
        | CAST NULL AS VARIANT returns SQL NULL             | CAST(NULL AS VARIANT)                  | NULL   |
        | is_variant_null on SQL NULL variant returns false | is_variant_null(CAST(NULL AS VARIANT)) | false  |
        | is_variant_null on json null returns true         | is_variant_null(parse_json('null'))    | true   |
        | to_json on SQL NULL variant returns NULL          | to_json(CAST(NULL AS VARIANT))         | NULL   |
        | to_json on json null returns null string          | to_json(parse_json('null'))            | null   |

    Scenario: parse_json null string returns variant null
      When query
        """
        SELECT parse_json('null') AS result
        """
      Then query result
        | result |
        | null   |

    Scenario: CAST NULL AS VARIANT returns SQL NULL
      When query
        """
        SELECT CAST(NULL AS VARIANT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: is_variant_null on SQL NULL variant returns false
      When query
        """
        SELECT is_variant_null(CAST(NULL AS VARIANT)) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: to_json on SQL NULL variant returns NULL
      When query
        """
        SELECT to_json(CAST(NULL AS VARIANT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_json on json null returns null string
      When query
        """
        SELECT to_json(parse_json('null')) AS result
        """
      Then query result
        | result |
        | null   |

  Rule: Spark variant path compatibility

    Scenario Outline: Path compatibility: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, 'int') AS result
        """
      Then query result
        | result |
        | 42     |

      Examples:
        | case                                                     | json                      | path        |
        | Extract array element under field path                   | '{"a":[42]}'              | '$.a[0]'    |
        | Extract field after array index path                     | '{"a":[{"b":42}]}'        | '$.a[0].b'  |
        | Quoted field containing dot is treated as one field      | '{"a.b":42,"a":{"b":99}}' | '$["a.b"]'  |
        | Quoted field containing brackets is treated as one field | '{"a[0]":42,"a":[99]}'    | '$["a[0]"]' |

  Rule: try_parse_json

    Scenario: try_parse_json valid JSON integer
      When query
        """
        SELECT variant_get(try_parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_parse_json valid JSON string
      When query
        """
        SELECT variant_get(try_parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: try_parse_json valid JSON object
      When query
        """
        SELECT variant_get(try_parse_json('{"a":1}'), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: try_parse_json invalid JSON returns NULL
      When query
        """
        SELECT try_parse_json('not json') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json empty string returns NULL
      When query
        """
        SELECT try_parse_json('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json NULL input returns NULL
      When query
        """
        SELECT try_parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json valid JSON array
      When query
        """
        SELECT variant_get(try_parse_json('[1,2,3]'), '$[0]', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: schema_of_variant

    Scenario: schema_of_variant integer
      When query
        """
        SELECT schema_of_variant(parse_json('42')) AS result
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: schema_of_variant string
      When query
        """
        SELECT schema_of_variant(parse_json('"hello"')) AS result
        """
      Then query result
        | result |
        | STRING |

    Scenario: schema_of_variant boolean
      When query
        """
        SELECT schema_of_variant(parse_json('true')) AS result
        """
      Then query result
        | result  |
        | BOOLEAN |

    # parquet-variant-json parses 3.14 as f64 (DOUBLE) instead of Decimal like Spark
    Scenario: schema_of_variant simple object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":1}')) AS result
        """
      Then query result
        | result            |
        | OBJECT<a: BIGINT> |

    Scenario: schema_of_variant array of integers
      When query
        """
        SELECT schema_of_variant(parse_json('[1,2,3]')) AS result
        """
      Then query result
        | result         |
        | ARRAY<BIGINT>  |

    Scenario: schema_of_variant empty object
      When query
        """
        SELECT schema_of_variant(parse_json('{}')) AS result
        """
      Then query result
        | result   |
        | OBJECT<> |

    Scenario: schema_of_variant empty array
      When query
        """
        SELECT schema_of_variant(parse_json('[]')) AS result
        """
      Then query result
        | result       |
        | ARRAY<VOID>  |

    Scenario: schema_of_variant nested object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":{"b":1}}')) AS result
        """
      Then query result
        | result                       |
        | OBJECT<a: OBJECT<b: BIGINT>> |

    Scenario: schema_of_variant mixed array
      When query
        """
        SELECT schema_of_variant(parse_json('[1, "hello", true]')) AS result
        """
      Then query result
        | result                               |
        | ARRAY<VARIANT>                       |

    Scenario: schema_of_variant object with multiple fields
      When query
        """
        SELECT schema_of_variant(parse_json('{"name":"sail","age":5,"active":true}')) AS result
        """
      Then query result
        | result                                          |
        | OBJECT<active: BOOLEAN, age: BIGINT, name: STRING> |

  Rule: schema_of_variant_agg

    Scenario: schema_of_variant_agg with nulls
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('42'), ('null'), ('99') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |

  Rule: to_variant_object

    Scenario: to_variant_object simple struct
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', 1, 'b', 'hello'))) AS result
        """
      Then query result
        | result            |
        | {"a":1,"b":"hello"} |

    Scenario: to_variant_object single field
      When query
        """
        SELECT to_json(to_variant_object(named_struct('x', 42))) AS result
        """
      Then query result
        | result    |
        | {"x":42}  |

    # cast_to_variant from parquet-variant-compute omits NULL struct fields
    Scenario: to_variant_object with boolean
      When query
        """
        SELECT to_json(to_variant_object(named_struct('flag', true, 'count', 5))) AS result
        """
      Then query result
        | result                    |
        | {"count":5,"flag":true}   |

    Scenario: to_variant_object with array input
      When query
        """
        SELECT to_json(to_variant_object(array(1, 2, 3))) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

    Scenario: to_variant_object with map input
      When query
        """
        SELECT to_json(to_variant_object(map('x', 1, 'y', 2))) AS result
        """
      Then query result
        | result          |
        | {"x":1,"y":2}   |

    Scenario: to_variant_object with array of structs
      When query
        """
        SELECT to_json(to_variant_object(array(named_struct('a', 1)))) AS result
        """
      Then query result
        | result      |
        | [{"a":1}]   |

    Scenario: to_variant_object rejects primitive int
      When query
        """
        SELECT to_variant_object(42) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT)

    Scenario: to_variant_object rejects string
      When query
        """
        SELECT to_variant_object('hello') AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT)

  Rule: variant_get on a NULL variant preserves null

    Scenario: Field path on a NULL variant is NULL
      When query
        """
        SELECT variant_get(CAST(NULL AS VARIANT), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Field path on parse_json of NULL is NULL
      When query
        """
        SELECT variant_get(parse_json(CAST(NULL AS STRING)), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_variant_get field path on a NULL variant is NULL
      When query
        """
        SELECT try_variant_get(CAST(NULL AS VARIANT), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |
