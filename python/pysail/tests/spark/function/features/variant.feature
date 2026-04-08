@variant @spark-4
Feature: Variant type functions (parse_json, is_variant_null, variant_get)

  Rule: parse_json + variant_get roundtrip

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

    Scenario: JSON null is variant null
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

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

  Rule: is_variant_null additional cases from doctest

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

  Rule: Decimal type extraction

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

    Scenario: parse_json of NULL scalar returns NULL
      When query
        """
        SELECT parse_json(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

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

  Rule: CAST to VARIANT

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

    Scenario: parse_json NULL returns SQL NULL
      When query
        """
        SELECT parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

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

    Scenario: is_variant_null on json null returns true
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

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
