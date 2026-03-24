@variant
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

    Scenario: Integer 13 is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('13')) AS result
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

    Scenario: Extract string from nested object
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

    Scenario: Missing field returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.b', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Null field in object returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":null}'), '$.a', 'string') AS result
        """
      Then query result
        | result |
        | NULL   |

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
