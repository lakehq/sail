@try_parse_json @spark-4
Feature: try_parse_json comprehensive tests

  Rule: Argument count validation

    Scenario: zero arguments errors
      When query
        """
        SELECT try_parse_json() AS result
        """
      Then query error .*

    Scenario: two arguments errors
      When query
        """
        SELECT try_parse_json('{}', 'extra') AS result
        """
      Then query error .*

  Rule: NULL handling

    Scenario: NULL input returns NULL
      When query
        """
        SELECT try_parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL input returns NULL
      When query
        """
        SELECT try_parse_json(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Valid JSON parsing

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

    Scenario: try_parse_json valid JSON null
      When query
        """
        SELECT is_variant_null(try_parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: try_parse_json valid JSON array
      When query
        """
        SELECT variant_get(try_parse_json('[1,2,3]'), '$[0]', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Invalid JSON returns NULL

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

  Rule: Trailing content

    Scenario: try_parse_json trailing garbage parses valid prefix
      When query
        """
        SELECT try_parse_json('42 extra') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_parse_json trailing whitespace is valid
      When query
        """
        SELECT to_json(try_parse_json('42   ')) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_parse_json multi-row with invalid
      When query
        """
        SELECT try_parse_json(v) AS result
        FROM VALUES ('42'), ('bad json'), ('null'), ('{"a":1}') AS t(v)
        """
      Then query result
        | result  |
        | 42      |
        | NULL    |
        | null    |
        | {"a":1} |

    Scenario: try_parse_json multi-row all invalid returns all NULL
      When query
        """
        SELECT try_parse_json(v) AS result
        FROM VALUES ('bad'), ('worse'), ('nope') AS t(v)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Various invalid JSON formats

    Scenario: try_parse_json unclosed brace returns NULL
      When query
        """
        SELECT try_parse_json('{"a":1') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json unclosed bracket returns NULL
      When query
        """
        SELECT try_parse_json('[1,2') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json just whitespace returns NULL
      When query
        """
        SELECT try_parse_json('   ') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json just comma returns NULL
      When query
        """
        SELECT try_parse_json(',') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json just colon returns NULL
      When query
        """
        SELECT try_parse_json(':') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Valid edge cases

    Scenario: try_parse_json nested empty array
      When query
        """
        SELECT to_json(try_parse_json('[[]]')) AS result
        """
      Then query result
        | result |
        | [[]]   |

    Scenario: try_parse_json deeply nested object
      When query
        """
        SELECT to_json(try_parse_json('{"a":{"b":{"c":1}}}')) AS result
        """
      Then query result
        | result                |
        | {"a":{"b":{"c":1}}} |

    Scenario: try_parse_json unicode string
      When query
        """
        SELECT variant_get(try_parse_json('"héllo"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | héllo  |

    Scenario: try_parse_json large number
      When query
        """
        SELECT variant_get(try_parse_json('99999999999999999'), '$', 'bigint') AS result
        """
      Then query result
        | result            |
        | 99999999999999999 |

    Scenario: try_parse_json negative number
      When query
        """
        SELECT variant_get(try_parse_json('-42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | -42    |

    Scenario: try_parse_json boolean true
      When query
        """
        SELECT variant_get(try_parse_json('true'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: try_parse_json boolean false
      When query
        """
        SELECT variant_get(try_parse_json('false'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: try_parse_json float zero
      When query
        """
        SELECT variant_get(try_parse_json('0.0'), '$', 'double') AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: try_parse_json empty object
      When query
        """
        SELECT to_json(try_parse_json('{}')) AS result
        """
      Then query result
        | result |
        | {}     |

    Scenario: try_parse_json empty array
      When query
        """
        SELECT to_json(try_parse_json('[]')) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: try_parse_json empty string value
      When query
        """
        SELECT variant_get(try_parse_json('""'), '$', 'string') AS result
        """
      Then query result
        | result |
        |        |

  Rule: Edge cases (advanced)

    Scenario: try_parse_json duplicate keys returns NULL (Spark rejects as malformed)
      When query
        """
        SELECT try_parse_json('{"a":1,"a":2}') IS NULL AS result
        """
      Then query result
        | result |
        | true   |

    @sail-bug
    Scenario: try_parse_json scientific notation preserves decimal
      When query
        """
        SELECT to_json(try_parse_json('1.5e3')) AS result
        """
      Then query result
        | result |
        | 1500.0 |

    Scenario: try_parse_json negative scientific notation
      When query
        """
        SELECT to_json(try_parse_json('1.5e-1')) AS result
        """
      Then query result
        | result |
        | 0.15   |

    @sail-bug
    Scenario: try_parse_json preserves large number beyond i64
      When query
        """
        SELECT to_json(try_parse_json('99999999999999999999')) AS result
        """
      Then query result
        | result                |
        | 99999999999999999999  |

    Scenario: try_parse_json raw control char returns NULL
      When query
        """
        SELECT try_parse_json('"a\tb"') IS NULL AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: try_parse_json unicode escape
      When query
        """
        SELECT variant_get(try_parse_json('"\u00e9"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | é      |

    Scenario: try_parse_json heterogeneous nested structure
      When query
        """
        SELECT to_json(try_parse_json('{"a":[1,"two",null,{"b":true}]}')) AS result
        """
      Then query result
        | result                          |
        | {"a":[1,"two",null,{"b":true}]} |

  Rule: JSON number edge cases

    Scenario: leading zero is invalid JSON
      When query
        """
        SELECT try_parse_json('01') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negative zero becomes zero
      When query
        """
        SELECT to_json(try_parse_json('-0')) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: negative zero float becomes zero
      When query
        """
        SELECT to_json(try_parse_json('-0.0')) AS result
        """
      Then query result
        | result |
        | 0      |

    @sail-bug
    # Sail doesn't convert very large integers to scientific notation in Variant
    Scenario: very large integer uses scientific notation
      When query
        """
        SELECT to_json(try_parse_json('999999999999999999999999999999999999999')) AS result
        """
      Then query result
        | result |
        | 1.0E39 |

    Scenario: very small float preserves precision
      When query
        """
        SELECT to_json(try_parse_json('0.000000000000000001')) AS result
        """
      Then query result
        | result               |
        | 0.000000000000000001 |

    Scenario: float without leading zero is invalid
      When query
        """
        SELECT try_parse_json('.5') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: plus sign number is invalid
      When query
        """
        SELECT try_parse_json('+42') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: double negative is invalid
      When query
        """
        SELECT try_parse_json('--42') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity string is invalid JSON
      When query
        """
        SELECT try_parse_json('Infinity') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NaN string is invalid JSON
      When query
        """
        SELECT try_parse_json('NaN') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: hex number is invalid JSON
      When query
        """
        SELECT try_parse_json('0xff') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Sail doesn't produce scientific notation for 1e10 in Variant
    Scenario: scientific notation 1e10
      When query
        """
        SELECT to_json(try_parse_json('1e10')) AS result
        """
      Then query result
        | result |
        | 1.0E10 |

  Rule: String and unicode edge cases

    Scenario: empty key in object
      When query
        """
        SELECT to_json(try_parse_json('{"":1}')) AS result
        """
      Then query result
        | result |
        | {"":1} |

    Scenario: key with space
      When query
        """
        SELECT to_json(try_parse_json('{"a b":1}')) AS result
        """
      Then query result
        | result    |
        | {"a b":1} |

    Scenario: numeric-like key
      When query
        """
        SELECT to_json(try_parse_json('{"123":1}')) AS result
        """
      Then query result
        | result    |
        | {"123":1} |

  Rule: Whitespace handling

    Scenario: leading whitespace
      When query
        """
        SELECT to_json(try_parse_json('  42')) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: trailing whitespace
      When query
        """
        SELECT to_json(try_parse_json('42  ')) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: whitespace both sides
      When query
        """
        SELECT to_json(try_parse_json('  42  ')) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: whitespace inside object
      When query
        """
        SELECT to_json(try_parse_json('{ "a" : 1 }')) AS result
        """
      Then query result
        | result  |
        | {"a":1} |

  Rule: Array edge cases

    Scenario: array with nulls
      When query
        """
        SELECT to_json(try_parse_json('[null, null]')) AS result
        """
      Then query result
        | result      |
        | [null,null] |

    Scenario: array mixed types
      When query
        """
        SELECT to_json(try_parse_json('[1, "a", true, null, 1.5]')) AS result
        """
      Then query result
        | result                |
        | [1,"a",true,null,1.5] |

    Scenario: array of arrays
      When query
        """
        SELECT to_json(try_parse_json('[[1],[2],[3]]')) AS result
        """
      Then query result
        | result        |
        | [[1],[2],[3]] |

  Rule: Object edge cases

    Scenario: deeply nested object
      When query
        """
        SELECT to_json(try_parse_json('{"a":{"b":{"c":{"d":1}}}}')) AS result
        """
      Then query result
        | result                     |
        | {"a":{"b":{"c":{"d":1}}}} |

    Scenario: object with array value
      When query
        """
        SELECT to_json(try_parse_json('{"a":[1,2,3]}')) AS result
        """
      Then query result
        | result         |
        | {"a":[1,2,3]} |

    Scenario: object with null value
      When query
        """
        SELECT to_json(try_parse_json('{"a":null}')) AS result
        """
      Then query result
        | result     |
        | {"a":null} |

    Scenario: object with boolean values
      When query
        """
        SELECT to_json(try_parse_json('{"a":true,"b":false}')) AS result
        """
      Then query result
        | result               |
        | {"a":true,"b":false} |

  Rule: Multi-row column tests

    Scenario: multi-row with various JSON types
      When query
        """
        SELECT to_json(try_parse_json(v)) AS result FROM VALUES ('42'), ('"hello"'), ('true'), ('null'), ('[1,2]'), ('{}'), (NULL), ('bad') AS t(v)
        """
      Then query result
        | result  |
        | 42      |
        | "hello" |
        | true    |
        | null    |
        | [1,2]   |
        | {}      |
        | NULL    |
        | NULL    |

    @sail-bug
    # Sail renders 1e10 as 10000000000 instead of Spark's 1.0E10 (scientific notation).
    Scenario: multi-row with number edge cases
      When query
        """
        SELECT to_json(try_parse_json(v)) AS result FROM VALUES ('0'), ('-1'), ('3.14'), ('-0'), ('1e10'), ('01'), ('+5') AS t(v)
        """
      Then query result
        | result |
        | 0      |
        | -1     |
        | 3.14   |
        | 0      |
        | 1.0E10 |
        | NULL   |
        | NULL   |

  Rule: Expressions and nesting

    Scenario: try_parse_json in WHERE clause
      When query
        """
        SELECT v FROM VALUES ('42'), ('bad'), ('null') AS t(v) WHERE try_parse_json(v) IS NOT NULL
        """
      Then query result
        | v    |
        | 42   |
        | null |

    Scenario: comparison with parse_json
      When query
        """
        SELECT to_json(try_parse_json('42')) = to_json(parse_json('42')) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Error conditions

    Scenario: integer input errors
      When query
        """
        SELECT try_parse_json(42) AS result
        """
      Then query error .*

    Scenario: boolean input errors
      When query
        """
        SELECT try_parse_json(true) AS result
        """
      Then query error .*

    Scenario: double input errors
      When query
        """
        SELECT try_parse_json(1.0) AS result
        """
      Then query error .*

    Scenario: array input errors
      When query
        """
        SELECT try_parse_json(array(1)) AS result
        """
      Then query error .*

    Scenario: binary input errors
      When query
        """
        SELECT try_parse_json(X'7B7D') AS result
        """
      Then query error .*

  Rule: All-null input column returns all NULL (fast-path invariant)

    Scenario: try_parse_json multi-row all-null column returns all NULL
      When query
        """
        SELECT try_parse_json(v) AS result FROM VALUES
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING))
        AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | NULL   |
