@spark-4
Feature: try_parse_json comprehensive tests

  Rule: Argument count validation

    Scenario Outline: Arity: <case>
      When query
        """
        SELECT try_parse_json(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                  | args          |
        | zero arguments errors |               |
        | two arguments errors  | '{}', 'extra' |

  Rule: NULL handling

    Scenario Outline: NULL handling: <case>
      When query
        """
        SELECT try_parse_json(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                          | input                |
        | NULL input returns NULL       | NULL                 |
        | typed NULL input returns NULL | CAST(NULL AS STRING) |

  Rule: Valid JSON parsing

    Scenario Outline: Valid JSON: <case>
      When query
        """
        SELECT variant_get(try_parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | json      | path   | type     | result |
        | try_parse_json valid JSON integer | '42'      | '$'    | 'int'    | 42     |
        | try_parse_json valid JSON string  | '"hello"' | '$'    | 'string' | hello  |
        | try_parse_json valid JSON object  | '{"a":1}' | '$.a'  | 'int'    | 1      |
        | try_parse_json valid JSON array   | '[1,2,3]' | '$[0]' | 'int'    | 1      |

    Scenario: try_parse_json valid JSON null
      When query
        """
        SELECT is_variant_null(try_parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Invalid JSON returns NULL

    Scenario Outline: Invalid JSON: <case>
      When query
        """
        SELECT try_parse_json(<json>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                     | json       |
        | try_parse_json invalid JSON returns NULL | 'not json' |
        | try_parse_json empty string returns NULL | ''         |

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

    Scenario Outline: Invalid JSON format: <case>
      When query
        """
        SELECT try_parse_json(<json>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                         | json     |
        | try_parse_json unclosed brace returns NULL   | '{"a":1' |
        | try_parse_json unclosed bracket returns NULL | '[1,2'   |
        | try_parse_json just whitespace returns NULL  | '   '    |
        | try_parse_json just comma returns NULL       | ','      |
        | try_parse_json just colon returns NULL       | ':'      |

  Rule: Valid edge cases

    Scenario Outline: Valid edge case (to_json): <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | json                  | result              |
        | try_parse_json nested empty array   | '[[]]'                | [[]]                |
        | try_parse_json deeply nested object | '{"a":{"b":{"c":1}}}' | {"a":{"b":{"c":1}}} |
        | try_parse_json empty object         | '{}'                  | {}                  |
        | try_parse_json empty array          | '[]'                  | []                  |

    Scenario Outline: Valid edge case (variant_get): <case>
      When query
        """
        SELECT variant_get(try_parse_json(<json>), '$', <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | json                | type      | result            |
        | try_parse_json unicode string     | '"héllo"'           | 'string'  | héllo             |
        | try_parse_json large number       | '99999999999999999' | 'bigint'  | 99999999999999999 |
        | try_parse_json negative number    | '-42'               | 'int'     | -42               |
        | try_parse_json boolean true       | 'true'              | 'boolean' | true              |
        | try_parse_json boolean false      | 'false'             | 'boolean' | false             |
        | try_parse_json float zero         | '0.0'               | 'double'  | 0.0               |
        | try_parse_json empty string value | '""'                | 'string'  |                   |

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
        | result               |
        | 99999999999999999999 |

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

    Scenario Outline: Number edge case (invalid): <case>
      When query
        """
        SELECT try_parse_json(<json>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                  | json       |
        | leading zero is invalid JSON          | '01'       |
        | float without leading zero is invalid | '.5'       |
        | plus sign number is invalid           | '+42'      |
        | double negative is invalid            | '--42'     |
        | Infinity string is invalid JSON       | 'Infinity' |
        | NaN string is invalid JSON            | 'NaN'      |
        | hex number is invalid JSON            | '0xff'     |

    Scenario Outline: Number edge case (to_json): <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | json                   | result               |
        | negative zero becomes zero           | '-0'                   | 0                    |
        | negative zero float becomes zero     | '-0.0'                 | 0                    |
        | very small float preserves precision | '0.000000000000000001' | 0.000000000000000001 |

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

    Scenario Outline: Key edge case: <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | json        | result    |
        | empty key in object | '{"":1}'    | {"":1}    |
        | key with space      | '{"a b":1}' | {"a b":1} |
        | numeric-like key    | '{"123":1}' | {"123":1} |

  Rule: Whitespace handling

    Scenario Outline: Whitespace: <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | json          | result  |
        | leading whitespace       | '  42'        | 42      |
        | trailing whitespace      | '42  '        | 42      |
        | whitespace both sides    | '  42  '      | 42      |
        | whitespace inside object | '{ "a" : 1 }' | {"a":1} |

  Rule: Array edge cases

    Scenario Outline: Array edge case: <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | json                        | result                |
        | array with nulls  | '[null, null]'              | [null,null]           |
        | array mixed types | '[1, "a", true, null, 1.5]' | [1,"a",true,null,1.5] |
        | array of arrays   | '[[1],[2],[3]]'             | [[1],[2],[3]]         |

  Rule: Object edge cases

    Scenario Outline: Object edge case: <case>
      When query
        """
        SELECT to_json(try_parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | json                        | result                    |
        | deeply nested object       | '{"a":{"b":{"c":{"d":1}}}}' | {"a":{"b":{"c":{"d":1}}}} |
        | object with array value    | '{"a":[1,2,3]}'             | {"a":[1,2,3]}             |
        | object with null value     | '{"a":null}'                | {"a":null}                |
        | object with boolean values | '{"a":true,"b":false}'      | {"a":true,"b":false}      |

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

    Scenario Outline: Input type: <case>
      When query
        """
        SELECT try_parse_json(<input>) AS result
        """
      Then query error .*

      Examples:
        | case                 | input    |
        | integer input errors | 42       |
        | boolean input errors | true     |
        | double input errors  | 1.0      |
        | array input errors   | array(1) |
        | binary input errors  | X'7B7D'  |

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

  @function(nullability)
  Rule: Output schema

    Scenario: try_parse_json stays nullable because it returns NULL on invalid input
      When query
        """
        SELECT try_parse_json('{"a":1}') AS result
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = true)
        """

    Scenario: try_parse_json of a non-null column stays nullable because it can NULL on invalid input
      When query
        """
        SELECT try_parse_json(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = true)
        """

    Scenario: try_parse_json of a nullable column stays nullable
      When query
        """
        SELECT try_parse_json(c) AS result FROM VALUES ('{"a":1}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = true)
        """
