@try_parse_json @spark-4
Feature: try_parse_json (safe version of parse_json)

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

    Scenario: try_parse_json NULL input returns NULL
      When query
        """
        SELECT try_parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Trailing content and multi-row

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

    Scenario: try_parse_json matches parse_json on valid input
      When query
        """
        SELECT to_json(try_parse_json('{"x":1}')) = to_json(parse_json('{"x":1}')) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Edge cases (advanced)

    @sail-bug
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

    @sail-bug
    Scenario: try_parse_json rejects non-string input with Spark code
      When query
        """
        SELECT try_parse_json(42)
        """
      Then query error DATATYPE_MISMATCH
