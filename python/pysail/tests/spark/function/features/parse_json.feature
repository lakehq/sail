@parse_json @spark-4
Feature: parse_json (strict version; errors on invalid JSON)

  Rule: Valid JSON parsing

    Scenario: parse_json valid integer
      When query
        """
        SELECT variant_get(parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: parse_json valid string
      When query
        """
        SELECT variant_get(parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: parse_json valid object
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: parse_json valid array
      When query
        """
        SELECT variant_get(parse_json('[1,2,3]'), '$[1]', 'int') AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: parse_json valid JSON null
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: parse_json boolean true
      When query
        """
        SELECT variant_get(parse_json('true'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: NULL propagation

    Scenario: parse_json SQL NULL input returns NULL
      When query
        """
        SELECT parse_json(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_json multi-row with NULL
      When query
        """
        SELECT to_json(parse_json(v)) AS result
        FROM VALUES ('1'), (NULL), ('"x"') AS t(v)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |
        | "x"    |

  Rule: Invalid JSON raises MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json invalid text errors with Spark code
      When query
        """
        SELECT parse_json('bad json')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json empty string errors with Spark code
      When query
        """
        SELECT parse_json('')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json unclosed brace errors with Spark code
      When query
        """
        SELECT parse_json('{')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json unclosed bracket errors with Spark code
      When query
        """
        SELECT parse_json('[')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json raw control char errors with Spark code
      When query
        """
        SELECT parse_json('"a\tb"')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: parse_json duplicate keys errors (Spark rejects as malformed)
      When query
        """
        SELECT parse_json('{"a":1,"a":2}')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

  Rule: Type handling

    Scenario: parse_json rejects non-string input with Spark code
      When query
        """
        SELECT parse_json(42)
        """
      Then query error DATATYPE_MISMATCH

  Rule: Numeric preservation (Sail currently diverges from Spark)

    @sail-bug
    Scenario: parse_json scientific notation preserves decimal
      When query
        """
        SELECT to_json(parse_json('1.5e3')) AS result
        """
      Then query result
        | result |
        | 1500.0 |

    Scenario: parse_json negative scientific notation
      When query
        """
        SELECT to_json(parse_json('1.5e-1')) AS result
        """
      Then query result
        | result |
        | 0.15   |

    Scenario: parse_json negative zero
      When query
        """
        SELECT to_json(parse_json('-0')) AS result
        """
      Then query result
        | result |
        | -0     |

    @sail-bug
    Scenario: parse_json preserves large number beyond i64
      When query
        """
        SELECT to_json(parse_json('99999999999999999999')) AS result
        """
      Then query result
        | result                |
        | 99999999999999999999  |

    Scenario: parse_json accepts trailing garbage (Spark parses prefix)
      When query
        """
        SELECT to_json(parse_json('42 extra')) AS result
        """
      Then query result
        | result |
        | 42     |

  Rule: Edge cases

    Scenario: parse_json unicode escape
      When query
        """
        SELECT variant_get(parse_json('"\u00e9"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | é      |

    Scenario: parse_json empty object
      When query
        """
        SELECT to_json(parse_json('{}')) AS result
        """
      Then query result
        | result |
        | {}     |

    Scenario: parse_json empty array
      When query
        """
        SELECT to_json(parse_json('[]')) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: parse_json heterogeneous nested structure
      When query
        """
        SELECT to_json(parse_json('{"a":[1,"two",null,{"b":true}]}')) AS result
        """
      Then query result
        | result                          |
        | {"a":[1,"two",null,{"b":true}]} |

    Scenario: parse_json deeply nested
      When query
        """
        SELECT variant_get(parse_json('{"a":{"b":{"c":{"d":42}}}}'), '$.a.b.c.d', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: parse_json whitespace around value
      When query
        """
        SELECT variant_get(parse_json('   123   '), '$', 'int') AS result
        """
      Then query result
        | result |
        | 123    |

  Rule: All-null input column returns all NULL (fast-path invariant)

    Scenario: parse_json multi-row all-null column returns all NULL
      When query
        """
        SELECT parse_json(v) AS result FROM VALUES
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
