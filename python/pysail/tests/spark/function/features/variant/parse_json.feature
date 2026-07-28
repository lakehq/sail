@spark-4
@parse_json
Feature: parse_json (strict version; errors on invalid JSON)

  Rule: Valid JSON parsing

    Scenario Outline: Valid JSON: <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | json      | path   | type      | result |
        | parse_json valid integer | '42'      | '$'    | 'int'     | 42     |
        | parse_json valid string  | '"hello"' | '$'    | 'string'  | hello  |
        | parse_json valid object  | '{"a":1}' | '$.a'  | 'int'     | 1      |
        | parse_json valid array   | '[1,2,3]' | '$[1]' | 'int'     | 2      |
        | parse_json boolean true  | 'true'    | '$'    | 'boolean' | true   |

    Scenario: parse_json valid JSON null
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
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

    Scenario Outline: Malformed: <case>
      When query
        """
        SELECT parse_json(<json>)
        """
      Then query error MALFORMED_RECORD_IN_PARSING

      Examples:
        | case                                                          | json            |
        | parse_json invalid text errors with Spark code                | 'bad json'      |
        | parse_json empty string errors with Spark code                | ''              |
        | parse_json unclosed brace errors with Spark code              | '{'             |
        | parse_json unclosed bracket errors with Spark code            | '['             |
        | parse_json duplicate keys errors (Spark rejects as malformed) | '{"a":1,"a":2}' |

    Scenario: parse_json raw control char errors with Spark code
      When query
        """
        SELECT parse_json('"a\tb"')
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

    Scenario Outline: Numeric: <case>
      When query
        """
        SELECT to_json(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                      | json       | result |
        | parse_json negative scientific notation                   | '1.5e-1'   | 0.15   |
        | parse_json negative zero                                  | '-0'       | 0      |
        | parse_json accepts trailing garbage (Spark parses prefix) | '42 extra' | 42     |

    @sail-bug
    Scenario Outline: Numeric (sail-bug): <case>
      When query
        """
        SELECT to_json(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | json                   | result               |
        | parse_json scientific notation preserves decimal | '1.5e3'                | 1500.0               |
        | parse_json preserves large number beyond i64     | '99999999999999999999' | 99999999999999999999 |

    @sail-bug
    # Spark keeps scientific negative zero as -0.0 (the exponent makes it a DOUBLE);
    # only the non-exponent forms `-0`/`-0.0` normalize to 0. Sail can't match this:
    # serde_json discards the exponent, and Sail renders -0.0 as `-0` anyway
    # (Sail-wide double->string formatting gap, same root cause as `1e10` -> `1.0E10`).
    Scenario: parse_json scientific negative zero keeps sign
      When query
        """
        SELECT to_json(parse_json('-0e0')) AS result
        """
      Then query result
        | result |
        | -0.0   |

  Rule: Edge cases

    Scenario: parse_json unicode escape
      When query
        """
        SELECT variant_get(parse_json('"\u00e9"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | é      |

    Scenario Outline: Edge case (to_json): <case>
      When query
        """
        SELECT to_json(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | json                              | result                          |
        | parse_json empty object                   | '{}'                              | {}                              |
        | parse_json empty array                    | '[]'                              | []                              |
        | parse_json heterogeneous nested structure | '{"a":[1,"two",null,{"b":true}]}' | {"a":[1,"two",null,{"b":true}]} |

    Scenario Outline: Edge case (variant_get): <case>
      When query
        """
        SELECT variant_get(parse_json(<json>), <path>, 'int') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | json                         | path        | result |
        | parse_json deeply nested           | '{"a":{"b":{"c":{"d":42}}}}' | '$.a.b.c.d' | 42     |
        | parse_json whitespace around value | '   123   '                  | '$'         | 123    |

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

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: strict parse_json of a non-null literal yields a non-nullable variant
      When query
        """
        SELECT parse_json('{"a":1}') AS result
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = false)
        """

    Scenario: strict parse_json of a nullable column stays nullable
      When query
        """
        SELECT parse_json(c) AS result FROM VALUES ('{"a":1}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = true)
        """

    @sail-bug
    Scenario: strict parse_json of a non-null column yields a non-nullable variant
      When query
        """
        SELECT parse_json(CONCAT('{"n":', CAST(id AS STRING), '}')) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = false)
        """
