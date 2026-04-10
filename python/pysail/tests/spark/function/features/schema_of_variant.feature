@schema_of_variant @spark-4
Feature: schema_of_variant

  Rule: Primitive types

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
    @sail-bug
    Scenario: schema_of_variant double
      When query
        """
        SELECT schema_of_variant(parse_json('3.14')) AS result
        """
      Then query result
        | result       |
        | DECIMAL(3,2) |

    Scenario: schema_of_variant null
      When query
        """
        SELECT schema_of_variant(parse_json('null')) AS result
        """
      Then query result
        | result |
        | VOID   |

  Rule: Complex types

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
        | result        |
        | ARRAY<BIGINT> |

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
        | result      |
        | ARRAY<VOID> |

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
        | result         |
        | ARRAY<VARIANT> |

    Scenario: schema_of_variant object with multiple fields
      When query
        """
        SELECT schema_of_variant(parse_json('{"name":"sail","age":5,"active":true}')) AS result
        """
      Then query result
        | result                                             |
        | OBJECT<active: BOOLEAN, age: BIGINT, name: STRING> |

  Rule: Recursive and nested types

    Scenario: schema_of_variant nested array of arrays
      When query
        """
        SELECT schema_of_variant(parse_json('[[1,2],[3,4]]')) AS result
        """
      Then query result
        | result               |
        | ARRAY<ARRAY<BIGINT>> |

    Scenario: schema_of_variant 3-level nested array
      When query
        """
        SELECT schema_of_variant(parse_json('[[[1]]]')) AS result
        """
      Then query result
        | result                      |
        | ARRAY<ARRAY<ARRAY<BIGINT>>> |

    Scenario: schema_of_variant array of objects
      When query
        """
        SELECT schema_of_variant(parse_json('[{"a":1},{"a":2}]')) AS result
        """
      Then query result
        | result                 |
        | ARRAY<OBJECT<a: BIGINT>> |

    Scenario: schema_of_variant array of mixed objects merges fields
      When query
        """
        SELECT schema_of_variant(parse_json('[{"a":1},{"b":2}]')) AS result
        """
      Then query result
        | result                              |
        | ARRAY<OBJECT<a: BIGINT, b: BIGINT>> |

    Scenario: schema_of_variant object with array value
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":[1,2,3]}')) AS result
        """
      Then query result
        | result                  |
        | OBJECT<a: ARRAY<BIGINT>> |

    Scenario: schema_of_variant object with nested array
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":[[1],[2]]}')) AS result
        """
      Then query result
        | result                         |
        | OBJECT<a: ARRAY<ARRAY<BIGINT>>> |

    Scenario: schema_of_variant deeply nested object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":{"b":{"c":{"d":1}}}}')) AS result
        """
      Then query result
        | result                                              |
        | OBJECT<a: OBJECT<b: OBJECT<c: OBJECT<d: BIGINT>>>> |

    Scenario: schema_of_variant array of array of objects
      When query
        """
        SELECT schema_of_variant(parse_json('[[{"x":1}]]')) AS result
        """
      Then query result
        | result                          |
        | ARRAY<ARRAY<OBJECT<x: BIGINT>>> |

    Scenario: schema_of_variant array of empty arrays
      When query
        """
        SELECT schema_of_variant(parse_json('[[],[]]')) AS result
        """
      Then query result
        | result              |
        | ARRAY<ARRAY<VOID>> |

    Scenario: schema_of_variant array of empty objects
      When query
        """
        SELECT schema_of_variant(parse_json('[{},{}]')) AS result
        """
      Then query result
        | result            |
        | ARRAY<OBJECT<>> |

    Scenario: schema_of_variant object with empty array
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":[]}')) AS result
        """
      Then query result
        | result                 |
        | OBJECT<a: ARRAY<VOID>> |

    Scenario: schema_of_variant object with empty object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":{}}')) AS result
        """
      Then query result
        | result               |
        | OBJECT<a: OBJECT<>> |

  Rule: NULL edge cases

    Scenario: schema_of_variant SQL NULL returns NULL
      When query
        """
        SELECT schema_of_variant(parse_json(NULL)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: schema_of_variant CAST NULL AS VARIANT returns NULL
      When query
        """
        SELECT schema_of_variant(CAST(NULL AS VARIANT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: schema_of_variant array with null element
      When query
        """
        SELECT schema_of_variant(parse_json('[null]')) AS result
        """
      Then query result
        | result      |
        | ARRAY<VOID> |

    Scenario: schema_of_variant array with null and int merges type
      When query
        """
        SELECT schema_of_variant(parse_json('[null, 1]')) AS result
        """
      Then query result
        | result        |
        | ARRAY<BIGINT> |

    Scenario: schema_of_variant object with null value
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":null}')) AS result
        """
      Then query result
        | result          |
        | OBJECT<a: VOID> |

    Scenario: schema_of_variant object with null and non-null fields
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":null,"b":1}')) AS result
        """
      Then query result
        | result                      |
        | OBJECT<a: VOID, b: BIGINT> |

    Scenario: schema_of_variant nested null array
      When query
        """
        SELECT schema_of_variant(parse_json('[[null]]')) AS result
        """
      Then query result
        | result              |
        | ARRAY<ARRAY<VOID>> |

    Scenario: schema_of_variant array with objects and null merges
      When query
        """
        SELECT schema_of_variant(parse_json('[{"a":1}, null, {"a":2}]')) AS result
        """
      Then query result
        | result                 |
        | ARRAY<OBJECT<a: BIGINT>> |
