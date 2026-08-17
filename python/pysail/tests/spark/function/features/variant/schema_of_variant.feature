@spark-4
Feature: schema_of_variant

  Rule: Primitive types

    Scenario Outline: Primitive: <case>
      When query
        """
        SELECT schema_of_variant(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | json      | result  |
        | schema_of_variant integer | '42'      | BIGINT  |
        | schema_of_variant string  | '"hello"' | STRING  |
        | schema_of_variant boolean | 'true'    | BOOLEAN |

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

    Scenario Outline: Complex: <case>
      When query
        """
        SELECT schema_of_variant(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | json                                    | result                                             |
        | schema_of_variant simple object               | '{"a":1}'                               | OBJECT<a: BIGINT>                                  |
        | schema_of_variant array of integers           | '[1,2,3]'                               | ARRAY<BIGINT>                                      |
        | schema_of_variant empty object                | '{}'                                    | OBJECT<>                                           |
        | schema_of_variant empty array                 | '[]'                                    | ARRAY<VOID>                                        |
        | schema_of_variant nested object               | '{"a":{"b":1}}'                         | OBJECT<a: OBJECT<b: BIGINT>>                       |
        | schema_of_variant mixed array                 | '[1, "hello", true]'                    | ARRAY<VARIANT>                                     |
        | schema_of_variant object with multiple fields | '{"name":"sail","age":5,"active":true}' | OBJECT<active: BOOLEAN, age: BIGINT, name: STRING> |

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

    Scenario: schema_of_variant merges object fields with decimal types
      When query
        """
        SELECT schema_of_variant(
          to_variant_object(
            array(
              map('a', CAST(1.23 AS DECIMAL(3,2))),
              map('b', CAST(4.56 AS DECIMAL(3,2)))
            )
          )
        ) AS result
        """
      Then query result
        | result                                          |
        | ARRAY<OBJECT<a: DECIMAL(3,2), b: DECIMAL(3,2)>> |

    Scenario Outline: Nested: <case>
      When query
        """
        SELECT schema_of_variant(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | json                        | result                                             |
        | schema_of_variant array of objects                     | '[{"a":1},{"a":2}]'         | ARRAY<OBJECT<a: BIGINT>>                           |
        | schema_of_variant array of mixed objects merges fields | '[{"a":1},{"b":2}]'         | ARRAY<OBJECT<a: BIGINT, b: BIGINT>>                |
        | schema_of_variant object with array value              | '{"a":[1,2,3]}'             | OBJECT<a: ARRAY<BIGINT>>                           |
        | schema_of_variant object with nested array             | '{"a":[[1],[2]]}'           | OBJECT<a: ARRAY<ARRAY<BIGINT>>>                    |
        | schema_of_variant deeply nested object                 | '{"a":{"b":{"c":{"d":1}}}}' | OBJECT<a: OBJECT<b: OBJECT<c: OBJECT<d: BIGINT>>>> |
        | schema_of_variant array of array of objects            | '[[{"x":1}]]'               | ARRAY<ARRAY<OBJECT<x: BIGINT>>>                    |
        | schema_of_variant array of empty arrays                | '[[],[]]'                   | ARRAY<ARRAY<VOID>>                                 |
        | schema_of_variant array of empty objects               | '[{},{}]'                   | ARRAY<OBJECT<>>                                    |
        | schema_of_variant object with empty array              | '{"a":[]}'                  | OBJECT<a: ARRAY<VOID>>                             |
        | schema_of_variant object with empty object             | '{"a":{}}'                  | OBJECT<a: OBJECT<>>                                |

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

    Scenario Outline: Null element: <case>
      When query
        """
        SELECT schema_of_variant(parse_json(<json>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | json                       | result                     |
        | schema_of_variant array with null element              | '[null]'                   | ARRAY<VOID>                |
        | schema_of_variant array with null and int merges type  | '[null, 1]'                | ARRAY<BIGINT>              |
        | schema_of_variant object with null value               | '{"a":null}'               | OBJECT<a: VOID>            |
        | schema_of_variant object with null and non-null fields | '{"a":null,"b":1}'         | OBJECT<a: VOID, b: BIGINT> |
        | schema_of_variant nested null array                    | '[[null]]'                 | ARRAY<ARRAY<VOID>>         |
        | schema_of_variant array with objects and null merges   | '[{"a":1}, null, {"a":2}]' | ARRAY<OBJECT<a: BIGINT>>   |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to schema_of_variant yields the schema Spark declares
      When query
        """
        SELECT schema_of_variant(parse_json('null')) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to schema_of_variant stays nullable
      When query
        """
        SELECT schema_of_variant(c) AS result FROM VALUES (parse_json('null')), (CAST(NULL AS VARIANT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
