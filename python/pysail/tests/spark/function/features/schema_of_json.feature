@schema_of_json
Feature: schema_of_json() returns the schema of a JSON string as DDL

  Rule: Basic struct inference

    Scenario: simple types
      When query
        """
        SELECT schema_of_json('{"name":"Alice","age":30,"active":true}') AS result
        """
      Then query result
        | result                                            |
        | STRUCT<active: BOOLEAN, age: BIGINT, name: STRING> |

    Scenario: numeric types integer and double
      When query
        """
        SELECT schema_of_json('{"id":100,"price":29.99,"count":5}') AS result
        """
      Then query result
        | result                                           |
        | STRUCT<count: BIGINT, id: BIGINT, price: DOUBLE> |

    Scenario: negative integer
      When query
        """
        SELECT schema_of_json('{"v":-42}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: negative float
      When query
        """
        SELECT schema_of_json('{"v":-3.14}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: DOUBLE>    |

    Scenario: scientific notation
      When query
        """
        SELECT schema_of_json('{"v":1.5e10}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: DOUBLE>    |

    Scenario: zero
      When query
        """
        SELECT schema_of_json('{"v":0}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: large integer
      When query
        """
        SELECT schema_of_json('{"v":9999999999999}') AS result
        """
      Then query result
        | result               |
        | STRUCT<v: BIGINT>    |

    Scenario: string containing numbers
      When query
        """
        SELECT schema_of_json('{"id":"123","value":"456.78"}') AS result
        """
      Then query result
        | result                               |
        | STRUCT<id: STRING, value: STRING>    |

    Scenario: boolean true and false
      When query
        """
        SELECT schema_of_json('{"a":true,"b":false}') AS result
        """
      Then query result
        | result                              |
        | STRUCT<a: BOOLEAN, b: BOOLEAN>      |

  Rule: Nested structures

    Scenario: nested object
      When query
        """
        SELECT schema_of_json('{"user":{"name":"Bob","age":25},"active":true}') AS result
        """
      Then query result
        | result                                                        |
        | STRUCT<active: BOOLEAN, user: STRUCT<age: BIGINT, name: STRING>> |

    Scenario: deeply nested structure
      When query
        """
        SELECT schema_of_json('{"a":{"b":{"c":{"d":1}}}}') AS result
        """
      Then query result
        | result                                               |
        | STRUCT<a: STRUCT<b: STRUCT<c: STRUCT<d: BIGINT>>>>  |

    Scenario: array and nested object
      When query
        """
        SELECT schema_of_json('{"data":[1,2,3],"meta":{"count":3}}') AS result
        """
      Then query result
        | result                                                  |
        | STRUCT<data: ARRAY<BIGINT>, meta: STRUCT<count: BIGINT>> |

  Rule: Array type inference

    Scenario: array of primitives
      When query
        """
        SELECT schema_of_json('{"tags":["a","b","c"],"count":3}') AS result
        """
      Then query result
        | result                                      |
        | STRUCT<count: BIGINT, tags: ARRAY<STRING>>  |

    Scenario: array of objects with same schema
      When query
        """
        SELECT schema_of_json('{"items":[{"id":1,"name":"x"},{"id":2,"name":"y"}]}') AS result
        """
      Then query result
        | result                                                    |
        | STRUCT<items: ARRAY<STRUCT<id: BIGINT, name: STRING>>>   |

    Scenario: single element array
      When query
        """
        SELECT schema_of_json('{"v":[42]}') AS result
        """
      Then query result
        | result                    |
        | STRUCT<v: ARRAY<BIGINT>>  |

    Scenario: array of arrays
      When query
        """
        SELECT schema_of_json('{"v":[[1,2],[3,4]]}') AS result
        """
      Then query result
        | result                            |
        | STRUCT<v: ARRAY<ARRAY<BIGINT>>>   |

  Rule: Array supertype inference (mixed types)

    Scenario: int and string and bool in array
      When query
        """
        SELECT schema_of_json('{"v":[1, "two", true]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: int and double in array
      When query
        """
        SELECT schema_of_json('{"v":[1, 2.5]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<DOUBLE>>   |

    Scenario: int and null in array
      When query
        """
        SELECT schema_of_json('{"v":[1, null]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<BIGINT>>   |

    Scenario: bool and null in array
      When query
        """
        SELECT schema_of_json('{"v":[true, null]}') AS result
        """
      Then query result
        | result                      |
        | STRUCT<v: ARRAY<BOOLEAN>>   |

    Scenario: all null array
      When query
        """
        SELECT schema_of_json('{"v":[null, null]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: double and string in array
      When query
        """
        SELECT schema_of_json('{"v":[1.5, "hi"]}') AS result
        """
      Then query result
        | result                     |
        | STRUCT<v: ARRAY<STRING>>   |

    Scenario: nested arrays with mixed types
      When query
        """
        SELECT schema_of_json('{"v":[[1],["a"]]}') AS result
        """
      Then query result
        | result                              |
        | STRUCT<v: ARRAY<ARRAY<STRING>>>     |

    Scenario: object and null in array
      When query
        """
        SELECT schema_of_json('{"v":[{"a":1}, null]}') AS result
        """
      Then query result
        | result                                    |
        | STRUCT<v: ARRAY<STRUCT<a: BIGINT>>>       |

    Scenario: array of objects with different fields merges schemas
      When query
        """
        SELECT schema_of_json('[{"a":1},{"a":2,"b":"x"}]') AS result
        """
      Then query result
        | result                                |
        | ARRAY<STRUCT<a: BIGINT, b: STRING>>   |

    Scenario: array of objects with mixed field types
      When query
        """
        SELECT schema_of_json('{"v":[{"a":1},{"a":"x"}]}') AS result
        """
      Then query result
        | result                                |
        | STRUCT<v: ARRAY<STRUCT<a: STRING>>>   |

  Rule: Null handling

    Scenario: null field in struct
      When query
        """
        SELECT schema_of_json('{"name":"Alice","age":null}') AS result
        """
      Then query result
        | result                                  |
        | STRUCT<age: STRING, name: STRING>       |

    Scenario: all null fields
      When query
        """
        SELECT schema_of_json('{"a":null,"b":null}') AS result
        """
      Then query result
        | result                          |
        | STRUCT<a: STRING, b: STRING>    |

    Scenario: top-level null
      When query
        """
        SELECT schema_of_json('null') AS result
        """
      Then query result
        | result |
        | STRING |

  Rule: Empty structures

    Scenario: empty object
      When query
        """
        SELECT schema_of_json('{}') AS result
        """
      Then query result
        | result     |
        | STRUCT<>   |

    Scenario: empty array
      When query
        """
        SELECT schema_of_json('{"items":[]}') AS result
        """
      Then query result
        | result                       |
        | STRUCT<items: ARRAY<STRING>> |

    Scenario: top-level empty array
      When query
        """
        SELECT schema_of_json('[]') AS result
        """
      Then query result
        | result          |
        | ARRAY<STRING>   |

  Rule: Top-level types

    Scenario: top-level array of ints
      When query
        """
        SELECT schema_of_json('[1,2,3]') AS result
        """
      Then query result
        | result          |
        | ARRAY<BIGINT>   |

    Scenario: top-level string
      When query
        """
        SELECT schema_of_json('"hello"') AS result
        """
      Then query result
        | result |
        | STRING |

    Scenario: top-level integer
      When query
        """
        SELECT schema_of_json('42') AS result
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: top-level boolean
      When query
        """
        SELECT schema_of_json('true') AS result
        """
      Then query result
        | result  |
        | BOOLEAN |

    Scenario: top-level double
      When query
        """
        SELECT schema_of_json('3.14') AS result
        """
      Then query result
        | result |
        | DOUBLE |

  Rule: Multiple rows

    Scenario: multiple rows via UNION ALL
      When query
        """
        SELECT result FROM (
          SELECT schema_of_json('{"name":"Alice","age":30}') AS result
          UNION ALL
          SELECT schema_of_json('{"x":1.5,"y":true}') AS result
        ) ORDER BY result
        """
      Then query result ordered
        | result                                |
        | STRUCT<age: BIGINT, name: STRING>     |
        | STRUCT<x: DOUBLE, y: BOOLEAN>         |

  Rule: Error cases

    Scenario: rejects non-foldable column input
      When query
        """
        SELECT schema_of_json(json_col) AS result
        FROM VALUES ('{"name":"Alice"}') AS t(json_col)
        """
      Then query error NON_FOLDABLE_INPUT
