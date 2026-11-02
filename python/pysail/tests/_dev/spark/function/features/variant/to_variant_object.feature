@spark-4
Feature: to_variant_object

  Rule: Struct input

    Scenario Outline: Struct input: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | input                                  | result                  |
        | to_variant_object simple struct | named_struct('a', 1, 'b', 'hello')     | {"a":1,"b":"hello"}     |
        | to_variant_object single field  | named_struct('x', 42)                  | {"x":42}                |
        | to_variant_object with boolean  | named_struct('flag', true, 'count', 5) | {"count":5,"flag":true} |

    # cast_to_variant from parquet-variant-compute omits NULL struct fields
    @sail-bug
    Scenario: to_variant_object with null field
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', 1, 'b', CAST(NULL AS STRING)))) AS result
        """
      Then query result
        | result           |
        | {"a":1,"b":null} |

    Scenario: to_variant_object NULL input returns NULL
      When query
        """
        SELECT to_variant_object(CAST(NULL AS STRUCT<a: INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Array and map input

    Scenario Outline: Array and map input: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | input                       | result        |
        | to_variant_object with array input      | array(1, 2, 3)              | [1,2,3]       |
        | to_variant_object with map input        | map('x', 1, 'y', 2)         | {"x":1,"y":2} |
        | to_variant_object with array of structs | array(named_struct('a', 1)) | [{"a":1}]     |

  Rule: Nested structs

    Scenario Outline: Nested struct: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | input                                                       | result               |
        | to_variant_object nested struct        | named_struct('a', named_struct('b', 1))                     | {"a":{"b":1}}        |
        | to_variant_object deeply nested struct | named_struct('a', named_struct('b', named_struct('c', 42))) | {"a":{"b":{"c":42}}} |

  Rule: Struct with various types

    Scenario Outline: Struct field type: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | input                             | result          |
        | to_variant_object struct with double      | named_struct('x', 3.14)           | {"x":3.14}      |
        | to_variant_object struct with array field | named_struct('arr', array(1,2,3)) | {"arr":[1,2,3]} |
        | to_variant_object struct with map field   | named_struct('m', map('k', 'v'))  | {"m":{"k":"v"}} |

    # parquet-variant omits NULL struct fields
    @sail-bug
    Scenario: to_variant_object struct all null fields
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING)))) AS result
        """
      Then query result
        | result              |
        | {"a":null,"b":null} |

  Rule: Map edge cases

    Scenario Outline: Map edge case: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | input                       | result              |
        | to_variant_object map single entry | map('key', 'value')         | {"key":"value"}     |
        | to_variant_object map 3 keys       | map('a', 1, 'b', 2, 'c', 3) | {"a":1,"b":2,"c":3} |

    # parquet-variant omits NULL map values
    @sail-bug
    Scenario: to_variant_object map with null value
      When query
        """
        SELECT to_json(to_variant_object(map('a', CAST(NULL AS INT), 'b', 2))) AS result
        """
      Then query result
        | result           |
        | {"a":null,"b":2} |

  Rule: Nested arrays and collections

    Scenario Outline: Nested collection: <case>
      When query
        """
        SELECT to_json(to_variant_object(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | input                           | result            |
        | to_variant_object array of arrays | array(array(1,2), array(3,4))   | [[1,2],[3,4]]     |
        | to_variant_object array of maps   | array(map('a', 1), map('b', 2)) | [{"a":1},{"b":2}] |

  Rule: Multi-row

    Scenario: to_variant_object multi-row
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', id))) AS result
        FROM VALUES (1), (2), (3) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | result  |
        | {"a":1} |
        | {"a":2} |
        | {"a":3} |

  Rule: Error cases

    Scenario Outline: Rejects scalar: <case>
      When query
        """
        SELECT to_variant_object(<input>) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT)

      Examples:
        | case                                    | input   |
        | to_variant_object rejects primitive int | 42      |
        | to_variant_object rejects string        | 'hello' |

    Scenario Outline: Rejects empty collection: <case>
      When query
        """
        SELECT to_variant_object(<input>) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT|VOID)

      Examples:
        | case                                  | input   |
        | to_variant_object rejects empty array | array() |
        | to_variant_object rejects empty map   | map()   |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to to_variant_object yields the schema Spark declares
      When query
        """
        SELECT to_variant_object(named_struct('a', 1, 'b', 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = false)
        """
