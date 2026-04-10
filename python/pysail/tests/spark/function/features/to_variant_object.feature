@to_variant_object @spark-4
Feature: to_variant_object

  Rule: Struct input

    Scenario: to_variant_object simple struct
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', 1, 'b', 'hello'))) AS result
        """
      Then query result
        | result              |
        | {"a":1,"b":"hello"} |

    Scenario: to_variant_object single field
      When query
        """
        SELECT to_json(to_variant_object(named_struct('x', 42))) AS result
        """
      Then query result
        | result   |
        | {"x":42} |

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

    Scenario: to_variant_object with boolean
      When query
        """
        SELECT to_json(to_variant_object(named_struct('flag', true, 'count', 5))) AS result
        """
      Then query result
        | result                  |
        | {"count":5,"flag":true} |

    Scenario: to_variant_object NULL input returns NULL
      When query
        """
        SELECT to_variant_object(CAST(NULL AS STRUCT<a: INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Array and map input

    Scenario: to_variant_object with array input
      When query
        """
        SELECT to_json(to_variant_object(array(1, 2, 3))) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

    Scenario: to_variant_object with map input
      When query
        """
        SELECT to_json(to_variant_object(map('x', 1, 'y', 2))) AS result
        """
      Then query result
        | result        |
        | {"x":1,"y":2} |

    Scenario: to_variant_object with array of structs
      When query
        """
        SELECT to_json(to_variant_object(array(named_struct('a', 1)))) AS result
        """
      Then query result
        | result    |
        | [{"a":1}] |

  Rule: Nested structs

    Scenario: to_variant_object nested struct
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', named_struct('b', 1)))) AS result
        """
      Then query result
        | result         |
        | {"a":{"b":1}} |

    Scenario: to_variant_object deeply nested struct
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', named_struct('b', named_struct('c', 42))))) AS result
        """
      Then query result
        | result                  |
        | {"a":{"b":{"c":42}}} |

  Rule: Struct with various types

    Scenario: to_variant_object struct with double
      When query
        """
        SELECT to_json(to_variant_object(named_struct('x', 3.14))) AS result
        """
      Then query result
        | result      |
        | {"x":3.14} |

    Scenario: to_variant_object struct with array field
      When query
        """
        SELECT to_json(to_variant_object(named_struct('arr', array(1,2,3)))) AS result
        """
      Then query result
        | result            |
        | {"arr":[1,2,3]} |

    Scenario: to_variant_object struct with map field
      When query
        """
        SELECT to_json(to_variant_object(named_struct('m', map('k', 'v')))) AS result
        """
      Then query result
        | result           |
        | {"m":{"k":"v"}} |

    # parquet-variant omits NULL struct fields
    @sail-bug
    Scenario: to_variant_object struct all null fields
      When query
        """
        SELECT to_json(to_variant_object(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING)))) AS result
        """
      Then query result
        | result               |
        | {"a":null,"b":null} |

  Rule: Map edge cases

    Scenario: to_variant_object map single entry
      When query
        """
        SELECT to_json(to_variant_object(map('key', 'value'))) AS result
        """
      Then query result
        | result            |
        | {"key":"value"} |

    # parquet-variant omits NULL map values
    @sail-bug
    Scenario: to_variant_object map with null value
      When query
        """
        SELECT to_json(to_variant_object(map('a', CAST(NULL AS INT), 'b', 2))) AS result
        """
      Then query result
        | result              |
        | {"a":null,"b":2} |

    Scenario: to_variant_object map 3 keys
      When query
        """
        SELECT to_json(to_variant_object(map('a', 1, 'b', 2, 'c', 3))) AS result
        """
      Then query result
        | result                |
        | {"a":1,"b":2,"c":3} |

  Rule: Nested arrays and collections

    Scenario: to_variant_object array of arrays
      When query
        """
        SELECT to_json(to_variant_object(array(array(1,2), array(3,4)))) AS result
        """
      Then query result
        | result          |
        | [[1,2],[3,4]] |

    Scenario: to_variant_object array of maps
      When query
        """
        SELECT to_json(to_variant_object(array(map('a', 1), map('b', 2)))) AS result
        """
      Then query result
        | result                |
        | [{"a":1},{"b":2}] |

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

    Scenario: to_variant_object rejects primitive int
      When query
        """
        SELECT to_variant_object(42) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT)

    Scenario: to_variant_object rejects string
      When query
        """
        SELECT to_variant_object('hello') AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT)

    Scenario: to_variant_object rejects empty array
      When query
        """
        SELECT to_variant_object(array()) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT|VOID)

    Scenario: to_variant_object rejects empty map
      When query
        """
        SELECT to_variant_object(map()) AS result
        """
      Then query error (DATATYPE_MISMATCH|cannot cast|VARIANT|VOID)
