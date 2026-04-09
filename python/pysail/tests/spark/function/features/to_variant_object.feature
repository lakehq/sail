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
