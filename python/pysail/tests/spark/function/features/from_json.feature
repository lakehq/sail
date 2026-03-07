Feature: from_json converts json strings to spark nested types

  Rule: basic parsing of struct, maps, or arrays
    Scenario: from_json basic struct
      When query
        """
        SELECT from_json('{"a": 1, "b": 2}', 'struct<a: int, b: int>') AS result
        """
      Then query result
        | result         |
        | {1, 2}  |

    Scenario: from_json basic map
      When query
        """
        SELECT from_json('{"a": 1, "b": 2}', 'map<string, int>') AS result
        """
      Then query result
        | result         |
        | {a -> 1, b -> 2}  |

    Scenario: from_json basic array
      When query
        """
        SELECT from_json('[1, 2, 3]', 'array<int>') AS result
        """
      Then query result
        | result     |
        | [1, 2, 3]  |

  Rule: Slightly more complex parsing
    Scenario: from_json multiple rows
      When query
        """
        with rows as (
            select '{"a": 1, "b": 2}' as json
            union all
            select '{"a": 1, "b": 2}'
        )

        select from_json(json, 'struct<a: int, b: int>') as result
        from rows
        """
      Then query result
        | result |
        | {1, 2}  |
        | {1, 2}  |

  Rule: nested structs
    Scenario: from_json nested objects
      When query
        """
        SELECT from_json('{"a": "a", "b": [{"c": 1}]}', 'struct<a: string, b: array<map<string, int>>>') AS result
        """
      Then query result
        | result         |
        | {a, [{c -> 1}]}  |

    Scenario: from_json nulls
      When query
        """
        SELECT from_json('{"a": null, "b": null}', 'struct<a: int, b: int>') AS result
        """
      Then query result
        | result         |
        | {a, [{c -> 1}]}  |

    Scenario: from_json schema wrapped struct in array
        plain structs can be wrapped in an array if the schema
        specifies it
      When query
        """
        SELECT from_json('{"a": 1, "b": 2, "c": 3}', 'array<struct<a: int, b: int, c: int>>') AS result
        """
      Then query result
        | result     |
        | [{1, 2, 3}]  |

  Rule: parsing all non-nested types
    Scenario: from_json all types
      When query
      """
      select
        from_json(
          '{
              "decimal": 1.0,
              "float": 2.0,
              "string": "string"
          }',
          'struct<decimal: decimal, float: float, string: string>'
        ) as result
      """
      Then query result
        | result |
        | {1.0, 2.0, string} |

  Rule: invalid json

  Rule: invalid schema

  Rule: invalid options
