Feature: from_json converts json strings to spark nested types

  Rule: basic parsing functionality of struct, maps, or arrays
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

  Rule: parsing of more complex schemas works
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
        | {NULL, NULL}  |

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

  Rule: parsing all types
    Scenario: from_json decimal
      When query
      """
      select from_json('{"d10_0": 1.0, "d10_2": 1.1}', 'struct<d10_0: decimal, d10_2: decimal(10, 2)>') as result
      """
      Then query result
        | result |
        | {1, 1.10} |

    Scenario: from_json float
      When query
      """
      select from_json('{"float": 2.0}', 'struct<float: float>') as result
      """
      Then query result
        | result |
        | {2.0} |

    Scenario: from_json timestamp
      When query
      """
      select from_json('{"ts": "2026-01-01T01:01:01"}', 'struct<ts: timestamp>') as result
      """
      Then query result
        | result |
        | {2026-01-01 01:01:01} |

    Scenario: from_json date as timestamp
      When query
      """
      select from_json('{"ts": "2026-01-01"}', 'struct<ts: timestamp>', map("timestampFormat", "yyyy-MM-dd")) as result
      """
      Then query result
        | result |
        | {2026-01-01 00:00:00} |

  Rule: invalid schema

  Rule: invalid options
