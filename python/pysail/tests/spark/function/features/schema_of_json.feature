Feature: to_json function converts complex types to JSON strings

  Rule: Infer basic types
    Scenario: schema of json basic
      When query
        """
        SELECT schema_of_json('{"a": 1, "b": 1.0, "c": "s", "d": true}') AS result
        """
      Then query result
        | result         |
        | STRUCT<a: BIGINT, b: DOUBLE, c: STRING, d: BOOL>  |

  Rule: Infer nested structures
    Scenario: schema of json nested
      When query
        """
        SELECT schema_of_json('[{"a": {"a": 1}}]') AS result
        """
      Then query result
        | result         |
        | ARRAY<STRUCT<a: STRUCT<a: BIGINT>>>  |
