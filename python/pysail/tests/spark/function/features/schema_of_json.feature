Feature: schema_of_json function detects the schema of a literal json string

  Rule: Infer basic types
    Scenario: schema of json basic
      When query
        """
        SELECT schema_of_json('{"a": 1, "b": 1.0, "c": "s", "d": true}') AS result
        """
      Then query result
        | result         |
        | STRUCT<a: BIGINT, b: DOUBLE, c: STRING, d: BOOL>  |

    Scenario: schema of json empty list
      When query
        """
        SELECT schema_of_json('{"a": []}') AS result
        """
      Then query result
        | result         |
        | STRUCT<a: ARRAY<STRING>>  |

    Scenario: schema of json scalar
      When query
        """
        SELECT schema_of_json('1') AS result
        """
      Then query result
        | result         |
        | BIGINT  |

    Scenario: schema of json empty string
      When query
        """
        SELECT schema_of_json('') AS result
        """
      Then query result
        | result         |
        | STRING  |

  Rule: Infer nested structures
    Scenario: schema of json nested
      When query
        """
        SELECT schema_of_json('[{"a": {"a": 1}}]') AS result
        """
      Then query result
        | result         |
        | ARRAY<STRUCT<a: STRUCT<a: BIGINT>>>  |

  Rule: Exceptions handled appropriately
    Scenario: schema of json not scalar
      When query
        """
        with rows as (
            select '{"a": 1}' as json
            union all
            select '{"a": 1}' as json
        )

        select schema_of_json(json) as result
        from rows
        """
      Then query error Expected a literal value

    Scenario: schema of json null input
      When query
        """
        SELECT schema_of_json(null) AS result
        """
      Then query error .*found invalid arg types: \[Null\]

    Scenario: schema of json leading zeros
        Testing that this fails
      When query
        """
        SELECT schema_of_json('{"01b": 01, "b": "01"}', map('allowNumericLeadingZeros', 'true')) AS result
        """
      Then query error .*doesn't support option allowNumericLeadingZeros
