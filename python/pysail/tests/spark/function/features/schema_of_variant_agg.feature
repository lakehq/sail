@schema_of_variant_agg @spark-4
Feature: schema_of_variant_agg

  Rule: Uniform types

    Scenario: schema_of_variant_agg uniform types
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), ('2'), ('3') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |

  Rule: Mixed types

    Scenario: schema_of_variant_agg mixed scalars
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), ('"hello"'), ('true') AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

  Rule: Object field merging

    Scenario: schema_of_variant_agg objects merge fields
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('{"a":2,"b":"x"}') AS t(v)
        """
      Then query result
        | result                       |
        | OBJECT<a: BIGINT, b: STRING> |

  Rule: NULL handling

    Scenario: schema_of_variant_agg with nulls
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('42'), ('null'), ('99') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |
