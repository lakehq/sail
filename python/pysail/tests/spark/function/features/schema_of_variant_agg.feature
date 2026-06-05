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

  Rule: More uniform types

    Scenario: schema_of_variant_agg all strings
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('"a"'), ('"b"'), ('"c"') AS t(v)
        """
      Then query result
        | result |
        | STRING |

    Scenario: schema_of_variant_agg all booleans
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('true'), ('false'), ('true') AS t(v)
        """
      Then query result
        | result  |
        | BOOLEAN |

    Scenario: schema_of_variant_agg all arrays same type
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('[1,2]'), ('[3,4,5]') AS t(v)
        """
      Then query result
        | result        |
        | ARRAY<BIGINT> |

    Scenario: schema_of_variant_agg all objects same fields
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('{"a":2}') AS t(v)
        """
      Then query result
        | result            |
        | OBJECT<a: BIGINT> |

  Rule: More mixed types

    Scenario: schema_of_variant_agg int and string
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), ('"hello"') AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

    Scenario: schema_of_variant_agg int and array
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), ('[1,2]') AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

    Scenario: schema_of_variant_agg object and array
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('[1,2]') AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

    Scenario: schema_of_variant_agg int and bool
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), ('true') AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

  Rule: NULL and VOID handling

    Scenario: schema_of_variant_agg with json nulls absorbed
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('42'), ('null'), ('99') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: schema_of_variant_agg all json nulls
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('null'), ('null') AS t(v)
        """
      Then query result
        | result |
        | VOID   |

    Scenario: schema_of_variant_agg null and object
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('null'), ('{"a":1}') AS t(v)
        """
      Then query result
        | result            |
        | OBJECT<a: BIGINT> |

    Scenario: schema_of_variant_agg SQL NULL rows skipped
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('1'), (CAST(NULL AS STRING)), ('2') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: schema_of_variant_agg all SQL NULL
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(v)
        """
      Then query result
        | result |
        | VOID   |

  Rule: Object merging advanced

    Scenario: schema_of_variant_agg objects different fields
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('{"b":"x"}') AS t(v)
        """
      Then query result
        | result                       |
        | OBJECT<a: BIGINT, b: STRING> |

    Scenario: schema_of_variant_agg objects overlapping fields different types
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('{"a":"x"}') AS t(v)
        """
      Then query result
        | result              |
        | OBJECT<a: VARIANT> |

    Scenario: schema_of_variant_agg objects 3 rows merge
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1}'), ('{"b":2}'), ('{"c":3}') AS t(v)
        """
      Then query result
        | result                                  |
        | OBJECT<a: BIGINT, b: BIGINT, c: BIGINT> |

    Scenario: schema_of_variant_agg objects nested merge
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":{"x":1}}'), ('{"a":{"y":2}}') AS t(v)
        """
      Then query result
        | result                                   |
        | OBJECT<a: OBJECT<x: BIGINT, y: BIGINT>> |

    Scenario: schema_of_variant_agg deeply nested objects
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":{"b":1}}'), ('{"a":{"c":2}}') AS t(v)
        """
      Then query result
        | result                                   |
        | OBJECT<a: OBJECT<b: BIGINT, c: BIGINT>> |

  Rule: Array merging

    Scenario: schema_of_variant_agg arrays different element types
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('[1,2]'), ('["a"]') AS t(v)
        """
      Then query result
        | result          |
        | ARRAY<VARIANT> |

    Scenario: schema_of_variant_agg array and empty array
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('[1,2]'), ('[]') AS t(v)
        """
      Then query result
        | result        |
        | ARRAY<BIGINT> |

  Rule: Empty objects

    Scenario: schema_of_variant_agg empty objects
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{}'), ('{}') AS t(v)
        """
      Then query result
        | result   |
        | OBJECT<> |

    Scenario: schema_of_variant_agg empty and non-empty object
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{}'), ('{"a":1}') AS t(v)
        """
      Then query result
        | result            |
        | OBJECT<a: BIGINT> |

  Rule: Single row

    Scenario: schema_of_variant_agg single row
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('42') AS t(v)
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: schema_of_variant_agg single row object
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES ('{"a":1,"b":"x"}') AS t(v)
        """
      Then query result
        | result                       |
        | OBJECT<a: BIGINT, b: STRING> |
