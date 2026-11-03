@spark-4
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

  Rule: Numeric schema widening

    Scenario Outline: schema_of_variant_agg widens numeric values: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | values                                                        | result         |
        | decimal and exponent  | ('1.1'), ('1e0'), (CAST(NULL AS STRING))                      | DOUBLE         |
        | normalized decimals   | ('1.01'), ('1.00')                                            | DECIMAL(3,2)   |
        | integer and decimal   | ('null'), ('1'), ('1.0')                                      | DECIMAL(20,0)  |
        | tiny and zero decimal | ('0.000000000000000001'), ('10.1000'), ('-0.0')               | DECIMAL(20,18) |
        | precision overflow    | ('1.1'), ('11111111111111111111111111111111111111')           | DOUBLE         |

    Scenario: schema_of_variant_agg merges cast decimal values
      When query
        """
        SELECT schema_of_variant_agg(CAST(v AS VARIANT)) AS result
        FROM VALUES (1.01), (1.00), (CAST(NULL AS DECIMAL(3,2))) AS t(v)
        """
      Then query result
        | result       |
        | DECIMAL(3,2) |

    Scenario Outline: schema_of_variant_agg widens Variant numeric families: <case>
      When query
        """
        SELECT schema_of_variant_agg(v) AS result FROM (
          SELECT CAST(CAST(1 AS <left>) AS VARIANT) AS v
          UNION ALL
          SELECT CAST(CAST(1.1 AS <right>) AS VARIANT) AS v
        )
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | left         | right        | result |
        | bigint and float | BIGINT       | FLOAT        | FLOAT  |
        | float and double | FLOAT        | DOUBLE       | DOUBLE |
        | float and decimal | FLOAT       | DECIMAL(2,1) | DOUBLE |
        | double and decimal | DOUBLE     | DECIMAL(2,1) | DOUBLE |

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

    Scenario Outline: Uniform: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | values                        | result            |
        | schema_of_variant_agg all strings             | ('"a"'), ('"b"'), ('"c"')     | STRING            |
        | schema_of_variant_agg all booleans            | ('true'), ('false'), ('true') | BOOLEAN           |
        | schema_of_variant_agg all arrays same type    | ('[1,2]'), ('[3,4,5]')        | ARRAY<BIGINT>     |
        | schema_of_variant_agg all objects same fields | ('{"a":1}'), ('{"a":2}')      | OBJECT<a: BIGINT> |

  Rule: More mixed types

    Scenario Outline: Mixed: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result  |
        | VARIANT |

      Examples:
        | case                                   | values                 |
        | schema_of_variant_agg int and string   | ('1'), ('"hello"')     |
        | schema_of_variant_agg int and array    | ('1'), ('[1,2]')       |
        | schema_of_variant_agg object and array | ('{"a":1}'), ('[1,2]') |
        | schema_of_variant_agg int and bool     | ('1'), ('true')        |

  Rule: NULL and VOID handling

    Scenario Outline: NULL and VOID: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | values                                         | result            |
        | schema_of_variant_agg with json nulls absorbed | ('42'), ('null'), ('99')                       | BIGINT            |
        | schema_of_variant_agg all json nulls           | ('null'), ('null')                             | VOID              |
        | schema_of_variant_agg null and object          | ('null'), ('{"a":1}')                          | OBJECT<a: BIGINT> |
        | schema_of_variant_agg SQL NULL rows skipped    | ('1'), (CAST(NULL AS STRING)), ('2')           | BIGINT            |
        | schema_of_variant_agg all SQL NULL             | (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) | VOID              |

  Rule: Object merging advanced

    Scenario Outline: Object merging: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                             | values                                | result                                  |
        | schema_of_variant_agg objects different fields                   | ('{"a":1}'), ('{"b":"x"}')            | OBJECT<a: BIGINT, b: STRING>            |
        | schema_of_variant_agg objects overlapping fields different types | ('{"a":1}'), ('{"a":"x"}')            | OBJECT<a: VARIANT>                      |
        | schema_of_variant_agg objects 3 rows merge                       | ('{"a":1}'), ('{"b":2}'), ('{"c":3}') | OBJECT<a: BIGINT, b: BIGINT, c: BIGINT> |
        | schema_of_variant_agg objects nested merge                       | ('{"a":{"x":1}}'), ('{"a":{"y":2}}')  | OBJECT<a: OBJECT<x: BIGINT, y: BIGINT>> |
        | schema_of_variant_agg deeply nested objects                      | ('{"a":{"b":1}}'), ('{"a":{"c":2}}')  | OBJECT<a: OBJECT<b: BIGINT, c: BIGINT>> |

  Rule: Array merging

    Scenario Outline: Array merging: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | values               | result         |
        | schema_of_variant_agg arrays different element types | ('[1,2]'), ('["a"]') | ARRAY<VARIANT> |
        | schema_of_variant_agg array and empty array          | ('[1,2]'), ('[]')    | ARRAY<BIGINT>  |

  Rule: Empty objects

    Scenario Outline: Empty objects: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | values              | result            |
        | schema_of_variant_agg empty objects              | ('{}'), ('{}')      | OBJECT<>          |
        | schema_of_variant_agg empty and non-empty object | ('{}'), ('{"a":1}') | OBJECT<a: BIGINT> |

  Rule: Single row

    Scenario Outline: Single row: <case>
      When query
        """
        SELECT schema_of_variant_agg(parse_json(v)) AS result
        FROM VALUES <values> AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | values              | result                       |
        | schema_of_variant_agg single row        | ('42')              | BIGINT                       |
        | schema_of_variant_agg single row object | ('{"a":1,"b":"x"}') | OBJECT<a: BIGINT, b: STRING> |
