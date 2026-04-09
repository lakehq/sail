@schema_of_variant @spark-4
Feature: schema_of_variant

  Rule: Primitive types

    Scenario: schema_of_variant integer
      When query
        """
        SELECT schema_of_variant(parse_json('42')) AS result
        """
      Then query result
        | result |
        | BIGINT |

    Scenario: schema_of_variant string
      When query
        """
        SELECT schema_of_variant(parse_json('"hello"')) AS result
        """
      Then query result
        | result |
        | STRING |

    Scenario: schema_of_variant boolean
      When query
        """
        SELECT schema_of_variant(parse_json('true')) AS result
        """
      Then query result
        | result  |
        | BOOLEAN |

    # parquet-variant-json parses 3.14 as f64 (DOUBLE) instead of Decimal like Spark
    @sail-bug
    Scenario: schema_of_variant double
      When query
        """
        SELECT schema_of_variant(parse_json('3.14')) AS result
        """
      Then query result
        | result       |
        | DECIMAL(3,2) |

    Scenario: schema_of_variant null
      When query
        """
        SELECT schema_of_variant(parse_json('null')) AS result
        """
      Then query result
        | result |
        | VOID   |

  Rule: Complex types

    Scenario: schema_of_variant simple object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":1}')) AS result
        """
      Then query result
        | result            |
        | OBJECT<a: BIGINT> |

    Scenario: schema_of_variant array of integers
      When query
        """
        SELECT schema_of_variant(parse_json('[1,2,3]')) AS result
        """
      Then query result
        | result        |
        | ARRAY<BIGINT> |

    Scenario: schema_of_variant empty object
      When query
        """
        SELECT schema_of_variant(parse_json('{}')) AS result
        """
      Then query result
        | result   |
        | OBJECT<> |

    Scenario: schema_of_variant empty array
      When query
        """
        SELECT schema_of_variant(parse_json('[]')) AS result
        """
      Then query result
        | result      |
        | ARRAY<VOID> |

    Scenario: schema_of_variant nested object
      When query
        """
        SELECT schema_of_variant(parse_json('{"a":{"b":1}}')) AS result
        """
      Then query result
        | result                       |
        | OBJECT<a: OBJECT<b: BIGINT>> |

    Scenario: schema_of_variant mixed array
      When query
        """
        SELECT schema_of_variant(parse_json('[1, "hello", true]')) AS result
        """
      Then query result
        | result         |
        | ARRAY<VARIANT> |

    Scenario: schema_of_variant object with multiple fields
      When query
        """
        SELECT schema_of_variant(parse_json('{"name":"sail","age":5,"active":true}')) AS result
        """
      Then query result
        | result                                             |
        | OBJECT<active: BOOLEAN, age: BIGINT, name: STRING> |

  Rule: NULL handling

    Scenario: schema_of_variant SQL NULL returns NULL
      When query
        """
        SELECT schema_of_variant(parse_json(NULL)) AS result
        """
      Then query result
        | result |
        | NULL   |
