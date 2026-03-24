@variant
Feature: Variant type functions (parse_json, is_variant_null, variant_get)

  Rule: parse_json + variant_get roundtrip

    Scenario: Parse and extract integer
      When query
        """
        SELECT variant_get(parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Parse and extract string
      When query
        """
        SELECT variant_get(parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: Parse and extract boolean true
      When query
        """
        SELECT variant_get(parse_json('true'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Parse and extract boolean false
      When query
        """
        SELECT variant_get(parse_json('false'), '$', 'boolean') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Parse and extract double
      When query
        """
        SELECT variant_get(parse_json('3.14'), '$', 'double') AS result
        """
      Then query result
        | result |
        | 3.14   |

    Scenario: Parse and extract nested field
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Parse and extract deeply nested field
      When query
        """
        SELECT variant_get(parse_json('{"a":{"b":{"c":99}}}'), '$.a.b.c', 'int') AS result
        """
      Then query result
        | result |
        | 99     |

  Rule: parse_json NULL handling

    Scenario: Parse NULL input returns NULL
      When query
        """
        SELECT parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: is_variant_null

    Scenario: JSON null is variant null
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Integer is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('42')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: String is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('"hello"')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Object is not variant null
      When query
        """
        SELECT is_variant_null(parse_json('{"a":1}')) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: SQL NULL input to is_variant_null returns false
      When query
        """
        SELECT is_variant_null(parse_json(NULL)) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Array access

    Scenario: Array index 0
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[0]', 'int') AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: Array index 2
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[2]', 'int') AS result
        """
      Then query result
        | result |
        | 30     |

    Scenario: Array out of bounds returns NULL
      When query
        """
        SELECT variant_get(parse_json('[10,20,30]'), '$[5]', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Nested array access
      When query
        """
        SELECT variant_get(parse_json('[[1,2],[3,4]]'), '$[1][0]', 'int') AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: Edge cases

    Scenario: Missing field returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":1}'), '$.b', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Null field in object returns NULL
      When query
        """
        SELECT variant_get(parse_json('{"a":null}'), '$.a', 'string') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty string value
      When query
        """
        SELECT variant_get(parse_json('""'), '$', 'string') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Negative double
      When query
        """
        SELECT variant_get(parse_json('-3.14'), '$', 'double') AS result
        """
      Then query result
        | result |
        | -3.14  |

    Scenario: Zero integer
      When query
        """
        SELECT variant_get(parse_json('0'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: Deep nesting 5 levels
      When query
        """
        SELECT variant_get(parse_json('{"a":{"b":{"c":{"d":{"e":42}}}}}'), '$.a.b.c.d.e', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Mixed types in array
      When query
        """
        SELECT variant_get(parse_json('[1, "two", true]'), '$[1]', 'string') AS result
        """
      Then query result
        | result |
        | two    |

    Scenario: try_variant_get returns NULL for wrong type
      When query
        """
        SELECT try_variant_get(parse_json('"hello"'), '$', 'int') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Multiple rows with variant
      When query
        """
        SELECT variant_get(parse_json(v), '$', 'int') AS result
        FROM VALUES ('1'), ('2'), ('3') AS t(v)
        ORDER BY result
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | 3      |

  Rule: Error cases

    Scenario: Invalid JSON raises error
      When query
        """
        SELECT parse_json('not json') AS result
        """
      Then query error (MALFORMED_RECORD_IN_PARSING|JSON format error)

    Scenario: Empty string raises error
      When query
        """
        SELECT parse_json('') AS result
        """
      Then query error (MALFORMED_RECORD_IN_PARSING|JSON format error|empty)
