@variant @spark-4
Feature: variant_explode and variant_explode_outer

  Rule: variant_explode with array input

    Scenario: Explode a variant array of strings
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('["hello", "world"]'))
        """
      Then query result ordered
        | pos | key  | value   |
        | 0   | NULL | "hello" |
        | 1   | NULL | "world" |

    Scenario: Explode a variant array of integers
      When query
        """
        SELECT pos, key, variant_get(value, '$', 'int') AS value
        FROM variant_explode(parse_json('[1, 2, 3]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | 1     |
        | 1   | NULL | 2     |
        | 2   | NULL | 3     |

    Scenario: Explode a variant array of mixed types
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('[true, "abc", 42, null]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | true  |
        | 1   | NULL | "abc" |
        | 2   | NULL | 42    |
        | 3   | NULL | null  |

    Scenario: Explode a single-element variant array
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('[99]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | 99    |

  Rule: variant_explode with object input

    Scenario: Explode a variant object
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

    Scenario: Explode a single-field variant object
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('{"x": "hello"}'))
        """
      Then query result
        | pos | key | value   |
        | 0   | x   | "hello" |

    Scenario: Explode a variant object with various value types
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('{"n": null, "i": 1, "s": "hi", "a": [1,2]}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | [1,2] |
        | 1   | i   | 1     |
        | 2   | n   | null  |
        | 3   | s   | "hi"  |

  Rule: variant_explode with empty or non-container input

    Scenario: Explode empty array returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('[]'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode empty object returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('{}'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode variant null returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('null'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode SQL NULL returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(CAST(NULL AS VARIANT))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode scalar variant returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('42'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode string scalar variant returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('"hello"'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Explode boolean scalar variant returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(parse_json('true'))
        """
      Then query result
        | cnt |
        | 0   |

  Rule: variant_explode with nested values

    Scenario: Explode array with nested structures
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('[1, [2, 3], {"a": 4}]'))
        """
      Then query result ordered
        | pos | key  | value    |
        | 0   | NULL | 1        |
        | 1   | NULL | [2,3]    |
        | 2   | NULL | {"a":4}  |

    Scenario: Explode object with nested values
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode(parse_json('{"x": [1,2], "y": {"z": 3}}'))
        """
      Then query result
        | pos | key | value     |
        | 0   | x   | [1,2]     |
        | 1   | y   | {"z":3}   |

  Rule: variant_explode_outer with non-empty input

    Scenario: Outer explode a variant array of strings
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode_outer(parse_json('["hello", "world"]'))
        """
      Then query result ordered
        | pos  | key  | value   |
        | 0    | NULL | "hello" |
        | 1    | NULL | "world" |

    Scenario: Outer explode a variant object
      When query
        """
        SELECT pos, key, variant_to_json(value) AS value
        FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

  Rule: variant_explode_outer ignores null or non-container input

    Scenario: Outer explode variant null returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('null'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Outer explode SQL NULL returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(CAST(NULL AS VARIANT))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Outer explode scalar returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('42'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Outer explode string scalar returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('"text"'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Outer explode boolean scalar returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('false'))
        """
      Then query result
        | cnt |
        | 0   |

  Rule: variant_explode_outer with empty containers returns no rows

    Scenario: Outer explode empty array returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('[]'))
        """
      Then query result
        | cnt |
        | 0   |

    Scenario: Outer explode empty object returns no rows
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(parse_json('{}'))
        """
      Then query result
        | cnt |
        | 0   |

  Rule: variant_explode with table column

    Scenario: Explode variant column from a table with mixed values
      When query
        """
        SELECT id, pos, key, variant_to_json(value) AS value
        FROM (
          SELECT 1 AS id, parse_json('[10, 20]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('{"k": "v"}') AS v
        ),
        LATERAL variant_explode(v)
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 10    |
        | 1  | 1   | NULL | 20    |
        | 2  | 0   | k    | "v"   |

    Scenario: Explode variant column skips non-container rows
      When query
        """
        SELECT id, pos, key, variant_to_json(value) AS value
        FROM (
          SELECT 1 AS id, parse_json('[1]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('42') AS v
          UNION ALL
          SELECT 3 AS id, parse_json('{"a": 1}') AS v
        ),
        LATERAL variant_explode(v)
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 1     |
        | 3  | 0   | a    | 1     |

    Scenario: Outer explode variant column skips non-container rows
      When query
        """
        SELECT id, pos, key, variant_to_json(value) AS value
        FROM (
          SELECT 1 AS id, parse_json('[1]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('42') AS v
          UNION ALL
          SELECT 3 AS id, parse_json('{"a": 1}') AS v
        ),
        LATERAL variant_explode_outer(v)
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 1     |
        | 3  | 0   | a    | 1     |
