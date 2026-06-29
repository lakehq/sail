@variant_explode
@spark-4
Feature: variant_explode and variant_explode_outer

  Rule: variant_explode analysis and schema

    @sail-only
    Scenario: EXPLAIN variant_explode shows physical plan
      When query
        """
        EXPLAIN SELECT pos, key, value
        FROM (SELECT parse_json('[1, 2]') AS v) t
        LATERAL VIEW variant_explode(v) ve AS pos, key, value
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN variant_explode_outer shows physical plan
      When query
        """
        EXPLAIN SELECT pos, key, value
        FROM (SELECT parse_json('{"a": 1}') AS v) t
        LATERAL VIEW variant_explode_outer(v) ve AS pos, key, value
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: variant_explode returns pos key value columns
      When query
        """
        SELECT pos, key, value
        FROM (SELECT parse_json('[1]') AS v) t
        LATERAL VIEW variant_explode(v) ve AS pos, key, value
        """
      Then query schema
        """
        root
         |-- pos: integer (nullable = true)
         |-- key: string (nullable = true)
         |-- value: variant (nullable = true)
        """

    @sail-only
    Scenario: variant_explode_outer rejects non-variant input
      When query
        """
        SELECT pos, key, value
        FROM (SELECT array(1, 2, 3) AS v) t
        LATERAL VIEW variant_explode_outer(v) ve AS pos, key, value
        """
      Then query error (?s).*VariantType.*

  Rule: variant_explode with array input

    @sail-bug
    Scenario: Explode a variant array of strings
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('["hello", "world"]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | hello |
        | 1   | NULL | world |

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

    @sail-bug
    Scenario: Explode a variant array of mixed types
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('[true, "abc", 42, null]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | true  |
        | 1   | NULL | abc   |
        | 2   | NULL | 42    |
        | 3   | NULL | NULL  |

    Scenario: Explode a single-element variant array
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('[99]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | 99    |

  Rule: variant_explode with object input

    Scenario: Explode a variant object
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

    @sail-bug
    Scenario: Explode a single-field variant object
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"x": "hello"}'))
        """
      Then query result
        | pos | key | value |
        | 0   | x   | hello |

    @sail-bug
    Scenario: Explode a variant object with various value types
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"n": null, "i": 1, "s": "hi", "a": [1,2]}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | [1,2] |
        | 1   | i   | 1     |
        | 2   | n   | NULL  |
        | 3   | s   | hi    |

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
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('[1, [2, 3], {"a": 4}]'))
        """
      Then query result ordered
        | pos | key  | value   |
        | 0   | NULL | 1       |
        | 1   | NULL | [2,3]   |
        | 2   | NULL | {"a":4} |

    Scenario: Explode object with nested values
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"x": [1,2], "y": {"z": 3}}'))
        """
      Then query result
        | pos | key | value   |
        | 0   | x   | [1,2]   |
        | 1   | y   | {"z":3} |

  Rule: variant_explode_outer with non-empty input

    @sail-bug
    Scenario: Outer explode a variant array of strings
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('["hello", "world"]'))
        """
      Then query result ordered
        | pos  | key  | value |
        | 0    | NULL | hello |
        | 1    | NULL | world |

    Scenario: Outer explode a variant object
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

  Rule: variant_explode_outer ignores null or non-container input

    Scenario: Outer explode variant null returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('null'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

    Scenario: Outer explode SQL NULL returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(CAST(NULL AS VARIANT))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

    Scenario: Outer explode scalar returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('42'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

    Scenario: Outer explode string scalar returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('"text"'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

    Scenario: Outer explode boolean scalar returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('false'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

  Rule: variant_explode_outer with empty containers returns one null row

    Scenario: Outer explode empty array returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('[]'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

    Scenario: Outer explode empty object returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('{}'))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

  Rule: variant_explode with table column

    @sail-bug
    Scenario: Explode variant column from a table with mixed values
      When query
        """
        SELECT id, t.pos, t.key, CAST(t.value AS STRING) AS value
        FROM (
          SELECT 1 AS id, parse_json('[10, 20]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('{"k": "v"}') AS v
        ) base
        CROSS JOIN LATERAL variant_explode(base.v) t
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 10    |
        | 1  | 1   | NULL | 20    |
        | 2  | 0   | k    | v     |

    @sail-bug
    Scenario: Explode variant column skips null, empty, and non-container rows
      When query
        """
        SELECT id, t.pos, t.key, CAST(t.value AS STRING) AS value
        FROM (
          SELECT 1 AS id, parse_json('[1]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('42') AS v
          UNION ALL
          SELECT 3 AS id, parse_json('{"a": 1}') AS v
          UNION ALL
          SELECT 4 AS id, parse_json('null') AS v
          UNION ALL
          SELECT 5 AS id, CAST(NULL AS VARIANT) AS v
          UNION ALL
          SELECT 6 AS id, parse_json('[]') AS v
          UNION ALL
          SELECT 7 AS id, parse_json('{}') AS v
        ) base
        CROSS JOIN LATERAL variant_explode(base.v) t
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 1     |
        | 3  | 0   | a    | 1     |

    Scenario: Outer explode variant column preserves null, empty, and non-container rows
      When query
        """
        SELECT id, t.pos, t.key, CAST(t.value AS STRING) AS value
        FROM (
          SELECT 1 AS id, parse_json('[1]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('42') AS v
          UNION ALL
          SELECT 3 AS id, parse_json('{"a": 1}') AS v
          UNION ALL
          SELECT 4 AS id, parse_json('null') AS v
          UNION ALL
          SELECT 5 AS id, CAST(NULL AS VARIANT) AS v
          UNION ALL
          SELECT 6 AS id, parse_json('[]') AS v
          UNION ALL
          SELECT 7 AS id, parse_json('{}') AS v
        ) base
        CROSS JOIN LATERAL variant_explode_outer(base.v) t
        """
      Then query result
        | id | pos  | key  | value |
        | 1  | 0    | NULL | 1     |
        | 2  | NULL | NULL | NULL  |
        | 3  | 0    | a    | 1     |
        | 4  | NULL | NULL | NULL  |
        | 5  | NULL | NULL | NULL  |
        | 6  | NULL | NULL | NULL  |
        | 7  | NULL | NULL | NULL  |

  Rule: variant_explode key ordering and types

    Scenario: Object keys are returned in alphabetical order
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"z": 1, "a": 2, "m": 3}'))
        """
      Then query result ordered
        | pos | key | value |
        | 0   | a   | 2     |
        | 1   | m   | 3     |
        | 2   | z   | 1     |

    Scenario: Array keys are NULL and positions are zero-based integers
      When query
        """
        SELECT pos, key IS NULL AS key_is_null, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('[10, 20, 30]'))
        """
      Then query result ordered
        | pos | key_is_null | value |
        | 0   | true        | 10    |
        | 1   | true        | 20    |
        | 2   | true        | 30    |

    @sail-bug
    Scenario: typeof pos is int and typeof key and value are string and variant
      When query
        """
        SELECT typeof(pos) AS pos_type, typeof(key) AS key_type, typeof(value) AS val_type
        FROM variant_explode(parse_json('{"a": 1}'))
        """
      Then query result
        | pos_type | key_type | val_type |
        | int      | string   | variant  |

  Rule: variant_explode with null values inside collections

    @sail-bug
    Scenario: Null values in variant object are returned as NULL values
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"a": null, "b": 1}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | NULL  |
        | 1   | b   | 1     |

    @sail-bug
    Scenario: Null values in variant array are returned as NULL values
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('[null, 1, null]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | NULL  |
        | 1   | NULL | 1     |
        | 2   | NULL | NULL  |

  Rule: variant_explode with named argument syntax

    Scenario: Named argument input works like positional argument
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(input => parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

  Rule: variant_explode with unicode keys

    Scenario: Unicode keys are preserved and sorted
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"abc_after": 1, "aaa_before": 2}'))
        """
      Then query result
        | pos | key        | value |
        | 0   | aaa_before | 2     |
        | 1   | abc_after  | 1     |

  Rule: variant_explode with deeply nested values

    Scenario: Only one level of nesting is exploded
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode(parse_json('{"a": {"b": {"c": 1}}}'))
        """
      Then query result
        | pos | key | value         |
        | 0   | a   | {"b":{"c":1}} |

  Rule: variant_explode_outer with parse_json(NULL) input

    Scenario: Outer explode parse_json of NULL string returns one null row
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json(NULL))
        """
      Then query result
        | pos  | key  | value |
        | NULL | NULL | NULL  |

  Rule: variant_explode_outer column types

    @sail-bug
    Scenario: typeof pos key value from outer explode of null input
      When query
        """
        SELECT typeof(pos) AS pos_type, typeof(key) AS key_type, typeof(value) AS val_type
        FROM variant_explode_outer(CAST(NULL AS VARIANT))
        """
      Then query result
        | pos_type | key_type | val_type |
        | int      | string   | variant  |

    @sail-bug
    Scenario: typeof pos key value from outer explode of non-empty input
      When query
        """
        SELECT typeof(pos) AS pos_type, typeof(key) AS key_type, typeof(value) AS val_type
        FROM variant_explode_outer(parse_json('{"a": 1}'))
        """
      Then query result
        | pos_type | key_type | val_type |
        | int      | string   | variant  |

  Rule: variant_explode_outer with named argument syntax

    Scenario: Named argument input works for outer explode
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(input => parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

  Rule: variant_explode_outer key ordering

    Scenario: Outer explode object keys are returned in alphabetical order
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('{"z": 1, "a": 2, "m": 3}'))
        """
      Then query result ordered
        | pos | key | value |
        | 0   | a   | 2     |
        | 1   | m   | 3     |
        | 2   | z   | 1     |

  Rule: variant_explode_outer with null values inside containers

    @sail-bug
    Scenario: Outer explode object with null value field
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('{"a": null, "b": 1}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | NULL  |
        | 1   | b   | 1     |

    @sail-bug
    Scenario: Outer explode array with null elements
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('[null, 1, null]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | NULL  |
        | 1   | NULL | 1     |
        | 2   | NULL | NULL  |

  Rule: variant_explode_outer with nested values

    Scenario: Outer explode object with nested array and object values
      When query
        """
        SELECT pos, key, CAST(value AS STRING) AS value
        FROM variant_explode_outer(parse_json('{"a": [1,2], "b": {"c": 3}}'))
        """
      Then query result
        | pos | key | value   |
        | 0   | a   | [1,2]   |
        | 1   | b   | {"c":3} |

  Rule: variant_explode_outer with CAST on non-string types

    Scenario: Outer explode array with CAST to INT
      When query
        """
        SELECT pos, key, CAST(value AS INT) AS value
        FROM variant_explode_outer(parse_json('[1, 2, 3]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | 1     |
        | 1   | NULL | 2     |
        | 2   | NULL | 3     |

  Rule: variant_explode_outer multi-row lateral join with mixed values

    Scenario: Outer explode lateral join with id-tagged rows preserves non-container rows
      When query
        """
        SELECT base.id, t.pos, t.key, CAST(t.value AS STRING) AS value
        FROM (
          SELECT 1 AS id, parse_json('{"x": 1}') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('[]') AS v
          UNION ALL
          SELECT 3 AS id, CAST(NULL AS VARIANT) AS v
          UNION ALL
          SELECT 4 AS id, parse_json('null') AS v
          UNION ALL
          SELECT 5 AS id, parse_json('[1, 2]') AS v
        ) base
        CROSS JOIN LATERAL variant_explode_outer(base.v) t
        ORDER BY base.id, t.pos
        """
      Then query result ordered
        | id | pos  | key  | value |
        | 1  | 0    | x    | 1     |
        | 2  | NULL | NULL | NULL  |
        | 3  | NULL | NULL | NULL  |
        | 4  | NULL | NULL | NULL  |
        | 5  | 0    | NULL | 1     |
        | 5  | 1    | NULL | 2     |
