# `variant_explode` is a table-valued function: Spark accepts it in a `FROM` clause (and as a
# `LATERAL` join), but not through `LATERAL VIEW`, which resolves generators against the
# function registry and fails with `ROUTINE_NOT_FOUND`. Sail accepts both. Scenarios here use
# the `FROM` form so they are verifiable against JVM Spark; the `LATERAL VIEW` form is covered
# separately under `@sail-only`. Likewise `to_json(value)` is used instead of Sail's
# `variant_to_json`, which Spark does not have.
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

    # `pos` and `value` are non-nullable in Spark's TVF output schema; Sail widens both.
    @sail-bug
    @function(nullability)
    Scenario: variant_explode returns pos key value columns
      When query
        """
        SELECT pos, key, value
        FROM variant_explode(parse_json('[1]'))
        """
      Then query schema
        """
        root
         |-- pos: integer (nullable = false)
         |-- key: string (nullable = true)
         |-- value: variant (nullable = false)
        """

    # Spark rejects this during analysis with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`;
    # Sail only fails at execution, with "field does not have extension type VariantType"
    # (`scalar/variant/utils/helper.rs`). The divergence is the phase, not just the wording.
    @sail-bug
    Scenario: variant_explode_outer rejects non-variant input
      When query
        """
        SELECT pos, key, value
        FROM variant_explode_outer(array(1, 2, 3))
        """
      Then query error (?s).*requires the "VARIANT" type.*

  Rule: variant_explode with array input

    Scenario: Explode a variant array of strings
      When query
        """
        SELECT pos, key, to_json(value) AS value
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
        SELECT pos, key, to_json(value) AS value
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
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode(parse_json('[99]'))
        """
      Then query result ordered
        | pos | key  | value |
        | 0   | NULL | 99    |

  Rule: variant_explode with object input

    Scenario: Explode a variant object
      When query
        """
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

    Scenario: Explode a single-field variant object
      When query
        """
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode(parse_json('{"x": "hello"}'))
        """
      Then query result
        | pos | key | value   |
        | 0   | x   | "hello" |

    Scenario: Explode a variant object with various value types
      When query
        """
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode(parse_json('{"n": null, "i": 1, "s": "hi", "a": [1,2]}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | [1,2] |
        | 1   | i   | 1     |
        | 2   | n   | null  |
        | 3   | s   | "hi"  |

  Rule: variant_explode with empty or non-container input

    Scenario Outline: Non-container: <case>
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode(<src>)
        """
      Then query result
        | cnt |
        | 0   |

      Examples:
        | case                                           | src                   |
        | Explode empty array returns no rows            | parse_json('[]')      |
        | Explode empty object returns no rows           | parse_json('{}')      |
        | Explode variant null returns no rows           | parse_json('null')    |
        | Explode SQL NULL returns no rows               | CAST(NULL AS VARIANT) |
        | Explode scalar variant returns no rows         | parse_json('42')      |
        | Explode string scalar variant returns no rows  | parse_json('"hello"') |
        | Explode boolean scalar variant returns no rows | parse_json('true')    |

  Rule: variant_explode with nested values

    Scenario: Explode array with nested structures
      When query
        """
        SELECT pos, key, to_json(value) AS value
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
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode(parse_json('{"x": [1,2], "y": {"z": 3}}'))
        """
      Then query result
        | pos | key | value   |
        | 0   | x   | [1,2]   |
        | 1   | y   | {"z":3} |

  Rule: variant_explode_outer with non-empty input

    Scenario: Outer explode a variant array of strings
      When query
        """
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode_outer(parse_json('["hello", "world"]'))
        """
      Then query result ordered
        | pos | key  | value   |
        | 0   | NULL | "hello" |
        | 1   | NULL | "world" |

    Scenario: Outer explode a variant object
      When query
        """
        SELECT pos, key, to_json(value) AS value
        FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))
        """
      Then query result
        | pos | key | value |
        | 0   | a   | true  |
        | 1   | b   | 3.14  |

  # `variant_explode_outer` emits one all-NULL row when there is nothing to explode: that is
  # what the `_outer` suffix means. Sail returns no rows for both non-container input and
  # empty containers.
  Rule: variant_explode_outer emits a single NULL row when there is nothing to explode

    @sail-bug
    Scenario Outline: Outer non-container: <case>
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(<src>)
        """
      Then query result
        | cnt |
        | 1   |

      Examples:
        | case                                         | src                   |
        | Outer explode variant null yields one NULL row   | parse_json('null')    |
        | Outer explode SQL NULL yields one NULL row       | CAST(NULL AS VARIANT) |
        | Outer explode scalar yields one NULL row         | parse_json('42')      |
        | Outer explode string scalar yields one NULL row  | parse_json('"text"')  |
        | Outer explode boolean scalar yields one NULL row | parse_json('false')   |

    @sail-bug
    Scenario Outline: Outer empty container: <case>
      When query
        """
        SELECT count(*) AS cnt
        FROM variant_explode_outer(<src>)
        """
      Then query result
        | cnt |
        | 1   |

      Examples:
        | case                                       | src              |
        | Outer explode empty array yields one NULL row  | parse_json('[]') |
        | Outer explode empty object yields one NULL row | parse_json('{}') |

  Rule: variant_explode with table column

    Scenario: Explode variant column from a table with mixed values
      When query
        """
        SELECT id, pos, key, to_json(value) AS value
        FROM (
          SELECT 1 AS id, parse_json('[10, 20]') AS v
          UNION ALL
          SELECT 2 AS id, parse_json('{"k": "v"}') AS v
        ) t, LATERAL variant_explode(t.v) ve
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 10    |
        | 1  | 1   | NULL | 20    |
        | 2  | 0   | k    | "v"   |

    Scenario: Explode variant column skips null, empty, and non-container rows
      When query
        """
        SELECT id, pos, key, to_json(value) AS value
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
        ) t, LATERAL variant_explode(t.v) ve
        """
      Then query result
        | id | pos | key  | value |
        | 1  | 0   | NULL | 1     |
        | 3  | 0   | a    | 1     |

    # The inner form drops rows with nothing to explode; the outer form keeps them with an
    # all-NULL right side. Sail drops them in both.
    @sail-bug
    Scenario: Outer explode variant column keeps null, empty, and non-container rows
      When query
        """
        SELECT id, pos, key, to_json(value) AS value
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
        ) t, LATERAL variant_explode_outer(t.v) ve
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
