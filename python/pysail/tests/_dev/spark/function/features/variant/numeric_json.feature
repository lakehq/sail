@spark-4
Feature: Variant JSON numeric types and rendering
  # Spark 4.2 VariantEndToEndSuite.schema_of_variant and VariantBuilder decimal
  # capacity rules; expectations also checked with Spark 4.2.0, UTC, ANSI enabled.

  Rule: Numeric tokens select integer, decimal, or double before rendering

    Scenario Outline: Variant numeric token: <case>
      When query
        """
        SELECT schema_of_variant(parse_json(<json>)) AS strict_type,
               to_json(parse_json(<json>)) AS strict_json,
               schema_of_variant(try_parse_json(<json>)) AS safe_type,
               to_json(try_parse_json(<json>)) AS safe_json
        """
      Then query result
        | strict_type | strict_json | safe_type | safe_json |
        | <type>      | <result>    | <type>    | <result>  |

      Examples:
        | case                          | json                                            | type           | result                                    |
        | integer negative zero         | '-0'                                            | BIGINT         | 0                                         |
        | decimal negative zero         | '-0.0'                                          | DECIMAL(1,0)   | 0                                         |
        | decimal zero trailing zeros   | '0.000'                                         | DECIMAL(1,0)   | 0                                         |
        | integral decimal              | '1.00'                                          | DECIMAL(1,0)   | 1                                         |
        | decimal trailing zeros        | '10.1000'                                       | DECIMAL(3,1)   | 10.1                                      |
        | integral trailing zeros       | '1000.00'                                       | DECIMAL(4,0)   | 1000                                      |
        | tiny decimal                  | '0.000000000000000001'                           | DECIMAL(18,18) | 0.000000000000000001                      |
        | negative tiny decimal         | '-0.000000000000000001'                          | DECIMAL(18,18) | -0.000000000000000001                     |
        | i64 min                       | '-9223372036854775808'                           | BIGINT         | -9223372036854775808                      |
        | i64 max                       | '9223372036854775807'                            | BIGINT         | 9223372036854775807                       |
        | below i64                     | '-9223372036854775809'                           | DECIMAL(19,0)  | -9223372036854775809                      |
        | above i64                     | '9223372036854775808'                            | DECIMAL(19,0)  | 9223372036854775808                       |
        | beyond u64                    | '99999999999999999999'                           | DECIMAL(20,0)  | 99999999999999999999                      |
        | decimal4 limit                | '99999999.9'                                    | DECIMAL(9,1)   | 99999999.9                               |
        | decimal8 lower                | '999999999.9'                                   | DECIMAL(10,1)  | 999999999.9                              |
        | decimal8 limit                | '99999999999999999.9'                           | DECIMAL(18,1)  | 99999999999999999.9                      |
        | decimal16 lower               | '999999999999999999.9'                          | DECIMAL(19,1)  | 999999999999999999.9                     |
        | precision38 integer           | '99999999999999999999999999999999999999'         | DECIMAL(38,0)  | 99999999999999999999999999999999999999   |
        | precision39 integer           | '999999999999999999999999999999999999999'        | DOUBLE         | 1.0E39                                   |
        | precision38 decimal           | '9999999999999999999.9999999999999999999'        | DECIMAL(38,19) | 9999999999999999999.9999999999999999999  |
        | precision39 decimal           | '9999999999999999999.99999999999999999999'       | DOUBLE         | 1.0E19                                   |
        | precision38 before trimming   | '1.0000000000000000000000000000000000000'        | DECIMAL(1,0)   | 1                                        |
        | precision39 before trimming   | '1.00000000000000000000000000000000000000'       | DOUBLE         | 1.0                                      |
        | scale38                       | '0.00000000000000000000000000000000000001'       | DECIMAL(38,38) | 0.00000000000000000000000000000000000001 |
        | scale39                       | '0.000000000000000000000000000000000000001'      | DOUBLE         | 1.0E-39                                  |
        | zero scale38                  | '-0.00000000000000000000000000000000000000'      | DECIMAL(1,0)   | 0                                        |
        | zero scale39                  | '-0.000000000000000000000000000000000000000'     | DOUBLE         | -0.0                                     |
        | scale39 before trimming       | '0.000000000000000000000000000000000000010'      | DOUBLE         | 1.0E-38                                  |
        | exponent positive zero        | '0e0'                                           | DOUBLE         | 0.0                                      |
        | exponent negative zero        | '-0e0'                                          | DOUBLE         | -0.0                                     |
        | exponent integral             | '1E0'                                           | DOUBLE         | 1.0                                      |
        | exponent decimal              | '1.5e-1'                                        | DOUBLE         | 0.15                                     |
        | exponent plus                 | '1.5e+3'                                        | DOUBLE         | 1500.0                                   |
        | exponent upper                | '1e10'                                          | DOUBLE         | 1.0E10                                   |
        | exponent lower                | '1e-18'                                         | DOUBLE         | 1.0E-18                                  |
        | plain double lower            | '1e-3'                                          | DOUBLE         | 0.001                                    |
        | scientific double lower       | '1e-4'                                          | DOUBLE         | 1.0E-4                                   |
        | plain double upper            | '1e6'                                           | DOUBLE         | 1000000.0                                |
        | scientific double upper       | '1e7'                                           | DOUBLE         | 1.0E7                                    |
        | positive underflow            | '1e-400'                                        | DOUBLE         | 0.0                                      |
        | negative underflow            | '-1e-400'                                       | DOUBLE         | -0.0                                     |
        | subnormal                     | '5e-324'                                        | DOUBLE         | 4.9E-324                                 |
        | positive overflow             | '1e309'                                         | DOUBLE         | "Infinity"                               |
        | negative overflow             | '-1e309'                                        | DOUBLE         | "-Infinity"                              |
        | numeric string                | '"-0.0 1e10 1.00"'                              | STRING         | "-0.0 1e10 1.00"                         |
        | decimal valid prefix          | '1.2300 trailing'                               | DECIMAL(3,2)   | 1.23                                     |
        | exponent valid prefix         | '-0e0 trailing'                                 | DOUBLE         | -0.0                                     |

  Rule: Compatible numeric types widen without becoming VARIANT

    Scenario Outline: Variant numeric schema widening: <case>
      When query
        """
        SELECT schema_of_variant(parse_json('<json>')) AS strict_type,
               schema_of_variant(try_parse_json('<json>')) AS safe_type
        """
      Then query result
        | strict_type | safe_type |
        | <type>      | <type>    |

      Examples:
        | case                  | json                                            | type                 |
        | decimal and exponent  | [1.1,1e0]                                       | ARRAY<DOUBLE>        |
        | normalized decimals   | [1.01,1.00]                                     | ARRAY<DECIMAL(3,2)>  |
        | decimal range overflow | [1.1,11111111111111111111111111111111111111]     | ARRAY<DOUBLE>        |
        | bigint scale limit    | [1,0.000000000000000001]                         | ARRAY<DECIMAL(38,18)> |
        | bigint scale overflow | [1,0.0000000000000000001]                        | ARRAY<DOUBLE>        |

    Scenario: Cast decimal arrays merge normalized element schemas
      When query
        """
        SELECT schema_of_variant(CAST(array(1.01, 1.00) AS VARIANT)) AS schema
        """
      Then query result
        | schema               |
        | ARRAY<DECIMAL(3,2)>  |

  Rule: Recursive parsing and formatting preserve other JSON values

    Scenario: Variant numeric tokens in dynamic nested JSON and ignored options
      When query
        """
        SELECT id, schema_of_variant(parse_json(j)) AS schema,
               to_json(parse_json(j)) AS json,
               to_json(try_parse_json(j), map('ignoreNullFields', 'true')) AS safe_json
        FROM (
          SELECT id, concat('{"decimal":[0.000000000000000001,10.1000,-0.0],"double":-0e0,"id":',
                            CAST(id AS STRING), ',"text":"-0.0 1e10 1.00","z":null}') AS j
          FROM range(2)
        ) ORDER BY id
        """
      Then query result ordered
        | id | schema                                                                              | json                                                                                                        | safe_json                                                                                                   |
        | 0  | OBJECT<decimal: ARRAY<DECIMAL(20,18)>, double: DOUBLE, id: BIGINT, text: STRING, z: VOID> | {"decimal":[0.000000000000000001,10.1,0],"double":-0.0,"id":0,"text":"-0.0 1e10 1.00","z":null} | {"decimal":[0.000000000000000001,10.1,0],"double":-0.0,"id":0,"text":"-0.0 1e10 1.00","z":null} |
        | 1  | OBJECT<decimal: ARRAY<DECIMAL(20,18)>, double: DOUBLE, id: BIGINT, text: STRING, z: VOID> | {"decimal":[0.000000000000000001,10.1,0],"double":-0.0,"id":1,"text":"-0.0 1e10 1.00","z":null} | {"decimal":[0.000000000000000001,10.1,0],"double":-0.0,"id":1,"text":"-0.0 1e10 1.00","z":null} |

    Scenario: Variant numeric formatting does not rewrite escaped keys or strings
      When query
        """
        SELECT to_json(parse_json(r'{"a\"b":-0e0,"text":"\\1e10\n-0.0\""}'))
                 = r'{"a\"b":-0.0,"text":"\\1e10\n-0.0\""}' AS preserved
        """
      Then query result
        | preserved |
        | true      |

    Scenario: Variant decimal schema merges integer and decimal elements
      When query
        """
        SELECT schema_of_variant(parse_json('[null,1,1.0]')) AS schema
        """
      Then query result
        | schema               |
        | ARRAY<DECIMAL(20,0)> |

    Scenario: Variant nonfinite doubles render as JSON strings
      When query
        """
        SELECT to_json(CAST(CAST('NaN' AS DOUBLE) AS VARIANT)) AS nan,
               to_json(CAST(CAST('Infinity' AS DOUBLE) AS VARIANT)) AS inf,
               to_json(CAST(CAST('-Infinity' AS DOUBLE) AS VARIANT)) AS neg_inf
        """
      Then query result
        | nan   | inf        | neg_inf     |
        | "NaN" | "Infinity" | "-Infinity" |

  Rule: Excessive nesting is rejected before parsing the subtree
    # Sail retains serde's 127-container budget, independently of Spark's limit.

    @sail-only
    Scenario: Deep safe parsing preserves the following valid column row
      When query
        """
        SELECT id, to_json(try_parse_json(j)) AS json FROM (
          SELECT id, CASE WHEN id = 0
            THEN concat(repeat('[', 1000000), '0', repeat(']', 1000000))
            ELSE concat('{"n":', CAST(id AS STRING), '.01}') END AS j
          FROM range(2)
        ) ORDER BY id
        """
      Then query result ordered
        | id | json       |
        | 0  | NULL       |
        | 1  | {"n":1.01} |

    @sail-only
    Scenario: Strict column parsing reports the canonical error at the depth boundary
      When query
        """
        SELECT parse_json(concat(repeat('[', CAST(128 + id AS INT)), '0',
                                 repeat(']', CAST(128 + id AS INT))))
        FROM range(1)
        """
      Then query error MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION

  Rule: Numeric parsing still validates complete objects and arrays

    Scenario Outline: Malformed nested numeric JSON: <case>
      When query
        """
        SELECT parse_json(<json>)
        """
      Then query error MALFORMED_RECORD_IN_PARSING

      Examples:
        | case                 | json                         |
        | nested duplicate     | '[{"x":-0.0,"x":1e0}]'       |
        | duplicate and suffix | '{"a":1,"a":2} trailing'    |
        | incomplete exponent  | '[1e0,1e]'                   |

    Scenario: Escaped duplicate keys with numeric values are rejected
      When query
        """
        SELECT parse_json(r'{"a":1,"\u0061":2}')
        """
      Then query error MALFORMED_RECORD_IN_PARSING

    Scenario: Safe numeric parse failures do not corrupt subsequent rows
      When query
        """
        SELECT id, schema_of_variant(try_parse_json(j)) AS schema,
               to_json(try_parse_json(j)) AS json
        FROM VALUES
          (0, '1.2300 trailing'),
          (1, '[{"x":-0.0,"x":1e0}]'),
          (2, r'{"a":1,"\u0061":2}'),
          (3, '[1e0,1e]'),
          (4, '-1e-400'),
          (5, 'null'),
          (6, CAST(NULL AS STRING)),
          (7, '{"a":[0.000000000000000001]}') AS t(id, j)
        ORDER BY id
        """
      Then query result ordered
        | id | schema                           | json                         |
        | 0  | DECIMAL(3,2)                     | 1.23                         |
        | 1  | NULL                             | NULL                         |
        | 2  | NULL                             | NULL                         |
        | 3  | NULL                             | NULL                         |
        | 4  | DOUBLE                           | -0.0                         |
        | 5  | VOID                             | null                         |
        | 6  | NULL                             | NULL                         |
        | 7  | OBJECT<a: ARRAY<DECIMAL(18,18)>> | {"a":[0.000000000000000001]} |
