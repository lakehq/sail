Feature: to_json

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_json yields the schema Spark declares
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: an empty struct input to to_json yields the schema Spark declares
      When query
        """
        SELECT to_json(struct()) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Empty struct

    Scenario: an empty struct has no fields, is non-null, and has a byte-exact JSON representation
      When query
        """
        SELECT
          typeof(struct()) AS struct_type,
          struct() IS NOT NULL AS is_non_null,
          to_json(struct()) AS result,
          hex(to_json(struct())) AS bytes
        """
      Then query result
        | struct_type | is_non_null | result | bytes |
        | struct<>    | true        | {}     | 7B7D  |

    Scenario: to_json preserves row cardinality for an empty struct
      When query
        """
        SELECT id, to_json(struct()) AS result
        FROM range(3)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | {}     |
        | 1  | {}     |
        | 2  | {}     |

    Scenario: to_json distinguishes nested empty structs from null structs
      When query
        """
        SELECT
          to_json(named_struct('empty', struct())) AS nested,
          to_json(CAST(NULL AS STRUCT<>)) AS null_struct
        """
      Then query result
        | nested       | null_struct |
        | {"empty":{}} | NULL        |

  Rule: Struct field order

    Scenario: to_json preserves struct field declaration order byte-for-byte
      When query
        """
        SELECT to_json(struct(zeta, alpha, mid)) AS result
        FROM VALUES ('z', 'a', 'm') AS t(zeta, alpha, mid)
        """
      Then query result
        | result                                 |
        | {"zeta":"z","alpha":"a","mid":"m"}     |

    Scenario: to_json preserves nested struct field declaration order byte-for-byte
      When query
        """
        SELECT to_json(named_struct(
          'zeta', named_struct('zeta_inner', 'z', 'alpha_inner', 'a'),
          'alpha', 'a',
          'mid', 'm'
        )) AS result
        """
      Then query result
        | result                                                                         |
        | {"zeta":{"zeta_inner":"z","alpha_inner":"a"},"alpha":"a","mid":"m"}            |

    Scenario: omitted null fields do not reorder the remaining struct fields
      When query
        """
        SELECT to_json(struct(zeta, alpha, mid)) AS result
        FROM VALUES ('z', CAST(NULL AS STRING), 'm') AS t(zeta, alpha, mid)
        """
      Then query result
        | result                     |
        | {"zeta":"z","mid":"m"}     |

  Rule: Null struct fields

    Scenario Outline: to_json omits typed and untyped null literal fields equally
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', <null>, 'c', 3)) AS result
        """
      Then query result
        | result        |
        | {"a":1,"c":3} |

      Examples:
        | null                 |
        | CAST(NULL AS STRING) |
        | NULL                 |

    Scenario: to_json omits a void field alongside VALUES columns
      When query
        """
        SELECT to_json(struct(a,b)) AS result
        FROM (SELECT a,null AS b FROM VALUES (1),(2) AS t(a))
        """
      Then query result
        | result  |
        | {"a":1} |
        | {"a":2} |

    Scenario: to_json omits a void field alongside range columns
      When query
        """
        SELECT id, to_json(named_struct('a', id, 'b', NULL)) AS result
        FROM range(3)
        ORDER BY id
        """
      Then query result ordered
        | id | result  |
        | 0  | {"a":0} |
        | 1  | {"a":1} |
        | 2  | {"a":2} |

    Scenario: omitting null struct fields preserves null containers and collection entries
      When query
        """
        SELECT
          to_json(named_struct('void', NULL, 'typed', CAST(NULL AS STRING))) AS all_null,
          to_json(CAST(NULL AS STRUCT<a: INT>)) AS null_struct,
          to_json(array(NULL, NULL)) AS array_nulls,
          to_json(map('missing', NULL)) AS map_nulls
        """
      Then query result
        | all_null | null_struct | array_nulls | map_nulls        |
        | {}       | NULL        | [null,null] | {"missing":null} |

    Scenario: to_json omits void fields recursively in structs and collection values
      When query
        """
        SELECT
          to_json(named_struct(
            'nested', named_struct('a', id, 'b', NULL),
            'all_null', named_struct('b', NULL)
          )) AS nested,
          to_json(array(named_struct('a', id, 'b', NULL), NULL)) AS array_structs,
          to_json(map('value', named_struct('a', id, 'b', NULL), 'missing', NULL)) AS map_structs
        FROM range(1)
        """
      Then query result
        | nested                           | array_structs  | map_structs                      |
        | {"nested":{"a":0},"all_null":{}} | [{"a":0},null] | {"value":{"a":0},"missing":null} |

    Scenario: to_json null field omission survives a Parquet roundtrip
      Given variable location for temporary directory to_json_null_fields
      Given statement template
        """
        INSERT OVERWRITE DIRECTORY {{ location.sql }} USING parquet
        SELECT id, to_json(named_struct(
          'id', id,
          'void', NULL,
          'typed', CASE WHEN id = 0 THEN CAST(NULL AS STRING) ELSE 'present' END
        )) AS result,
        to_json(CASE WHEN id = 0 THEN CAST(NULL AS STRUCT<a: BIGINT>)
          ELSE named_struct('a', id) END) AS nullable_struct
        FROM range(2)
        """
      When query template
        """
        SELECT id, result, nullable_struct FROM parquet.`{{ location.string }}` ORDER BY id
        """
      Then query result ordered
        | id | result                     | nullable_struct |
        | 0  | {"id":0}                   | NULL            |
        | 1  | {"id":1,"typed":"present"} | {"a":1}         |

    # TODO: honor the per-call ignoreNullFields option; Sail currently always omits null fields.
    @sail-bug
    Scenario: to_json can retain null fields with the ignoreNullFields option
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS STRING), 'c', NULL),
          map('ignoreNullFields', 'false')) AS result
        """
      Then query result
        | result                    |
        | {"a":1,"b":null,"c":null} |

    # TODO: use spark.sql.jsonGenerator.ignoreNullFields when no per-call option is supplied.
    @sail-bug
    Scenario: to_json can retain null fields with the session configuration
      Given config spark.sql.jsonGenerator.ignoreNullFields = false
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS STRING), 'c', NULL)) AS result
        """
      Then query result
        | result                    |
        | {"a":1,"b":null,"c":null} |

  Rule: Map key order

    Scenario: to_json preserves map entry order when sortKeys is disabled
      When query
        """
        SELECT
          to_json(value) AS default_result,
          to_json(value, map('sortKeys', 'false')) AS false_result
        FROM (
          SELECT map_from_arrays(
            array('zeta', 'alpha', 'mid'),
            array(1, 2, 3)
          ) AS value
        )
        """
      Then query result
        | default_result                   | false_result                     |
        | {"zeta":1,"alpha":2,"mid":3} | {"zeta":1,"alpha":2,"mid":3} |

  Rule: Sorted JSON object keys

    # The sortKeys JSON writer option was added in Spark 4.2.
    @spark-4.2
    Scenario: sortKeys recursively sorts struct fields including structs inside arrays
      When query
        """
        SELECT
          to_json(named_struct(
            'zeta', named_struct('zeta_inner', 'z', 'alpha_inner', 'a'),
            'alpha', 'a',
            'mid', 'm'
          ), map('sortKeys', 'true')) AS nested,
          to_json(array(named_struct(
            'zeta', 'z',
            'alpha', 'a'
          )), map('sortKeys', 'true')) AS array_result
        """
      Then query result
        | nested                                                                         | array_result                   |
        | {"alpha":"a","mid":"m","zeta":{"alpha_inner":"a","zeta_inner":"z"}} | [{"alpha":"a","zeta":"z"}] |

    @spark-4.2
    Scenario: sortKeys recursively sorts map keys
      When query
        """
        SELECT to_json(map_from_arrays(
          array('zeta', 'alpha'),
          array(
            map_from_arrays(array('zeta_inner', 'alpha_inner'), array('z', 'a')),
            map_from_arrays(array('zeta_inner', 'alpha_inner'), array('Z', 'A'))
          )
        ), map('sortKeys', 'true')) AS result
        """
      Then query result
        | result                                                                                         |
        | {"alpha":{"alpha_inner":"A","zeta_inner":"Z"},"zeta":{"alpha_inner":"a","zeta_inner":"z"}} |
