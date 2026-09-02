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
