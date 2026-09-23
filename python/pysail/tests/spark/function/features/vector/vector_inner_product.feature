@spark-4.2
Feature: vector_inner_product

  Rule: Basic results

    Scenario: basic, orthogonal, and self product
      When query
        """
        SELECT
          vector_inner_product(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)) AS basic,
          vector_inner_product(array(1.0F, 0.0F), array(0.0F, 1.0F)) AS orthogonal,
          vector_inner_product(array(3.0F, 4.0F), array(3.0F, 4.0F)) AS self_product
        """
      Then query result
        | basic | orthogonal | self_product |
        | 32.0  | 0.0        | 25.0         |

    Scenario: vector columns from VALUES execute the UDF
      When query
        """
        SELECT vector_inner_product(left_vector, right_vector) AS result
        FROM VALUES
          (array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (array(1.0F, 0.0F), array(0.0F, 1.0F)),
          (array(3.0F, 4.0F), array(3.0F, 4.0F))
        AS t(left_vector, right_vector)
        """
      Then query result
        | result |
        | 32.0   |
        | 0.0    |
        | 25.0   |

    Scenario: empty ARRAY<FLOAT> inputs return 0.0
      When query
        """
        SELECT vector_inner_product(
          CAST(array() AS ARRAY<FLOAT>),
          CAST(array() AS ARRAY<FLOAT>)
        ) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: upstream 16-element case returns sum of squares 1496.0
      When query
        """
        SELECT vector_inner_product(
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)
        ) AS result
        """
      Then query result
        | result |
        | 1496.0 |

    Scenario: accumulation matches Spark for vectors with large cancellation
      When query
        """
        SELECT vector_inner_product(left_vector, right_vector) AS result
        FROM VALUES (
          array(
            1.0E20F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F,
            -1.0E20F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F
          ),
          array(
            1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F,
            1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F
          )
        ) AS t(left_vector, right_vector)
        """
      Then query result
        | result |
        | 0.0    |

  Rule: Null handling

    Scenario: typed null vector returns NULL
      When query
        """
        SELECT vector_inner_product(CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array containing a null element returns NULL
      When query
        """
        SELECT vector_inner_product(array(1.0F, CAST(NULL AS FLOAT), 3.0F), array(4.0F, 5.0F, 6.0F)) AS result
        """
      Then query result
        | result |
        | NULL   |

  # Spark accumulates in FLOAT (VectorFunctionImplUtils.vectorInnerProduct, Spark 4.2.0), eight
  # products at a time and then the remainder, left to right. So `1.0E8 + 1.0` stays `1.0E8` and the
  # small terms are lost: the "block order" row is 1.0, not the exact 9.0 a DOUBLE or reordered sum
  # would give. Overflow goes to Infinity and never raises, with or without ANSI.
  Rule: Float arithmetic at the extremes

    Scenario Outline: inner product at the float extremes: <case>
      When query
        """
        SELECT vector_inner_product(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | left                                                                  | right                                                                 | result    |
        | the products overflow FLOAT to Infinity        | array(1.0E20F)                                                        | array(1.0E20F)                                                        | Infinity  |
        | the largest FLOAT against itself               | array(3.4028234E38F)                                                  | array(3.4028234E38F)                                                  | Infinity  |
        | the largest FLOAT against its negation         | array(3.4028234E38F)                                                  | array(-3.4028234E38F)                                                 | -Infinity |
        | two in-range products overflow when summed     | array(3.4028234E38F, 3.4028234E38F)                                   | array(1.0F, 1.0F)                                                     | Infinity  |
        | two extreme products cancel exactly            | array(3.4028234E38F, -3.4028234E38F)                                  | array(1.0F, 1.0F)                                                     | 0.0       |
        | the smallest normal FLOAT squared underflows   | array(1.17549435E-38F)                                                | array(1.17549435E-38F)                                                | 0.0       |
        | infinity minus infinity is NaN                 | array(CAST('Infinity' AS FLOAT), CAST('-Infinity' AS FLOAT))          | array(1.0F, 1.0F)                                                     | NaN       |
        | a NaN element propagates                       | array(CAST('NaN' AS FLOAT), 1.0F)                                     | array(1.0F, 2.0F)                                                     | NaN       |
        | negative infinity against itself               | array(CAST('-Infinity' AS FLOAT))                                     | array(CAST('-Infinity' AS FLOAT))                                     | Infinity  |
        | a huge and a tiny element                      | array(1.0E30F, 1.0E-30F)                                              | array(1.0E-30F, 1.0E30F)                                              | 2.0       |
        | a negative zero element                        | array(-0.0F, 1.0F)                                                    | array(1.0F, 1.0F)                                                     | 1.0       |
        | a single element                               | array(5.0F)                                                           | array(-3.0F)                                                          | -15.0     |
        | eight elements fill exactly one unrolled block | array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F)                 | array(8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F)                 | 120.0     |
        | block order loses the small terms              | array(1.0E8F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, -1.0E8F, 1.0F) | array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F)    | 1.0       |

    # Independence from ANSI is a claim about both modes, so both are run.
    Scenario Outline: the result does not depend on ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          vector_inner_product(array(1.0E20F), array(1.0E20F)) AS overflow,
          vector_inner_product(
            array(1.0E8F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, -1.0E8F, 1.0F),
            array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F)
          ) AS block_order
        """
      Then query result
        | overflow | block_order |
        | Infinity | 1.0         |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: the same extremes through columns
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0E20F), array(1.0E20F)),
          (2, array(3.4028234E38F), array(-3.4028234E38F)),
          (3, array(CAST('-Infinity' AS FLOAT)), array(CAST('-Infinity' AS FLOAT))),
          (4, array(1.0E8F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, -1.0E8F, 1.0F),
              array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | Infinity  |
        | 2  | -Infinity |
        | 3  | Infinity  |
        | 4  | 1.0       |

  Rule: Multiple rows

    Scenario: each row is computed from its own vectors
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 0.0F), array(1.0F, 0.0F)),
          (2, array(1.0F, 0.0F), array(0.0F, 1.0F)),
          (3, array(1.0F, 2.0F), array(-1.0F, -2.0F)),
          (4, array(3.0F, 4.0F), array(4.0F, 3.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 1.0    |
        | 2  | 0.0    |
        | 3  | -5.0   |
        | 4  | 24.0   |

    Scenario: the dimension is checked per row, not once for the column
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0F), array(2.0F)),
          (2, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (3, array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (4, CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 2.0    |
        | 2  | 11.0   |
        | 3  | 32.0   |
        | 4  | 0.0    |

    Scenario: every special case resolves per row in one column
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F)),
          (3, array(1.0F, CAST(NULL AS FLOAT)), array(1.0F, 2.0F)),
          (4, CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>)),
          (5, array(0.0F, 0.0F), array(1.0F, 1.0F)),
          (6, array(CAST('NaN' AS FLOAT), 1.0F), array(1.0F, 1.0F)),
          (7, array(1.0E20F), array(1.0E20F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result   |
        | 1  | 11.0     |
        | 2  | NULL     |
        | 3  | NULL     |
        | 4  | 0.0      |
        | 5  | 0.0      |
        | 6  | NaN      |
        | 7  | Infinity |

    Scenario: a column against a literal vector
      When query
        """
        SELECT id, vector_inner_product(a, array(1.0F, 1.0F)) AS result
        FROM VALUES
          (1, array(1.0F, 0.0F)),
          (2, array(0.0F, 1.0F)),
          (3, CAST(NULL AS ARRAY<FLOAT>)),
          (4, array(3.0F, 4.0F))
        AS t(id, a)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 1.0    |
        | 2  | 1.0    |
        | 3  | NULL   |
        | 4  | 7.0    |

    Scenario: a NULL vector skips the dimension check of its row
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F, 3.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 11.0   |
        | 2  | NULL   |

    Scenario: a row with mismatched dimensions that a filter removes is never evaluated
      When query
        """
        SELECT id, vector_inner_product(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, array(1.0F), array(1.0F, 2.0F))
        AS t(id, a, b)
        WHERE id = 1
        """
      Then query result
        | id | result |
        | 1  | 11.0   |

    Scenario: a row with mismatched dimensions that a CASE guards is never evaluated
      When query
        """
        SELECT id, CASE WHEN size(a) = size(b) THEN vector_inner_product(a, b) END AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, array(1.0F), array(1.0F, 2.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 11.0   |
        | 2  | NULL   |

    Scenario: the result feeds a grouped aggregate
      When query
        """
        SELECT g, sum(vector_inner_product(a, b)) AS total
        FROM VALUES
          ('x', array(1.0F, 0.0F), array(1.0F, 0.0F)),
          ('x', array(0.0F, 1.0F), array(0.0F, 1.0F)),
          ('y', array(1.0F, 1.0F), array(-1.0F, -1.0F))
        AS t(g, a, b)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | total |
        | x | 2.0   |
        | y | -2.0  |

  # Spark renders FLOAT with Java Float.toString: scientific notation below 1.0E-3 and from 1.0E7 up.
  # The value matches; Sail prints `10000000.0`, `1e-24`. Same root cause as the rendering scenarios
  # in conditional/nullif.feature, reached here through an ordinary inner product.
  Rule: Rendering of the FLOAT result

    @sail-bug
    Scenario Outline: an inner product of <case> renders in scientific notation
      When query
        """
        SELECT vector_inner_product(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | left                                                          | right                                                       | result  |
        | exactly 1.0E7     | array(10000000.0F)                                            | array(1.0F)                                                 | 1.0E7   |
        | above 1.0E7       | array(123456.0F)                                              | array(1000.0F)                                              | 1.23456E8 |
        | just below 1.0E-3 | array(0.0009F)                                                | array(1.0F)                                                 | 9.0E-4  |
        | a tiny product    | array(1.0E-12F)                                               | array(1.0E-12F)                                             | 1.0E-24 |

  Rule: Output schema

    @function(nullability)
    Scenario: declared FLOAT output schema and nullability
      When query
        """
        SELECT vector_inner_product(array(1.0F, 2.0F), array(3.0F, 4.0F)) AS result
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

    # The result is nullable whatever the inputs: Spark replaces the expression with a StaticInvoke
    # whose returnNullable defaults to true, because a NULL element yields NULL even when neither
    # vector is NULL. Deriving it from the inputs would report `nullable = false` here.
    @function(nullability)
    Scenario: non-nullable vector columns still produce a nullable result
      When query
        """
        SELECT vector_inner_product(array(CAST(id AS FLOAT), 1.0F), array(1.0F, 1.0F)) AS result
        FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

    @function(nullability)
    Scenario: a nullable vector column produces a nullable result
      When query
        """
        SELECT vector_inner_product(a, array(1.0F, 1.0F)) AS result
        FROM VALUES (array(1.0F, 0.0F)), (CAST(NULL AS ARRAY<FLOAT>)) AS t(a)
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  # Every error below has the same verdict in Sail, only a different message: Sail answers
  # `expects two ARRAY<FLOAT> arguments, got [List(Field { ... })]` (a Rust Debug dump that does not
  # say which parameter is wrong), `requires vectors with matching dimensions`, and DataFusion's
  # signature error for the arity. The scenarios used to accept any message containing ARRAY or
  # FLOAT, so they could not tell the two engines apart.
  Rule: Error cases

    @sail-bug
    Scenario: unequal dimensions produce a dimension mismatch error
      When query
        """
        SELECT vector_inner_product(array(1.0F), array(1.0F, 2.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\] Vectors passed to `vector_inner_product` must have the same dimension, but got 1 and 2

    @sail-bug
    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_inner_product(NULL, array(1.0F, 2.0F)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_inner_product(array(1.0D, 2.0D), array(3.0D, 4.0D)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: ARRAY<INT> inputs are rejected
      When query
        """
        SELECT vector_inner_product(array(1, 2), array(3, 4)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: non-array values are rejected
      When query
        """
        SELECT vector_inner_product(1.0F, 2.0F) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    # Spark checks the left argument first, so only a wrong right argument names the second one.
    @sail-bug
    Scenario Outline: a wrong type in the second argument: <case>
      When query
        """
        SELECT vector_inner_product(array(1.0F, 2.0F), <right>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The second parameter requires the "ARRAY<FLOAT>" type

      Examples:
        | case           | right                    |
        | ARRAY<DOUBLE>  | array(1.0D, 2.0D)        |
        | ARRAY<DECIMAL> | array(1.0, 2.0)          |
        | untyped NULL   | NULL                     |
        | nested array   | array(array(1.0F, 2.0F)) |

    @sail-bug
    Scenario: when both arguments are wrong Spark reports the first one
      When query
        """
        SELECT vector_inner_product(array(1.0D), array(1)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario Outline: the wrong number of arguments: <case>
      When query
        """
        SELECT vector_inner_product(<args>) AS result
        """
      Then query error \[WRONG_NUM_ARGS\.WITHOUT_SUGGESTION\] The `vector_inner_product` requires 2 parameters but the actual number is <n>

      Examples:
        | case  | args                                  | n |
        | zero  |                                       | 0 |
        | three | array(1.0F), array(1.0F), array(1.0F) | 3 |

    # The dimension check is not ANSI-gated and runs before the empty and NULL-element checks.
    @sail-bug
    Scenario Outline: unequal dimensions still raise with ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT vector_inner_product(array(1.0F, 2.0F), array(1.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

      Examples:
        | ansi  |
        | true  |
        | false |

    @sail-bug
    Scenario: the dimension is checked before a NULL element can return NULL
      When query
        """
        SELECT vector_inner_product(array(CAST(NULL AS FLOAT)), array(1.0F, 2.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

    @sail-bug
    Scenario: the dimension is checked before an empty vector can return 0.0
      When query
        """
        SELECT vector_inner_product(CAST(array() AS ARRAY<FLOAT>), array(1.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

    @sail-bug
    Scenario: one row with unequal dimensions fails the whole query
      When query
        """
        SELECT vector_inner_product(a, b) AS result
        FROM VALUES
          (array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (array(1.0F), array(1.0F, 2.0F))
        AS t(a, b)
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]
