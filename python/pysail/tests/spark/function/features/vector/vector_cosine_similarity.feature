@spark-4.2
Feature: vector_cosine_similarity

  Rule: Basic results

    Scenario: basic, identical, orthogonal, and opposite vectors
      When query
        """
        SELECT
          vector_cosine_similarity(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)) AS basic,
          vector_cosine_similarity(array(1.0F, 0.0F), array(1.0F, 0.0F)) AS identical,
          vector_cosine_similarity(array(1.0F, 0.0F), array(0.0F, 1.0F)) AS orthogonal,
          vector_cosine_similarity(array(1.0F, 0.0F), array(-1.0F, 0.0F)) AS opposite
        """
      Then query result
        | basic     | identical | orthogonal | opposite |
        | 0.9746319 | 1.0       | 0.0        | -1.0     |

    Scenario: vector columns from VALUES execute the UDF
      When query
        """
        SELECT vector_cosine_similarity(left_vector, right_vector) AS result
        FROM VALUES
          (array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (array(1.0F, 0.0F), array(0.0F, 1.0F)),
          (array(1.0F, 0.0F), array(-1.0F, 0.0F))
        AS t(left_vector, right_vector)
        """
      Then query result
        | result    |
        | 0.9746319 |
        | 0.0       |
        | -1.0      |

    Scenario: a 16-element vector exercises the unrolled accumulation path
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)
        ) AS result
        """
      Then query result
        | result |
        | 1.0    |

  Rule: Null handling

    Scenario: empty and zero-magnitude vectors return NULL
      When query
        """
        SELECT
          vector_cosine_similarity(
            CAST(array() AS ARRAY<FLOAT>),
            CAST(array() AS ARRAY<FLOAT>)
          ) AS empty,
          vector_cosine_similarity(array(0.0F, 0.0F), array(1.0F, 2.0F)) AS zero_magnitude
        """
      Then query result
        | empty | zero_magnitude |
        | NULL  | NULL           |

    Scenario: null vectors return NULL
      When query
        """
        SELECT vector_cosine_similarity(
          CAST(NULL AS ARRAY<FLOAT>),
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: an array containing a null element returns NULL
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0F, CAST(NULL AS FLOAT), 3.0F),
          array(1.0F, 2.0F, 3.0F)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

  # Spark accumulates in FLOAT, not DOUBLE (VectorFunctionImplUtils.vectorCosineSimilarity, Spark 4.2.0):
  # `norm1Sq * norm2Sq` is a float multiply that can overflow to Infinity or underflow to 0, and a
  # norm product below Float.MIN_NORMAL returns NULL. A DOUBLE implementation would return 1.0 for
  # the first three rows below, so they pin the float arithmetic, not just the formula.
  Rule: Float arithmetic at the extremes

    Scenario Outline: cosine at the float extremes: <case>
      When query
        """
        SELECT vector_cosine_similarity(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | left                                                           | right                                     | result     |
        | squared norms overflow FLOAT to NaN              | array(1.0E20F)                                                 | array(1.0E20F)                            | NaN        |
        | the largest FLOAT against itself is NaN          | array(3.4028234E38F)                                           | array(3.4028234E38F)                      | NaN        |
        | the product of squared norms underflows to NULL  | array(1.0E-12F)                                                | array(1.0E-12F)                           | NULL       |
        | the smallest normal FLOAT against itself is NULL | array(1.17549435E-38F)                                         | array(1.17549435E-38F)                    | NULL       |
        | infinity minus infinity is NaN                   | array(CAST('Infinity' AS FLOAT), CAST('-Infinity' AS FLOAT))   | array(1.0F, 1.0F)                         | NaN        |
        | a NaN element propagates                         | array(CAST('NaN' AS FLOAT), 1.0F)                              | array(1.0F, 2.0F)                         | NaN        |
        | a huge and a tiny element cancel out             | array(1.0E30F, 1.0E-30F)                                       | array(1.0E-30F, 1.0E30F)                  | 0.0        |
        | a negative zero element                          | array(-0.0F, 1.0F)                                             | array(1.0F, 1.0F)                         | 0.70710677 |
        | a single element                                 | array(5.0F)                                                    | array(-3.0F)                              | -1.0       |
        | eight elements fill exactly one unrolled block   | array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F)          | array(8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F) | 0.5882353  |
        | nine elements add a remainder after the block    | array(1.0E8F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F)  | array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F) | 0.33333334 |

    Scenario: the result does not depend on ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          vector_cosine_similarity(array(1.0E20F), array(1.0E20F)) AS overflow,
          vector_cosine_similarity(array(1.0E-12F), array(1.0E-12F)) AS underflow
        """
      Then query result
        | overflow | underflow |
        | NaN      | NULL      |

    Scenario: the same extremes through columns
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (1, array(1.0E20F), array(1.0E20F)),
          (2, array(1.0E-12F), array(1.0E-12F)),
          (3, array(3.4028234E38F), array(-3.4028234E38F)),
          (4, array(CAST('-Infinity' AS FLOAT)), array(CAST('-Infinity' AS FLOAT)))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | NaN    |
        | 2  | NULL   |
        | 3  | NaN    |
        | 4  | NaN    |

  Rule: Multiple rows

    Scenario: each row is computed from its own vectors
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
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
        | 3  | -1.0   |
        | 4  | 0.96   |

    Scenario: the dimension is checked per row, not once for the column
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (1, array(1.0F), array(2.0F)),
          (2, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (3, array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (4, CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | 1.0       |
        | 2  | 0.9838699 |
        | 3  | 0.9746319 |
        | 4  | NULL      |

    Scenario: every NULL-producing case resolves per row in one column
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F)),
          (3, array(1.0F, CAST(NULL AS FLOAT)), array(1.0F, 2.0F)),
          (4, CAST(array() AS ARRAY<FLOAT>), CAST(array() AS ARRAY<FLOAT>)),
          (5, array(0.0F, 0.0F), array(1.0F, 1.0F)),
          (6, array(CAST('NaN' AS FLOAT), 1.0F), array(1.0F, 1.0F)),
          (7, array(1.0E20F), array(1.0E20F)),
          (8, array(1.0E-12F), array(1.0E-12F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | 0.9838699 |
        | 2  | NULL      |
        | 3  | NULL      |
        | 4  | NULL      |
        | 5  | NULL      |
        | 6  | NaN       |
        | 7  | NaN       |
        | 8  | NULL      |

    Scenario: a column against a literal vector
      When query
        """
        SELECT id, vector_cosine_similarity(a, array(1.0F, 1.0F)) AS result
        FROM VALUES
          (1, array(1.0F, 0.0F)),
          (2, array(0.0F, 1.0F)),
          (3, CAST(NULL AS ARRAY<FLOAT>)),
          (4, array(3.0F, 4.0F))
        AS t(id, a)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 1  | 0.70710677 |
        | 2  | 0.70710677 |
        | 3  | NULL       |
        | 4  | 0.98994946 |

    Scenario: a NULL vector skips the dimension check of its row
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F, 3.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | 0.9838699 |
        | 2  | NULL      |

    Scenario: a row with mismatched dimensions that a filter removes is never evaluated
      When query
        """
        SELECT id, vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, array(1.0F), array(1.0F, 2.0F))
        AS t(id, a, b)
        WHERE id = 1
        """
      Then query result
        | id | result    |
        | 1  | 0.9838699 |

    Scenario: a row with mismatched dimensions that a CASE guards is never evaluated
      When query
        """
        SELECT id, CASE WHEN size(a) = size(b) THEN vector_cosine_similarity(a, b) END AS result
        FROM VALUES
          (1, array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (2, array(1.0F), array(1.0F, 2.0F))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | 0.9838699 |
        | 2  | NULL      |

    Scenario: the result feeds a grouped aggregate
      When query
        """
        SELECT g, sum(vector_cosine_similarity(a, b)) AS total
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
        | y | -1.0  |

  # Spark renders FLOAT with Java Float.toString: scientific notation below 1.0E-3 and from 1.0E7 up.
  # The value matches; Sail prints `2.236068e-9`. Same root cause as the rendering scenarios in
  # conditional/nullif.feature, reached here through an ordinary cosine result.
  Rule: Rendering of the FLOAT result

    @sail-bug
    Scenario: a cosine below 1.0E-3 renders in scientific notation
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0E8F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, -1.0E8F, 1.0F),
          array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F)
        ) AS result
        """
      Then query result
        | result      |
        | 2.236068E-9 |

  Rule: Output schema

    @function(nullability)
    Scenario: declared FLOAT output schema and nullability
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0F, 2.0F),
          array(3.0F, 4.0F)
        ) AS result
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

    # The result is nullable whatever the inputs: Spark replaces the expression with a StaticInvoke
    # whose returnNullable defaults to true, because a zero norm or a NULL element yields NULL even
    # when neither vector is NULL. Deriving it from the inputs would report `nullable = false` here.
    @function(nullability)
    Scenario: non-nullable vector columns still produce a nullable result
      When query
        """
        SELECT vector_cosine_similarity(array(CAST(id AS FLOAT), 1.0F), array(1.0F, 1.0F)) AS result
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
        SELECT vector_cosine_similarity(a, array(1.0F, 1.0F)) AS result
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
    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_cosine_similarity(
          NULL,
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: unequal dimensions produce a dimension mismatch error
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0F),
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\] Vectors passed to `vector_cosine_similarity` must have the same dimension, but got 1 and 2

    @sail-bug
    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0D, 2.0D),
          array(3.0D, 4.0D)
        ) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: ARRAY<INT> inputs are rejected
      When query
        """
        SELECT vector_cosine_similarity(
          array(1, 2),
          array(3, 4)
        ) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario: non-array values are rejected
      When query
        """
        SELECT vector_cosine_similarity(1.0F, 2.0F) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    # Spark checks the left argument first, so only a wrong right argument names the second one.
    @sail-bug
    Scenario Outline: a wrong type in the second argument: <case>
      When query
        """
        SELECT vector_cosine_similarity(array(1.0F, 2.0F), <right>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The second parameter requires the "ARRAY<FLOAT>" type

      Examples:
        | case           | right                     |
        | ARRAY<DOUBLE>  | array(1.0D, 2.0D)         |
        | ARRAY<DECIMAL> | array(1.0, 2.0)           |
        | untyped NULL   | NULL                      |
        | nested array   | array(array(1.0F, 2.0F))  |

    @sail-bug
    Scenario: when both arguments are wrong Spark reports the first one
      When query
        """
        SELECT vector_cosine_similarity(array(1.0D), array(1)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\].*The first parameter requires the "ARRAY<FLOAT>" type

    @sail-bug
    Scenario Outline: the wrong number of arguments: <case>
      When query
        """
        SELECT vector_cosine_similarity(<args>) AS result
        """
      Then query error \[WRONG_NUM_ARGS\.WITHOUT_SUGGESTION\] The `vector_cosine_similarity` requires 2 parameters but the actual number is <n>

      Examples:
        | case  | args                                     | n |
        | zero  |                                          | 0 |
        | three | array(1.0F), array(1.0F), array(1.0F)    | 3 |

    # The dimension check is not ANSI-gated and runs before the empty and NULL-element checks.
    @sail-bug
    Scenario: unequal dimensions still raise with ANSI mode off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT vector_cosine_similarity(array(1.0F, 2.0F), array(1.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

    @sail-bug
    Scenario: the dimension is checked before a NULL element can return NULL
      When query
        """
        SELECT vector_cosine_similarity(array(CAST(NULL AS FLOAT)), array(1.0F, 2.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

    @sail-bug
    Scenario: the dimension is checked before an empty vector can return NULL
      When query
        """
        SELECT vector_cosine_similarity(CAST(array() AS ARRAY<FLOAT>), array(1.0F)) AS result
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]

    @sail-bug
    Scenario: one row with unequal dimensions fails the whole query
      When query
        """
        SELECT vector_cosine_similarity(a, b) AS result
        FROM VALUES
          (array(1.0F, 2.0F), array(3.0F, 4.0F)),
          (array(1.0F), array(1.0F, 2.0F))
        AS t(a, b)
        """
      Then query error \[VECTOR_DIMENSION_MISMATCH\]
