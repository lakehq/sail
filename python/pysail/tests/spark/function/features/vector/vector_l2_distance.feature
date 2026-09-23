@spark-4.2
Feature: vector_l2_distance

  Rule: Basic results

    Scenario: basic, identical, and signed vectors
      When query
        """
        SELECT
          vector_l2_distance(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)) AS basic,
          vector_l2_distance(array(1.0F, 2.0F), array(1.0F, 2.0F)) AS identical,
          vector_l2_distance(array(1.0F, -2.0F), array(-2.0F, 2.0F)) AS signed
        """
      Then query result
        | basic    | identical | signed |
        | 5.196152 | 0.0       | 5.0    |

    Scenario: vector columns from VALUES execute the UDF
      When query
        """
        SELECT vector_l2_distance(left_vector, right_vector) AS result
        FROM VALUES
          (array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (array(1.0F, 2.0F), array(1.0F, 2.0F)),
          (array(1.0F, -2.0F), array(-2.0F, 2.0F))
        AS t(left_vector, right_vector)
        """
      Then query result
        | result   |
        | 5.196152 |
        | 0.0      |
        | 5.0      |

    Scenario: empty ARRAY<FLOAT> inputs return 0.0
      When query
        """
        SELECT vector_l2_distance(
          CAST(array() AS ARRAY<FLOAT>),
          CAST(array() AS ARRAY<FLOAT>)
        ) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: a 16-element vector exercises the unrolled accumulation path
      When query
        """
        SELECT vector_l2_distance(
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
          array(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F)
        ) AS result
        """
      Then query result
        | result    |
        | 38.678158 |

  Rule: Null handling

    Scenario: typed null vector returns NULL
      When query
        """
        SELECT vector_l2_distance(
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
        SELECT vector_l2_distance(
          array(1.0F, CAST(NULL AS FLOAT), 3.0F),
          array(1.0F, 2.0F, 3.0F)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Output schema

    @function(nullability)
    Scenario: declared FLOAT output schema and nullability
      When query
        """
        SELECT vector_l2_distance(
          array(1.0F, 2.0F),
          array(3.0F, 4.0F)
        ) AS result
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Error cases

    Scenario: unequal dimensions produce a dimension mismatch error
      When query
        """
        SELECT vector_l2_distance(
          array(1.0F),
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions|dimension)

    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_l2_distance(
          NULL,
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_l2_distance)

    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_l2_distance(
          array(1.0D, 2.0D),
          array(3.0D, 4.0D)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_l2_distance)

    Scenario: ARRAY<INT> inputs are rejected
      When query
        """
        SELECT vector_l2_distance(
          array(1, 2),
          array(3, 4)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_l2_distance)

    Scenario: non-array values are rejected
      When query
        """
        SELECT vector_l2_distance(1.0F, 2.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_l2_distance)

  # The axes its two siblings already carry and this file did not: the float extremes, several rows
  # in one batch, and how the FLOAT result is printed. The value is right in every one of these --
  # only the printing is not.
  Rule: Float arithmetic at the extremes

    Scenario Outline: the distance at the float extremes: <case>
      When query template
        """
        SELECT vector_l2_distance(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | left                                                  | right                                                 | result    |
        | the squared difference overflows FLOAT         | array(1.0E20F)                                        | array(-1.0E20F)                                       | Infinity  |
        | the largest FLOAT against its negative         | array(3.4028234E38F)                                  | array(-3.4028234E38F)                                 | Infinity  |
        | the smallest normal FLOAT against zero         | array(1.17549435E-38F)                                | array(0.0F)                                           | 0.0       |
        | infinity minus infinity is NaN                 | array(CAST('Infinity' AS FLOAT))                      | array(CAST('Infinity' AS FLOAT))                      | NaN       |
        | a NaN element propagates                       | array(CAST('NaN' AS FLOAT), 1.0F)                     | array(1.0F, 2.0F)                                     | NaN       |
        | a negative zero element                        | array(-0.0F, 1.0F)                                    | array(0.0F, 1.0F)                                     | 0.0       |
        | identical vectors are at distance zero         | array(1.0F, 2.0F)                                     | array(1.0F, 2.0F)                                     | 0.0       |
        | a single element                               | array(5.0F)                                           | array(-3.0F)                                          | 8.0       |
        | eight elements fill exactly one unrolled block | array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F) | array(8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F, 1.0F) | 12.961481 |
        | nine elements add a remainder after the block  | array(1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F, 1.0F) | array(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F) | 3.0 |

    # Independence from ANSI is a claim about both modes, so both are run.
    Scenario Outline: the result does not depend on ANSI mode <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          vector_l2_distance(array(1.0E20F), array(-1.0E20F)) AS overflow,
          vector_l2_distance(array(1.17549435E-38F), array(0.0F)) AS underflow
        """
      Then query result
        | overflow | underflow |
        | Infinity | 0.0       |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Multiple rows

    Scenario: the distance is computed per row and NULL travels with its row
      When query
        """
        SELECT vector_l2_distance(a, b) AS result
        FROM VALUES (array(1.0F), array(2.0F)),
                    (array(3.0F), array(3.0F)),
                    (CAST(NULL AS ARRAY<FLOAT>), array(1.0F)) AS t(a, b)
        """
      Then query result ordered
        | result |
        | 1.0    |
        | 0.0    |
        | NULL   |

  # Java prints a FLOAT below 1e-3 with an uppercase exponent; Sail writes a lowercase one. The
  # value is identical, so only a lens that looks at the printed form can see it.
  Rule: Rendering of the FLOAT result

    @sail-bug
    Scenario Outline: a distance below 1.0E-3 renders with an uppercase exponent: <case>
      When query template
        """
        SELECT vector_l2_distance(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | left           | right          | result       |
        | an underflowing value   | array(1.0E-20F) | array(2.0E-20F) | 9.999973E-21 |
        | a tiny difference      | array(1.0F)    | array(1.000001F) | 9.536743E-7 |
