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

  Rule: Error cases

    Scenario: unequal dimensions produce a dimension mismatch error
      When query
        """
        SELECT vector_inner_product(array(1.0F), array(1.0F, 2.0F)) AS result
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions|dimension)

    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_inner_product(NULL, array(1.0F, 2.0F)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_inner_product)

    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_inner_product(array(1.0D, 2.0D), array(3.0D, 4.0D)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_inner_product)

    Scenario: ARRAY<INT> inputs are rejected
      When query
        """
        SELECT vector_inner_product(array(1, 2), array(3, 4)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_inner_product)

    Scenario: non-array values are rejected
      When query
        """
        SELECT vector_inner_product(1.0F, 2.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_inner_product)
