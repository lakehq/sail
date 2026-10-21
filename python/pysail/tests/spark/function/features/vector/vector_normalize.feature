@spark-4.2
Feature: vector_normalize

  Rule: Supported norms

    Scenario: default, L1, L2, and infinity norms
      When query
        """
        SELECT
          vector_normalize(array(3.0F, 4.0F)) AS default_norm,
          vector_normalize(array(3.0F, 4.0F), 1.0F) AS l1_norm,
          vector_normalize(array(3.0F, 4.0F), 2.0F) AS l2_norm,
          vector_normalize(array(3.0F, 4.0F), float('inf')) AS infinity_norm
        """
      Then query result
        | default_norm | l1_norm                 | l2_norm    | infinity_norm |
        | [0.6, 0.8]   | [0.42857143, 0.5714286] | [0.6, 0.8] | [0.75, 1.0]   |

    Scenario: signed vectors keep their signs
      When query
        """
        SELECT
          vector_normalize(array(-3.0F, 4.0F), 1.0F) AS l1_norm,
          vector_normalize(array(-3.0F, 4.0F), 2.0F) AS l2_norm,
          vector_normalize(array(-3.0F, 4.0F), float('inf')) AS infinity_norm
        """
      Then query result
        | l1_norm                  | l2_norm     | infinity_norm |
        | [-0.42857143, 0.5714286] | [-0.6, 0.8] | [-0.75, 1.0]  |

    Scenario: vector columns execute the UDF
      When query
        """
        SELECT
          vector_normalize(vector) AS default_norm,
          vector_normalize(vector, 1.0F) AS l1_norm
        FROM VALUES
          (array(3.0F, 4.0F)),
          (array(5.0F, 12.0F))
        AS t(vector)
        """
      Then query result
        | default_norm           | l1_norm                 |
        | [0.6, 0.8]             | [0.42857143, 0.5714286] |
        | [0.3846154, 0.9230769] | [0.29411766, 0.7058824] |

    Scenario: empty vectors are returned unchanged
      When query
        """
        SELECT
          vector_normalize(CAST(array() AS ARRAY<FLOAT>)) AS default_norm,
          vector_normalize(CAST(array() AS ARRAY<FLOAT>), 1.0F) AS l1_norm,
          vector_normalize(CAST(array() AS ARRAY<FLOAT>), float('inf')) AS infinity_norm
        """
      Then query result
        | default_norm | l1_norm | infinity_norm |
        | []           | []      | []            |

    Scenario: a 16-element vector exercises the unrolled accumulation path
      When query
        """
        SELECT element_at(
          vector_normalize(
            array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)
          ),
          16
        ) AS result
        """
      Then query result
        | result     |
        | 0.41367015 |

  Rule: Null handling

    Scenario: null vector, element, and degree return NULL
      When query
        """
        SELECT
          vector_normalize(CAST(NULL AS ARRAY<FLOAT>), 2.0F) AS null_vector,
          vector_normalize(array(1.0F, CAST(NULL AS FLOAT)), 2.0F) AS null_element,
          vector_normalize(array(1.0F, 2.0F), CAST(NULL AS FLOAT)) AS null_degree
        """
      Then query result
        | null_vector | null_element | null_degree |
        | NULL        | NULL         | NULL        |

    Scenario: a zero vector returns NULL instead of dividing by zero
      When query
        """
        SELECT
          vector_normalize(array(0.0F, 0.0F)) AS zero_l2,
          vector_normalize(array(0.0F, 0.0F), 1.0F) AS zero_l1,
          vector_normalize(array(0.0F, 0.0F), float('inf')) AS zero_infinity,
          vector_normalize(array(1.0e-39F, 0.0F)) AS subnormal
        """
      Then query result
        | zero_l2 | zero_l1 | zero_infinity | subnormal |
        | NULL    | NULL    | NULL          | NULL      |

  Rule: Extreme values

    Scenario: an overflowing norm maps every element to 0.0
      When query
        """
        SELECT vector_normalize(array(3.0e19F, 4.0e19F)) AS result
        """
      Then query result
        | result     |
        | [0.0, 0.0] |

    Scenario: NaN propagates through the infinity norm
      When query
        """
        SELECT vector_normalize(array(float('nan'), 1.0F), float('inf')) AS result
        """
      Then query result
        | result     |
        | [NaN, NaN] |

  Rule: Output schema

    @function(nullability)
    Scenario: declared ARRAY<FLOAT> output schema and nullability
      When query
        """
        SELECT vector_normalize(array(3.0F, 4.0F)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: float (containsNull = true)
        """

  Rule: Error cases

    Scenario: unsupported degree is rejected
      When query
        """
        SELECT vector_normalize(array(1.0F, 2.0F), 3.0F) AS result
        """
      Then query error (?i)(INVALID_VECTOR_NORM_DEGREE|degree must be)

    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_normalize(NULL) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_normalize)

    Scenario: ARRAY<DOUBLE> input is rejected
      When query
        """
        SELECT vector_normalize(array(1.0D, 2.0D)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_normalize)

    Scenario: ARRAY<INT> input is rejected
      When query
        """
        SELECT vector_normalize(array(1, 2)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_normalize)

    Scenario: non-FLOAT degree is rejected
      When query
        """
        SELECT vector_normalize(array(1.0F, 2.0F), 2.0D) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|FLOAT|vector_normalize)

    Scenario: non-array input is rejected
      When query
        """
        SELECT vector_normalize(1.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_normalize)
