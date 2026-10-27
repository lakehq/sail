@spark-4.2
Feature: vector_norm

  Rule: Supported norms

    Scenario: default, L1, L2, and infinity norms
      When query
        """
        SELECT
          vector_norm(array(3.0F, 4.0F)) AS default_norm,
          vector_norm(array(-3.0F, 4.0F), 1.0F) AS l1_norm,
          vector_norm(array(3.0F, 4.0F), 2.0F) AS l2_norm,
          vector_norm(array(-3.0F, 4.0F), float('inf')) AS infinity_norm
        """
      Then query result
        | default_norm | l1_norm | l2_norm | infinity_norm |
        | 5.0          | 7.0     | 5.0     | 4.0           |

    Scenario: vector columns execute the UDF
      When query
        """
        SELECT
          vector_norm(vector, 1.0F) AS l1_norm,
          vector_norm(vector, 2.0F) AS l2_norm
        FROM VALUES
          (array(3.0F, 4.0F)),
          (array(5.0F, 12.0F))
        AS t(vector)
        """
      Then query result
        | l1_norm | l2_norm |
        | 7.0     | 5.0     |
        | 17.0    | 13.0    |

    Scenario: empty vectors return 0.0
      When query
        """
        SELECT
          vector_norm(CAST(array() AS ARRAY<FLOAT>)) AS default_norm,
          vector_norm(CAST(array() AS ARRAY<FLOAT>), 1.0F) AS l1_norm,
          vector_norm(CAST(array() AS ARRAY<FLOAT>), float('inf')) AS infinity_norm
        """
      Then query result
        | default_norm | l1_norm | infinity_norm |
        | 0.0          | 0.0     | 0.0           |

    Scenario: a 16-element vector exercises the unrolled accumulation path
      When query
        """
        SELECT vector_norm(
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)
        ) AS result
        """
      Then query result
        | result    |
        | 38.678158 |

  Rule: Null handling

    Scenario: null vector, element, and degree return NULL
      When query
        """
        SELECT
          vector_norm(CAST(NULL AS ARRAY<FLOAT>), 2.0F) AS null_vector,
          vector_norm(array(1.0F, CAST(NULL AS FLOAT)), 2.0F) AS null_element,
          vector_norm(array(1.0F, 2.0F), CAST(NULL AS FLOAT)) AS null_degree
        """
      Then query result
        | null_vector | null_element | null_degree |
        | NULL        | NULL         | NULL        |

  Rule: Output schema

    @function(nullability)
    Scenario: declared FLOAT output schema and nullability
      When query
        """
        SELECT vector_norm(array(3.0F, 4.0F)) AS result
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Error cases

    Scenario: unsupported degree is rejected
      When query
        """
        SELECT vector_norm(array(1.0F, 2.0F), 3.0F) AS result
        """
      Then query error (?i)(INVALID_VECTOR_NORM_DEGREE|degree must be)

    Scenario: ARRAY<DOUBLE> input is rejected
      When query
        """
        SELECT vector_norm(array(1.0D, 2.0D)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)

    Scenario: non-FLOAT degree is rejected
      When query
        """
        SELECT vector_norm(array(1.0F, 2.0F), 2.0D) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|FLOAT|vector_norm)

    Scenario: non-array input is rejected
      When query
        """
        SELECT vector_norm(1.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)
