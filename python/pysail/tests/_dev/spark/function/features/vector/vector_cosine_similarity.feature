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

  Rule: Error cases

    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_cosine_similarity(
          NULL,
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_cosine_similarity)

    Scenario: unequal dimensions produce a dimension mismatch error
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0F),
          array(1.0F, 2.0F)
        ) AS result
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions|dimension)

    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_cosine_similarity(
          array(1.0D, 2.0D),
          array(3.0D, 4.0D)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_cosine_similarity)

    Scenario: ARRAY<INT> inputs are rejected
      When query
        """
        SELECT vector_cosine_similarity(
          array(1, 2),
          array(3, 4)
        ) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_cosine_similarity)

    Scenario: non-array values are rejected
      When query
        """
        SELECT vector_cosine_similarity(1.0F, 2.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_cosine_similarity)
