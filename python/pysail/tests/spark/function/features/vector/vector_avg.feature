@spark-4.2
Feature: vector_avg

  Rule: Element-wise mean

    Scenario: averages the vectors in a group
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.0F, 2.0F)), (array(3.0F, 4.0F)) AS tab(col)
        """
      Then query result
        | result     |
        | [2.0, 3.0] |

    Scenario: averages per group
      When query
        """
        SELECT k, vector_avg(v) AS result
        FROM VALUES
          ('a', array(1.0F, 2.0F)),
          ('a', array(3.0F, 4.0F)),
          ('b', array(10.0F, 20.0F))
        AS t(k, v)
        GROUP BY k
        ORDER BY k
        """
      Then query result
        | k | result       |
        | a | [2.0, 3.0]   |
        | b | [10.0, 20.0] |

    Scenario: signed vectors keep their signs
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(-3.0F, 4.0F)), (array(-5.0F, -4.0F)) AS tab(col)
        """
      Then query result
        | result      |
        | [-4.0, 0.0] |

    Scenario: a single vector is returned unchanged
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.5F, -2.5F, 3.0F)) AS tab(col)
        """
      Then query result
        | result           |
        | [1.5, -2.5, 3.0] |

    Scenario: empty vectors average to an empty vector
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (CAST(array() AS ARRAY<FLOAT>)), (CAST(array() AS ARRAY<FLOAT>)) AS tab(col)
        """
      Then query result
        | result |
        | []     |

  Rule: Precision

    Scenario: a long ordered sequence follows Spark's single-precision running mean
      # The running mean depends on accumulation order, so an ordered window
      # keeps the result deterministic. A sum / count mean would give 5.05.
      When query
        """
        SELECT result FROM (
          SELECT
            id,
            vector_avg(array(CAST(id AS FLOAT) * 0.1F, CAST(id AS FLOAT))) OVER (ORDER BY id) AS result
          FROM range(1, 101)
        )
        WHERE id = 100
        """
      Then query result
        | result            |
        | [5.0500016, 50.5] |

    Scenario: a sliding window recomputes the running mean over the frame
      # Removing the rows that leave the frame from the mean would round
      # differently. A sum / count mean would give 15.05.
      When query
        """
        SELECT result FROM (
          SELECT
            id,
            vector_avg(array(CAST(id AS FLOAT) * 0.1F)) OVER (
              ORDER BY id ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
            ) AS result
          FROM range(1, 201)
        )
        WHERE id = 200
        """
      Then query result
        | result      |
        | [15.050018] |

  Rule: Null handling

    Scenario: null vectors and vectors with null elements are skipped
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (array(1.0F, CAST(NULL AS FLOAT))),
          (array(2.0F, 4.0F)),
          (array(4.0F, 8.0F))
        AS tab(col)
        """
      Then query result
        | result     |
        | [3.0, 6.0] |

    Scenario: a group without valid vectors returns NULL
      When query
        """
        SELECT
          vector_avg(CAST(NULL AS ARRAY<FLOAT>)) AS null_vector,
          vector_avg(array(1.0F, CAST(NULL AS FLOAT))) AS null_element
        """
      Then query result
        | null_vector | null_element |
        | NULL        | NULL         |

    Scenario: an empty input returns NULL
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.0F, 2.0F)) AS tab(col)
        WHERE false
        """
      Then query result
        | result |
        | NULL   |

  Rule: Window usage

    Scenario: running average over an ordered window
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id) AS result
        FROM VALUES (1, array(1.0F, 2.0F)), (2, array(3.0F, 4.0F)), (3, array(5.0F, 6.0F)) AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | [1.0, 2.0] |
        | 2  | [2.0, 3.0] |
        | 3  | [3.0, 4.0] |

    Scenario: average over a window of the previous and current rows
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, array(1.0F, 2.0F)), (2, array(3.0F, 4.0F)), (3, array(5.0F, 6.0F)) AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | [1.0, 2.0] |
        | 2  | [2.0, 3.0] |
        | 3  | [4.0, 5.0] |

    Scenario: average over a window that shrinks from the front
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS result
        FROM VALUES (1, array(1.0F, 2.0F)), (2, array(3.0F, 4.0F)), (3, array(5.0F, 6.0F)) AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | [3.0, 4.0] |
        | 2  | [4.0, 5.0] |
        | 3  | [5.0, 6.0] |

    Scenario: skipped vectors enter and leave a sliding window
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES
          (1, array(1.0F, 2.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>)),
          (3, array(10.0F, CAST(NULL AS FLOAT))),
          (4, array(3.0F, 4.0F)),
          (5, array(5.0F, 6.0F))
        AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | [1.0, 2.0] |
        | 2  | [1.0, 2.0] |
        | 3  | NULL       |
        | 4  | [3.0, 4.0] |
        | 5  | [4.0, 5.0] |

    Scenario: an empty range frame returns NULL
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) AS result
        FROM VALUES (1, array(1.0F, 2.0F)), (2, array(3.0F, 4.0F)), (5, array(5.0F, 6.0F)) AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | NULL       |
        | 2  | [1.0, 2.0] |
        | 5  | NULL       |

    Scenario: vectors with different dimensions in separate frames are accepted
      When query
        """
        SELECT id, vector_avg(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS result
        FROM VALUES (1, array(1.0F, 2.0F)), (2, array(3.0F)) AS t(id, v)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 1  | [1.0, 2.0] |
        | 2  | [3.0]      |

  Rule: Output schema

    @function(nullability)
    Scenario: declared ARRAY<FLOAT> output schema and nullability
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.0F, 2.0F)) AS tab(col)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: float (containsNull = false)
        """

  Rule: Error cases

    Scenario: vectors with different dimensions are rejected
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.0F, 2.0F)), (array(3.0F)) AS tab(col)
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions)

    Scenario: ARRAY<DOUBLE> input is rejected
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (array(1.0D, 2.0D)) AS tab(col)
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_avg)

    Scenario: non-array input is rejected
      When query
        """
        SELECT vector_avg(col) AS result
        FROM VALUES (1.0F) AS tab(col)
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_avg)
