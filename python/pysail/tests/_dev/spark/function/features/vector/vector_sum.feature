@spark-4.2
Feature: vector_sum

  Rule: Aggregate results

    Scenario: sums float vectors by group
      When query
        """
        SELECT k, vector_sum(v) AS total
        FROM VALUES
          (1, array(1.0F, 2.0F)),
          (1, array(3.0F, 4.0F)),
          (2, array(-1.0F, 2.0F))
        AS t(k, v)
        GROUP BY k ORDER BY k
        """
      Then query result ordered
        | k | total      |
        | 1 | [4.0, 6.0]  |
        | 2 | [-1.0, 2.0] |

    Scenario: valid empty vectors produce an empty array
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES
          (CAST(array() AS ARRAY<FLOAT>)),
          (CAST(array() AS ARRAY<FLOAT>))
        AS t(v)
        """
      Then query result
        | total |
        | []    |

    Scenario: distinct vectors are counted only once alongside an ordinary sum
      When query
        """
        SELECT vector_sum(DISTINCT v) AS distinct_total, vector_sum(v) AS total
        FROM VALUES
          (array(1.0F, 2.0F)),
          (array(1.0F, 2.0F)),
          (array(3.0F, 4.0F))
        AS t(v)
        """
      Then query result
        | distinct_total | total       |
        | [4.0, 6.0]     | [5.0, 8.0]  |

    Scenario: partitioned running window sum skips null vectors
      When query
        """
        SELECT k, n,
          vector_sum(v) OVER (
            PARTITION BY k ORDER BY n
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS total
        FROM VALUES
          (1, 1, array(1.0F, 2.0F)),
          (1, 2, array(3.0F, 4.0F)),
          (2, 1, array(5.0F, 6.0F)),
          (2, 2, CAST(NULL AS ARRAY<FLOAT>))
        AS t(k, n, v)
        ORDER BY k, n
        """
      Then query result ordered
        | k | n | total      |
        | 1 | 1 | [1.0, 2.0] |
        | 1 | 2 | [4.0, 6.0] |
        | 2 | 1 | [5.0, 6.0] |
        | 2 | 2 | [5.0, 6.0] |

    Scenario: bounded window removes departing vectors
      When query
        """
        SELECT n,
          vector_sum(v) OVER (
            ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
          ) AS total
        FROM VALUES
          (1, array(1.0F)),
          (2, array(2.0F)),
          (3, array(3.0F))
        AS t(n, v)
        ORDER BY n
        """
      Then query result ordered
        | n | total |
        | 1 | [1.0] |
        | 2 | [3.0] |
        | 3 | [5.0] |

    Scenario: bounded window returns NULL when only null vectors or no rows remain
      When query
        """
        SELECT n,
          vector_sum(v) OVER (
            ORDER BY n ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
          ) AS total
        FROM VALUES
          (1, array(1.0F)),
          (2, CAST(NULL AS ARRAY<FLOAT>)),
          (3, array(3.0F)),
          (4, CAST(NULL AS ARRAY<FLOAT>))
        AS t(n, v)
        ORDER BY n
        """
      Then query result ordered
        | n | total |
        | 1 | [3.0] |
        | 2 | [3.0] |
        | 3 | NULL  |
        | 4 | NULL  |

    Scenario: bounded window validates dimensions only within the current frame
      When query
        """
        SELECT n,
          vector_sum(v) OVER (
            ORDER BY n ROWS BETWEEN CURRENT ROW AND CURRENT ROW
          ) AS total
        FROM VALUES
          (1, array(1.0F)),
          (2, array(2.0F, 3.0F)),
          (3, CAST(array() AS ARRAY<FLOAT>))
        AS t(n, v)
        ORDER BY n
        """
      Then query result ordered
        | n | total      |
        | 1 | [1.0]      |
        | 2 | [2.0, 3.0] |
        | 3 | []         |

  Rule: Null handling

    Scenario: null vectors and vectors containing null elements are skipped
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (array(9.0F, CAST(NULL AS FLOAT), 10.0F)),
          (array(1.0F, 2.0F)),
          (array(3.0F, 4.0F))
        AS t(v)
        """
      Then query result
        | total     |
        | [4.0, 6.0] |

    Scenario: a group without valid vectors returns NULL
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (array(CAST(NULL AS FLOAT)))
        AS t(v)
        """
      Then query result
        | total |
        | NULL  |

    Scenario: distinct aggregation skips null vectors but keeps an empty vector
      When query
        """
        SELECT vector_sum(DISTINCT v) AS total
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (array(CAST(NULL AS FLOAT))),
          (CAST(array() AS ARRAY<FLOAT>)),
          (CAST(array() AS ARRAY<FLOAT>))
        AS t(v)
        """
      Then query result
        | total |
        | []    |

    Scenario: an empty group returns NULL
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES (array(1.0F)) AS t(v)
        WHERE false
        """
      Then query result
        | total |
        | NULL  |

  Rule: Output schema

    @function(nullability)
    Scenario: returns a nullable array of non-null float elements
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES (array(1.0F, 2.0F)) AS t(v)
        """
      Then query schema
        """
        root
         |-- total: array (nullable = true)
         |    |-- element: float (containsNull = false)
        """

  Rule: Error cases

    Scenario: DISTINCT window aggregates are not supported
      When query
        """
        SELECT vector_sum(DISTINCT v) OVER (
          ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total
        FROM VALUES (1, array(1.0F)), (2, array(1.0F)) AS t(n, v)
        """
      Then query error (?i)(DISTINCT_WINDOW_FUNCTION_UNSUPPORTED|DISTINCT window functions are not supported)

    Scenario: a bounded window rejects mismatched dimensions within a frame
      When query
        """
        SELECT vector_sum(v) OVER (
          ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS total
        FROM VALUES
          (1, array(1.0F)),
          (2, array(2.0F, 3.0F))
        AS t(n, v)
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions|dimension)

    Scenario: different dimensions produce an error
      When query
        """
        SELECT vector_sum(v) AS total
        FROM VALUES
          (array(1.0F)),
          (array(1.0F, 2.0F))
        AS t(v)
        """
      Then query error (?i)(VECTOR_DIMENSION_MISMATCH|matching dimensions|dimension)

    Scenario: double vectors are rejected
      When query
        """
        SELECT vector_sum(array(1.0D, 2.0D)) AS total
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_sum)

    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_sum(NULL) AS total
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_sum)
