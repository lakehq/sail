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
