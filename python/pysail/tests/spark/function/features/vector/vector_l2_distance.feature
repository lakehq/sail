@spark-4.2
Feature: vector_l2_distance

  Rule: Basic results

    Scenario: basic, right-triangle, and self distance
      When query
        """
        SELECT
          vector_l2_distance(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)) AS basic,
          vector_l2_distance(array(3.0F, 4.0F), array(0.0F, 0.0F)) AS right_triangle,
          vector_l2_distance(array(3.0F, 4.0F), array(3.0F, 4.0F)) AS self_distance
        """
      Then query result
        | basic    | right_triangle | self_distance |
        | 5.196152 | 5.0            | 0.0           |

    Scenario: vector columns from VALUES execute the UDF
      When query
        """
        SELECT vector_l2_distance(left_vector, right_vector) AS result
        FROM VALUES
          (array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F)),
          (array(3.0F, 4.0F), array(0.0F, 0.0F)),
          (array(3.0F, 4.0F), array(3.0F, 4.0F))
        AS t(left_vector, right_vector)
        """
      Then query result
        | result   |
        | 5.196152 |
        | 5.0      |
        | 0.0      |

    Scenario: orthogonal unit vectors are sqrt(2) apart
      When query
        """
        SELECT vector_l2_distance(array(1.0F, 0.0F), array(0.0F, 1.0F)) AS result
        """
      Then query result
        | result    |
        | 1.4142135 |

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

    Scenario: distance across a 16-element vector matches sqrt of the summed squares
      When query
        """
        SELECT vector_l2_distance(
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F),
          array(2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F, 17.0F)
        ) AS result
        """
      Then query result
        | result |
        | 4.0    |

  Rule: Null handling

    Scenario: typed null vector returns NULL
      When query
        """
        SELECT vector_l2_distance(CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 2.0F)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array containing a null element returns NULL
      When query
        """
        SELECT vector_l2_distance(array(1.0F, CAST(NULL AS FLOAT), 3.0F), array(4.0F, 5.0F, 6.0F)) AS result
        """
      Then query result
        | result |
        | NULL   |
