@spark-4.2
Feature: vector_norm
  Rule: Basic results
    Scenario: L1, L2, and L-infinity norms
      When query
        """
        SELECT
          vector_norm(array(3.0F, 4.0F), 1.0F) AS l1,
          vector_norm(array(3.0F, 4.0F)) AS l2,
          vector_norm(array(-3.0F, 4.0F), float('inf')) AS l_infinity
        """
      Then query result
        | l1  | l2 | l_infinity |
        | 7.0 | 5.0 | 4.0         |
    Scenario: vector columns from VALUES execute the UDF
      When query
        """
        SELECT vector_norm(vector, degree) AS result
        FROM VALUES
          (array(3.0F, 4.0F), 1.0F),
          (array(3.0F, 4.0F), 2.0F),
          (array(-3.0F, 4.0F), float('inf'))
        AS t(vector, degree)
        """
      Then query result
        | result |
        | 7.0    |
        | 5.0    |
        | 4.0    |
    Scenario: empty ARRAY<FLOAT> input returns 0.0
      When query
        """
        SELECT vector_norm(CAST(array() AS ARRAY<FLOAT>)) AS result
        """
      Then query result
        | result |
        | 0.0    |
    Scenario: upstream 16-element case supports each norm degree
      When query
        """
        SELECT
          vector_norm(vector, 1.0F) AS l1,
          vector_norm(vector, 2.0F) AS l2,
          vector_norm(vector, float('inf')) AS l_infinity
        FROM VALUES (
          array(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F,
                9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F, 15.0F, 16.0F)
        ) AS t(vector)
        """
      Then query result
        | l1    | l2       | l_infinity |
        | 136.0 | 38.67816 | 16.0        |
  Rule: Null handling
    Scenario: typed null vector returns NULL
      When query
        """
        SELECT vector_norm(CAST(NULL AS ARRAY<FLOAT>)) AS result
        """
      Then query result
        | result |
        | NULL   |
    Scenario: array containing a null element returns NULL
      When query
        """
        SELECT vector_norm(array(3.0F, CAST(NULL AS FLOAT), 4.0F)) AS result
        """
      Then query result
        | result |
        | NULL   |
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
    Scenario: untyped NULL is rejected
      When query
        """
        SELECT vector_norm(NULL) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)
    Scenario: ARRAY<DOUBLE> input is rejected
      When query
        """
        SELECT vector_norm(array(1.0D, 2.0D)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)
    Scenario: ARRAY<INT> input is rejected
      When query
        """
        SELECT vector_norm(array(1, 2)) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)
    Scenario: non-array value is rejected
      When query
        """
        SELECT vector_norm(1.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_norm)
    Scenario: invalid norm degree is rejected
      When query
        """
        SELECT vector_norm(array(3.0F, 4.0F), 3.0F) AS result
        """
      Then query error (?i)(INVALID_VECTOR_NORM_DEGREE|degree must be 1.0|2.0|positive infinity)
