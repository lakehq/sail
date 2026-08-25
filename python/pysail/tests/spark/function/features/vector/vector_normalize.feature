Feature: vector_normalize
  Spark-compatible float vector unit normalization.

  Background:
    Given spark session

  Rule: Basic normalization

    Scenario: L2 normalize of a simple vector
      When query
        """
        SELECT vector_normalize(array(3.0F, 4.0F), 2.0F) AS result
        """
      Then query result
        | result     |
        | [0.6, 0.8] |

    Scenario: L1 normalize of a simple vector
      When query
        """
        SELECT vector_normalize(array(3.0F, 4.0F), 1.0F) AS result
        """
      Then query result
        | result                 |
        | [0.42857143, 0.5714286] |

    Scenario: infinity-norm normalize of a simple vector
      When query
        """
        SELECT vector_normalize(array(3.0F, 4.0F), float('inf')) AS result
        """
      Then query result
        | result      |
        | [0.75, 1.0] |

    Scenario: degree defaults to L2 when omitted
      When query
        """
        SELECT vector_normalize(array(3.0F, 4.0F)) AS result
        """
      Then query result
        | result     |
        | [0.6, 0.8] |

  Rule: Null handling

    Scenario: typed null vector returns NULL
      When query
        """
        SELECT vector_normalize(CAST(NULL AS ARRAY<FLOAT>), 2.0F) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array containing a null element returns NULL
      When query
        """
        SELECT vector_normalize(array(1.0F, CAST(NULL AS FLOAT), 3.0F), 2.0F) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Error cases

    Scenario: invalid degree is rejected
      When query
        """
        SELECT vector_normalize(array(1.0F, 2.0F), 3.0F) AS result
        """
      Then query error (?i)(INVALID_VECTOR_NORM_DEGREE|degree)

    Scenario: ARRAY<DOUBLE> inputs are rejected
      When query
        """
        SELECT vector_normalize(array(1.0D, 2.0D), 2.0F) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH|UNEXPECTED_INPUT_TYPE|ARRAY|FLOAT|vector_normalize)
