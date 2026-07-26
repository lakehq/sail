Feature: width_bucket() assigns values to buckets

  Scenario: width_bucket returns a Spark BIGINT result
    When query
      """
      SELECT width_bucket(5.0, 0.0, 10.0, 5) AS result
      """
    Then query result
      | result |
      | 3      |
    And query schema
      """
      root
       |-- result: long (nullable = true)
      """
