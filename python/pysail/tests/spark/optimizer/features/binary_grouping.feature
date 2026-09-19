Feature: Binary grouping optimization

  Scenario: Binary DISTINCT uses BinaryView grouping
    When query
      """
      SELECT DISTINCT k FROM VALUES (X'01'), (X'01') t(k)
      """
    Then query result
      | k    |
      | [01] |
    When query
      """
      EXPLAIN CODEGEN SELECT DISTINCT k FROM VALUES (X'01'), (X'01') t(k)
      """
    Then query plan matches snapshot
