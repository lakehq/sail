Feature: Qualified temporary view resolution

  Scenario: Access temporary view with database-qualified name
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT 1 AS id
      """
    When query
      """
      SELECT * FROM default.v1
      """
    Then query result
      | id |
      | 1  |

  Scenario: Access temporary view with fully qualified name
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT 1 AS id
      """
    When query
      """
      SELECT * FROM spark_catalog.default.v1
      """
    Then query result
      | id |
      | 1  |

  Scenario: Count rows from a temporary view using database-qualified name
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW v1 AS SELECT * FROM range(5)
      """
    When query
      """
      SELECT count(*) AS cnt FROM default.v1
      """
    Then query result
      | cnt |
      | 5   |
