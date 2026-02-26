Feature: Delta Lake CREATE TABLE AS SELECT

  Scenario: CREATE TABLE AS SELECT with CLUSTER BY clause
    Given variable location for temporary directory delta_ctas_cluster_by
    Given final statement
      """
      DROP TABLE IF EXISTS delta_ctas_cluster_by
      """
    Given statement template
      """
      CREATE TABLE delta_ctas_cluster_by
      USING DELTA
      LOCATION {{ location.sql }}
      CLUSTER BY (id)
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM delta_ctas_cluster_by ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |

  Scenario: CREATE TABLE AS SELECT with CLUSTERED BY (bucket by) clause
    Given variable location for temporary directory delta_ctas_bucket_by
    Given final statement
      """
      DROP TABLE IF EXISTS delta_ctas_bucket_by
      """
    Given statement template
      """
      CREATE TABLE delta_ctas_bucket_by
      USING DELTA
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM delta_ctas_bucket_by ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |
