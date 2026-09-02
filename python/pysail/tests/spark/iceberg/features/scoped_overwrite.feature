Feature: Iceberg scoped overwrite

  Background:
    Given variable location for temporary directory iceberg_scoped_overwrite
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_scoped_overwrite_table
      """
    Given statement template
      """
      CREATE TABLE iceberg_scoped_overwrite_table (id BIGINT, category STRING, value BIGINT)
      USING iceberg
      PARTITIONED BY (category)
      LOCATION {{ location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_scoped_overwrite_table VALUES
        (1, 'A', 10),
        (3, 'A', 30)
      """
    Given statement
      """
      INSERT INTO iceberg_scoped_overwrite_table VALUES
        (2, 'B', 20),
        (4, 'B', 40)
      """

  Scenario: Predicate overwrite rewrites only candidate manifests
    Given remember current iceberg data manifest paths
    Given overwrite query into iceberg table iceberg_scoped_overwrite_table where category = 'A'
      """
      SELECT * FROM VALUES
        (5, 'A', 100),
        (6, 'A', 200)
      AS replacement(id, category, value)
      """
    Then iceberg snapshot operation is overwrite
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg metadata matches snapshot
    Then iceberg current manifest list matches snapshot
    Then iceberg current snapshot summary matches snapshot
    Then iceberg schema history matches snapshot
    Then iceberg snapshot count is 3
    When query
      """
      SELECT id, category, value
      FROM iceberg_scoped_overwrite_table
      ORDER BY id
      """
    Then query result ordered
      | id | category | value |
      | 2  | B        | 20    |
      | 4  | B        | 40    |
      | 5  | A        | 100   |
      | 6  | A        | 200   |

  Scenario: Predicate overwrite with empty input removes matching files
    Given overwrite query into iceberg table iceberg_scoped_overwrite_table where category = 'A'
      """
      SELECT CAST(NULL AS BIGINT) AS id,
             CAST(NULL AS STRING) AS category,
             CAST(NULL AS BIGINT) AS value
      WHERE FALSE
      """
    Then iceberg snapshot operation is overwrite
    Then iceberg current snapshot summary matches snapshot
    Then iceberg snapshot count is 3
    When query
      """
      SELECT id, category, value
      FROM iceberg_scoped_overwrite_table
      ORDER BY id
      """
    Then query result ordered
      | id | category | value |
      | 2  | B        | 20    |
      | 4  | B        | 40    |

  Scenario: Dynamic overwrite replaces only touched partitions and empty input is a no-op
    Given remember current iceberg data manifest paths
    Given overwrite partitions of iceberg table iceberg_scoped_overwrite_table with query
      """
      SELECT * FROM VALUES
        (5, 'A', 100),
        (6, 'A', 200)
      AS replacement(id, category, value)
      """
    Then iceberg snapshot operation is overwrite
    Then iceberg current data manifests reuse 1 remembered paths
    Then iceberg metadata matches snapshot
    Then iceberg current manifest list matches snapshot
    Then iceberg current snapshot summary matches snapshot
    Then iceberg schema history matches snapshot
    Then iceberg snapshot count is 3
    Given overwrite partitions of iceberg table iceberg_scoped_overwrite_table with query
      """
      SELECT CAST(NULL AS BIGINT) AS id,
             CAST(NULL AS STRING) AS category,
             CAST(NULL AS BIGINT) AS value
      WHERE FALSE
      """
    Then iceberg snapshot count is 3
    Then iceberg snapshot operation is overwrite
    When query
      """
      SELECT id, category, value
      FROM iceberg_scoped_overwrite_table
      ORDER BY id
      """
    Then query result ordered
      | id | category | value |
      | 2  | B        | 20    |
      | 4  | B        | 40    |
      | 5  | A        | 100   |
      | 6  | A        | 200   |

  Scenario: Dynamic overwrite adds a new partition without rewriting live manifests
    Given remember current iceberg data manifest paths
    Given overwrite partitions of iceberg table iceberg_scoped_overwrite_table with query
      """
      SELECT * FROM VALUES
        (5, 'C', 100)
      AS addition(id, category, value)
      """
    Then iceberg snapshot operation is overwrite
    Then iceberg current data manifests reuse 2 remembered paths
    Then iceberg current snapshot summary matches snapshot
    Then iceberg snapshot count is 3
    When query
      """
      SELECT id, category, value
      FROM iceberg_scoped_overwrite_table
      ORDER BY id
      """
    Then query result ordered
      | id | category | value |
      | 1  | A        | 10    |
      | 2  | B        | 20    |
      | 3  | A        | 30    |
      | 4  | B        | 40    |
      | 5  | C        | 100   |
