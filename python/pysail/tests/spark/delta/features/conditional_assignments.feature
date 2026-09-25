@sail-bug
Feature: Conditional row-level assignments

  Background:
    Given config spark.sql.ansi.enabled = true
    And variable location for temporary directory conditional_assignments
    And final statement
      """
      DROP TABLE IF EXISTS conditional_assignments
      """
    And statement template
      """
      CREATE TABLE conditional_assignments (
        id INT, n BIGINT, r STRUCT<a: BIGINT>, a ARRAY<BIGINT>, s STRING
      ) USING DELTA LOCATION {{ location.sql }}
      """
    And statement
      """
      INSERT INTO conditional_assignments VALUES
        (1, 10, named_struct('a', 10L), array(10L), '5'),
        (2, 20, named_struct('a', 20L), array(20L), '7')
      """

  Scenario: UPDATE preserves mixed numeric and STRING conditional assignments
    Given statement
      """
      UPDATE conditional_assignments
      SET n = if(id = 1, 1, s),
          r = CASE WHEN id = 1 THEN named_struct('a', 1) ELSE named_struct('a', '7') END,
          a = array(if(id = 1, CAST(1 AS BIGINT), CASE WHEN id = 2 THEN 7 ELSE '9' END))
      """
    When query
      """
      SELECT id, n, r.a AS struct_value, a[0] AS array_value
      FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | n | struct_value | array_value |
      | 1  | 1 | 1            | 1           |
      | 2  | 7 | 7            | 7           |

  Scenario: MERGE preserves conditional assignments in every write action
    Given statement
      """
      MERGE INTO conditional_assignments AS t
      USING (SELECT * FROM VALUES (1), (3) AS src(id)) AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET
        n = CASE WHEN s.id = 1 THEN 1 ELSE '7' END,
        r = if(s.id = 1, named_struct('a', 1), named_struct('a', '7')),
        a = CASE WHEN s.id = 1 THEN array(1) ELSE array('7') END
      WHEN NOT MATCHED THEN INSERT (id, n, r, a, s) VALUES (
        s.id,
        if(s.id = 1, 1, '7'),
        CASE WHEN s.id = 1 THEN named_struct('a', 1) ELSE named_struct('a', '7') END,
        if(s.id = 1, array(1), array('7')),
        'new'
      )
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
        n = if(t.id = 2, 2, '7'),
        r = named_struct('a', if(t.id = 2, 2, '7')),
        a = array(CASE WHEN t.id = 2 THEN 2 ELSE '7' END)
      """
    When query
      """
      SELECT id, n, r.a AS struct_value, a[0] AS array_value
      FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | n | struct_value | array_value |
      | 1  | 1 | 1            | 1           |
      | 2  | 2 | 2            | 2           |
      | 3  | 7 | 7            | 7           |

  Scenario Outline: MERGE preserves a conditional numeric source through <source_kind>
    Given final statement
      """
      DROP VIEW IF EXISTS conditional_assignment_source
      """
    And statement
      """
      CREATE TEMP VIEW conditional_assignment_source AS
      SELECT id, CASE WHEN id = 1 THEN 1 ELSE '7' END AS n
      FROM VALUES (1), (3) AS src(id)
      """
    And statement
      """
      MERGE INTO conditional_assignments AS t
      USING <source> AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET n = s.n
      WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n)
      """
    When query
      """
      SELECT id, n FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | n  |
      | 1  | 1  |
      | 2  | 20 |
      | 3  | 7  |

    Examples:
      | source_kind   | source                                                                                 |
      | a projection  | (SELECT id, CASE WHEN id = 1 THEN 1 ELSE '7' END AS n FROM VALUES (1), (3) AS src(id)) |
      | a cached view | conditional_assignment_source                                                          |
