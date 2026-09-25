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
        id INT, n BIGINT, r STRUCT<a: BIGINT>, a ARRAY<BIGINT>, s STRING, mixed STRUCT<s: STRING, n: DOUBLE>, mapping MAP<STRING, BIGINT>
      ) USING DELTA LOCATION {{ location.sql }}
      """
    And statement
      """
      INSERT INTO conditional_assignments VALUES
        (1, 10, named_struct('a', 10L), array(10L), '5', named_struct('s', 'old', 'n', 0D), map('a', 10L)),
        (2, 20, named_struct('a', 20L), array(20L), '7', named_struct('s', 'old', 'n', 0D), map('a', 20L))
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
      <create_view> conditional_assignment_source AS
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
      | source_kind       | create_view      | source                                                                             |
      | a projection      | CREATE TEMP VIEW | (SELECT id, CASE WHEN id = 1 THEN 1 ELSE '7' END AS n FROM VALUES (1), (3) AS src(id)) |
      | a cached view     | CREATE TEMP VIEW | conditional_assignment_source                                                      |
      | a persistent view | CREATE VIEW      | conditional_assignment_source                                                      |

  Scenario: Conditional MAP values retain supported numeric assignments
    Given statement
      """
      UPDATE conditional_assignments
      SET mapping = if(id = 1, map('a', 1), map('a', '7'))
      """
    When query
      """
      SELECT id, mapping['a'] AS result FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 1  | 1      |
      | 2  | 7      |

  Scenario: Mixed STRING fields do not undo numeric sibling precision
    Given statement
      """
      UPDATE conditional_assignments
      SET mixed = named_struct(
        's', if(id = 1, 1, '7'),
        'n', if(id = 1, 16777217L, CAST(2 AS FLOAT))
      )
      """
    When query
      """
      SELECT id, mixed.s AS text_value, CAST(mixed.n AS BIGINT) AS numeric_value
      FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | text_value | numeric_value |
      | 1  | 1          | 16777217      |
      | 2  | 7          | 2             |

  Scenario: STRICT assignment accepts the promoted BIGINT conditional
    Given config spark.sql.storeAssignmentPolicy = STRICT
    And statement
      """
      UPDATE conditional_assignments
      SET n = CASE WHEN id = 1 THEN 1 ELSE '7' END
      """
    When query
      """
      SELECT id, n FROM conditional_assignments ORDER BY id
      """
    Then query result ordered
      | id | n |
      | 1  | 1 |
      | 2  | 7 |

  Scenario: STRICT assignment rejects narrowing the promoted BIGINT conditional
    Given config spark.sql.storeAssignmentPolicy = STRICT
    When query
      """
      UPDATE conditional_assignments
      SET id = CASE WHEN id = 1 THEN 1 ELSE '7' END
      """
    Then query error CANNOT_SAFELY_CAST

  Scenario: An explicit STRING cast still requires numeric assignment conversion
    When query
      """
      UPDATE conditional_assignments
      SET n = CAST(CASE WHEN id = 1 THEN 1 ELSE '7' END AS STRING)
      """
    Then query error CANNOT_SAFELY_CAST
