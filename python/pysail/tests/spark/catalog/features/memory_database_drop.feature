Feature: Memory catalog database drop semantics

  @sail-only
  Scenario: Drop memory database without CASCADE rejects child databases
    Given final statement
      """
      DROP DATABASE IF EXISTS no_cascade_parent CASCADE
      """
    Given statement
      """
      CREATE DATABASE no_cascade_parent.child.leaf
      """
    Given statement with error .*
      """
      DROP DATABASE no_cascade_parent
      """
    When query
      """
      SHOW DATABASES IN no_cascade_parent
      """
    Then query result
      | name                    | catalog | description | locationUri |
      | no_cascade_parent.child | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES IN no_cascade_parent.child
      """
    Then query result
      | name                         | catalog | description | locationUri |
      | no_cascade_parent.child.leaf | sail    | NULL        | NULL        |

  @sail-only
  Scenario: Drop memory database with CASCADE removes descendant databases
    Given final statement
      """
      DROP DATABASE IF EXISTS cascade_parent CASCADE
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS cascade_parent_other CASCADE
      """
    Given statement
      """
      CREATE DATABASE cascade_parent.child.leaf
      """
    Given statement
      """
      CREATE DATABASE cascade_parent_other
      """
    Given statement
      """
      CREATE TABLE cascade_parent.child.tbl (id INT)
      """
    Given statement
      """
      DROP DATABASE cascade_parent CASCADE
      """
    When query
      """
      SHOW DATABASES LIKE 'cascade_parent*'
      """
    Then query result
      | name                 | catalog | description | locationUri |
      | cascade_parent_other | sail    | NULL        | NULL        |
    When query
      """
      SHOW DATABASES IN cascade_parent
      """
    Then query result
      | name | catalog | description | locationUri |
    When query
      """
      SHOW DATABASES IN cascade_parent.child
      """
    Then query result
      | name | catalog | description | locationUri |
    When query
      """
      SELECT * FROM cascade_parent.child.tbl
      """
    Then query error .*

  Scenario: Drop non-empty memory database without CASCADE rejects and keeps objects
    Given final statement
      """
      DROP DATABASE IF EXISTS no_cascade_memory_db CASCADE
      """
    Given statement
      """
      CREATE DATABASE no_cascade_memory_db
      """
    Given statement
      """
      CREATE TABLE no_cascade_memory_db.tbl (id INT)
      """
    Given statement
      """
      INSERT INTO no_cascade_memory_db.tbl VALUES (1), (2)
      """
    Given statement
      """
      CREATE VIEW no_cascade_memory_db.vw AS SELECT * FROM no_cascade_memory_db.tbl
      """
    Given statement with error .*
      """
      DROP DATABASE no_cascade_memory_db
      """
    When query
      """
      SHOW TABLES IN no_cascade_memory_db LIKE '*'
      """
    Then query result
      | database                 | tableName | isTemporary |
      | no_cascade_memory_db     | tbl       | false       |
      | no_cascade_memory_db     | vw        | false       |
    When query
      """
      SELECT * FROM no_cascade_memory_db.vw ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

    When query
      """
      SELECT * FROM no_cascade_memory_db.tbl ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Drop non-empty memory database with CASCADE removes it and contained objects
    Given final statement
      """
      DROP DATABASE IF EXISTS cascade_memory_db CASCADE
      """
    Given statement
      """
      CREATE DATABASE cascade_memory_db
      """
    Given statement
      """
      CREATE TABLE cascade_memory_db.tbl (id INT)
      """
    Given statement
      """
      INSERT INTO cascade_memory_db.tbl VALUES (1), (2)
      """
    Given statement
      """
      CREATE VIEW cascade_memory_db.vw AS SELECT * FROM cascade_memory_db.tbl
      """
    Given statement
      """
      DROP DATABASE cascade_memory_db CASCADE
      """
    When query
      """
      SHOW DATABASES LIKE 'cascade_memory_db'
      """
    Then query result
      | name | catalog | description | locationUri |

    When query
      """
      SELECT * FROM cascade_memory_db.tbl
      """
    Then query error .*

    When query
      """
      SELECT * FROM cascade_memory_db.vw
      """
    Then query error .*
