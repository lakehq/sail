Feature: Describe tables without the TABLE keyword

  Background:
    Given final statement
      """
      DROP VIEW IF EXISTS describe_target
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW describe_target AS SELECT CAST(1 AS INT) AS x
      """

  Scenario Outline: Describe a view with an optional TABLE keyword
    When query
      """
      <describe> describe_target
      """
    Then query result ordered
      | col_name | data_type | comment |
      | x        | int       | NULL    |

    Examples:
      | describe       |
      | DESCRIBE       |
      | DESC           |
      | DESCRIBE TABLE |
      | DESC TABLE     |

  Scenario Outline: Describe extended and formatted view metadata
    When query
      """
      <describe> describe_target
      """
    Then query result row where "col_name" is "x" has "data_type" equal to "int"
    And query result has row where "col_name" is "# Detailed Table Information"

    Examples:
      | describe                 |
      | DESCRIBE EXTENDED        |
      | DESC FORMATTED           |
      | DESCRIBE TABLE EXTENDED  |
      | DESCRIBE TABLE FORMATTED |

  Scenario: Describe a qualified persistent table
    Given final statement
      """
      DROP TABLE IF EXISTS default.describe_persistent_target
      """
    Given statement
      """
      CREATE TABLE default.describe_persistent_target (x INT COMMENT 'value')
      """
    When query
      """
      DESCRIBE default.describe_persistent_target
      """
    Then query result ordered
      | col_name | data_type | comment |
      | x        | int       | value   |

  Scenario Outline: A keyword can name a table to describe
    Given final statement
      """
      DROP VIEW IF EXISTS `<name>`
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW `<name>` AS SELECT CAST(1 AS INT) AS x
      """
    When query
      """
      <describe> <name>
      """
    Then query result ordered
      | col_name | data_type | comment |
      | x        | int       | NULL    |

    Examples:
      | describe       | name      |
      | DESCRIBE       | select    |
      | DESCRIBE       | values    |
      | DESCRIBE       | query     |
      | DESCRIBE       | table     |
      | DESCRIBE TABLE | table     |
      | DESCRIBE       | formatted |
      | DESCRIBE       | extended  |
      | DESCRIBE TABLE | formatted |
      | DESCRIBE TABLE | extended  |
      | DESC TABLE     | formatted |
      | DESC TABLE     | extended  |
