Feature: ANSI numeric and STRING conditional results

  Background:
    Given config spark.sql.ansi.enabled = true

  Scenario Outline: <kind> preserves large integral STRING values in either branch order
    When query
      """
      SELECT <expression> AS result FROM range(2) ORDER BY id
      """
    Then query result ordered
      | result   |
      | <first>  |
      | <second> |
    And query schema
      """
      root
       |-- result: long (nullable = true)
      """

    Examples:
      | kind | expression                                          | first            | second           |
      | CASE | CASE WHEN id = 0 THEN 1 ELSE '9007199254740993' END | 1                | 9007199254740993 |
      | CASE | CASE WHEN id = 0 THEN '9007199254740993' ELSE 1 END | 9007199254740993 | 1                |
      | IF   | if(id = 0, 1, '9007199254740993')                   | 1                | 9007199254740993 |
      | IF   | if(id = 0, '9007199254740993', 1)                   | 9007199254740993 | 1                |
      | NVL2 | nvl2(nullif(id, 0), 1, '9007199254740993')          | 9007199254740993 | 1                |
      | NVL2 | nvl2(nullif(id, 0), '9007199254740993', 1)          | 1                | 9007199254740993 |

  Scenario: STRING and DECIMAL branches produce DOUBLE
    When query
      """
      SELECT if(id = 0, CAST(1.25 AS DECIMAL(5,2)), '2.75') AS result
      FROM range(2) ORDER BY id
      """
    Then query result ordered
      | result |
      | 1.25   |
      | 2.75   |
    And query schema
      """
      root
       |-- result: double (nullable = true)
      """

  Scenario Outline: CASE folds three result types in branch order: <order>
    When query
      """
      SELECT result, typeof(result) AS result_type
      FROM (
        SELECT id, CASE WHEN id = 0 THEN <first_branch>
                       WHEN id = 1 THEN <second_branch>
                       ELSE <third_branch> END AS result
        FROM range(3)
      ) AS q ORDER BY id
      """
    Then query result ordered
      | result   | result_type   |
      | <first>  | <result_type> |
      | <second> | <result_type> |
      | <third>  | <result_type> |
    And query schema
      """
      root
       |-- result: <result_type> (nullable = true)
       |-- result_type: string (nullable = false)
      """

    Examples:
      | order                | first_branch               | second_branch | third_branch               | first | second | third | result_type   |
      | INT, STRING, DECIMAL | 1                          | '7'           | CAST(1.25 AS DECIMAL(5,2)) | 1.00  | 7.00   | 1.25  | decimal(22,2) |
      | DECIMAL, INT, STRING | CAST(1.25 AS DECIMAL(5,2)) | 1             | '7'                        | 1.25  | 1.0    | 7.0   | double        |

  Scenario: Conditional coercion reaches an array nested in a struct
    When query
      """
      SELECT result, typeof(result) AS result_type
      FROM (
        SELECT id, if(id = 0, named_struct('a', array(1), 'n', 1),
                             named_struct('a', array('7'), 'n', '7')) AS result
        FROM range(2)
      ) AS q ORDER BY id
      """
    Then query result ordered
      | result   | result_type                      |
      | {[1], 1} | struct<a:array<bigint>,n:bigint> |
      | {[7], 7} | struct<a:array<bigint>,n:bigint> |

  Scenario: STRING promotion preserves nullable array elements
    When query
      """
      SELECT if(id = 0, array(1), array('7')) AS result FROM range(2) ORDER BY id
      """
    Then query result ordered
      | result |
      | [1]    |
      | [7]    |
    And query schema
      """
      root
       |-- result: array (nullable = false)
       |    |-- element: long (containsNull = true)
      """

  Scenario Outline: An unselected invalid STRING is not cast: <expression>
    When query
      """
      SELECT <expression> AS result
      """
    Then query result
      | result |
      | 1      |

    Examples:
      | expression                            |
      | CASE WHEN true THEN 1 ELSE 'bad' END  |
      | CASE WHEN false THEN 'bad' ELSE 1 END |
      | if(true, 1, 'bad')                    |
      | if(false, 'bad', 1)                   |
      | nvl2(1, 1, 'bad')                     |
      | nvl2(NULL, 'bad', 1)                  |

  Scenario Outline: A persistent view retains STRING promotion from creation ANSI <creation_ansi> when read with ANSI <read_ansi>
    Given config spark.sql.ansi.enabled = <creation_ansi>
    And final statement
      """
      DROP VIEW IF EXISTS conditional_string_numeric_view
      """
    And statement
      """
      CREATE OR REPLACE VIEW conditional_string_numeric_view AS
      SELECT id, CASE WHEN id = 0 THEN 1 ELSE '<text>' END AS result FROM range(2)
      """
    And config spark.sql.ansi.enabled = <read_ansi>
    When query
      """
      SELECT id, result, typeof(result) AS result_type
      FROM conditional_string_numeric_view ORDER BY id
      """
    Then query result ordered
      | id | result | result_type   |
      | 0  | 1      | <result_type> |
      | 1  | <text> | <result_type> |

    Examples:
      | creation_ansi | read_ansi | text | result_type |
      | false         | false     | x    | string      |
      | false         | true      | x    | string      |
      | true          | false     | 7    | bigint      |
      | true          | true      | 7    | bigint      |

  Scenario: A persistent ANSI view preserves precision while folding STRING, INT and FLOAT branches
    Given final statement
      """
      DROP VIEW IF EXISTS conditional_string_float_view
      """
    And statement
      """
      CREATE OR REPLACE VIEW conditional_string_float_view AS
      SELECT id, CASE WHEN id = 0 THEN '16777217'
                      WHEN id = 1 THEN 1
                      ELSE CAST(0 AS FLOAT) END AS result,
             CASE WHEN id = 0 THEN x ELSE CAST(0 AS FLOAT) END AS outer_numeric_result
      FROM (
        SELECT id, CASE WHEN id = 0 THEN '16777217' ELSE 1 END AS x FROM range(3)
      ) AS q
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(result AS BIGINT) AS result, typeof(result) AS result_type,
             CAST(outer_numeric_result AS BIGINT) AS outer_numeric_result,
             typeof(outer_numeric_result) AS outer_numeric_type
      FROM conditional_string_float_view ORDER BY id
      """
    Then query result ordered
      | id | result   | result_type | outer_numeric_result | outer_numeric_type |
      | 0  | 16777217 | double      | 16777217             | double             |
      | 1  | 1        | double      | 0                    | double             |
      | 2  | 0        | double      | 0                    | double             |
