Feature: Numeric STRING coercion in UNION inputs

  Scenario Outline: ANSI UNION casts <left> and <right> to their numeric common type
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, v, typeof(v) AS result_type
      FROM (
        SELECT 0 AS id, <left> AS v
        UNION ALL
        SELECT 1 AS id, <right> AS v
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | v       | result_type |
      | 0  | <first> | <type>      |
      | 1  | <last>  | <type>      |

    Examples:
      | left                     | right                    | type   | first | last |
      | CAST(1 AS TINYINT)        | '2'                      | bigint | 1     | 2    |
      | '1'                      | CAST(2 AS TINYINT)        | bigint | 1     | 2    |
      | CAST(1 AS SMALLINT)       | '2'                      | bigint | 1     | 2    |
      | '1'                      | CAST(2 AS SMALLINT)       | bigint | 1     | 2    |
      | CAST(1 AS INT)            | '2'                      | bigint | 1     | 2    |
      | '1'                      | CAST(2 AS INT)            | bigint | 1     | 2    |
      | CAST(1 AS BIGINT)         | '2'                      | bigint | 1     | 2    |
      | '1'                      | CAST(2 AS BIGINT)         | bigint | 1     | 2    |
      | CAST(1 AS FLOAT)          | '2'                      | double | 1.0   | 2.0  |
      | '1'                      | CAST(2 AS FLOAT)          | double | 1.0   | 2.0  |
      | CAST(1 AS DOUBLE)         | '2'                      | double | 1.0   | 2.0  |
      | '1'                      | CAST(2 AS DOUBLE)         | double | 1.0   | 2.0  |
      | CAST(1 AS DECIMAL(10, 2)) | '2'                      | double | 1.0   | 2.0  |
      | '1'                      | CAST(2 AS DECIMAL(10, 2)) | double | 1.0   | 2.0  |

  Scenario: ANSI UNION DISTINCT compares coerced numeric values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT v, typeof(v) AS result_type
      FROM (SELECT '01' AS v UNION SELECT 1 AS v)
      """
    Then query result collected
      | v | result_type |
      | 1 | bigint      |

  Scenario Outline: ANSI UNION <function> skips an unselected STRING cast with <order> input first
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, <expression> AS v
      FROM (<first> UNION ALL <second>) ORDER BY id
      """
    Then query result collected ordered
      | id | v   |
      | 0  | 0.0 |
      | 1  | 2.5 |

    Examples:
      | function    | order   | expression                                                           | first                                            | second                                           |
      | IF          | STRING  | IF(id = 0, 0, v)                                                     | SELECT 0 AS id, 'bad' AS v                        | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v |
      | IF          | numeric | IF(id = 0, 0, v)                                                     | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v | SELECT 0 AS id, 'bad' AS v                        |
      | nested CASE | STRING  | CASE WHEN id = 0 THEN 0 ELSE CASE WHEN id = 1 THEN v ELSE 0 END END | SELECT 0 AS id, 'bad' AS v                        | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v |
      | nested CASE | numeric | CASE WHEN id = 0 THEN 0 ELSE CASE WHEN id = 1 THEN v ELSE 0 END END | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v | SELECT 0 AS id, 'bad' AS v                        |
      | NVL2        | STRING  | NVL2(NULLIF(id, 1), 0, v)                                            | SELECT 0 AS id, 'bad' AS v                        | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v |
      | NVL2        | numeric | NVL2(NULLIF(id, 1), 0, v)                                            | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v | SELECT 0 AS id, 'bad' AS v                        |
      | IFNULL | STRING | IFNULL(NULLIF(id, 1), v) | SELECT 0 AS id, 'bad' AS v | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v |
      | IFNULL | numeric | IFNULL(NULLIF(id, 1), v) | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v | SELECT 0 AS id, 'bad' AS v |
      | NVL | STRING | NVL(NULLIF(id, 1), v) | SELECT 0 AS id, 'bad' AS v | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v |
      | NVL | numeric | NVL(NULLIF(id, 1), v) | SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v | SELECT 0 AS id, 'bad' AS v |

  Scenario: ANSI UNION skips a STRING cast shared by two unselected conditional branches
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, IF(id = 0, 0, v) AS first_value,
             CASE WHEN id = 0 THEN 1 ELSE v END AS second_value
      FROM (
        SELECT 0 AS id, 'bad' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | first_value | second_value |
      | 0  | 0.0         | 1.0          |
      | 1  | 2.5         | 2.5          |

  Scenario Outline: ANSI UNION preserves lazy casts with a <kind> lambda
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, CAST(IF(<condition>, 0, v) AS DOUBLE) AS v, <array> AS a
      FROM (
        SELECT 0 AS id, 'bad' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | v   | a        |
      | 0  | 0.0 | <first>  |
      | 1  | 2.5 | <second> |

    Examples:
      | kind      | condition                                             | array                               | first | second |
      | sibling   | id = 0                                                | transform(array(id), x -> x + 1)     | [1]   | [2]    |
      | condition | element_at(transform(array(id), x -> x + 1), 1) = 1   | array(id)                           | [0]   | [1]    |
      | capture   | id = 0                                                | transform(array(id), x -> x + id)    | [0]   | [2]    |

  Scenario: ANSI UNION keeps row-dependent shared conditions lazy
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, CAST(IF(id = 0, 0, v) AS DOUBLE) AS first_value,
             CAST(IF(id = 0, 1, v) AS DOUBLE) AS second_value
      FROM (
        SELECT id, 'bad' AS v FROM range(1)
        UNION ALL
        SELECT id, CAST(2.5 AS DECIMAL(2,1)) AS v FROM range(1, 2)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | first_value | second_value |
      | 0  | 0.0         | 1.0          |
      | 1  | 2.5         | 2.5          |

  Scenario Outline: ANSI UNION preserves lazy casts through <clause>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT count(*) AS row_count,
             sum(CASE WHEN v IN (0.0, 2.5) THEN 1 ELSE 0 END) AS valid_count
      FROM (
        SELECT CAST(IF(id = 0, 0, v) AS DOUBLE) AS v
        FROM (
          SELECT id, concat('bad', id) AS v FROM range(1)
          UNION ALL
          SELECT id, CAST(2.5 AS DECIMAL(2,1)) AS v FROM range(1, 10)
          <clause>
        )
      )
      """
    Then query result collected
      | row_count | valid_count |
      | <count>   | <count>     |

    Examples:
      | clause           | count |
      | LIMIT 2          | 2     |
      | OFFSET 1         | 9     |
      | LIMIT 2 OFFSET 1 | 2     |

  @sail-bug
  Scenario Outline: ANSI UNION retains constant cast errors below <clause>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT IF(id = 0, 0, v) AS v
      FROM (
        SELECT id, 'bad' AS v FROM range(1)
        UNION ALL
        SELECT id, 2.5D AS v FROM range(1, 3)
        <clause>
      )
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

    Examples:
      | clause           |
      | LIMIT 1          |
      | OFFSET 1         |
      | LIMIT 1 OFFSET 1 |

  Scenario: Sorting a limited conditional UNION preserves OFFSET rows
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT IF(id = 0, 0, v) AS v
      FROM (
        SELECT id, concat('bad', id) AS v FROM range(1)
        UNION ALL
        SELECT id, 0D AS v FROM range(1, 2)
        LIMIT 1 OFFSET 1
      ) ORDER BY id
      """
    Then query result collected
      | v   |
      | 0.0 |

  Scenario: ANSI UNION retains a repeated expensive row-dependent cast
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, IF(id = 0, 0, v) AS first_value,
             IF(id = 0, 1, v) AS second_value
      FROM (
        SELECT id, concat('bad', id) AS v FROM range(1)
        UNION ALL
        SELECT id, CAST(2.5 AS DECIMAL(2,1)) AS v FROM range(1, 2)
      )
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario: ANSI UNION retains casts below a volatile projection
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, IF(id = 0, 0, v) AS value, rand() AS random_value
      FROM (
        SELECT 0 AS id, 'bad' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      )
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario: ANSI UNION retains errors from repeated compound constant producers
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, CAST(IF(id = 0, 0, v) AS DOUBLE) AS value
      FROM (
        SELECT id, v + v AS v FROM (
          SELECT id, v + v AS v FROM (
            SELECT id, v + v AS v FROM (
              SELECT id, v + v AS v FROM (
                SELECT id, CAST(v AS DOUBLE) AS v FROM (
                  SELECT 0 AS id, 'bad' AS v
                  UNION ALL
                  SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
                )
              )
            )
          )
        )
      )
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario Outline: ANSI UNION preserves lazy shared <producer> literals
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, CAST(IF(id = 0, <zero>, v) AS <type>) AS first_value,
             CAST(IF(id = 0, <one>, v) AS <type>) AS second_value
      FROM (
        SELECT 0 AS id, <text> AS v
        UNION ALL
        SELECT 1 AS id, <fraction> AS v
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | first_value | second_value |
      | 0  | <first>     | <second>     |
      | 1  | <last>      | <last>       |

    Examples:
      | producer | zero           | one            | text               | fraction                            | type             | first      | second     | last       |
      | ARRAY    | array(0)       | array(1)       | array('bad')       | array(CAST(2.5 AS DECIMAL(2,1)))      | ARRAY<DOUBLE>    | [0.0]      | [1.0]      | [2.5]      |
      | STRUCT   | struct(0 AS x) | struct(1 AS x) | struct('bad' AS x) | struct(CAST(2.5 AS DECIMAL(2,1)) AS x) | STRUCT<x:DOUBLE> | Row(x=0.0) | Row(x=1.0) | Row(x=2.5) |
      | CONCAT   | 0              | 1              | concat('ba','d')   | CAST(2.5 AS DECIMAL(2,1))             | DOUBLE           | 0.0        | 1.0        | 2.5        |

  Scenario: ANSI UNION retains strict intermediate STRING casts
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT '1.5' AS v
      UNION ALL
      SELECT 1 AS v
      UNION ALL
      SELECT CAST(2 AS FLOAT) AS v
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario: ANSI UNION respects explicitly grouped numeric inputs
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT v, typeof(v) AS result_type
      FROM (
        SELECT '1.5' AS v
        UNION ALL
        (SELECT 1 AS v UNION ALL SELECT CAST(2 AS FLOAT) AS v)
      ) ORDER BY v
      """
    Then query result collected ordered
      | v   | result_type |
      | 1.0 | double      |
      | 1.5 | double      |
      | 2.0 | double      |

  Scenario: ANSI UNION preserves NULL and exposes nullable STRING casts in nested fields
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT v, a, m, s.x AS x
      FROM (
        SELECT 0 AS id, 1 AS v, array(1) AS a, map('k', 1) AS m, named_struct('x', 1) AS s
        UNION ALL
        SELECT id, text AS v, array(text) AS a, map('k', text) AS m, named_struct('x', text) AS s
        FROM VALUES (1, '2'), (2, CAST(NULL AS STRING)) AS t(id, text)
      ) ORDER BY id
      """
    Then query result collected ordered
      | v    | a      | m           | x    |
      | 1    | [1]    | {'k': 1}    | 1    |
      | 2    | [2]    | {'k': 2}    | 2    |
      | NULL | [None] | {'k': None} | NULL |
    And query schema
      """
      root
       |-- v: long (nullable = true)
       |-- a: array (nullable = false)
       |    |-- element: long (containsNull = true)
       |-- m: map (nullable = false)
       |    |-- key: string
       |    |-- value: long (valueContainsNull = true)
       |-- x: long (nullable = true)
      """

  Scenario: ANSI UNION promotes fractional STRING leaves through arrays and map values
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, a[0].x AS array_value, m['k'].x AS map_value,
             typeof(a) AS array_type, typeof(m) AS map_type
      FROM (
        SELECT 0 AS id, array(named_struct('x', CAST(1.25 AS FLOAT))) AS a,
               map('k', named_struct('x', CAST(1.25 AS DECIMAL(10, 2)))) AS m
        UNION ALL
        SELECT id, array(named_struct('x', text)) AS a,
               map('k', named_struct('x', text)) AS m
        FROM VALUES (1, '16777217.25') AS t(id, text)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | array_value | map_value   | array_type              | map_type                     |
      | 0  | 1.25        | 1.25        | array<struct<x:double>> | map<string,struct<x:double>> |
      | 1  | 16777217.25 | 16777217.25 | array<struct<x:double>> | map<string,struct<x:double>> |

  Scenario Outline: ANSI UNION rejects malformed <numeric> input <text>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(1 AS <numeric>) AS v
      UNION ALL
      SELECT text FROM VALUES (<text>) AS t(text)
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

    Examples:
      | numeric        | text                  |
      | INT            | '1.5'                 |
      | BIGINT         | '9223372036854775808' |
      | FLOAT          | 'x'                   |
      | DECIMAL(10, 2) | 'x'                   |

  @sail-bug
  Scenario: ANSI UNION rejects numeric STRING map-key coercion
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT map(1, 'a') AS m
      UNION ALL
      SELECT map('2', 'b') AS m
      """
    Then query error INCOMPATIBLE_COLUMN_TYPE

  Scenario Outline: Non-ANSI UNION retains STRING values in <left> and <right>
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, v, typeof(v) AS result_type
      FROM (
        SELECT 0 AS id, <left> AS v
        UNION ALL
        SELECT 1 AS id, <right> AS v
      ) ORDER BY id
      """
    Then query result ordered
      | id | v       | result_type |
      | 0  | <first> | string      |
      | 1  | <last>  | string      |

    Examples:
      | left                       | right                      | first | last |
      | 1                          | 'x'                        | 1     | x    |
      | 'x'                        | 1                          | x     | 1    |
      | CAST(1.25 AS FLOAT)         | 'x'                        | 1.25  | x    |
      | 'x'                        | CAST(1.25 AS DECIMAL(10,2)) | x     | 1.25 |
