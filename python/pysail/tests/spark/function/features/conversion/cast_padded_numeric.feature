Feature: Casting padded numeric text to floating point types

  Scenario Outline: padded DOUBLE literals with ANSI mode <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        CAST(' 883.33 ' AS DOUBLE) AS cast_result,
        TRY_CAST(' 883.33 ' AS DOUBLE) AS try_result,
        DOUBLE(' 883.33 ') AS constructor_result
      """
    Then query result
      | cast_result | try_result | constructor_result |
      | 883.33      | 883.33     | 883.33             |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: padded FLOAT literals with ANSI mode <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        CAST(' 883.33 ' AS FLOAT) = CAST('883.33' AS FLOAT) AS cast_matches,
        TRY_CAST(' 883.33 ' AS FLOAT) = CAST('883.33' AS FLOAT) AS try_matches,
        FLOAT(' 883.33 ') = CAST('883.33' AS FLOAT) AS constructor_matches
      """
    Then query result
      | cast_matches | try_matches | constructor_matches |
      | true         | true        | true                |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: padded <type> column values with ANSI mode <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        CAST(value AS <type>) <=> CAST(expected AS <type>) AS cast_matches,
        TRY_CAST(value AS <type>) <=> CAST(expected AS <type>) AS try_matches
      FROM VALUES
        (0, ' 883.33 ', '883.33'),
        (1, ' 883.33', '883.33'),
        (2, '883.33 ', '883.33'),
        (3, '\t883.33\n', '883.33'),
        (4, concat('\r883.33', chr(12)), '883.33'),
        (5, '883.33', '883.33'),
        (6, ' +883.33 ', '883.33'),
        (7, ' -8.8333e2 ', '-883.33'),
        (8, CAST(NULL AS STRING), CAST(NULL AS STRING))
      AS data(id, value, expected)
      ORDER BY id
      """
    Then query result ordered
      | id | cast_matches | try_matches |
      | 0  | true         | true        |
      | 1  | true         | true        |
      | 2  | true         | true        |
      | 3  | true         | true        |
      | 4  | true         | true        |
      | 5  | true         | true        |
      | 6  | true         | true        |
      | 7  | true         | true        |
      | 8  | true         | true        |

    Examples:
      | type   | ansi  |
      | DOUBLE | true  |
      | DOUBLE | false |
      | FLOAT  | true  |
      | FLOAT  | false |

  Scenario Outline: padded <type> runtime expressions with ANSI mode <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        CAST(value AS <type>) AS cast_result,
        TRY_CAST(value AS <type>) AS try_result,
        <type>(value) AS constructor_result
      FROM (
        SELECT id, concat(' ', CAST(id AS STRING), '.25 ') AS value
        FROM range(2)
      ) AS data
      ORDER BY id
      """
    Then query result ordered
      | id | cast_result | try_result | constructor_result |
      | 0  | 0.25        | 0.25       | 0.25               |
      | 1  | 1.25        | 1.25       | 1.25               |

    Examples:
      | type   | ansi  |
      | DOUBLE | true  |
      | DOUBLE | false |
      | FLOAT  | true  |
      | FLOAT  | false |

  Scenario Outline: internal whitespace remains invalid for <type> with ANSI mode <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, TRY_CAST(value AS <type>) AS result
      FROM VALUES
        (0, ' 883 .33 '),
        (1, ' + 883.33 '),
        (2, ' 8.8333e 2 '),
        (3, ' 883.\t33 ')
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |
      | 2  | NULL   |
      | 3  | NULL   |

    Examples:
      | type   | ansi  |
      | DOUBLE | true  |
      | DOUBLE | false |
      | FLOAT  | true  |
      | FLOAT  | false |
