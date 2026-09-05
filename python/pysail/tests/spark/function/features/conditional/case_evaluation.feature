Feature: CASE validates every branch and evaluates only selected values

  Scenario Outline: CASE skips invalid casts in unselected branches with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT CASE WHEN p THEN 1 ELSE CAST('bad' AS INT) END AS skipped_else,
             CASE WHEN NOT p THEN CAST('bad' AS INT) ELSE 2 END AS skipped_then,
             CASE WHEN p THEN 3 WHEN CAST(concat('b', 'ad') AS BOOLEAN) THEN 4
                  ELSE 5 END AS skipped_condition,
             CASE WHEN p THEN CAST(concat('1', '2') AS INT)
                  ELSE CAST(concat('b', 'ad') AS INT) END AS folded_input
      FROM VALUES (true) AS t(p)
      """
    Then query result collected
      | skipped_else | skipped_then | skipped_condition | folded_input |
      | 1            | 2            | 3                 | 12           |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: CASE raises selected ANSI cast errors in <position>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> AS result FROM VALUES (<predicate>) AS t(p)
      """
    Then query error (?i)(cast|malformed|invalid|parse)

    Examples:
      | position        | predicate | expression                                                                         |
      | THEN            | true      | CASE WHEN p THEN CAST('bad' AS INT) ELSE 1 END                                       |
      | ELSE            | false     | CASE WHEN p THEN 1 ELSE CAST(concat('b', 'ad') AS INT) END                            |
      | later condition | false     | CASE WHEN p THEN 1 WHEN CAST('bad' AS BOOLEAN) THEN 2 ELSE 3 END                      |
      | nested CASE     | true      | CASE WHEN p THEN CASE WHEN p THEN CAST('bad' AS INT) ELSE 1 END ELSE 2 END           |

  Scenario: CASE returns NULL for selected invalid integer casts outside ANSI mode
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CASE WHEN p THEN CAST('bad' AS INT) ELSE 1 END AS cast_then,
             CASE WHEN p THEN 1 ELSE CAST(concat('b', 'ad') AS INT) END AS cast_else
      FROM VALUES (true), (false) AS t(p)
      """
    Then query result collected
      | cast_then | cast_else |
      | NULL      | 1         |
      | 1         | NULL      |

  Scenario Outline: CASE rejects impossible casts even in unreachable <position> with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <expression> AS result
      """
    Then query error (?i)(cast|type|coerc)

    Examples:
      | ansi  | position  | expression                                                                 |
      | false | value     | CASE WHEN true THEN 1 ELSE CAST(array(1) AS INT) END                         |
      | true  | value     | CASE WHEN true THEN 1 ELSE CAST(array(1) AS INT) END                         |
      | false | condition | CASE WHEN true THEN 1 WHEN CAST(array(1) AS BOOLEAN) THEN 2 ELSE 3 END        |
      | true  | condition | CASE WHEN true THEN 1 WHEN CAST(array(1) AS BOOLEAN) THEN 2 ELSE 3 END        |
