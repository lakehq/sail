Feature: Conditional type observations retain scoped producer types

  # Spark's regexp_count and regexp_instr return INT. Their Sail execution
  # implementations retain their existing Arrow types; these tests exercise
  # only their contribution to CASE/IF observations and literal consumers.

  Scenario Outline: Signed parents distinguish nested producers and explicit casts with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        typeof(if(false, CAST(0 AS SMALLINT),
          if(false, 0, regexp_count('aba', 'a')))) AS nested_type,
        typeof(if(false, CAST(0 AS SMALLINT), c)) AS projected_type,
        typeof(if(false, CAST(0 AS SMALLINT), wide)) AS bigint_cast_type,
        typeof(if(false, CAST(0 AS SMALLINT), narrow)) AS int_cast_type
      FROM (
        SELECT c, CAST(c AS BIGINT) AS wide, CAST(c AS INT) AS narrow
        FROM (SELECT if(false, 0, regexp_count('aba', 'a')) AS c) producer
      ) aliases
      """
    Then query result collected
      | nested_type | projected_type | bigint_cast_type | int_cast_type |
      | int         | int            | bigint           | int           |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Filters observe the types of their input producers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id
      FROM (
        SELECT id, regexp_instr(s, 'a') AS c,
          CAST(regexp_instr(s, 'a') AS BIGINT) AS wide
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      ) producer
      WHERE typeof(if(false, 0, c)) = 'int'
        AND typeof(if(false, 0, wide)) = 'bigint'
      """
    Then query result collected
      | id |
      | 0  |
      | 1  |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Join predicates distinguish matching names in both inputs with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      WITH left_input AS (
        SELECT id, regexp_instr(s, 'a') AS c
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      ), right_input AS (
        SELECT id, CAST(regexp_instr(s, 'a') AS BIGINT) AS c
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      )
      SELECT l.id
      FROM left_input l JOIN right_input r
        ON l.id = r.id
        AND typeof(if(false, 0, l.c)) = 'int'
        AND typeof(if(false, 0, r.c)) = 'bigint'
      """
    Then query result collected
      | id |
      | 0  |
      | 1  |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Correlated observations retain the outer producer scope with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      WITH outer_input AS (
        SELECT id, regexp_instr(s, 'a') AS c,
          CAST(regexp_instr(s, 'a') AS BIGINT) AS wide
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      )
      SELECT o.id FROM outer_input o
      WHERE EXISTS (
        SELECT 1 FROM VALUES (0), (1) i(id)
        WHERE i.id = o.id
          AND typeof(if(false, 0, o.c)) = 'int'
          AND typeof(if(false, 0, o.wide)) = 'bigint'
      )
      """
    Then query result collected
      | id |
      | 0  |
      | 1  |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Lambda observations distinguish projected element producers and array casts with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        transform(a, x -> typeof(if(false, 0, x))) AS narrow_types,
        transform(wide, x -> typeof(if(false, 0, x))) AS wide_types
      FROM (
        SELECT id, array(regexp_instr(s, 'a')) AS a,
          CAST(array(regexp_instr(s, 'a')) AS ARRAY<BIGINT>) AS wide
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      ) producer
      """
    Then query result collected
      | id | narrow_types | wide_types |
      | 0  | ['int']      | ['bigint'] |
      | 1  | ['int']      | ['bigint'] |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Nested lambda observations respect parameter shadowing with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT transform(a, x -> named_struct(
        'outer_type', typeof(if(false, 0, x)),
        'inner_types', transform(wide, x -> typeof(if(false, 0, x)))
      )) AS result
      FROM (
        SELECT array(regexp_instr('aba', 'a')) AS a,
          CAST(array(regexp_instr('aba', 'a')) AS ARRAY<BIGINT>) AS wide
      ) producer
      """
    Then query result collected
      | result                                                   |
      | [Row(outer_type='int', inner_types=['bigint'])]             |

    Examples:
      | ansi  |
      | false |
      | true  |
