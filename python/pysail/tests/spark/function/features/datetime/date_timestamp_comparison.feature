Feature: Date comparisons preserve Spark local-day boundaries

  Scenario Outline: Date comparison at <case>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT
             (<date>) = TIMESTAMP '<bound>' AS eq,
             (<date>) <=> TIMESTAMP '<bound>' AS null_safe_eq,
             (<date>) <lt_op> TIMESTAMP '<bound>' AS lt,
             (<date>) <le_op> TIMESTAMP '<bound>' AS le,
             (<date>) > TIMESTAMP '<bound>' AS gt,
             (<date>) >= TIMESTAMP '<bound>' AS ge,
             (<date>) != TIMESTAMP '<bound>' AS ne,
             TIMESTAMP '<bound>' = (<date>) AS reverse_eq,
             TIMESTAMP '<bound>' <=> (<date>) AS reverse_null_safe_eq,
             TIMESTAMP '<bound>' <lt_op> (<date>) AS reverse_lt,
             TIMESTAMP '<bound>' <le_op> (<date>) AS reverse_le,
             TIMESTAMP '<bound>' > (<date>) AS reverse_gt,
             TIMESTAMP '<bound>' >= (<date>) AS reverse_ge,
             TIMESTAMP '<bound>' != (<date>) AS reverse_ne
      FROM <source>
      """
    Then query result
      | eq | null_safe_eq | lt | le | gt | ge | ne | reverse_eq | reverse_null_safe_eq | reverse_lt | reverse_le | reverse_gt | reverse_ge | reverse_ne |
      | <eq> | <null_safe_eq> | <lt> | <le> | <gt> | <ge> | <ne> | <reverse_eq> | <reverse_null_safe_eq> | <reverse_lt> | <reverse_le> | <reverse_gt> | <reverse_ge> | <reverse_ne> |

    Examples:
      | case | zone | date | bound | source | eq | null_safe_eq | lt | le | gt | ge | ne | reverse_eq | reverse_null_safe_eq | reverse_lt | reverse_le | reverse_gt | reverse_ge | reverse_ne | lt_op | le_op |
      | apia_literal | Pacific/Apia | DATE '2011-12-30' | 2011-12-31 00:00:00 | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | apia_constant_alias | Pacific/Apia | d | 2011-12-31 00:00:00 | (SELECT DATE '2011-12-30' d FROM range(1)) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | apia_constant_case | Pacific/Apia | CASE WHEN true THEN DATE '2011-12-30' ELSE date_add(DATE '2011-12-29',CAST(id AS INT)) END | 2011-12-31 00:00:00 | range(1) | false | false | true | true | false | false | true | false | false | false | false | true | true | true | < | <= |
      | apia_dynamic | Pacific/Apia | date_add(DATE '2011-12-30',CAST(id AS INT)) | 2011-12-31 00:00:00 | range(1) | false | false | true | true | false | false | true | false | false | false | false | true | true | true | < | <= |
      | toronto_gap_literal | America/Toronto | DATE '1919-03-31' | 1919-03-31 00:30:00 | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | toronto_gap_dynamic | America/Toronto | date_add(DATE '1919-03-31',CAST(id AS INT)) | 1919-03-31 00:30:00 | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | havana_first_literal | America/Havana | DATE '2015-11-01' | 2015-11-01 04:00:00Z | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | havana_first_dynamic | America/Havana | date_add(DATE '2015-11-01',CAST(id AS INT)) | 2015-11-01 04:00:00Z | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | havana_second_literal | America/Havana | DATE '2015-11-01' | 2015-11-01 05:00:00Z | range(1) | false | false | true | true | false | false | true | false | false | false | false | true | true | true | < | <= |
      | havana_second_dynamic | America/Havana | date_add(DATE '2015-11-01',CAST(id AS INT)) | 2015-11-01 05:00:00Z | range(1) | false | false | true | true | false | false | true | false | false | false | false | true | true | true | < | <= |
      | sao_gap_literal | America/Sao_Paulo | DATE '2018-11-04' | 2018-11-04 01:00:00 | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |
      | sao_gap_dynamic | America/Sao_Paulo | date_add(DATE '2018-11-04',CAST(id AS INT)) | 2018-11-04 01:00:00 | range(1) | true | true | false | true | false | true | false | true | true | false | true | false | true | false | < | <= |

  Scenario: Date comparison through alias dateadd
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT date_add(d,0) = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1))
      """
    Then query result
      | eq |
      | true |

  Scenario: Date comparison through alias case
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT CASE WHEN true THEN d ELSE DATE '2011-12-29' END = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1))
      """
    Then query result
      | eq |
      | true |

  Scenario: Date comparison through alias outer join
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM range(2) l LEFT JOIN (SELECT id, DATE '2011-12-30' d FROM range(1)) r ON l.id=r.id ORDER BY l.id
      """
    Then query result
      | eq |
      | false |
      | NULL |

  Scenario: Date comparison through alias inner join
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM range(2) l JOIN (SELECT id, DATE '2011-12-30' d FROM range(1)) r ON l.id=r.id
      """
    Then query result
      | eq |
      | true |

  Scenario: Date comparison through alias union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT DATE '2011-12-31' d FROM range(1))
      """
    Then query result
      | eq |
      | true |
      | true |

  Scenario: Date comparison through values date
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM VALUES(DATE '2011-12-30') AS t(d)
      """
    Then query result
      | eq |
      | true |

  Scenario: Date comparison through mixed union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1))
      """
    Then query result
      | eq |
      | true |
      | false |

  Scenario: Date comparison through filter over union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) WHERE d = TIMESTAMP '2011-12-31 00:00:00'
      """
    Then query result
      | d |
      | 2011-12-30 |

  Scenario: Date comparison through volatile filter union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) WHERE d = TIMESTAMP '2011-12-31 00:00:00' AND rand() < 1
      """
    Then query result
      | d |
      | 2011-12-30 |

  Scenario: Date comparison through projection over filter union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d, id FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d, id FROM range(1)) WHERE id = 0
      """
    Then query result
      | eq |
      | false |
      | false |

  Scenario: Date comparison through projection over constantfilter union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) WHERE true
      """
    Then query result
      | eq |
      | false |
      | false |

  Scenario: Date comparison through projection over volatilefilter union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) WHERE rand() < 1
      """
    Then query result
      | eq |
      | false |
      | false |

  Scenario: Date comparison through nested projection union
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT date_add(d,0) = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM (SELECT d FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)))
      """
    Then query result
      | eq |
      | true |
      | false |

  Scenario: Date comparison through lambda dynamic
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT transform(array(date_add(DATE '2011-12-30', CAST(id AS INT))), d -> d = TIMESTAMP '2011-12-31 00:00:00')[0] AS eq FROM range(1)
      """
    Then query result
      | eq |
      | false |

  Scenario: Date comparison through lambda local
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT transform(array(DATE '2011-12-30'), d -> d = TIMESTAMP '2011-12-31 00:00:00')[0] AS eq
      """
    Then query result
      | eq |
      | false |

  Scenario: Date comparison through nullable computed
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT id, d = TIMESTAMP '2011-12-31' AS eq, d <=> TIMESTAMP '2011-12-31' AS null_safe_eq, d < TIMESTAMP '2011-12-31' AS lt, d >= TIMESTAMP '2011-12-31' AS ge FROM (SELECT id, CASE WHEN id = 2 THEN CAST(NULL AS DATE) ELSE date_add(DATE '2011-12-29', CAST(id AS INT)) END AS d FROM range(4)) ORDER BY id
      """
    Then query result ordered
      | id | eq | null_safe_eq | lt | ge |
      | 0 | false | false | true | false |
      | 1 | false | false | true | false |
      | 2 | NULL | false | NULL | NULL |
      | 3 | false | false | false | true |

  Scenario: Date comparison through union sorted
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31' AS eq FROM (SELECT * FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) ORDER BY d)
      """
    Then query result ordered
      | eq |
      | false |
      | false |

  Scenario: Date comparison through union limited
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31' AS eq FROM (SELECT * FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1)) LIMIT 2)
      """
    Then query result
      | eq |
      | false |
      | false |

  Scenario: Date comparison through union volatile projection
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT count(*) AS matches FROM (SELECT rand() AS r, d = TIMESTAMP '2011-12-31' AS eq FROM (SELECT DATE '2011-12-30' d FROM range(1) UNION ALL SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) d FROM range(1))) WHERE r >= 0 AND eq
      """
    Then query result
      | matches |
      | 0 |

  Scenario: Date comparison through false error arm
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT CASE WHEN false THEN date_add(DATE '2011-12-30', CAST(id AS INT)) = to_timestamp('bad') ELSE true END AS eq FROM range(1)
      """
    Then query result
      | eq |
      | true |

  Scenario Outline: Python UDF dates are not foldable constants in <case>
    Given config spark.sql.session.timeZone = Pacific/Apia
    And scalar Python UDF date_offset_udf returns 0
    When query
      """
      SELECT <date> = TIMESTAMP '2011-12-31 00:00:00' AS eq FROM <source>
      """
    Then query result
      | eq |
      | false |

    Examples:
      | case | date | source |
      | direct expression | date_add(DATE '2011-12-30', CAST(date_offset_udf(1) AS INT)) | range(1) |
      | projection alias | d | (SELECT date_add(DATE '2011-12-30', CAST(date_offset_udf(1) AS INT)) AS d FROM range(1)) |

  Scenario: Python UDF prevents early local relation date comparison evaluation
    Given config spark.sql.session.timeZone = Pacific/Apia
    And scalar Python UDF date_offset_udf returns 0
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq, date_offset_udf(1) AS udf
      FROM VALUES(DATE '2011-12-30') t(d)
      """
    Then query result
      | eq | udf |
      | false | 0 |

  Scenario: Local relation date lambda is evaluated before cast unwrapping
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT transform(array(d), x -> x = TIMESTAMP '2011-12-31 00:00:00') AS eq
      FROM VALUES(DATE '2011-12-30') t(d)
      """
    Then query result
      | eq |
      | [true] |

  Scenario Outline: Null rejecting outer join filter preserves a foldable date for <filter>
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq
      FROM range(2) l LEFT JOIN (SELECT id, DATE '2011-12-30' d FROM range(1)) r ON l.id=r.id
      WHERE <filter>
      """
    Then query result
      | eq |
      | true |

    Examples:
      | filter |
      | r.id IS NOT NULL |
      | r.id = 0 |
      | d IS NOT NULL |
      | d = TIMESTAMP '2011-12-31 00:00:00' |
      | r.id = 0 OR r.id = 1 |
      | l.id = r.id |
      | l.id + r.id = 0 |
      | abs(r.id) = l.id |
      | date_add(r.d, CAST(l.id AS INT)) = DATE '2011-12-30' |
      | year(r.d) = 2011 + l.id |
      | date_part('MONTH', r.d) = 12 + l.id |
      | last_day(r.d) = date_add(DATE '2011-12-31', CAST(l.id AS INT)) |

  Scenario Outline: Null accepting outer join filter keeps date comparisons dynamic for <filter>
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS eq
      FROM range(2) l LEFT JOIN (SELECT id, DATE '2011-12-30' d FROM range(1)) r ON l.id=r.id
      WHERE <filter> ORDER BY l.id
      """
    Then query result ordered
      | eq |
      | false |
      | NULL |

    Examples:
      | filter |
      | r.id = 0 OR l.id = 1 |
      | coalesce(r.id,0) = 0 |
      | year(r.d) = 2011 OR r.d IS NULL |
      | date_part('MONTH', r.d) = 12 OR r.d IS NULL |
      | last_day(r.d) = DATE '2011-12-31' OR r.d IS NULL |

  Scenario Outline: Date membership comparison preserves Spark <case> optimization
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT <date> <predicate> AS eq FROM range(1)
      """
    Then query result
      | eq |
      | <eq> |

    Examples:
      | case | date | predicate | eq |
      | single literal | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (TIMESTAMP '2011-12-31') | false |
      | repeated literal | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (TIMESTAMP '2011-12-31', TIMESTAMP '2011-12-31') | false |
      | distinct literals | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (TIMESTAMP '2011-12-31', TIMESTAMP '2012-01-01') | true |
      | negated single literal | date_add(DATE '2011-12-30', CAST(id AS INT)) | NOT IN (TIMESTAMP '2011-12-31') | true |
      | constant folding | DATE '2011-12-30' | IN (TIMESTAMP '2011-12-31') | true |
      | folded string cast | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CAST('2011-12-31' AS TIMESTAMP)) | false |
      | folded timestamp function | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (to_timestamp('2011-12-31')) | false |
      | folded timestamp arithmetic | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (TIMESTAMP '2011-12-31' + INTERVAL 0 SECOND) | false |
      | folded conditional | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CASE WHEN true THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | false |
      | duplicates after folding | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (TIMESTAMP '2011-12-31', CAST('2011-12-31' AS TIMESTAMP)) | false |

  Scenario: Date membership uses a foldable timestamp alias
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT date_add(DATE '2011-12-30', CAST(id AS INT)) IN (t) AS eq
      FROM (SELECT id, TIMESTAMP '2011-12-31' AS t FROM range(1))
      """
    Then query result
      | eq |
      | false |

  Scenario Outline: Cross side <filter> does not infer a null rejecting constraint
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31' AS eq
      FROM range(2) l LEFT JOIN (SELECT id, DATE '2011-12-30' d FROM range(1)) r ON l.id=r.id
      WHERE <filter>
      """
    Then query result
      | eq |
      | false |

    Examples:
      | filter |
      | l.id <=> r.id |
      | CASE WHEN r.id IS NULL THEN false ELSE l.id = r.id END |

  Scenario Outline: Date comparison preserves <case> random timestamp bounds
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT <date> <predicate> AS eq FROM range(1)
      """
    Then query result
      | eq |
      | <eq> |

    Examples:
      | case | date | predicate | eq |
      | fixed lower bound | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CASE WHEN rand(0) >= 0 THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | false |
      | reversed fixed bound | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CASE WHEN 0 <= rand(0) THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | false |
      | fixed upper bound | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CASE WHEN rand(0) >= 1 THEN TIMESTAMP '2012-01-01' ELSE TIMESTAMP '2011-12-31' END) | false |
      | variable bound | date_add(DATE '2011-12-30', CAST(id AS INT)) | IN (CASE WHEN rand(0) > 0.5 THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | true |
      | constant date membership | DATE '2011-12-30' | IN (CASE WHEN rand(0) >= 0 THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | true |
      | constant date equality | DATE '2011-12-30' | = (CASE WHEN rand(0) >= 0 THEN TIMESTAMP '2011-12-31' ELSE TIMESTAMP '2012-01-01' END) | true |
