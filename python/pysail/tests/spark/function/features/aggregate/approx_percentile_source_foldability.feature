Feature: Approximate percentile validates source expression foldability

  Scenario Outline: Runtime wrappers remain non-foldable after lowering
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                            | accuracy                                       | window  |
      | 0.5D                                  | year(to_date('2020-01-01'))                     |         |
      | 0.5D                                  | year(to_timestamp('2020-01-01'))                | OVER () |
      | 0.5D                                  | year(to_timestamp_ltz('2020-01-01'))            |         |
      | 0.5D                                  | year(to_timestamp_ntz('2020-01-01'))            | OVER () |
      | 0.5D                                  | year(try_to_timestamp('2020-01-01'))            |         |
      | 0.5D                                  | CAST(date_part('YEAR', DATE '2020-01-01') AS INT) | OVER () |
      | 0.5D                                  | CAST(datepart('YEAR', DATE '2020-01-01') AS INT)  |         |
      | 0.5D                                  | CAST(extract(YEAR FROM DATE '2020-01-01') AS INT) | OVER () |
      | 0.5D                                  | array_size(array(1, 2))                        |         |
      | 0.5D                                  | size(arrays_zip(array(1, 2)))                   | OVER () |
      | 0.5D                                  | size(map_concat(map(1, 1), map(2, 2)))         |         |
      | IF(map_contains_key(map(1, 1), 1), 0.5D, 0D) | 100                                     | OVER () |
      | nullif(0.5D, 1D)                      | 100                                            |         |
      | IF(equal_null(1, 1), 0.5D, 0D)       | 100                                            | OVER () |
      | IF('a' ILIKE 'A', 0.5D, 0D)           | 100                                            |         |
      | IF(ilike('a', 'A'), 0.5D, 0D)         | 100                                            | OVER () |
      | CAST(regexp_substr('0.5', '0.5') AS DOUBLE) | 100                                        |         |
      | decode(1, 1, 0.5D, 0D)               | 100                                            | OVER () |
      | 0.5D                                  | length(version())                              |         |

  Scenario Outline: Source foldability is checked even for empty grouped input
    When query
      """
      SELECT approx_percentile(v, 0.5D, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | accuracy                          |
      | year(to_date('2020-01-01'))        |
      | array_size(array(1, 2))           |
      | size(map_concat(map(1, 1)))       |
      | length(regexp_substr('100', '1')) |

  @spark-4
  Scenario Outline: New scalar wrappers remain non-foldable percentile parameters
    When query
      """
      SELECT approx_percentile(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                             | accuracy                                    | window  |
      | nullifzero(0.5D)                      | 100                                         |         |
      | zeroifnull(0.5D)                      | 100                                         | OVER () |
      | CAST(validate_utf8('0.5') AS DOUBLE)  | 100                                         |         |
      | CAST(try_validate_utf8('0.5') AS DOUBLE) | 100                                       | OVER () |
      | 0.5D                                   | year(try_make_timestamp(2020, 1, 1, 0, 0, 0)) |         |

  @spark-4.1
  Scenario Outline: Time and geospatial wrappers remain non-foldable percentile parameters
    Given config spark.sql.timeType.enabled = true
    Given config spark.sql.geospatial.enabled = true
    When query
      """
      SELECT percentile_approx(v, 0.5D, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | accuracy                                                                                   | window  |
      | hour(make_time(10, 0, 0))                                                                    |         |
      | hour(to_time('10:00:00'))                                                                    | OVER () |
      | hour(try_to_time('10:00:00'))                                                                |         |
      | hour(TIME '10:00:00' - INTERVAL '1' HOUR)                                                     | OVER () |
      | length(st_asbinary(st_geomfromwkb(unhex('010100000000000000000000000000000000000000'))))     |         |
      | length(st_asbinary(st_geogfromwkb(unhex('010100000000000000000000000000000000000000'))))     | OVER () |

  Scenario Outline: Datetime subtraction retains its non-foldable wrapper
    When query
      """
      SELECT approx_percentile(v, 0.5D, year(<datetime> - <interval>)) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | datetime               | interval           | window  |
      | DATE '2020-01-01'       | INTERVAL '1' MONTH  |         |
      | DATE '2020-01-01'       | INTERVAL '1' SECOND | OVER () |
      | TIMESTAMP '2020-01-01'  | INTERVAL '1' MONTH  | OVER () |
      | TIMESTAMP '2020-01-01'  | INTERVAL '1' DAY    |         |

  Scenario Outline: Implicit array coercion does not make a foldable parameter a higher-order function
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT percentile_approx(v,
               IF(array(TIMESTAMP '2020-01-01') IN (array('2020-01-01 00:00:00')), 0.5D, 0.25D)
             ) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | p |
      | 2 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario: Implicit array coercion remains foldable in percentile windows
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT v, approx_percentile(v,
               IF(array(TIMESTAMP '2020-01-01') IN (array('2020-01-01 00:00:00')), 0.5D, 0.25D)
             ) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |

  Scenario: Foldable implementations shared with non-foldable wrappers remain valid
    When query
      """
      SELECT percentile_approx(v, 1D / 2D,
               year(CAST('2020-01-01' AS DATE))) AS cast_parameter,
             percentile_approx(v, IF(array_contains(array(1), 1), 0.5D, 0D),
               size(array(1, 2))) AS collection_parameter,
             percentile_approx(v, CAST(regexp_extract('0.5', '(.*)', 1) AS DOUBLE),
               get(array(100), 0)) AS string_parameter,
             percentile_approx(v, 0.5D,
               year(DATE '2020-01-01' - INTERVAL '1' DAY)) AS day_subtraction,
             percentile_approx(v, 0.5D,
               year(DATE '2020-01-01' + INTERVAL '1' MONTH)) AS interval_addition
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | cast_parameter | collection_parameter | string_parameter | day_subtraction | interval_addition |
      | 1              | 1                    | 1                | 1               | 1                 |

  Scenario: Typeof suppresses runtime wrapper restrictions without changing sibling scopes
    When query
      """
      SELECT percentile_approx(array_size(array(v)),
               IF(typeof(map_concat(map(1, 1))) = 'map<int,int>', 0.5D, 0D),
               length(typeof(to_date('2020-01-01'))) + 100) AS p,
             array_size(array(1, 2)) AS size
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p | size |
      | 1 | 2    |

  @spark-4
  Scenario Outline: SQL between predicates remain non-foldable percentile parameters
    Given config spark.sql.legacy.duplicateBetweenInput = false
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                          | accuracy                           | window  |
      | IF(1 BETWEEN 0 AND 2, 0.5D, 0.25D)  | 100                                |         |
      | IF(1 NOT BETWEEN 0 AND 2, 0D, 0.5D) | 100                                | OVER () |
      | 0.5D                                | IF(1 BETWEEN 0 AND 2, 100, 10)     | OVER () |
      | 0.5D                                | IF(1 NOT BETWEEN 0 AND 2, 10, 100) |         |

  @spark-4
  Scenario: Between foldability is checked before empty grouped execution
    Given config spark.sql.legacy.duplicateBetweenInput = false
    When query
      """
      SELECT approx_percentile(v, IF(1 BETWEEN 0 AND 2, 0.5D, 0D)) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query error (?i)foldable

  @spark-4
  Scenario: Legacy between parameters remain foldable
    Given config spark.sql.legacy.duplicateBetweenInput = true
    When query
      """
      SELECT approx_percentile(v, IF(1 BETWEEN 0 AND 2, 0.5D, 0D),
               IF(1 NOT BETWEEN 0 AND 2, 10, 100)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p |
      | 1 |

  @spark-4.1
  Scenario Outline: Time extractors retain source non-foldability
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT approx_percentile(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                   | accuracy                | window  |
      | hour(TIME '01:00:00') / 2D   | 100                     |         |
      | minute(TIME '00:01:00') / 2D | 100                     | OVER () |
      | second(TIME '00:00:01') / 2D | 100                     |         |
      | 0.5D                         | hour(TIME '10:00:00')   | OVER () |
      | 0.5D                         | minute(TIME '00:10:00') |         |
      | 0.5D                         | second(TIME '00:00:10') | OVER () |

  Scenario: Comparison and timestamp extractor parameters remain foldable
    When query
      """
      SELECT approx_percentile(v, IF(1 >= 0 AND 1 <= 2, 0.5D, 0D),
               hour(TIMESTAMP '2020-01-01 10:00:00')) AS h,
             approx_percentile(v, 0.5D, minute(TIMESTAMP '2020-01-01 00:10:00')) AS m,
             approx_percentile(v, 0.5D, second(TIMESTAMP '2020-01-01 00:00:10')) AS s,
             approx_percentile(v, IF(typeof(1 BETWEEN 0 AND 2) = 'boolean', 0.5D, 0D)) AS t
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | h | m | s | t |
      | 1 | 1 | 1 | 1 |

  @spark-4.1
  Scenario: Typeof suppresses time extractor source restrictions
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT approx_percentile(v, IF(typeof(hour(TIME '01:00:00')) = 'int', 0.5D, 0D)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p |
      | 1 |

  Scenario Outline: Binary predicates retain non-foldable wrappers
    When query
      """
      SELECT approx_percentile(v, <percentage>, <accuracy>) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                               | accuracy                                | window  |
      | IF(contains(X'6162', X'61'), 0.5D, 0D)   | 100                                     |         |
      | IF(startswith(X'6162', X'61'), 0.5D, 0D) | 100                                     | OVER () |
      | IF(endswith(X'6162', X'62'), 0.5D, 0D)   | 100                                     |         |
      | 0.5D                                     | IF(contains(X'6162', X'61'), 100, 10)   | OVER () |
      | 0.5D                                     | IF(startswith(X'6162', X'61'), 100, 10) |         |
      | 0.5D                                     | IF(endswith(X'6162', X'62'), 100, 10)   | OVER () |

  Scenario Outline: Binary padding retains its non-foldable wrapper
    Given config spark.sql.legacy.lpadRpadAlwaysReturnString = false
    When query
      """
      SELECT percentile_approx(v, 0.5D, length(<padding>)) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | padding               | window  |
      | lpad(X'61', 2, X'62') |         |
      | rpad(X'61', 2, X'62') | OVER () |
      | lpad(X'61', 2)        | OVER () |
      | rpad(X'61', 2)        |         |

  Scenario: String and mixed binary string operations remain foldable
    Given config spark.sql.legacy.lpadRpadAlwaysReturnString = false
    When query
      """
      SELECT approx_percentile(v, IF(contains('ab', 'a'), 0.5D, 0D)) AS c,
             approx_percentile(v, IF(startswith('ab', 'a'), 0.5D, 0D)) AS s,
             approx_percentile(v, IF(endswith('ab', 'b'), 0.5D, 0D)) AS e,
             approx_percentile(v, IF(contains(X'6162', 'a'), 0.5D, 0D)) AS mixed,
             approx_percentile(v, 0.5D, length(lpad('a', 2, 'b'))) AS lp,
             approx_percentile(v, 0.5D, length(rpad(X'61', 2, 'b'))) AS rp,
             approx_percentile(v,
               IF(typeof(contains(X'6162', X'61')) = 'boolean', 0.5D, 0D)) AS t
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | c | s | e | mixed | lp | rp | t |
      | 1 | 1 | 1 | 1     | 1  | 1  | 1 |

  Scenario: Legacy binary padding remains foldable
    Given config spark.sql.legacy.lpadRpadAlwaysReturnString = true
    When query
      """
      SELECT approx_percentile(v, 0.5D, length(lpad(X'61', 2, X'62'))) AS l,
             approx_percentile(v, 0.5D, length(rpad(X'61', 2, X'62'))) AS r
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | l | r |
      | 1 | 1 |

  @spark-4.1
  Scenario Outline: Date time timestamp constructors retain non-foldable wrappers
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT percentile_approx(v, 0.5D, year(<timestamp>)) <window> AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | timestamp                                                         | window  |
      | make_timestamp(DATE '2020-01-01')                                 |         |
      | make_timestamp(DATE '2020-01-01', TIME '12:00:00')                | OVER () |
      | make_timestamp(DATE '2020-01-01', TIME '12:00:00', 'UTC')         |         |
      | make_timestamp_ltz(DATE '2020-01-01', TIME '12:00:00')            |         |
      | make_timestamp_ltz(DATE '2020-01-01', TIME '12:00:00', 'UTC')     | OVER () |
      | try_make_timestamp_ltz(DATE '2020-01-01', TIME '12:00:00')        | OVER () |
      | try_make_timestamp_ltz(DATE '2020-01-01', TIME '12:00:00', 'UTC') |         |

  @spark-4.1
  Scenario Outline: Component and NTZ timestamp constructors remain foldable
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT approx_percentile(v, 0.5D, year(<timestamp>)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p |
      | 1 |

    Examples:
      | timestamp                                                  |
      | make_timestamp(2020, 1, 1, 12, 0, 0)                       |
      | make_timestamp_ltz(2020, 1, 1, 12, 0, 0)                   |
      | try_make_timestamp_ltz(2020, 1, 1, 12, 0, 0)               |
      | make_timestamp_ntz(DATE '2020-01-01', TIME '12:00:00')     |
      | try_make_timestamp_ntz(DATE '2020-01-01', TIME '12:00:00') |
