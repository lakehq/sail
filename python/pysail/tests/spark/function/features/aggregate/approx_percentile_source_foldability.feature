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
