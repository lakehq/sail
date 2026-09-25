Feature: Approximate percentile parameters require Spark-foldable expressions

  Scenario Outline: Higher-order parameters are rejected regardless of grouped input size
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE <keep>) AS t
      GROUP BY v
      """
    Then query error (?i)foldable

    Examples:
      | function          | percentage                     | accuracy                                      | keep  |
      | approx_percentile | transform(array(0.5D), x -> x) | 10000                                         | true  |
      | approx_percentile | transform(array(0.5D), x -> x) | 10000                                         | false |
      | percentile_approx | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) | true  |
      | percentile_approx | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) | false |
      | percentile_approx | array_sort(array(0.5D))        | 10000                                         | true  |
      | percentile_approx | array_sort(array(0.5D))        | 10000                                         | false |

  Scenario Outline: Higher-order parameters are rejected in window aggregates
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) OVER () AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                     | accuracy                                      |
      | transform(array(0.5D), x -> x) | 10000                                         |
      | 0.5D                           | aggregate(array(100), 0, (acc, x) -> acc + x) |

  Scenario: Foldable sequence and regexp expressions remain valid parameters
    When query
      """
      SELECT percentile_approx(v, sequence(0, 1), 100 * regexp_instr('abc', 'a')) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p      |
      | [1, 2] |

  Scenario Outline: Sort array remains foldable in either direction
    When query
      """
      SELECT percentile_approx(v, sort_array(array(0D, 1D), <ascending>)) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | ascending | result |
      | true      | [1, 2] |
      | false     | [2, 1] |

  Scenario Outline: Approximate percentile parameters simplify foldable coalesce expressions
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | function          | percentage                                           | accuracy                           | result |
      | approx_percentile | coalesce(CAST(NULL AS DOUBLE), 0.5D)                 | 10000                              | 2      |
      | percentile_approx | coalesce(CAST(NULL AS ARRAY<DOUBLE>), array(0D, 1D)) | 10000                              | [1, 4] |
      | approx_percentile | 0.5D                                                 | coalesce(CAST(NULL AS INT), 10000) | 2      |

  Scenario: Foldable coalesce parameters work in window aggregates
    When query
      """
      SELECT v, percentile_approx(v, coalesce(CAST(NULL AS DOUBLE), 0.5D),
                 coalesce(CAST(NULL AS INT), 10000)) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |

  Scenario: Foldable coalesce parameters retain empty grouped results
    When query
      """
      SELECT approx_percentile(v, coalesce(CAST(NULL AS DOUBLE), 0.5D)) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query result
      | p |

  Scenario Outline: Simplification does not make NVL wrappers foldable percentile parameters
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)foldable

    Examples:
      | percentage                         | accuracy                          |
      | ifnull(CAST(NULL AS DOUBLE), 0.5D) | 10000                             |
      | 0.5D                               | nvl2(CAST(NULL AS INT), 0, 10000) |

  @sail-bug
  Scenario: Nullif parameters retain Spark's non-foldable wrapper
    When query
      """
      SELECT percentile_approx(v, nullif(0.5D, 1D)) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query error (?i)foldable

  Scenario Outline: Approximate percentile parameters use the current query time
    When query
      """
      SELECT <function>(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | p |
      | 2 |

    Examples:
      | function          | percentage                                                     | accuracy                           |
      | percentile_approx | dayofmonth(current_date()) / (2D * dayofmonth(current_date())) | 10000                              |
      | approx_percentile | 0.5D                                                           | dayofmonth(current_date()) * 10000 |
      | percentile_approx | 0.5D                                                           | year(current_timestamp()) - 1970   |

  Scenario: Foldable current-date parameters work in window aggregates
    When query
      """
      SELECT v, percentile_approx(v,
                 dayofmonth(current_date()) / (2D * dayofmonth(current_date())),
                 dayofmonth(current_date()) * 10000) OVER () AS p
      FROM VALUES (1), (2) AS t(v) ORDER BY v
      """
    Then query result
      | v | p |
      | 1 | 1 |
      | 2 | 1 |

  Scenario: Foldable current-date parameters retain empty grouped results
    When query
      """
      SELECT percentile_approx(v,
                 dayofmonth(current_date()) / (2D * dayofmonth(current_date()))) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query result
      | p |
