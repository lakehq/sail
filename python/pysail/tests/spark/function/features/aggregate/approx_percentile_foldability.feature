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
