Feature: Approximate percentiles follow Spark's rank summary and preserve input types

  Scenario Outline: Approximate percentile aliases accept scalar and array percentages
    When query
      """
      SELECT <function>(v, 0.5) AS scalar,
             <function>(v, array(0.5, 0.4, 0.1, 1.0, 0.5), 100) AS percentiles
      FROM VALUES (0), (1), (2), (10), (NULL) AS t(v)
      """
    Then query result
      | scalar | percentiles      |
      | 1      | [1, 1, 0, 10, 1] |

    Examples:
      | function          |
      | approx_percentile |
      | percentile_approx |

  Scenario Outline: Accuracy controls the approximation rather than t-digest size
    When query
      """
      SELECT percentile_approx(v, array(0D, 0.25D, 0.5D, 0.75D, 1D), <accuracy>) AS p
      FROM VALUES (0), (1), (2), (10) AS t(v)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | accuracy | result            |
      | 1        | [0, 0, 0, 0, 0]   |
      | 2        | [0, 0, 0, 10, 10] |
      | 10000    | [0, 0, 1, 2, 10]  |

  Scenario Outline: Approximate percentile retains numeric input types
    When query
      """
      SELECT typeof(percentile_approx(CAST(v AS <type>), 0.5)) AS type,
             CAST(percentile_approx(CAST(v AS <type>), 0.5) AS DOUBLE) AS value
      FROM VALUES (1), (2), (3), (4) AS t(v)
      """
    Then query result
      | type   | value |
      | <type> | 2.0   |

    Examples:
      | type          |
      | tinyint       |
      | smallint      |
      | int           |
      | bigint        |
      | float         |
      | double        |
      | decimal(10,2) |

  Scenario Outline: Approximate percentile preserves a <type> column
    When query
      """
      SELECT CAST(percentile_approx(CAST(d AS <type>), 0.5) AS STRING) AS p
      FROM VALUES (DATE '2020-01-01'), (DATE '2020-01-02'), (DATE '2020-01-03') AS t(d)
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | type          | result              |
      | DATE          | 2020-01-02          |
      | TIMESTAMP     | 2020-01-02 00:00:00 |
      | TIMESTAMP_NTZ | 2020-01-02 00:00:00 |

  @sail-bug
  Scenario: Aggregate names distinguish a column from its cast to another temporal type
    When query
      """
      SELECT percentile_approx(d, 0.5) AS d,
             percentile_approx(CAST(d AS TIMESTAMP_NTZ), 0.5) AS ts
      FROM VALUES (DATE '2020-01-01'), (DATE '2020-01-02'), (DATE '2020-01-03') AS t(d)
      """
    Then query result
      | d          | ts                  |
      | 2020-01-02 | 2020-01-02 00:00:00 |

  Scenario: Approximate percentile preserves ANSI intervals
    When query
      """
      SELECT CAST(percentile_approx(m, 0.5) AS INT) AS months,
             CAST(percentile_approx(s, 0.5) AS BIGINT) AS seconds
      FROM VALUES (INTERVAL '0' MONTH, INTERVAL '0' SECOND),
                  (INTERVAL '1' MONTH, INTERVAL '1' SECOND),
                  (INTERVAL '2' MONTH, INTERVAL '2' SECOND),
                  (INTERVAL '10' MONTH, INTERVAL '10' SECOND) AS t(m, s)
      """
    Then query result
      | months | seconds |
      | 1      | 1       |

  Scenario: Empty percentages and empty input return null
    When query
      """
      SELECT percentile_approx(v, array()) AS empty_percentages,
             percentile_approx(CAST(NULL AS INT), array(0.5)) AS all_null,
             percentile_approx(v, 0.5) FILTER (WHERE false) AS no_rows
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | empty_percentages | all_null | no_rows |
      | NULL              | NULL     | NULL    |

  Scenario: Percentages and accuracy accept foldable expressions
    When query
      """
      SELECT percentile_approx(v, array(1D / 4, 1D / 2), 50 + 50) AS p
      FROM VALUES (0), (1), (2), (10) AS t(v)
      """
    Then query result
      | p      |
      | [0, 1] |

  Scenario: Approximate percentile supports grouping, distinct and filters
    When query
      """
      SELECT g, approx_percentile(v, 0.5) AS p,
             percentile_approx(DISTINCT v, 0.5) AS distinct_p,
             approx_percentile(v, 0.5) FILTER (WHERE v > 1) AS filtered_p
      FROM VALUES (1, 1), (1, 1), (1, 2), (1, 10), (2, 7), (2, 9) AS t(g, v)
      GROUP BY g ORDER BY g
      """
    Then query result
      | g | p | distinct_p | filtered_p |
      | 1 | 1 | 2          | 2          |
      | 2 | 7 | 7          | 7          |

  Scenario: Approximate percentile supports cumulative and bounded windows
    When query
      """
      SELECT id,
             approx_percentile(id, 0.5) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative,
             percentile_approx(id, array(0D, 0.5D, 1D)) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS bounded
      FROM range(5)
      """
    Then query result
      | id | cumulative | bounded   |
      | 0  | 0          | [0, 0, 1] |
      | 1  | 0          | [0, 1, 2] |
      | 2  | 1          | [1, 2, 3] |
      | 3  | 1          | [2, 3, 4] |
      | 4  | 2          | [3, 3, 4] |

  Scenario Outline: Invalid percentile parameters fail
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error .*

    Examples:
      | percentage       | accuracy    |
      | -0.1             | 10000       |
      | 1.1              | 10000       |
      | array(0.5, 1.1)  | 10000       |
      | NULL             | 10000       |
      | array(NULL, 0.5) | 10000       |
      | array('0.5')     | 10000       |
      | v / 2D           | 10000       |
      | 0.5              | v           |
      | 0.5              | 0           |
      | 0.5              | -1          |
      | 0.5              | 2147483648L |
      | 0.5              | 1.5         |
      | 0.5              | '100'       |
      | 0.5              | NULL        |

  @sail-bug
  Scenario: Decimal SQL string conversion uses Spark's runtime scale
    When query
      """
      SELECT CAST(percentile_approx(v, 0.5) AS STRING) AS p
      FROM VALUES (CAST(2.675123456789123456 AS DECIMAL(38,18))) AS t(v)
      """
    Then query result
      | p                  |
      | 2.6751234567891236 |

  Scenario: Already-double null percentage array elements select the minimum
    When query
      """
      SELECT percentile_approx(v, array(CAST(NULL AS DOUBLE), 0.5D)) AS p
      FROM VALUES (1), (2), (3) AS t(v)
      """
    Then query result
      | p      |
      | [1, 2] |

  Scenario: Distinct approximate percentiles normalize signed floating zero
    When query
      """
      SELECT percentile_approx(DISTINCT v, 0.5) AS p
      FROM VALUES (-0D), (0D), (1D), (2D) AS t(v)
      """
    Then query result
      | p   |
      | 1.0 |

  @sail-bug
  Scenario: NaN percentiles require matching local relation partitioning
    Given config spark.sql.leafNodeDefaultParallelism = 4
    When query
      """
      SELECT isnan(percentile_approx(v, 0.5, 1000)) AS p
      FROM VALUES (double('NaN')), (CAST(NULL AS DOUBLE)), (2.1D), (0.5D) AS t(v)
      """
    Then query result
      | p    |
      | true |


  Scenario Outline: Invalid approximate percentile parameters fail even when grouped input is empty
    When query
      """
      SELECT percentile_approx(v, <percentage>, <accuracy>) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query error .*

    Examples:
      | percentage      | accuracy    |
      | 0.5             | 0           |
      | 0.5             | NULL        |
      | 0.5             | 2147483648L |
      | 0.5             | 1 - 1       |
      | 2               | 10000       |
      | NULL            | 10000       |
      | array(0.5, 2D)  | 10000       |
      | 1D + 1D         | 10000       |

  Scenario Outline: Valid approximate percentile parameters retain empty grouped results
    When query
      """
      SELECT approx_percentile(v, <percentage>, 10000) AS p
      FROM (SELECT 1 AS v WHERE false) AS t GROUP BY v
      """
    Then query result
      | p |

    Examples:
      | percentage            |
      | 0.5                   |
      | array()               |
      | CAST('NaN' AS DOUBLE) |

  Scenario Outline: Approximate percentiles coerce numeric strings and quoted percentages in ANSI <ansi> mode
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT percentile_approx(v, '0.5') AS p,
             approx_percentile(v, array(0D, 0.5D, 1D)) AS percentiles
      FROM VALUES ('10'), ('2'), ('30'), (NULL) AS t(v)
      """
    Then query result
      | p    | percentiles       |
      | 10.0 | [2.0, 10.0, 30.0] |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario: Approximate percentile ignores malformed strings in non-ANSI mode
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT percentile_approx(v, '0.5') AS p
      FROM VALUES ('bad'), ('2'), ('10'), (NULL) AS t(v)
      """
    Then query result
      | p   |
      | 2.0 |

  Scenario: Approximate percentile rejects malformed strings in ANSI mode
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT approx_percentile(v, '0.5') AS p
      FROM VALUES ('bad'), ('2'), ('10') AS t(v)
      """
    Then query error (?i)cast

  Scenario: Approximate percentile filters malformed strings before the ANSI cast
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT approx_percentile(v, '0.5') FILTER (WHERE keep) AS p
      FROM VALUES ('bad', false), ('bad', NULL), ('2', true), ('10', true) AS t(v, keep)
      """
    Then query result
      | p   |
      | 2.0 |

  @sail-bug
  Scenario: Legacy decimal arithmetic must retain nullable percentage array elements
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT approx_percentile(v, array(0.25 + 0.25), 100 + 100) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query error (?i)unexpected_input_type|non-nullable

  Scenario: ANSI decimal arithmetic permits non-nullable percentage array elements
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT approx_percentile(v, array(0.25 + 0.25), 100 + 100) AS p
      FROM VALUES (1), (2) AS t(v)
      """
    Then query result
      | p   |
      | [1] |
