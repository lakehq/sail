Feature: Scalar subqueries in distributed execution
  Scenario: Sampling auxiliaries do not shadow correlated outer attributes
    When query
      """
      WITH filter_sample_input AS (SELECT 1 AS id)
      SELECT rand_value FROM VALUES (100), (-100) AS outer_t(rand_value)
      WHERE EXISTS (
        SELECT 1 FROM filter_sample_input
        TABLESAMPLE (100 PERCENT) REPEATABLE (42)
        WHERE rand_value > 1
      )
      """
    Then query result collected
      | rand_value |
      | 100        |

  Scenario Outline: Filter subqueries preserve lateral correlation scope
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW lateral_scalar_outer AS
      SELECT * FROM VALUES (0, 1), (1, 2) AS t(c1, c2)
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW lateral_scalar_inner AS
      SELECT * FROM VALUES (0, 2), (0, 3) AS t(c1, c2)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS lateral_scalar_outer
      """
    Given final statement
      """
      DROP VIEW IF EXISTS lateral_scalar_inner
      """
    When query
      """
      SELECT * FROM lateral_scalar_outer
      WHERE <predicate> (
        SELECT <projection> FROM lateral_scalar_inner, LATERAL (SELECT c1 AS a)
        <correlation>
      )
      """
    Then query result collected
      | c1 | c2 |
      | 0  | 1  |

    Examples:
      | predicate | projection | correlation                       |
      | c1 =      | MIN(a)     |                                   |
      | c1 =      | MIN(a)     | WHERE c1 = lateral_scalar_outer.c1 |
      | EXISTS    | 1          | WHERE a = lateral_scalar_outer.c1  |
      | c1 IN     | a          |                                   |

  Scenario: Filter subqueries resolve columns hidden by unaliased derived tables as outer references
    When query
      """
      SELECT c FROM VALUES (1), (3) AS u(c)
      WHERE EXISTS (
        SELECT 1 FROM (SELECT a FROM VALUES (1, 1), (2, 3) AS t(a, c))
        WHERE a = c
      )
      """
    Then query result collected
      | c |
      | 1 |

  Scenario: Filters do not recover columns hidden by unaliased derived tables
    When query
      """
      SELECT * FROM (SELECT a FROM VALUES (1, 10) AS t(a, b)) WHERE b = 10
      """
    Then query error (?i)cannot (be )?resolve

  Scenario Outline: Filter subqueries resolve correlation inside unaliased derived tables
    When query
      """
      SELECT a FROM VALUES (1, 10), (2, 20) AS t(a, b)
      WHERE <predicate> (
        SELECT c FROM (SELECT c FROM VALUES (1, 100), (3, 300), (1, 111) AS u(a, c) WHERE u.a = t.a)
      )
      """
    Then query result collected
      | a   |
      | <a> |

    Examples:
      | predicate     | a |
      | EXISTS        | 1 |
      | NOT EXISTS    | 2 |
      | b * 10 IN     | 1 |
      | b * 10 NOT IN | 2 |

  Scenario: Projected scalar subqueries resolve correlation inside unaliased derived tables
    When query
      """
      SELECT a, (
        SELECT COUNT(*) FROM (SELECT c FROM VALUES (1, 100), (3, 300), (1, 111) AS u(a, c) WHERE u.a = t.a)
      ) AS n
      FROM VALUES (1, 10), (2, 20) AS t(a, b)
      """
    Then query result collected
      | a | n |
      | 1 | 2 |
      | 2 | 0 |

  # TODO: Match Spark's nullable scalar-subquery schema even for COUNT. The same
  # mismatch exists without the unaliased derived table before this PR.
  @sail-bug
  Scenario: Correlated scalar counts retain nullable schemas through unaliased derived tables
    When query
      """
      SELECT a, (
        SELECT COUNT(*) FROM (SELECT c FROM VALUES (1, 100), (3, 300), (1, 111) AS u(a, c) WHERE u.a = t.a)
      ) AS n
      FROM VALUES (1, 10), (2, 20) AS t(a, b)
      """
    Then query result collected
      | a | n |
      | 1 | 2 |
      | 2 | 0 |
    Then query schema
      """
      root
       |-- a: integer (nullable = false)
       |-- n: long (nullable = true)
      """

  Scenario Outline: Lateral subqueries resolve correlation inside unaliased derived tables
    When query
      """
      SELECT t.a, c FROM VALUES (1), (2) AS t(a), LATERAL (SELECT c FROM (<derived>))
      """
    Then query result collected
      | a | c   |
      | 1 | 100 |
      | 2 | 200 |

    Examples:
      | derived                                                            |
      | SELECT c FROM VALUES (1, 100), (2, 200) AS u(a, c) WHERE u.a = t.a |
      | SELECT t.a * 100 AS c                                              |

  Scenario: Unaliased derived tables expose columns under the generated qualifier
    When query
      """
      SELECT __auto_generated_subquery_name.a FROM (SELECT a FROM VALUES (1) AS t(a))
      """
    Then query result collected
      | a |
      | 1 |

  Scenario: Scalar subquery in Parquet scan predicate
    Given variable location for temporary directory scalar_subquery_parquet
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ location.sql }} USING parquet
      SELECT * FROM VALUES (1), (2), (3) AS t(v)
      """
    When query template
      """
      SELECT v FROM parquet.`{{ location.string }}`
      WHERE v = (SELECT MAX(x) FROM VALUES (1), (2) AS s(x))
      """
    Then query result collected
      | v |
      | 2 |

  Scenario Outline: Scalar subqueries in Parquet scan before aggregate
    Given variable location for temporary directory scalar_subquery_parquet_aggregate
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ location.sql }} USING parquet
      SELECT * FROM VALUES (1), (2), (3) AS t(v)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS scalar_subquery_scan
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW scalar_subquery_scan USING parquet
      OPTIONS (path {{ location.sql }}, pushdown_filters 'true')
      """
    When query
      """
      SELECT COUNT(*) AS n FROM scalar_subquery_scan
      WHERE v > (<subquery>)
      """
    Then query result collected
      | n   |
      | <n> |

    Examples:
      | subquery                                                                                       | n |
      | SELECT MAX(x) FROM VALUES (1), (2) AS s(x)                                                     | 1 |
      | SELECT MIN(x) + (SELECT MIN(y) FROM VALUES (1), (2) AS u(y)) FROM VALUES (1), (2) AS s(x)      | 1 |
      | SELECT MIN(v) FROM scalar_subquery_scan WHERE v > (SELECT MIN(x) FROM VALUES (1), (2) AS s(x)) | 1 |
      | SELECT MAX(x) FROM VALUES (CAST(NULL AS INT)) AS s(x)                                          | 0 |
      | SELECT x FROM VALUES (1) AS s(x) WHERE x > 10                                                  | 0 |

  Scenario: Scalar subquery in filter before aggregate
    When query
      """
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3), (2, 10), (3, 1) AS t(k, v)
      WHERE v > (SELECT MAX(x) FROM VALUES (1), (2) AS s(x))
      GROUP BY k
      """
    Then query result collected
      | k | total |
      | 1 | 3     |
      | 2 | 10    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3), (2, 10), (3, 1) AS t(k, v)
      WHERE v > (SELECT MAX(x) FROM VALUES (1), (2) AS s(x))
      GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in ungrouped aggregate expression
    When query
      """
      SELECT SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) AS total
      FROM VALUES (1), (2), (3) AS t(v)
      """
    Then query result collected
      | total |
      | 36    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) AS total
      FROM VALUES (1), (2), (3) AS t(v)
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in grouped aggregate expression
    When query
      """
      SELECT k, SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) AS total
      FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      GROUP BY k
      """
    Then query result collected
      | k | total |
      | 1 | 23    |
      | 2 | 13    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) AS total
      FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: Correlated scalar subquery in filter
    When query
      """
      SELECT outer_t.k, outer_t.v
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      WHERE outer_t.v = (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      )
      ORDER BY outer_t.k
      """
    Then query result collected ordered
      | k | v |
      | 1 | 4 |
      | 2 | 3 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT outer_t.k, outer_t.v
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      WHERE outer_t.v = (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      )
      ORDER BY outer_t.k
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in projection before aggregate
    When query
      """
      SELECT k, SUM(shifted) AS total
      FROM (
        SELECT k, CAST(v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) AS shifted
        FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      ) p
      GROUP BY k
      """
    Then query result collected
      | k | total |
      | 1 | 23    |
      | 2 | 13    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, SUM(shifted) AS total
      FROM (
        SELECT k, CAST(v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) AS shifted
        FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      ) p
      GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in sort expression
    When query
      """
      SELECT v
      FROM VALUES (1), (2), (3) AS t(v)
      ORDER BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (2) AS s(x)
      ) = 2 THEN v ELSE -v END DESC
      """
    Then query result collected ordered
      | v |
      | 3 |
      | 2 |
      | 1 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v
      FROM VALUES (1), (2), (3) AS t(v)
      ORDER BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (2) AS s(x)
      ) = 2 THEN v ELSE -v END DESC
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in group by expression
    When query
      """
      SELECT CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      ) AS g, COUNT(*) AS n
      FROM VALUES (1), (2), (1) AS t(v)
      GROUP BY g
      """
    Then query result collected
      | g  | n |
      | 11 | 2 |
      | 12 | 1 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      ) AS g, COUNT(*) AS n
      FROM VALUES (1), (2), (1) AS t(v)
      GROUP BY g
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in join condition
    When query
      """
      SELECT l.v, r.w
      FROM VALUES (1), (2), (3) AS l(v)
      JOIN VALUES (11), (12), (13) AS r(w)
        ON CAST(l.v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) = CAST(r.w AS BIGINT)
      ORDER BY l.v
      """
    Then query result collected ordered
      | v | w  |
      | 1 | 11 |
      | 2 | 12 |
      | 3 | 13 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT l.v, r.w
      FROM VALUES (1), (2), (3) AS l(v)
      JOIN VALUES (11), (12), (13) AS r(w)
        ON CAST(l.v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) = CAST(r.w AS BIGINT)
      ORDER BY l.v
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in non-equi join condition
    When query
      """
      SELECT l.v, r.w
      FROM VALUES (1), (2), (3) AS l(v)
      JOIN VALUES (11), (12), (13) AS r(w)
        ON CAST(l.v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) < CAST(r.w AS BIGINT)
      ORDER BY l.v, r.w
      """
    Then query result collected ordered
      | v | w  |
      | 1 | 12 |
      | 1 | 13 |
      | 2 | 13 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT l.v, r.w
      FROM VALUES (1), (2), (3) AS l(v)
      JOIN VALUES (11), (12), (13) AS r(w)
        ON CAST(l.v AS BIGINT) + (
          SELECT MIN(CAST(x AS BIGINT))
          FROM VALUES (10), (20) AS s(x)
        ) < CAST(r.w AS BIGINT)
      ORDER BY l.v, r.w
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in having filter
    When query
      """
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 1), (1, 2), (2, 3), (2, 4) AS t(k, v)
      GROUP BY k
      HAVING SUM(v) > (SELECT MIN(x) FROM VALUES (3) AS s(x))
      """
    Then query result collected
      | k | total |
      | 2 | 7     |
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 1), (1, 2), (2, 3), (2, 4) AS t(k, v)
      GROUP BY k
      HAVING SUM(v) > (SELECT MIN(x) FROM VALUES (3) AS s(x))
      """
    Then query plan matches snapshot

  Scenario: Multiple scalar subqueries in one expression
    When query
      """
      SELECT SUM(CAST(v AS BIGINT) +
        (SELECT MIN(CAST(x AS BIGINT)) FROM VALUES (10), (20) AS s(x)) +
        (SELECT MAX(CAST(y AS BIGINT)) FROM VALUES (1), (2) AS u(y))
      ) AS total
      FROM VALUES (1), (2), (3) AS t(v)
      """
    Then query result collected
      | total |
      | 42    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT SUM(CAST(v AS BIGINT) +
        (SELECT MIN(CAST(x AS BIGINT)) FROM VALUES (10), (20) AS s(x)) +
        (SELECT MAX(CAST(y AS BIGINT)) FROM VALUES (1), (2) AS u(y))
      ) AS total
      FROM VALUES (1), (2), (3) AS t(v)
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in window order expression
    When query
      """
      SELECT v, ROW_NUMBER() OVER (ORDER BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (2) AS s(x)
      ) = 2 THEN v ELSE -v END) AS rn
      FROM VALUES (3), (1), (2) AS t(v)
      ORDER BY rn
      """
    Then query result collected ordered
      | v | rn |
      | 1 | 1  |
      | 2 | 2  |
      | 3 | 3  |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, ROW_NUMBER() OVER (ORDER BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (2) AS s(x)
      ) = 2 THEN v ELSE -v END) AS rn
      FROM VALUES (3), (1), (2) AS t(v)
      ORDER BY rn
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in window partition expression
    When query
      """
      SELECT v, COUNT(*) OVER (PARTITION BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (1) AS s(x)
      ) = 1 THEN v % 2 ELSE v END) AS n
      FROM VALUES (1), (2), (3), (4) AS t(v)
      ORDER BY v
      """
    Then query result collected ordered
      | v | n |
      | 1 | 2 |
      | 2 | 2 |
      | 3 | 2 |
      | 4 | 2 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, COUNT(*) OVER (PARTITION BY CASE WHEN (
        SELECT MIN(x) FROM VALUES (1) AS s(x)
      ) = 1 THEN v % 2 ELSE v END) AS n
      FROM VALUES (1), (2), (3), (4) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in window aggregate argument
    When query
      """
      SELECT v, SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) OVER () AS total
      FROM VALUES (1), (2), (3) AS t(v)
      ORDER BY v
      """
    Then query result collected ordered
      | v | total |
      | 1 | 36    |
      | 2 | 36    |
      | 3 | 36    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, SUM(CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (10), (20) AS s(x)
      )) OVER () AS total
      FROM VALUES (1), (2), (3) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Scalar subquery in aggregate filter
    When query
      """
      SELECT k, COUNT(*) FILTER (WHERE v > (
        SELECT MIN(x) FROM VALUES (1) AS s(x)
      )) AS n
      FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      GROUP BY k
      """
    Then query result collected
      | k | n |
      | 1 | 1 |
      | 2 | 1 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, COUNT(*) FILTER (WHERE v > (
        SELECT MIN(x) FROM VALUES (1) AS s(x)
      )) AS n
      FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      GROUP BY k
      """
    Then query plan matches snapshot

  @spark-4
  Scenario: Scalar subquery in aggregate order by
    When query
      """
      SELECT listagg(v, ',') WITHIN GROUP (
        ORDER BY CASE WHEN (
          SELECT MIN(x) FROM VALUES (1) AS s(x)
        ) = 1 THEN sort_key ELSE -sort_key END
      ) AS joined
      FROM VALUES ('a', 2), ('b', 1), ('c', 3) AS t(v, sort_key)
      """
    Then query result collected
      | joined |
      | b,a,c  |
    When query
      """
      EXPLAIN CODEGEN
      SELECT listagg(v, ',') WITHIN GROUP (
        ORDER BY CASE WHEN (
          SELECT MIN(x) FROM VALUES (1) AS s(x)
        ) = 1 THEN sort_key ELSE -sort_key END
      ) AS joined
      FROM VALUES ('a', 2), ('b', 1), ('c', 3) AS t(v, sort_key)
      """
    Then query plan matches snapshot

  Scenario: Nested scalar subquery
    When query
      """
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (10), (20) AS s(x)
      ) AS shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query result collected ordered
      | v | shifted |
      | 1 | 12      |
      | 2 | 13      |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (10), (20) AS s(x)
      ) AS shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Empty scalar subquery result
    When query
      """
      SELECT v, CAST(v AS BIGINT) + (
        SELECT CAST(x AS BIGINT)
        FROM VALUES (10) AS s(x)
        WHERE x > 100
      ) AS shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query result collected ordered
      | v | shifted |
      | 1 | NULL    |
      | 2 | NULL    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, CAST(v AS BIGINT) + (
        SELECT CAST(x AS BIGINT)
        FROM VALUES (10) AS s(x)
        WHERE x > 100
      ) AS shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Empty scalar subquery makes a non-null projection nullable
    When query
      """
      SELECT o.id AS v,
        o.id + (SELECT i.id FROM range(0) AS i) AS shifted
      FROM range(1, 3) AS o
      ORDER BY v
      """
    Then query result collected ordered
      | v | shifted |
      | 1 | NULL    |
      | 2 | NULL    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT o.id AS v,
        o.id + (SELECT i.id FROM range(0) AS i) AS shifted
      FROM range(1, 3) AS o
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Null scalar subquery result
    When query
      """
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (CAST(NULL AS INT)) AS s(x)
      ) AS maybe_shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query result collected ordered
      | v | maybe_shifted |
      | 1 | NULL          |
      | 2 | NULL          |
    When query
      """
      EXPLAIN CODEGEN
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (CAST(NULL AS INT)) AS s(x)
      ) AS maybe_shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

  Scenario: Multi-row scalar subquery errors
    When query
      """
      SELECT v, CAST(v AS BIGINT) + (
        SELECT CAST(x AS BIGINT)
        FROM VALUES (10), (20) AS s(x)
      ) AS shifted
      FROM VALUES (1) AS t(v)
      """
    Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

  # Spark 4 analyzes this query and raises the scalar-subquery cardinality error
  # at runtime. Sail currently rejects it during planning via DataFusion's
  # correlated scalar subquery invariant.
  @spark-4
  @sail-bug
  Scenario: Unaggregated correlated scalar subquery errors at runtime
    When query
      """
      SELECT outer_t.k, (
        SELECT inner_t.x
        FROM VALUES (1, 4), (1, 2), (2, 3) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS x
      FROM VALUES (1), (2) AS outer_t(k)
      ORDER BY outer_t.k
      """
    Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

  Scenario: Correlated scalar subquery in projection
    When query
      """
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS mx
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k, outer_t.v
      """
    Then query result collected ordered
      | k | v | mx |
      | 1 | 2 | 4  |
      | 1 | 4 | 4  |
      | 2 | 1 | 3  |
      | 2 | 3 | 3  |
    When query
      """
      EXPLAIN CODEGEN
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS mx
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k, outer_t.v
      """
    Then query plan matches snapshot

  Scenario: Correlated scalar subquery no-match result
    When query
      """
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS mx
      FROM VALUES (1, 2), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k
      """
    Then query result collected ordered
      | k | v | mx   |
      | 1 | 2 | 4    |
      | 2 | 3 | NULL |
    When query
      """
      EXPLAIN CODEGEN
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS mx
      FROM VALUES (1, 2), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k
      """
    Then query plan matches snapshot

  Scenario: Correlated scalar subquery with nested scalar in projection
    When query
      """
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS shifted
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k, outer_t.v
      """
    Then query result collected ordered
      | k | v | shifted |
      | 1 | 2 | 5       |
      | 1 | 4 | 5       |
      | 2 | 1 | 4       |
      | 2 | 3 | 4       |
    When query
      """
      EXPLAIN CODEGEN
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS shifted
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k, outer_t.v
      """
    Then query plan matches snapshot

  Scenario: Correlated scalar subquery with nested scalar in filter
    When query
      """
      SELECT outer_t.k, outer_t.v
      FROM VALUES (1, 2), (1, 5), (2, 1), (2, 4) AS outer_t(k, v)
      WHERE outer_t.v = (
        SELECT MAX(inner_t.x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      )
      ORDER BY outer_t.k
      """
    Then query result collected ordered
      | k | v |
      | 1 | 5 |
      | 2 | 4 |
    When query
      """
      EXPLAIN CODEGEN
      SELECT outer_t.k, outer_t.v
      FROM VALUES (1, 2), (1, 5), (2, 1), (2, 4) AS outer_t(k, v)
      WHERE outer_t.v = (
        SELECT MAX(inner_t.x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      )
      ORDER BY outer_t.k
      """
    Then query plan matches snapshot

  Rule: Projected EXISTS returns a non-null scalar boolean
    Scenario Outline: Projected EXISTS preserves subquery row existence
      When query
        """
        SELECT EXISTS(<subquery>) AS present, NOT EXISTS(<subquery>) AS absent
        """
      Then query result collected
        | present   | absent   |
        | <present> | <absent> |
      Then query schema
        """
        root
         |-- present: boolean (nullable = false)
         |-- absent: boolean (nullable = false)
        """

      Examples:
        | subquery                                                             | present | absent |
        | SELECT * FROM VALUES (1), (2) AS t(v)                                | true    | false  |
        | SELECT CAST(NULL AS INT)                                             | true    | false  |
        | SELECT * FROM VALUES (1) AS t(v) WHERE v > 1                         | false   | true   |
        | SELECT * FROM VALUES (1), (2) AS t(v) LIMIT 0                        | false   | true   |
        | SELECT * FROM VALUES (1), (2) AS t(v) LIMIT 1 OFFSET 1               | true    | false  |
        | SELECT * FROM VALUES (1), (2) AS t(v) LIMIT 1 OFFSET 2               | false   | true   |
        | SELECT COUNT(*) FROM VALUES (1) AS t(v) WHERE v > 1                  | true    | false  |
        | SELECT v FROM VALUES (1), (2) AS t(v) GROUP BY v HAVING COUNT(*) > 2 | false   | true   |

    Scenario: Projected EXISTS composes with conditional and boolean expressions
      When query
        """
        SELECT
          CASE WHEN EXISTS(SELECT * FROM VALUES (1) AS t(v)) THEN 'present' ELSE 'empty' END AS state,
          EXISTS(SELECT * FROM VALUES (1) AS t(v) WHERE v > 1)
            OR EXISTS(SELECT * FROM VALUES (1) AS t(v)) AS any_rows,
          NOT (EXISTS(SELECT * FROM VALUES (1) AS t(v) WHERE v > 1)) AS absent
        """
      Then query result collected
        | state   | any_rows | absent |
        | present | true     | true   |

    Scenario: Projected EXISTS beside an aggregate preserves the aggregate result
      When query
        """
        SELECT COUNT(*) AS row_count,
          EXISTS(SELECT * FROM VALUES (1) AS lookup(v)) AS present
        FROM VALUES (1), (2) AS t(v)
        """
      Then query result collected
        | row_count | present |
        | 2         | true    |

    Scenario: Projected correlated EXISTS preserves duplicates and null keys
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT * FROM VALUES (1), (1), (CAST(NULL AS INT)) AS lookup(id)
            WHERE lookup.id = candidate.id
          ) AS present,
          NOT EXISTS(
            SELECT * FROM VALUES (1), (1), (CAST(NULL AS INT)) AS lookup(id)
            WHERE lookup.id = candidate.id
          ) AS absent
        FROM VALUES (1), (1), (2), (CAST(NULL AS INT)) AS candidate(id)
        ORDER BY candidate.id NULLS LAST
        """
      Then query result collected ordered
        | id   | present | absent |
        | 1    | true    | false  |
        | 1    | true    | false  |
        | 2    | false   | true   |
        | NULL | false   | true   |
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
         |-- present: boolean (nullable = false)
         |-- absent: boolean (nullable = false)
        """

    Scenario Outline: Projected correlated EXISTS preserves predicates and row bounds
      When query
        """
        SELECT candidate.id, EXISTS(<subquery>) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present  |
        | 1  | <first>  |
        | 2  | <second> |
        | 3  | <third>  |
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- present: boolean (nullable = false)
        """

      Examples:
        | subquery                                                                                                    | first | second | third |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id < candidate.id                             | false | true   | true  |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id = candidate.id OR lookup.id > candidate.id | true  | true   | false |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id = candidate.id LIMIT 0                     | false | false  | false |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id = candidate.id LIMIT 1                     | true  | true   | false |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id = candidate.id LIMIT 1 OFFSET 1            | true  | false  | false |
        | SELECT * FROM VALUES (1), (1), (2) AS lookup(id) WHERE lookup.id = candidate.id LIMIT 1 OFFSET 3            | false | false  | false |
        | SELECT COUNT(*) FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id                                | true  | true   | true  |
        | SELECT COUNT(*) FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id LIMIT 1 OFFSET 1               | false | false  | false |

    Scenario: Sorted projected EXISTS supports a smaller lookup
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id
          ) AS present
        FROM VALUES (1), (1), (2), (CAST(NULL AS INT)) AS candidate(id)
        ORDER BY candidate.id NULLS LAST
        """
      Then query result collected ordered
        | id   | present |
        | 1    | true    |
        | 1    | true    |
        | 2    | false   |
        | NULL | false   |
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
         |-- present: boolean (nullable = false)
        """

    Scenario: Sorted projected NOT EXISTS supports a smaller lookup
      When query
        """
        SELECT candidate.id,
          NOT EXISTS(
            SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id
          ) AS present
        FROM VALUES (1), (1), (2), (CAST(NULL AS INT)) AS candidate(id)
        ORDER BY candidate.id NULLS LAST
        """
      Then query result collected ordered
        | id   | present |
        | 1    | false   |
        | 1    | false   |
        | 2    | true    |
        | NULL | true    |
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
         |-- present: boolean (nullable = false)
        """

    Scenario: Sorted projected EXISTS supports a single-row lookup
      When query
        """
        SELECT candidate.id,
          EXISTS(SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present |
        | 1  | true    |
        | 2  | false   |
        | 3  | false   |

    Scenario: Sorted projected EXISTS composes with grouping and LIMIT
      When query
        """
        SELECT candidate.id, COUNT(*) AS row_count,
          EXISTS(SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id) AS present
        FROM VALUES (1), (1), (2) AS candidate(id)
        GROUP BY candidate.id
        ORDER BY candidate.id DESC
        LIMIT 1
        """
      Then query result collected ordered
        | id | row_count | present |
        | 2  | 1         | false   |

    Scenario: Projected EXISTS can order by the boolean result
      When query
        """
        SELECT candidate.id,
          EXISTS(SELECT * FROM VALUES (1) AS lookup(id) WHERE lookup.id = candidate.id) AS present
        FROM VALUES (2), (1), (3) AS candidate(id)
        ORDER BY present DESC, candidate.id DESC
        """
      Then query result collected ordered
        | id | present |
        | 1  | true    |
        | 3  | false   |
        | 2  | false   |

    Scenario: Projected EXISTS preserves a nested limit below the correlated filter
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT * FROM (SELECT * FROM VALUES (1), (1) AS lookup(id) LIMIT 1) limited
            WHERE limited.id = candidate.id
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        """
      Then query result collected
        | id | present |
        | 1  | true    |
        | 2  | false   |
        | 3  | false   |

    @sail-only
    Scenario Outline: Projected correlated EXISTS rejects correlation below a window
      When query
        """
        SELECT candidate.id,
          <exists>(
            SELECT * FROM (
              SELECT lookup.id, <window> AS n
              FROM VALUES (1), (1), (2) AS lookup(id)
              WHERE lookup.id = candidate.id
            ) AS numbered
            WHERE numbered.n = 1
            <bound>
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        """
      Then query error projected correlated EXISTS with correlation below a window

      Examples:
        | exists     | window                                      | bound            |
        | EXISTS     | ROW_NUMBER() OVER (ORDER BY lookup.id)       |                  |
        | NOT EXISTS | ROW_NUMBER() OVER (ORDER BY lookup.id)       |                  |
        | EXISTS     | COUNT(*) OVER ()                            |                  |
        | NOT EXISTS | COUNT(*) OVER ()                            |                  |
        | EXISTS     | ROW_NUMBER() OVER (ORDER BY lookup.id)       | LIMIT 1 OFFSET 1 |
        | NOT EXISTS | ROW_NUMBER() OVER (ORDER BY lookup.id)       | LIMIT 1 OFFSET 1 |
        | EXISTS     | COUNT(*) OVER ()                            | LIMIT 1 OFFSET 1 |
        | NOT EXISTS | COUNT(*) OVER ()                            | LIMIT 1 OFFSET 1 |

    @sail-only
    Scenario Outline: Projected correlated EXISTS rejects cast correlation below aggregation
      When query
        """
        SELECT candidate.id,
          <exists>(
            SELECT lookup.g
            FROM VALUES (1.1, 0), (1.2, 0), (2.1, 0) AS lookup(x, g)
            WHERE <key> = candidate.id
            GROUP BY lookup.g
            HAVING COUNT(*) > 1
            <bound>
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        """
      Then query error projected correlated EXISTS with cast correlation below aggregation

      Examples:
        | exists     | key                        | bound            |
        | EXISTS     | CAST(lookup.x AS INT)       |                  |
        | NOT EXISTS | CAST(lookup.x AS INT)       |                  |
        | EXISTS     | TRY_CAST(lookup.x AS INT)   |                  |
        | EXISTS     | CAST(lookup.x AS INT)       | LIMIT 1 OFFSET 1 |
        | NOT EXISTS | CAST(lookup.x AS INT)       | LIMIT 1 OFFSET 1 |

    @sail-only
    Scenario Outline: Projected correlated EXISTS rejects cast correlation before counting offset rows
      When query
        """
        SELECT candidate.id,
          <exists>(
            SELECT <projection>
            FROM VALUES (1.1, 0), (1.2, 0), (2.1, 0) AS lookup(x, g)
            WHERE CAST(lookup.x AS INT) = candidate.id
            LIMIT 1 OFFSET 1
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        """
      Then query error projected correlated EXISTS with cast correlation below aggregation

      Examples:
        | exists     | projection        |
        | EXISTS     | lookup.g          |
        | NOT EXISTS | lookup.g          |
        | EXISTS     | DISTINCT lookup.g |

    Scenario: Projected correlated EXISTS preserves correlation above a window
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT * FROM (
              SELECT lookup.id, ROW_NUMBER() OVER (ORDER BY lookup.id) AS n
              FROM VALUES (1), (2), (3) AS lookup(id)
            ) AS numbered
            WHERE numbered.id = candidate.id AND numbered.n = 1
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present |
        | 1  | true    |
        | 2  | false   |
        | 3  | false   |

    Scenario: Projected correlated EXISTS preserves cast correlation without aggregation
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT *
            FROM VALUES (1.1), (1.2), (2.1) AS lookup(x)
            WHERE CAST(lookup.x AS INT) = candidate.id
            LIMIT 1
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present |
        | 1  | true    |
        | 2  | true    |
        | 3  | false   |

    Scenario: Projected correlated EXISTS preserves casts of outer grouping keys
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT lookup.g
            FROM VALUES (1, 0), (1, 0), (2, 0) AS lookup(x, g)
            WHERE lookup.x = CAST(candidate.id AS INT)
            GROUP BY lookup.g
            HAVING COUNT(*) > 1
          ) AS present
        FROM VALUES (1.1), (1.2), (2.1), (CAST(NULL AS DECIMAL(2, 1))) AS candidate(id)
        ORDER BY candidate.id NULLS LAST
        """
      Then query result collected ordered
        | id   | present |
        | 1.1  | true    |
        | 1.2  | true    |
        | 2.1  | false   |
        | NULL | false   |

    Scenario: Projected correlated EXISTS preserves an independent cast predicate before grouping
      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT lookup.g
            FROM VALUES (1, 0, 1.1), (1, 0, 1.2), (2, 0, 0.1) AS lookup(x, g, v)
            WHERE lookup.x = candidate.id AND CAST(lookup.v AS INT) > 0
            GROUP BY lookup.g
            HAVING COUNT(*) > 1
          ) AS present
        FROM VALUES (1), (2), (3) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present |
        | 1  | true    |
        | 2  | false   |
        | 3  | false   |

  Rule: Scalar aggregate correlation groups the compared cast value
    Scenario: Lossy casts combine matching inner keys and preserve duplicate outer rows
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM VALUES ('0'), ('00'), ('1'), (CAST(NULL AS STRING)) AS i(k)
           WHERE CAST(i.k AS BIGINT) = o.id) AS n
        FROM VALUES (0L), (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

    Scenario: Cast correlation preserves raw aggregate inputs
      When query
        """
        SELECT o.id,
          (SELECT max(i.k) FROM VALUES ('0'), ('00'), ('1') AS i(k)
           WHERE CAST(i.k AS BIGINT) = o.id) AS v
        FROM range(3) AS o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | v    |
        | 0  | 00   |
        | 1  | 1    |
        | 2  | NULL |

    Scenario: Each cast correlation contributes its compared key
      When query
        """
        SELECT o.a, o.b,
          (SELECT count(*) FROM VALUES ('01', '1'), ('1', '01'), ('1', '2') AS i(a, b)
           WHERE CAST(i.a AS BIGINT) = o.a AND CAST(i.b AS BIGINT) = o.b) AS n
        FROM VALUES (1L, 1L), (1L, 2L), (2L, 1L) AS o(a, b)
        ORDER BY o.a, o.b
        """
      Then query result collected ordered
        | a | b | n |
        | 1 | 1 | 2 |
        | 1 | 2 | 1 |
        | 2 | 1 | 0 |

    Scenario: Local filters reject invalid values before cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM VALUES ('01'), ('1'), ('bad') AS i(k)
           WHERE i.k <> 'bad' AND CAST(i.k AS BIGINT) = o.id) AS n
        FROM range(3) AS o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 0 |
        | 1  | 2 |
        | 2  | 0 |

    Scenario: Injective cast correlations group by the raw inner key
      When query
        """
        SELECT o.s,
          (SELECT count(*) FROM VALUES (1, 10), (1, 20), (2, 30), (CAST(NULL AS INT), 40) AS i(k, v)
           WHERE CAST(i.k AS STRING) = o.s) AS by_string,
          (SELECT sum(v) FROM VALUES (1, 10), (1, 20), (2, 30), (CAST(NULL AS INT), 40) AS i(k, v)
           WHERE i.k = o.l) AS by_bigint,
          (SELECT max(v) FROM VALUES (1, 10), (1, 20), (2, 30), (CAST(NULL AS INT), 40) AS i(k, v)
           WHERE CAST(i.k AS DECIMAL(12, 2)) = o.d) AS by_decimal
        FROM VALUES ('1', 1L, CAST(1 AS DECIMAL(12, 2))),
                    ('2', 2L, CAST(2 AS DECIMAL(12, 2))),
                    ('3', 3L, CAST(3 AS DECIMAL(12, 2))),
                    (CAST(NULL AS STRING), CAST(NULL AS BIGINT), CAST(NULL AS DECIMAL(12, 2))) AS o(s, l, d)
        ORDER BY o.s NULLS FIRST
        """
      Then query result collected ordered
        | s    | by_string | by_bigint | by_decimal |
        | NULL | 0         | NULL      | NULL       |
        | 1    | 2         | 30        | 20         |
        | 2    | 1         | 30        | 30         |
        | 3    | 0         | NULL      | NULL       |

    Scenario: Injective cast correlation partitions windows by the raw inner key
      When query
        """
        SELECT o.id,
          (SELECT max(w) FROM (
             SELECT k, row_number() OVER (ORDER BY v) AS w
             FROM VALUES (0, 2), (0, 3), (1, 4), (CAST(NULL AS INT), 5) AS i(k, v)
             WHERE CAST(i.k AS STRING) = o.id
           )) AS n
        FROM VALUES ('0'), ('1'), ('2'), (CAST(NULL AS STRING)) AS o(id)
        ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 2    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Injective and lossy cast correlations each contribute a key
      When query
        """
        SELECT o.a, o.b,
          (SELECT count(*) FROM VALUES (1, '0'), (1, '00'), (1, '1'), (2, '0') AS i(k, s)
           WHERE CAST(i.k AS STRING) = o.a AND CAST(i.s AS BIGINT) = o.b) AS n
        FROM VALUES ('1', 0L), ('1', 1L), ('2', 0L), ('2', 1L) AS o(a, b)
        ORDER BY o.a, o.b
        """
      Then query result collected ordered
        | a | b | n |
        | 1 | 0 | 2 |
        | 1 | 1 | 1 |
        | 2 | 0 | 1 |
        | 2 | 1 | 0 |

    Scenario: Cast correlation below a window preserves matching rows
      When query
        """
        SELECT o.id,
          (SELECT max(w) FROM (
             SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS w
             FROM VALUES ('0', 2), ('0', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id
           )) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | 2    |
        | 1  | 1    |
        | 2  | NULL |

    Scenario: Cast correlation below a generator preserves matching rows
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (
             SELECT k, explode(array(v, v + 1)) AS x
             FROM VALUES ('0', 2), ('1', 3) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id
           )) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 2 |
        | 2  | 0 |

    Scenario: Cast correlation above a window still combines keys with another correlation below
      When query
        """
        SELECT o.g, o.id,
          (SELECT count(*) FROM (
             SELECT k, row_number() OVER (PARTITION BY g ORDER BY v) AS w
             FROM VALUES (1L, '0', 2), (1L, '00', 3), (1L, '1', 4) AS i(g, k, v)
             WHERE i.g = o.g
           ) WHERE CAST(k AS BIGINT) = o.id) AS n
        FROM VALUES (1L, 0L), (1L, 1L), (2L, 0L) AS o(g, id)
        ORDER BY o.g, o.id, n
        """
      Then query result collected ordered
        | g | id | n |
        | 1 | 0  | 2 |
        | 1 | 1  | 1 |
        | 2 | 0  | 0 |

    Scenario: Cast correlation above a generator still combines keys with another correlation below
      When query
        """
        SELECT o.g, o.id,
          (SELECT count(*) FROM (
             SELECT g, k, explode(array(v, v + 1)) AS x
             FROM VALUES (1L, '0', 2), (1L, '00', 3), (1L, '1', 4) AS i(g, k, v)
             WHERE i.g = o.g
           ) WHERE CAST(k AS BIGINT) = o.id) AS n
        FROM VALUES (1L, 0L), (1L, 1L), (2L, 0L) AS o(g, id)
        ORDER BY o.g, o.id, n
        """
      Then query result collected ordered
        | g | id | n |
        | 1 | 0  | 4 |
        | 1 | 1  | 2 |
        | 2 | 0  | 0 |

    Scenario: Cast collisions below a generator combine every generated row
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (
             SELECT k, explode(array(v, v + 1)) AS x
             FROM VALUES ('0', 2), ('00', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id
           )) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 4 |
        | 1  | 2 |
        | 2  | 0 |

    Scenario: Cast correlation partitions windows by the compared key
      When query
        """
        SELECT o.id,
          (SELECT max(w) FROM (
             SELECT k, row_number() OVER (ORDER BY v) AS w
             FROM VALUES ('0', 2), ('00', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id
           )) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | 2    |
        | 1  | 1    |
        | 2  | NULL |

    Scenario: Cast correlation preserves existing window partitions
      When query
        """
        SELECT o.id,
          (SELECT sum(w) FROM (
             SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS w
             FROM VALUES ('0', 2), ('00', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id
           )) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | 2    |
        | 1  | 1    |
        | 2  | NULL |

    # Windows keep their existing partitions as well as the compared correlation keys.
    # Generators and joins retain hidden keys without changing row multiplicity.
    Scenario: Cast key propagation window rank
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT rank() OVER (ORDER BY g) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 8    |
        | 0    | 8    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation window dense rank
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT dense_rank() OVER (ORDER BY g) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 6    |
        | 0    | 6    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation window bounded frame
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 25   |
        | 0    | 25   |
        | 1    | 5    |
        | 2    | NULL |

    Scenario: Cast key propagation window running frame
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT sum(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 14   |
        | 0    | 14   |
        | 1    | 5    |
        | 2    | NULL |

    Scenario: Cast key propagation window range frame
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 25   |
        | 0    | 25   |
        | 1    | 5    |
        | 2    | NULL |

    Scenario: Cast key propagation window lag
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT lag(v, 1, -1) OVER (PARTITION BY g ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 2    |
        | 0    | 2    |
        | 1    | -1   |
        | 2    | NULL |

    Scenario: Cast key propagation window lead
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT lead(v, 1, -1) OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 8    |
        | 0    | 8    |
        | 1    | -1   |
        | 2    | NULL |

    Scenario: Cast key propagation two windows
      When query
        """
        SELECT o.id, (SELECT sum(z) FROM (SELECT row_number() OVER (PARTITION BY g ORDER BY w DESC) AS z FROM (SELECT k,g,v,row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 6    |
        | 0    | 6    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation window local filter
      When query
        """
        SELECT o.id, (SELECT count(*) FROM (SELECT w FROM (SELECT k,g,v,row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a WHERE w > 1) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n |
        | NULL | 0 |
        | 0    | 3 |
        | 0    | 3 |
        | 1    | 0 |
        | 2    | 0 |

    Scenario: Cast key propagation window empty local filter
      When query
        """
        SELECT o.id, (SELECT count(*) FROM (SELECT w FROM (SELECT k,g,v,row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a WHERE w > 100) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n |
        | NULL | 0 |
        | 0    | 0 |
        | 0    | 0 |
        | 1    | 0 |
        | 2    | 0 |

    Scenario: Cast key propagation posexplode value
      When query
        """
        SELECT o.id, (SELECT sum(x) FROM (SELECT posexplode(array(v, v+1)) AS (p,x) FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 24   |
        | 0    | 24   |
        | 1    | 11   |
        | 2    | NULL |

    Scenario: Cast key propagation explode null empty value
      When query
        """
        SELECT o.id, (SELECT sum(x) FROM (SELECT explode(IF(k = '00', CAST(NULL AS ARRAY<INT>), IF(k = '1', array(), array(v)))) AS x FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 4    |
        | 0    | 4    |
        | 1    | NULL |
        | 2    | NULL |

    Scenario: Cast key propagation explode outer null value
      When query
        """
        SELECT o.id, (SELECT sum(x) FROM (SELECT explode_outer(IF(k = '00', CAST(NULL AS ARRAY<INT>), array(v))) AS x FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 4    |
        | 0    | 4    |
        | 1    | 5    |
        | 2    | NULL |

    Scenario: Cast key propagation inline value
      When query
        """
        SELECT o.id, (SELECT sum(x) FROM (SELECT inline(array(named_struct('x',v),named_struct('x',v+1))) FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 24   |
        | 0    | 24   |
        | 1    | 11   |
        | 2    | NULL |

    Scenario: Cast key propagation window then explode
      When query
        """
        SELECT o.id, (SELECT sum(w) FROM (SELECT w, explode(array(v,v+1)) AS x FROM (SELECT k,g,v,row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 20   |
        | 0    | 20   |
        | 1    | 2    |
        | 2    | NULL |

    Scenario: Cast key propagation explode then window
      When query
        """
        SELECT o.id, (SELECT max(w) FROM (SELECT row_number() OVER (ORDER BY x,v) AS w FROM (SELECT k,g,v,explode(array(v,v+1)) AS x FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 8    |
        | 0    | 8    |
        | 1    | 2    |
        | 2    | NULL |

    Scenario: Cast key propagation two generators
      When query
        """
        SELECT o.id, (SELECT sum(y) FROM (SELECT explode(array(x,x+1)) AS y FROM (SELECT k,g,v,explode(array(v,v+1)) AS x FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) a) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 56   |
        | 0    | 56   |
        | 1    | 24   |
        | 2    | NULL |

    Scenario: Cast key propagation reversed equality
      When query
        """
        SELECT o.id, (SELECT max(w) FROM (SELECT row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE o.id = CAST(i.k AS BIGINT)) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 4    |
        | 0    | 4    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation repeated cast key
      When query
        """
        SELECT o.id, (SELECT max(w) FROM (SELECT row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id AND o.id = CAST(i.k AS BIGINT)) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 4    |
        | 0    | 4    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation outer expression
      When query
        """
        SELECT o.id, (SELECT max(w) FROM (SELECT row_number() OVER (ORDER BY v) AS w FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id + 0L) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 4    |
        | 0    | 4    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation union input
      When query
        """
        SELECT o.id, (SELECT sum(v) FROM (SELECT v FROM (SELECT * FROM VALUES ('0',1),('00',2) AS a(k,v) UNION ALL SELECT * FROM VALUES ('1',3) AS b(k,v)) a WHERE CAST(k AS BIGINT) = o.id) q) AS n FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 3    |
        | 0    | 3    |
        | 1    | 3    |
        | 2    | NULL |

    Scenario: Cast key propagation multiple scalar subqueries
      When query
        """
        SELECT o.id, (SELECT count(*) FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) AS n, (SELECT sum(v) FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) AS s FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) ORDER BY o.id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n | s    |
        | NULL | 0 | NULL |
        | 0    | 4 | 10   |
        | 0    | 4 | 10   |
        | 1    | 1 | 5    |
        | 2    | 0 | NULL |

    Scenario: Cast key propagation filter consumer
      When query
        """
        SELECT o.id FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id) WHERE (SELECT count(*) FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5), (CAST(NULL AS STRING), 10, 6) AS i(k,g,v) WHERE CAST(i.k AS BIGINT) = o.id) > 1 ORDER BY o.id
        """
      Then query result collected ordered
        | id |
        | 0  |
        | 0  |

    Scenario: Cast key propagation reserved key name
      When query
        """
        SELECT o.id, (SELECT count(*) FROM VALUES ('0',7),('00',9),('1',2) AS i(k,`__sail_correlation_key_1`) WHERE CAST(i.k AS BIGINT)=o.id AND i.`__sail_correlation_key_1` > 2) AS n FROM VALUES (0L),(1L),(2L) AS o(id) ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 0 |
        | 2  | 0 |

    Scenario: Cast key propagation two cast keys window
      When query
        """
        SELECT o.id,o.g,(SELECT max(w) FROM (SELECT row_number() OVER(ORDER BY v) w FROM VALUES ('0','1',1),('00','01',2),('0','2',3),('1','1',4) i(k,g,v) WHERE CAST(i.k AS BIGINT)=o.id AND CAST(i.g AS BIGINT)=o.g)) n FROM VALUES (0L,1L),(0L,2L),(1L,1L),(2L,1L) o(id,g) ORDER BY o.id,o.g
        """
      Then query result collected ordered
        | id | g | n    |
        | 0  | 1 | 2    |
        | 0  | 2 | 1    |
        | 1  | 1 | 1    |
        | 2  | 1 | NULL |

    Scenario: Cast key propagation map nullable values
      When query
        """
        SELECT o.id,(SELECT sum(x) FROM (SELECT explode(map('a',CASE WHEN v<0 THEN NULL ELSE v END,'b',v+1)) AS (mk,x) FROM VALUES ('0',1),('00',2),('1',3),(CAST(NULL AS STRING),4) i(k,v) WHERE CAST(i.k AS BIGINT)=o.id)) n FROM VALUES (0L),(0L),(1L),(2L),(CAST(NULL AS BIGINT)) o(id) ORDER BY id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 8    |
        | 0    | 8    |
        | 1    | 7    |
        | 2    | NULL |

    Scenario: Cast key propagation outer generator typed empty
      When query
        """
        SELECT o.id,(SELECT sum(x) FROM (SELECT explode_outer(IF(k='00',CAST(array() AS ARRAY<INT>),array(v))) x FROM VALUES ('0',1),('00',2),('1',3),(CAST(NULL AS STRING),4) i(k,v) WHERE CAST(i.k AS BIGINT)=o.id)) n FROM VALUES (0L),(0L),(1L),(2L),(CAST(NULL AS BIGINT)) o(id) ORDER BY id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 1    |
        | 0    | 1    |
        | 1    | 3    |
        | 2    | NULL |

    Scenario: Cast key propagation limit below correlation
      When query
        """
        SELECT o.id,(SELECT count(*) FROM (SELECT * FROM VALUES ('0',1),('00',2),('1',3),(CAST(NULL AS STRING),4) i(k,v) ORDER BY v LIMIT 3) i WHERE CAST(i.k AS BIGINT)=o.id) n FROM VALUES (0L),(0L),(1L),(2L),(CAST(NULL AS BIGINT)) o(id) ORDER BY id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n |
        | NULL | 0 |
        | 0    | 2 |
        | 0    | 2 |
        | 1    | 1 |
        | 2    | 0 |

    Scenario: Cast key propagation volatile input control
      When query
        """
        SELECT o.id,(SELECT max(k) FROM VALUES ('0'),('1') i(k) WHERE CAST(i.k AS BIGINT)=o.id AND rand(7)>=0D) n FROM VALUES (0L),(0L),(1L),(2L),(CAST(NULL AS BIGINT)) o(id) ORDER BY id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 0    |
        | 0    | 0    |
        | 1    | 1    |
        | 2    | NULL |

    Scenario: Cast key propagation grouped raw key control
      When query
        """
        SELECT o.id,(SELECT max(v) FROM VALUES ('0',1),('1',2) i(k,v) WHERE CAST(i.k AS BIGINT)=o.id GROUP BY i.k) n FROM VALUES (0L),(0L),(1L),(2L),(CAST(NULL AS BIGINT)) o(id) ORDER BY id NULLS FIRST
        """
      Then query result collected ordered
        | id   | n    |
        | NULL | NULL |
        | 0    | 1    |
        | 0    | 1    |
        | 1    | 2    |
        | 2    | NULL |

    Scenario: Cast key propagation decimal cast window
      When query
        """
        SELECT o.id,(SELECT max(w) FROM (SELECT row_number() OVER(ORDER BY v) w FROM VALUES ('1.00',1),('01.00',2),('2.00',3) i(k,v) WHERE CAST(i.k AS DECIMAL(8,2))=o.id)) n FROM VALUES (CAST(1 AS DECIMAL(8,2))),(CAST(2 AS DECIMAL(8,2))),(CAST(3 AS DECIMAL(8,2))) o(id) ORDER BY o.id
        """
      Then query result collected ordered
        | id   | n    |
        | 1.00 | 2    |
        | 2.00 | 1    |
        | 3.00 | NULL |

    Scenario: Cast key propagation generator retains array correlation
      When query
        """
        SELECT o.label,(SELECT count(*) FROM (SELECT explode(a) x FROM VALUES ('0',array(1,2)),('00',array(1,2)),('00',array(3)),('1',CAST(array() AS ARRAY<INT>)) i(k,a) WHERE CAST(i.k AS BIGINT)=o.id AND i.a=o.a)) n FROM VALUES (0L,array(1,2),'pair'),(0L,array(3),'single'),(1L,CAST(array() AS ARRAY<INT>),'empty'),(0L,CAST(NULL AS ARRAY<INT>),'null') o(id,a,label) ORDER BY o.label
        """
      Then query result collected ordered
        | label  | n |
        | empty  | 0 |
        | null   | 0 |
        | pair   | 4 |
        | single | 1 |

    Scenario: Cast key propagation window retains struct correlation
      When query
        """
        SELECT o.label,(SELECT max(w) FROM (SELECT row_number() OVER(ORDER BY v) w FROM VALUES ('0',named_struct('g',1),1),('00',named_struct('g',1),2),('00',named_struct('g',2),3),('1',named_struct('g',1),4) i(k,s,v) WHERE CAST(i.k AS BIGINT)=o.id AND i.s=o.s)) n FROM VALUES (0L,named_struct('g',1),'first'),(0L,named_struct('g',2),'second'),(1L,named_struct('g',1),'third') o(id,s,label) ORDER BY label
        """
      Then query result collected ordered
        | label  | n |
        | first  | 2 |
        | second | 1 |
        | third  | 1 |

    Scenario: Cast key propagation two correlated join inputs
      When query
        """
        SELECT o.id,(SELECT count(*) FROM (SELECT * FROM VALUES ('0',1),('00',2),('1',3) i(k,v) WHERE CAST(i.k AS BIGINT)=o.id) a CROSS JOIN (SELECT * FROM VALUES ('0',1),('00',2),('1',3) j(k,v) WHERE CAST(j.k AS BIGINT)=o.id) b) n FROM VALUES (0L),(0L),(1L),(2L) o(id) ORDER BY id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 4 |
        | 0  | 4 |
        | 1  | 1 |
        | 2  | 0 |

  Rule: Cast correlation keys are computed on the rows that Spark casts
    Scenario: An outer filter on the correlated column keeps a guarded cast safe
      When query
        """
        SELECT o.id FROM VALUES (0L), (1L), (2L) AS o(id)
        WHERE o.id >= 0
          AND (SELECT count(*) FROM VALUES ('0', 1), ('1', 3), ('bad', 4) AS i(k, v)
               WHERE i.k <> 'bad' AND CAST(i.k AS BIGINT) = o.id) >= 0
        ORDER BY o.id
        """
      Then query result collected ordered
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario: A lateral view generator drops rows before the cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM VALUES ('0', 1), ('1', 3), ('bad', 4) AS i(k, v)
           LATERAL VIEW explode(IF(i.v = 4, CAST(array() AS ARRAY<INT>), array(i.v))) e AS x
           WHERE CAST(i.k AS BIGINT) = o.id) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 1 |
        | 1  | 1 |
        | 2  | 0 |

    Scenario: An explode of an empty array drops rows before the cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT explode(a) AS x
             FROM VALUES ('0', array(1, 2)), ('bad', CAST(array() AS ARRAY<INT>)), ('1', array(3)) AS i(k, a)
             WHERE CAST(i.k AS BIGINT) = o.id)) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

    Scenario: A semi join above an inner join drops rows before the cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT t.v FROM (SELECT * FROM VALUES ('0', 1), ('00', 2), ('1', 3), ('bad', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id) t JOIN VALUES (1), (2), (3), (4) AS j(w) ON t.v = j.w) q
           LEFT SEMI JOIN VALUES (1), (2), (3) AS s(w) ON q.v = s.w) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

    Scenario: A posexplode of a map above an inner join drops rows before the cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT posexplode(t.m) FROM (SELECT * FROM VALUES ('0', map('a', 1, 'b', 2)),
             ('bad', CAST(map() AS MAP<STRING, INT>)), ('1', map('c', 4)) AS i(k, m)
             WHERE CAST(i.k AS BIGINT) = o.id) t JOIN VALUES ('0'), ('1'), ('bad') AS j(kk) ON t.k = j.kk)) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

    # Spark 4.2 casts every input row of the inner join here and fails on 'bad', which
    # the join removes; it returns rows when its optimizer infers a filter that removes
    # the row first. Sail consistently casts only the rows that reach the aggregate.
    @sail-only
    Scenario: An inner join drops rows before the cast correlation
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT * FROM VALUES ('0', 1), ('00', 2), ('1', 3), ('bad', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id) t
           JOIN VALUES (1), (2), (3) AS j(w) ON t.v = j.w) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

  Rule: Cast correlation keys keep correct groups around aggregates and windows
    Scenario: A cast of a directly compared column keeps windows above a grouped aggregate
      When query
        """
        SELECT o.id, o.s,
          (SELECT max(w) FROM (SELECT row_number() OVER (PARTITION BY k ORDER BY count(*)) AS w
             FROM VALUES ('0', 1), ('0', 2), ('00', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.s GROUP BY k)) AS n
        FROM VALUES (0L, '0'), (0L, '00'), (1L, '1'), (2L, '2') AS o(id, s)
        ORDER BY o.id, o.s
        """
      Then query result collected ordered
        | id | s  | n    |
        | 0  | 0  | 1    |
        | 0  | 00 | 1    |
        | 1  | 1  | 1    |
        | 2  | 2  | NULL |

    Scenario Outline: A cast of a directly compared column keeps <operator> above a grouped aggregate
      When query
        """
        SELECT o.id, o.s,
          (SELECT <consumer> FROM (
            SELECT <projection> AS z
            FROM VALUES ('0', 1), ('0', 2), ('00', 3), ('1', 4),
                        (CAST(NULL AS STRING), 5) AS i(k, v)
            WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.s
            GROUP BY k
          )) AS n
        FROM VALUES (0L, '0'), (0L, '00'), (1L, '1'), (2L, '2'), (0L, '0'),
                    (CAST(NULL AS BIGINT), CAST(NULL AS STRING)),
                    (0L, CAST(NULL AS STRING)) AS o(id, s)
        ORDER BY o.id NULLS FIRST, o.s NULLS FIRST, n NULLS FIRST
        """
      Then query result collected ordered
        | id   | s    | n         |
        | NULL | NULL | <missing> |
        | 0    | NULL | <missing> |
        | 0    | 0    | <zero>    |
        | 0    | 0    | <zero>    |
        | 0    | 00   | <other>   |
        | 1    | 1    | <other>   |
        | 2    | 2    | <missing> |

      Examples:
        | operator                  | projection                                                                                          | consumer | zero | other | missing |
        | a window counted          | row_number() OVER (PARTITION BY k ORDER BY count(*))                                                | count(*) | 1    | 1     | 0       |
        | a running window          | sum(count(*)) OVER (PARTITION BY k ORDER BY max(v) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) | max(z)   | 2    | 1     | NULL    |
        | a generator               | explode(array(count(*), count(*) + 1))                                                              | max(z)   | 3    | 2     | NULL    |
        | a generator counted       | explode(array(count(*), count(*) + 1))                                                              | count(*) | 2    | 2     | 0       |
        | an empty generator        | explode(IF(count(*) = 2, CAST(array() AS ARRAY<BIGINT>), array(count(*))))                          | count(*) | 0    | 1     | 0       |
        | an outer null generator   | explode_outer(IF(count(*) = 2, CAST(NULL AS ARRAY<BIGINT>), array(count(*))))                       | count(*) | 1    | 1     | 0       |

    Scenario: A correlated cast in HAVING over a grouped aggregate
      When query
        """
        SELECT o.id, o.g,
          (SELECT count(*) FROM VALUES ('0', 10), ('00', 10), ('1', 20) AS i(k, g)
           WHERE i.g = o.g GROUP BY i.g HAVING CAST(max(i.k) AS BIGINT) = o.id) AS n
        FROM VALUES (0L, 10), (1L, 20), (0L, 20) AS o(id, g)
        ORDER BY o.id, o.g
        """
      Then query result collected ordered
        | id | g  | n    |
        | 0  | 10 | 2    |
        | 0  | 20 | NULL |
        | 1  | 20 | 1    |

    Scenario: A window above the grouped aggregate keeps the grouped cast correlation
      When query
        """
        SELECT o.id,
          (SELECT max(w) FROM (SELECT sum(count(*)) OVER (PARTITION BY k) AS w
             FROM VALUES ('0', 1), ('0', 2), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id GROUP BY k)) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | 2    |
        | 1  | 1    |
        | 2  | NULL |

    Scenario: A cast correlation below a nested aggregate combines colliding groups
      When query
        """
        SELECT o.id,
          (SELECT sum(c) FROM (SELECT count(*) AS c
             FROM VALUES ('0', 1), ('0', 2), ('00', 3), ('1', 4) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id GROUP BY k)) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | 3    |
        | 1  | 1    |
        | 2  | NULL |

    Scenario: A cast correlation above a grouped derived table combines colliding groups
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT k, sum(v) AS s FROM VALUES ('0', 1), ('00', 2), ('1', 3) AS i(k, v) GROUP BY k) t
           WHERE CAST(t.k AS BIGINT) = o.id) AS n
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 2 |
        | 1  | 1 |
        | 2  | 0 |

    Scenario: A cast correlation decorrelated in a later optimizer pass keeps its key
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM VALUES ('0'), ('00'), ('1') AS i(k) WHERE CAST(i.k AS BIGINT) = o.id) AS a,
          (SELECT count(*) FROM (SELECT * FROM VALUES ('0'), ('00'), ('1') AS j(k)
             WHERE CAST(j.k AS BIGINT) = o.id OFFSET 0)) AS b
        FROM VALUES (0L), (1L), (2L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | a | b |
        | 0  | 2 | 2 |
        | 1  | 1 | 1 |
        | 2  | 0 | 0 |

    # Match Spark's numeric grouping shortcut: signed zeros merge before the string cast.
    Scenario: A floating-point cast correlation uses Spark signed-zero grouping
      When query
        """
        SELECT o.d2, o.s,
          (SELECT count(*) FROM VALUES (CAST('-0.0' AS DOUBLE), 1), (0.0D, 2) AS i(d, v)
           WHERE i.d = o.d2 AND CAST(i.d AS STRING) = o.s) AS n
        FROM VALUES (0.0D, '-0.0'), (0.0D, '0.0') AS o(d2, s)
        ORDER BY o.s
        """
      Then query result collected ordered
        | d2  | s    | n |
        | 0.0 | -0.0 | 0 |
        | 0.0 | 0.0  | 2 |

    # Spark 4.2 treats INT -> FLOAT as a lossless up-cast and groups by the raw value,
    # returning (16777216.0, 1) twice. Sail groups by the compared FLOAT value.
    @sail-only
    Scenario: A lossy up-cast correlation groups the compared value
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM VALUES (16777216), (16777217) AS i(k)
           WHERE CAST(i.k AS FLOAT) = o.id) AS n
        FROM VALUES (CAST(0 AS FLOAT)), (CAST(16777216 AS FLOAT)) AS o(id)
        ORDER BY o.id, n
        """
      Then query result collected ordered
        | id         | n |
        | 0.0        | 0 |
        | 16777216.0 | 2 |

  # Spark rejects outer references in a grouping-set subquery, which Sail already ran before
  # cast keys. Expected values come from running each outer row's query uncorrelated on Spark.
  Rule: Cast correlation keys keep the GROUPING results of grouping sets
    @sail-only
    Scenario Outline: A cast correlation keeps <function> of a rolled-up column
      When query
        """
        SELECT o.id,
          (SELECT sum(z) FROM (SELECT <function> AS z
             FROM VALUES ('1', 1), ('1', 2), ('2', 3) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id GROUP BY GROUPING SETS ((k, v), (k)))) AS n
        FROM VALUES (1L), (2L), (3L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 1  | 1    |
        | 2  | 1    |
        | 3  | NULL |

      Examples:
        | function                                                       |
        | CAST(grouping(v) AS BIGINT)                                    |
        | grouping_id(k, v)                                              |
        | CAST(grouping(k) AS BIGINT) * 10 + CAST(grouping(v) AS BIGINT) |

    @sail-only
    Scenario: A cast correlation keeps a HAVING filter on GROUPING of a rolled-up column
      When query
        """
        SELECT o.id,
          (SELECT count(*) FROM (SELECT v FROM VALUES ('1', 1), ('1', 2), ('2', 3) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id GROUP BY GROUPING SETS ((k, v), (k)) HAVING grouping(v) = 1)) AS n
        FROM VALUES (1L), (2L), (3L) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 1  | 1 |
        | 2  | 1 |
        | 3  | 0 |

    @sail-only
    Scenario: A cast of a floating-point column compared directly keeps GROUPING of a rolled-up column
      When query
        """
        SELECT o.id,
          (SELECT sum(z) FROM (SELECT CAST(grouping(v) AS BIGINT) AS z
             FROM VALUES (1.0D, 1), (1.0D, 2), (2.0D, 3) AS i(k, v)
             WHERE CAST(i.k AS STRING) = o.id AND i.k = o.d GROUP BY GROUPING SETS ((k, v), (k)))) AS n
        FROM VALUES ('1.0', 1.0D), ('2.0', 2.0D), ('3.0', 3.0D) AS o(id, d)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id  | n    |
        | 1.0 | 1    |
        | 2.0 | 1    |
        | 3.0 | NULL |

    @sail-only
    Scenario: A cast correlation keeps GROUPING above a correlated inner aggregate
      When query
        """
        SELECT o.id, o.g,
          (SELECT sum(z) FROM (SELECT CAST(grouping(c1) AS BIGINT) AS z
             FROM (SELECT k, c1, c2, c3, c4, c5, count(*) AS n
                   FROM VALUES ('1', 1, 1, 1, 1, 1, 10), ('1', 1, 1, 1, 1, 1, 10), ('2', 1, 1, 1, 1, 1, 20)
                     AS i(k, c1, c2, c3, c4, c5, g)
                   WHERE i.g = o.g GROUP BY k, c1, c2, c3, c4, c5)
             WHERE CAST(k AS BIGINT) = o.id
             GROUP BY GROUPING SETS ((k, c1, c2, c3, c4, c5), (k, c1, c2, c3, c4, c5)))) AS n
        FROM VALUES (1L, 10), (2L, 20), (3L, 10), (1L, 20) AS o(id, g)
        ORDER BY o.id, o.g
        """
      Then query result collected ordered
        | id | g  | n    |
        | 1  | 10 | 0    |
        | 1  | 20 | NULL |
        | 2  | 20 | 0    |
        | 3  | 10 | NULL |

    @sail-only
    Scenario: A lossy up-cast correlation groups the compared value in grouping sets
      When query
        """
        SELECT o.id,
          (SELECT sum(z) FROM (SELECT count(*) * 10 + CAST(grouping(v) AS BIGINT) AS z
             FROM VALUES (16777216, 1), (16777217, 1), (33554432, 2) AS i(k, v)
             WHERE CAST(i.k AS FLOAT) = o.id GROUP BY GROUPING SETS ((v), ()))) AS n
        FROM VALUES (CAST(16777216 AS FLOAT)), (CAST(33554432 AS FLOAT)), (CAST(1 AS FLOAT)) AS o(id)
        ORDER BY o.id
        """
      Then query result collected ordered
        | id         | n    |
        | 1.0        | NULL |
        | 16777216.0 | 41   |
        | 33554432.0 | 21   |

    @sail-only
    Scenario Outline: A cast of a directly compared column keeps <operator> above <grouping>
      When query
        """
        SELECT o.id, o.s,
          (SELECT max(z) FROM (
             SELECT <producer> AS z
             FROM VALUES ('0', 1), ('0', 2), ('00', 3), ('1', 4),
                         (CAST(NULL AS STRING), 5) AS i(k, v)
             WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.s
             GROUP BY <grouping>
           )) AS n
        FROM VALUES (0L, '0'), (0L, '00'), (1L, '1'), (2L, '2'), (0L, '0'),
                    (CAST(NULL AS BIGINT), CAST(NULL AS STRING)) AS o(id, s)
        ORDER BY o.id NULLS FIRST, o.s NULLS FIRST, n NULLS FIRST
        """
      Then query result collected ordered
        | id   | s    | n     |
        | NULL | NULL | NULL  |
        | 0    | 0    | <n0>  |
        | 0    | 0    | <n0>  |
        | 0    | 00   | <n00> |
        | 1    | 1    | <n1>  |
        | 2    | 2    | NULL  |

      Examples:
        | grouping                 | operator    | producer                                             | n0 | n00 | n1 |
        | GROUPING SETS ((k), ())  | a generator | explode(array(count(*), count(*) + 1))               | 3  | 2   | 2  |
        | ROLLUP(k)                | a generator | explode(array(count(*), count(*) + 1))               | 3  | 2   | 2  |
        | CUBE(k)                  | a generator | explode(array(count(*), count(*) + 1))               | 3  | 2   | 2  |
        | GROUPING SETS ((k), (k)) | a window    | row_number() OVER (PARTITION BY k ORDER BY count(*)) | 2  | 2   | 2  |
        | GROUPING SETS ((k), (k)) | a generator | explode(array(count(*), count(*) + 1))               | 3  | 2   | 2  |
