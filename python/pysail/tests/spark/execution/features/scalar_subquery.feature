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
