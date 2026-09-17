Feature: Scalar subqueries in distributed execution
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
