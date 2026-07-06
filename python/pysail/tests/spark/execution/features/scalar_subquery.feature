Feature: Scalar subqueries in distributed execution
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
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
      EXPLAIN
      SELECT k, COUNT(*) FILTER (WHERE v > (
        SELECT MIN(x) FROM VALUES (1) AS s(x)
      )) AS n
      FROM VALUES (1, 1), (1, 2), (2, 3) AS t(k, v)
      GROUP BY k
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
      EXPLAIN
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(x) + (SELECT MIN(y) FROM VALUES (1) AS u(y))
        FROM VALUES (10), (20) AS s(x)
      ) AS shifted
      FROM VALUES (1), (2) AS t(v)
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
      EXPLAIN
      SELECT v, CAST(v AS BIGINT) + (
        SELECT MIN(CAST(x AS BIGINT))
        FROM VALUES (CAST(NULL AS INT)) AS s(x)
      ) AS maybe_shifted
      FROM VALUES (1), (2) AS t(v)
      ORDER BY v
      """
    Then query plan matches snapshot

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
      EXPLAIN
      SELECT outer_t.k, outer_t.v, (
        SELECT MAX(inner_t.x)
        FROM VALUES (1, 4), (1, 2), (2, 3), (2, 1) AS inner_t(k, x)
        WHERE inner_t.k = outer_t.k
      ) AS mx
      FROM VALUES (1, 2), (1, 4), (2, 1), (2, 3) AS outer_t(k, v)
      ORDER BY outer_t.k, outer_t.v
      """
    Then query plan matches snapshot
