Feature: Join reorder propagates selective dimension keys before fact joins

  Background:
    Given variable facts for temporary directory early_filter_facts
    Given variable dimension for temporary directory early_filter_dimension
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ facts.sql }} USING parquet
      SELECT CASE WHEN id % 10 = 9 THEN NULL ELSE id % 10 END AS k,
             id AS ticket, CAST(1 AS BIGINT) AS amount
      FROM range(100)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ dimension.sql }} USING parquet
      SELECT * FROM VALUES
        (CAST(1 AS BIGINT), 'red'), (CAST(1 AS BIGINT), 'red'),
        (CAST(NULL AS BIGINT), 'red'), (CAST(2 AS BIGINT), 'blue') AS d(k, color)
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW early_filter_facts AS
      SELECT * FROM parquet.`{{ facts.string }}`
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW early_filter_dimension AS
      SELECT * FROM parquet.`{{ dimension.string }}`
      """

  Scenario: Early key filters preserve duplicate dimensions and null-safe grouped joins
    When query
      """
      WITH grouped AS (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k <=> b.k AND a.ticket = b.ticket
        GROUP BY a.k HAVING SUM(a.amount) > 5
      )
      SELECT d.k, SUM(g.total) AS total, COUNT(*) AS n
      FROM early_filter_dimension d JOIN grouped g ON d.k <=> g.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | total | n |
      | NULL | 10    | 1 |
      | 1    | 20    | 2 |

  Scenario: Selective grouping keys reach both fact inputs in the physical plan
    When query
      """
      EXPLAIN
      SELECT d.k, g.total
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k = b.k AND a.ticket = b.ticket
        GROUP BY a.k HAVING SUM(a.amount) > 5
      ) g ON d.k = g.k
      WHERE d.color = 'red'
      """
    Then query plan matches snapshot

  Scenario: Early filters propagate through semi joins without multiplying fact rows
    When query
      """
      SELECT d.k, SUM(f.amount) AS total, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT a.* FROM early_filter_facts a
        LEFT SEMI JOIN (
          SELECT k FROM early_filter_facts GROUP BY k HAVING COUNT(*) > 5
        ) b ON a.k <=> b.k
      ) f ON d.k <=> f.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | total | n  |
      | NULL | 10    | 10 |
      | 1    | 20    | 20 |

  Scenario: Null-producing outer joins remain propagation boundaries
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT b.k FROM early_filter_facts a
        LEFT JOIN early_filter_facts b ON a.ticket = b.ticket AND b.ticket < 5
      ) f ON d.k <=> f.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | n  |
      | NULL | 95 |
      | 1    | 2  |

  Scenario: Early filters do not cross top-k boundaries
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k FROM early_filter_facts a
        JOIN (SELECT * FROM early_filter_facts ORDER BY ticket LIMIT 3) b
          ON a.ticket = b.ticket
      ) f ON d.k <=> f.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k | n |
      | 1 | 2 |

  Scenario: Restrictions on aggregate values do not filter aggregate inputs
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (SELECT k, COUNT(*) / 10 AS n FROM early_filter_facts GROUP BY k) f
        ON d.k = f.n
      WHERE d.color = 'red'
      GROUP BY d.k
      """
    Then query result collected
      | k | n  |
      | 1 | 20 |

  Scenario: Grouping sets preserve synthesized null keys
    When query
      """
      SELECT d.k, SUM(f.n) AS total
      FROM early_filter_dimension d
      JOIN (
        SELECT k, COUNT(*) AS n FROM early_filter_facts GROUP BY ROLLUP(k)
      ) f ON d.k <=> f.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | total |
      | NULL | 110   |
      | 1    | 20    |

  Scenario: Composite restrictions keep key tuples together
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, b.color
        FROM early_filter_facts a
        JOIN (
          SELECT k, CASE WHEN ticket < 50 THEN 'red' ELSE 'blue' END AS color, ticket
          FROM early_filter_facts
        ) b ON a.k <=> b.k AND a.ticket = b.ticket
      ) f ON d.k <=> f.k AND d.color = f.color
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | n  |
      | NULL | 5  |
      | 1    | 10 |
