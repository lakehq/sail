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

  Scenario: Early reductions survive Parquet filter pushdown on the dimension scan
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW early_filter_dimension
      USING parquet OPTIONS (path {{ dimension.sql }}, pushdown_filters 'true')
      """
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
    When query
      """
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
    Then query result collected
      | k | total |
      | 1 | 10    |
      | 1 | 10    |

  Scenario Outline: A selective filter does not make an expensive dimension scan cheap to duplicate
    Given variable large_dimension for temporary directory early_filter_large_dimension
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ facts.sql }} USING parquet
      SELECT CASE WHEN id % 10 = 9 THEN NULL ELSE id % 10 END AS k,
             id AS ticket, CAST(1 AS BIGINT) AS amount
      FROM range(20000)
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW early_filter_facts AS
      SELECT * FROM parquet.`{{ facts.string }}`
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ large_dimension.sql }} USING parquet
      SELECT id % 10 AS k, id AS marker FROM range(<rows>)
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW early_filter_large_dimension AS
      SELECT * FROM parquet.`{{ large_dimension.string }}`
      """
    When query
      """
      EXPLAIN
      SELECT d.k, g.total
      FROM early_filter_large_dimension d
      JOIN (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k = b.k AND a.ticket = b.ticket
        GROUP BY a.k HAVING SUM(a.amount) > 5
      ) g ON d.k = g.k
      WHERE d.marker = 1
      """
    Then query plan matches snapshot
    When query
      """
      SELECT d.k, g.total
      FROM early_filter_large_dimension d
      JOIN (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k = b.k AND a.ticket = b.ticket
        GROUP BY a.k HAVING SUM(a.amount) > 5
      ) g ON d.k = g.k
      WHERE d.marker = 1
      """
    Then query result collected
      | k | total |
      | 1 | 2000  |

    Examples:
      | rows   |
      | 131072 |
      | 131073 |

  Scenario: Early reductions avoid fact inputs already restricted to eligible keys
    When query
      """
      EXPLAIN
      SELECT d.k, g.total
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k = b.k AND a.ticket = b.ticket
        WHERE a.k = 1
        GROUP BY a.k
      ) g ON d.k = g.k
      WHERE d.color = 'red'
      """
    Then query plan matches snapshot
    When query
      """
      SELECT d.k, g.total
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, SUM(a.amount) AS total
        FROM early_filter_facts a JOIN early_filter_facts b
          ON a.k = b.k AND a.ticket = b.ticket
        WHERE a.k = 1
        GROUP BY a.k
      ) g ON d.k = g.k
      WHERE d.color = 'red'
      """
    Then query result collected
      | k | total |
      | 1 | 10    |
      | 1 | 10    |

  Scenario: Inner joins in the same reorder region as the dimension get no early reduction
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ facts.sql }} USING parquet
      SELECT id % 100 AS k, id AS ticket, CAST(1 AS BIGINT) AS amount FROM range(1000)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ dimension.sql }} USING parquet
      SELECT id AS k, CASE WHEN id = 1 THEN 'red' ELSE 'blue' END AS color FROM range(100)
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
    When query
      """
      EXPLAIN
      SELECT a.ticket, a.amount
      FROM early_filter_facts a
      JOIN early_filter_facts b ON a.k = b.k AND a.ticket = b.ticket
      JOIN early_filter_dimension d ON a.k = d.k
      WHERE d.color = 'red'
      """
    Then query plan matches snapshot
    When query
      """
      SELECT SUM(a.amount) AS total, COUNT(*) AS n
      FROM early_filter_facts a
      JOIN early_filter_facts b ON a.k = b.k AND a.ticket = b.ticket
      JOIN early_filter_dimension d ON a.k = d.k
      WHERE d.color = 'red'
      """
    Then query result collected
      | total | n  |
      | 10    | 10 |

  Scenario: User semijoins on a selective dimension keep their results
    When query
      """
      SELECT SUM(a.amount) AS total, COUNT(*) AS n
      FROM early_filter_facts a
      LEFT SEMI JOIN (
        SELECT k AS __early_join_key_0 FROM early_filter_dimension WHERE color = 'red'
      ) d ON a.k <=> d.__early_join_key_0
      JOIN early_filter_facts b ON a.k <=> b.k AND a.ticket = b.ticket
      """
    Then query result collected
      | total | n  |
      | 20    | 20 |

  Scenario Outline: A semi join that only filters by a scan is not a reduction boundary
    When query
      """
      EXPLAIN
      SELECT a.ticket
      FROM early_filter_facts a
      LEFT SEMI JOIN (
        SELECT <key> AS k FROM early_filter_dimension WHERE color = 'blue' OR color = 'red'
      ) s ON a.k = s.k
      JOIN early_filter_dimension d ON a.k = d.k
      WHERE d.color = 'red'
      """
    Then query plan matches snapshot
    When query
      """
      SELECT SUM(a.amount) AS total, COUNT(*) AS n
      FROM early_filter_facts a
      LEFT SEMI JOIN (
        SELECT <key> AS k FROM early_filter_dimension WHERE color = 'blue' OR color = 'red'
      ) s ON a.k = s.k
      JOIN early_filter_dimension d ON a.k = d.k
      WHERE d.color = 'red'
      """
    Then query result collected
      | total | n  |
      | 20    | 20 |

    Examples:
      | key   |
      | k     |
      | k + 0 |

  Scenario: Selective keys reach the fact inputs below a semi join filtered by an aggregate
    When query
      """
      EXPLAIN
      SELECT d.k, f.ticket
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, a.ticket
        FROM early_filter_facts a
        JOIN early_filter_facts b ON a.k = b.k AND a.ticket = b.ticket
        LEFT SEMI JOIN (
          SELECT k FROM early_filter_facts GROUP BY k HAVING COUNT(*) > 5
        ) g ON a.k = g.k
      ) f ON d.k = f.k
      WHERE d.color = 'red'
      """
    Then query plan matches snapshot
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, a.ticket
        FROM early_filter_facts a
        JOIN early_filter_facts b ON a.k = b.k AND a.ticket = b.ticket
        LEFT SEMI JOIN (
          SELECT k FROM early_filter_facts GROUP BY k HAVING COUNT(*) > 5
        ) g ON a.k = g.k
      ) f ON d.k = f.k
      WHERE d.color = 'red'
      GROUP BY d.k
      """
    Then query result collected
      | k | n  |
      | 1 | 20 |

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
        LEFT JOIN early_filter_facts b ON a.ticket = b.ticket AND a.k < 5
      ) f ON d.k <=> f.k
      WHERE d.color = 'red'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | n  |
      | NULL | 50 |
      | 1    | 20 |

  Scenario: Early filters do not cross top-k boundaries
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT b.k FROM early_filter_facts a
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
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ facts.sql }} USING parquet
      SELECT CASE WHEN id % 10 = 9 THEN NULL ELSE id % 10 END AS k, id AS ticket,
             CAST(CASE WHEN id < 50 THEN 10 ELSE 20 END AS BIGINT) AS shade
      FROM range(100)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ dimension.sql }} USING parquet
      SELECT * FROM VALUES
        (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'x'), (CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'x'),
        (CAST(NULL AS BIGINT), CAST(10 AS BIGINT), 'x'), (CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'x'),
        (CAST(3 AS BIGINT), CAST(10 AS BIGINT), 'y') AS d(k, shade, tag)
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
    When query
      """
      EXPLAIN
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT b.k, b.shade
        FROM early_filter_facts a
        JOIN early_filter_facts b ON a.ticket = b.ticket
        GROUP BY b.k, b.shade, b.ticket
      ) f ON d.k <=> f.k AND d.shade <=> f.shade
      WHERE d.tag = 'x'
      GROUP BY d.k ORDER BY d.k
      """
    Then query plan matches snapshot
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT b.k, b.shade
        FROM early_filter_facts a
        JOIN early_filter_facts b ON a.ticket = b.ticket
        GROUP BY b.k, b.shade, b.ticket
      ) f ON d.k <=> f.k AND d.shade <=> f.shade
      WHERE d.tag = 'x'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | n  |
      | NULL | 5  |
      | 1    | 10 |
      | 2    | 5  |
    When query
      """
      SELECT d.k, COUNT(*) AS n
      FROM early_filter_dimension d
      JOIN (
        SELECT a.k, b.shade
        FROM early_filter_facts a
        JOIN early_filter_facts b ON a.ticket = b.ticket
        GROUP BY a.k, b.shade, b.ticket
      ) f ON d.k <=> f.k AND d.shade <=> f.shade
      WHERE d.tag = 'x'
      GROUP BY d.k ORDER BY d.k
      """
    Then query result ordered
      | k    | n  |
      | NULL | 5  |
      | 1    | 10 |
      | 2    | 5  |

  Scenario: Early filters preserve volatile grouping expressions outside the join keys
    When query
      """
      SELECT d.k, g.n
      FROM early_filter_dimension d
      JOIN (
        SELECT k, COUNT(*) AS n
        FROM early_filter_facts
        GROUP BY k, CAST(rand(42) * 2 AS INT)
      ) g ON d.k = g.k
      WHERE d.color = 'red'
      """
    Then query result collected
      | k | n |
      | 1 | 3 |
      | 1 | 3 |
      | 1 | 7 |
      | 1 | 7 |
