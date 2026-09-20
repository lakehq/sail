Feature: Existing join filters preserve rows across fact and aggregate boundaries

  Background:
    Given variable facts for temporary directory join_filter_reuse_facts
    Given variable returns for temporary directory join_filter_reuse_returns
    Given variable dimension for temporary directory join_filter_reuse_dimension
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ facts.sql }} USING parquet
      SELECT /*+ COALESCE(1) */
             CASE WHEN id % 4 = 3 THEN NULL ELSE id % 4 + 1 END AS item_sk,
             id AS ticket, id + 1 AS amount, id + 100 AS store_amount,
             CASE WHEN id < 6 THEN 'a' ELSE 'b' END AS tag
      FROM range(12)
      ORDER BY ticket
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW join_filter_reuse_facts AS
      SELECT * FROM parquet.`{{ facts.string }}`
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ returns.sql }} USING parquet
      SELECT item_sk, ticket FROM join_filter_reuse_facts
      UNION ALL
      SELECT item_sk, ticket FROM join_filter_reuse_facts WHERE ticket = 5
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW join_filter_reuse_returns AS
      SELECT * FROM parquet.`{{ returns.string }}`
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ dimension.sql }} USING parquet
      SELECT * FROM VALUES
        (CAST(2 AS BIGINT), 'a', 'red'), (CAST(2 AS BIGINT), 'a', 'red'),
        (CAST(NULL AS BIGINT), 'b', 'red'),
        (CAST(1 AS BIGINT), 'a', 'blue'), (CAST(3 AS BIGINT), 'b', 'blue'),
        (CAST(2 AS BIGINT), 'b', 'blue') AS d(item_sk, tag, color)
      """
    Given statement template
      """
      CREATE OR REPLACE TEMP VIEW join_filter_reuse_dimension AS
      SELECT * FROM parquet.`{{ dimension.string }}`
      """

  Scenario: Existing dimension filters reach fact joins behind grouped user semijoins
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW join_filter_reuse_catalog AS
      SELECT a.item_sk, SUM(a.amount) AS sale
      FROM join_filter_reuse_facts a
      JOIN join_filter_reuse_returns b
        ON a.item_sk = b.item_sk AND a.ticket = b.ticket
      GROUP BY a.item_sk HAVING SUM(a.amount) > 20
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW join_filter_reuse_store AS
      SELECT s.item_sk, s.store_amount
      FROM join_filter_reuse_facts s
      JOIN join_filter_reuse_returns r
        ON s.item_sk = r.item_sk AND s.ticket = r.ticket
      LEFT SEMI JOIN join_filter_reuse_catalog c ON s.item_sk = c.item_sk
      """
    # Item 2 has catalog sale 2 + 6 + 6 + 10 = 24 and passes HAVING.
    # Its store rows sum to 101 + 105 + 105 + 109 = 420; two dimension
    # rows double that total. The user semijoin must not multiply either input.
    When query
      """
      SELECT d.item_sk, SUM(s.store_amount) AS total, COUNT(*) AS n
      FROM join_filter_reuse_dimension d
      JOIN join_filter_reuse_store s ON d.item_sk = s.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk
      """
    Then query result collected
      | item_sk | total | n |
      | 2       | 840   | 8 |
    When query
      """
      EXPLAIN
      SELECT d.item_sk, SUM(s.store_amount) AS total, COUNT(*) AS n
      FROM join_filter_reuse_dimension d
      JOIN join_filter_reuse_store s ON d.item_sk = s.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk
      """
    Then query plan matches snapshot

  Scenario: Null-safe grouped keys and renamed composite keys preserve whole groups
    When query
      """
      WITH grouped AS (
        SELECT a.item_sk AS grouped_item, a.tag AS grouped_tag,
               SUM(a.amount) AS total
        FROM join_filter_reuse_facts a
        JOIN join_filter_reuse_returns b
          ON a.item_sk <=> b.item_sk AND a.ticket <=> b.ticket
        GROUP BY a.item_sk, a.tag HAVING SUM(a.amount) >= 10
      )
      SELECT d.item_sk, SUM(g.total) AS total, COUNT(*) AS n
      FROM join_filter_reuse_dimension d
      JOIN grouped g
        ON d.item_sk <=> g.grouped_item AND d.tag <=> g.grouped_tag
      WHERE d.color = 'red'
      GROUP BY d.item_sk ORDER BY d.item_sk
      """
    Then query result ordered
      | item_sk | total | n |
      | NULL    | 20    | 1 |
      | 2       | 28    | 2 |
    When query
      """
      EXPLAIN
      WITH grouped AS (
        SELECT a.item_sk AS grouped_item, a.tag AS grouped_tag,
               SUM(a.amount) AS total
        FROM join_filter_reuse_facts a
        JOIN join_filter_reuse_returns b
          ON a.item_sk <=> b.item_sk AND a.ticket <=> b.ticket
        GROUP BY a.item_sk, a.tag HAVING SUM(a.amount) >= 10
      )
      SELECT d.item_sk, SUM(g.total) AS total, COUNT(*) AS n
      FROM join_filter_reuse_dimension d
      JOIN grouped g
        ON d.item_sk <=> g.grouped_item AND d.tag <=> g.grouped_tag
      WHERE d.color = 'red'
      GROUP BY d.item_sk ORDER BY d.item_sk
      """
    Then query plan matches snapshot

  Scenario: Plain user semijoins reuse their existing fact filter
    When query
      """
      SELECT SUM(f.amount) AS total, COUNT(*) AS n
      FROM join_filter_reuse_facts f
      LEFT SEMI JOIN (
        SELECT item_sk FROM join_filter_reuse_dimension WHERE color = 'red'
      ) d ON f.item_sk = d.item_sk
      """
    Then query result collected
      | total | n |
      | 18    | 3 |
    When query
      """
      EXPLAIN
      SELECT SUM(f.amount) AS total, COUNT(*) AS n
      FROM join_filter_reuse_facts f
      LEFT SEMI JOIN (
        SELECT item_sk FROM join_filter_reuse_dimension WHERE color = 'red'
      ) d ON f.item_sk = d.item_sk
      """
    Then query plan matches snapshot

  Scenario: Grouping sets retain subtotals from keys excluded by the dimension
    # The duplicate return adds 6 to the grand total: 78 + 6 = 84.
    # NULL matches both its own group (24) and the grand total (84).
    When query
      """
      SELECT d.item_sk, SUM(g.total) AS total, COUNT(*) AS n
      FROM join_filter_reuse_dimension d
      JOIN (
        SELECT a.item_sk, SUM(a.amount) AS total
        FROM join_filter_reuse_facts a
        JOIN join_filter_reuse_returns b
          ON a.item_sk <=> b.item_sk AND a.ticket = b.ticket
        GROUP BY ROLLUP(a.item_sk)
      ) g ON d.item_sk <=> g.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk ORDER BY d.item_sk
      """
    Then query result ordered
      | item_sk | total | n |
      | NULL    | 108   | 2 |
      | 2       | 48    | 2 |

  Scenario: A limited fact input selects its rows before the dimension join
    # The fact join's first three tickets are 0, 1, 2. Filtering item 2 below
    # the limit would replace them with 1, 5, 5 and change its sum from 2 to 14.
    When query
      """
      SELECT d.item_sk, SUM(f.total) AS total, SUM(f.n) AS n
      FROM join_filter_reuse_dimension d
      JOIN (
        SELECT limited.item_sk, SUM(limited.amount) AS total, COUNT(*) AS n
        FROM (
          SELECT a.item_sk, a.amount, a.ticket
          FROM join_filter_reuse_facts a
          JOIN join_filter_reuse_returns b
            ON a.item_sk = b.item_sk AND a.ticket = b.ticket
          ORDER BY a.ticket LIMIT 3
        ) limited
        GROUP BY limited.item_sk
      ) f ON d.item_sk = f.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk
      """
    Then query result collected
      | item_sk | total | n |
      | 2       | 4     | 2 |

  Scenario Outline: Outer fact joins preserve unmatched rows in nullable groups
    # Only tickets 0, 1, 2 match returns. Tickets 3 through 11 contribute
    # amounts 4 through 12 to the NULL group: nine rows totaling 72.
    # Filtering preserved facts to dimension keys would lose unmatched items
    # 1 and 3, even though their null-extended rows match the NULL dimension key.
    When query
      """
      SELECT d.item_sk, SUM(g.total) AS total, SUM(g.n) AS n
      FROM join_filter_reuse_dimension d
      JOIN (
        SELECT b.item_sk, SUM(a.amount) AS total, COUNT(*) AS n
        FROM <fact_join>
          ON a.item_sk = b.item_sk AND a.ticket = b.ticket AND b.ticket < 3
        GROUP BY b.item_sk
      ) g ON d.item_sk <=> g.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk ORDER BY d.item_sk
      """
    Then query result ordered
      | item_sk | total | n |
      | NULL    | 72    | 9 |
      | 2       | 4     | 2 |

    Examples:
      | fact_join                                                               |
      | join_filter_reuse_facts a LEFT JOIN join_filter_reuse_returns b           |
      | join_filter_reuse_returns b RIGHT JOIN join_filter_reuse_facts a          |

  Scenario: Window values include fact rows excluded by the dimension join
    # Item 2 contributes positions 2, 6, 6, 10 after the duplicate return,
    # doubled by the two matching dimension rows. Its windows still see 12 rows.
    When query
      """
      SELECT d.item_sk, SUM(f.positions) AS positions, MAX(f.all_rows) AS all_rows,
             SUM(f.n) AS n
      FROM join_filter_reuse_dimension d
      JOIN (
        SELECT a.item_sk, SUM(a.rn) AS positions, MAX(a.all_rows) AS all_rows,
               COUNT(*) AS n
        FROM (
          SELECT item_sk, ticket, ROW_NUMBER() OVER (ORDER BY ticket) AS rn,
                 COUNT(*) OVER () AS all_rows
          FROM join_filter_reuse_facts
        ) a
        JOIN join_filter_reuse_returns b
          ON a.item_sk = b.item_sk AND a.ticket = b.ticket
        GROUP BY a.item_sk
      ) f ON d.item_sk = f.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk
      """
    Then query result collected
      | item_sk | positions | all_rows | n |
      | 2       | 48        | 12       | 8 |

  Scenario: Stateful fact expressions retain their positions before dimension filtering
    # The ordered fixture is one Parquet partition, so IDs equal ticket numbers.
    # The fact join yields IDs 1, 5, 5, 9 for item 2, doubled by the dimension.
    # Moving filtering below ID generation would renumber these retained rows.
    When query
      """
      SELECT d.item_sk, SUM(f.positions) AS positions, SUM(f.n) AS n
      FROM join_filter_reuse_dimension d
      JOIN (
        SELECT a.item_sk, SUM(a.position) AS positions, COUNT(*) AS n
        FROM (
          SELECT item_sk, ticket, monotonically_increasing_id() AS position
          FROM join_filter_reuse_facts
        ) a
        JOIN join_filter_reuse_returns b
          ON a.item_sk = b.item_sk AND a.ticket = b.ticket
        GROUP BY a.item_sk
      ) f ON d.item_sk = f.item_sk
      WHERE d.color = 'red'
      GROUP BY d.item_sk
      """
    Then query result collected
      | item_sk | positions | n |
      | 2       | 40        | 8 |
