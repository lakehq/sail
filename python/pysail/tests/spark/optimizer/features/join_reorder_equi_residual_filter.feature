Feature: Join reorder preserves residual filters and required range joins

  Scenario: Equality-connected sales joins preserve strict filters and duplicates
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_sales AS
      SELECT * FROM VALUES
        (1, 10, 1, 4, 5),
        (1, 10, 1, 4, 5),
        (2, 10, 1, 3, 5),
        (3, 10, 1, 2, 5),
        (4, 20, 2, 5, 4),
        (5, 10, 1, 6, 5),
        (6, 10, 6, 4, 5),
        (7, 10, 1, 4, NULL),
        (8, NULL, 1, 4, 5),
        (9, 10, NULL, 4, 5),
        (10, 10, 1, NULL, 5)
      AS t(sale_id, item_key, sold_date_key, ship_date_key, quantity)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_dates AS
      SELECT * FROM VALUES
        (1, DATE '2020-01-01', 1),
        (2, DATE '2020-01-05', 1),
        (3, DATE '2020-01-06', 1),
        (4, DATE '2020-01-07', 1),
        (5, DATE '2020-01-11', 2),
        (6, CAST(NULL AS DATE), 1),
        (7, DATE '2020-01-02', 1),
        (8, DATE '2020-01-08', 2),
        (NULL, DATE '2020-01-01', 1),
        (NULL, DATE '2020-01-07', 1)
      AS t(date_key, date_value, week_seq)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_inventory AS
      SELECT * FROM VALUES
        (10, 7, 4),
        (10, 7, 4),
        (10, 7, 5),
        (10, 7, 6),
        (10, 7, NULL),
        (20, 7, 3),
        (NULL, 7, 4),
        (10, NULL, 4),
        (10, 8, 4)
      AS t(item_key, date_key, quantity)
      """

    When query
      """
      SELECT
        s.sale_id,
        sold.date_value AS sold_date,
        ship.date_value AS ship_date,
        i.quantity AS inventory_quantity,
        s.quantity AS sale_quantity
      FROM jrer_sales s
      JOIN jrer_inventory i ON s.item_key = i.item_key
      JOIN jrer_dates sold ON s.sold_date_key = sold.date_key
      JOIN jrer_dates stocked ON i.date_key = stocked.date_key
      JOIN jrer_dates ship ON s.ship_date_key = ship.date_key
      WHERE sold.week_seq = stocked.week_seq
        AND ship.date_value > sold.date_value + 5
        AND i.quantity < s.quantity
      ORDER BY s.sale_id, inventory_quantity
      """
    Then query result ordered
      | sale_id | sold_date  | ship_date  | inventory_quantity | sale_quantity |
      | 1       | 2020-01-01 | 2020-01-07 | 4                  | 5             |
      | 1       | 2020-01-01 | 2020-01-07 | 4                  | 5             |
      | 1       | 2020-01-01 | 2020-01-07 | 4                  | 5             |
      | 1       | 2020-01-01 | 2020-01-07 | 4                  | 5             |
      | 4       | 2020-01-05 | 2020-01-11 | 3                  | 4             |

  Scenario: Disconnected equality graph still supports a range join
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_points AS
      SELECT * FROM VALUES
        (1, 0),
        (2, 4),
        (3, 5),
        (4, 9),
        (5, 10),
        (6, NULL)
      AS t(id, value)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_labels AS
      SELECT * FROM VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e'),
        (6, 'f')
      AS t(id, label)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jrer_ranges AS
      SELECT * FROM VALUES
        ('low', 0, 5),
        ('high', 5, 10)
      AS t(bucket, lower_bound, upper_bound)
      """

    When query
      """
      SELECT p.id, l.label, p.value, r.bucket
      FROM jrer_points p
      JOIN jrer_labels l ON p.id = l.id
      JOIN jrer_ranges r ON p.value >= r.lower_bound AND p.value < r.upper_bound
      ORDER BY p.id, r.bucket
      """
    Then query result ordered
      | id | label | value | bucket |
      | 1  | a     | 0     | low    |
      | 2  | b     | 4     | low    |
      | 3  | c     | 5     | high   |
      | 4  | d     | 9     | high   |
