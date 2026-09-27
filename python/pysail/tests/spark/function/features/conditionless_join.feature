Feature: Joins without a join condition keep their join type

  Background:
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW cj_l AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, v)
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW cj_r AS SELECT * FROM VALUES (10, 'x') AS t(k, w)
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW cj_e AS SELECT * FROM VALUES (10, 'x') AS t(k, w) WHERE k < 0
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW cj_le AS SELECT * FROM VALUES (1, 'a') AS t(id, v) WHERE id < 0
      """

  Rule: Outer joins without a condition preserve unmatched rows

    Scenario Outline: an outer join without ON is not a cross join
      When query
        """
        SELECT count(*) AS n, count(id) AS left_rows, count(k) AS right_rows
        FROM <left> <join> <right>
        """
      Then query result
        | n   | left_rows | right_rows |
        | <n> | <l>       | <r>        |

      Examples:
        | left  | join       | right | n | l | r |
        | cj_l  | LEFT JOIN  | cj_r  | 2 | 2 | 2 |
        | cj_l  | LEFT JOIN  | cj_e  | 2 | 2 | 0 |
        | cj_l  | RIGHT JOIN | cj_e  | 0 | 0 | 0 |
        | cj_le | RIGHT JOIN | cj_r  | 1 | 0 | 1 |
        | cj_l  | FULL JOIN  | cj_e  | 2 | 2 | 0 |
        | cj_le | FULL JOIN  | cj_r  | 1 | 0 | 1 |

  Rule: Semi and anti joins without a condition test whether the right side is empty

    Scenario Outline: a semi or anti join without ON returns left rows only
      When query
        """
        SELECT count(*) AS n FROM <left> <join> <right>
        """
      Then query result
        | n   |
        | <n> |

      Examples:
        | left  | join           | right | n |
        | cj_l  | LEFT SEMI JOIN | cj_r  | 2 |
        | cj_l  | LEFT SEMI JOIN | cj_e  | 0 |
        | cj_l  | LEFT ANTI JOIN | cj_r  | 0 |
        | cj_l  | LEFT ANTI JOIN | cj_e  | 2 |
        | cj_le | LEFT SEMI JOIN | cj_r  | 0 |
        | cj_le | LEFT ANTI JOIN | cj_e  | 0 |

    Scenario: a semi join without ON exposes only the left columns
      When query
        """
        SELECT * FROM cj_l LEFT SEMI JOIN cj_r
        """
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- v: string (nullable = false)
        """

    Scenario: semi and anti joins without ON are allowed when cartesian products are disabled
      Given config spark.sql.crossJoin.enabled = false
      When query
        """
        SELECT
          (SELECT count(*) FROM cj_l LEFT SEMI JOIN cj_r) AS semi,
          (SELECT count(*) FROM cj_l LEFT ANTI JOIN cj_e) AS anti
        """
      Then query result
        | semi | anti |
        | 2    | 2    |

    Scenario: an outer join without ON is rejected when cartesian products are disabled
      Given config spark.sql.crossJoin.enabled = false
      When query
        """
        SELECT * FROM cj_l LEFT JOIN cj_r
        """
      Then query error (?i)cartesian product
