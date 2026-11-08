Feature: max_by and min_by window functions

  Rule: Ties

    Scenario: max_by and min_by keep the newer row on ties in a growing frame
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id) AS mx, min_by(s, k) OVER (ORDER BY id) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | a  | a  |
        | 2  | b  | b  |
        | 3  | b  | b  |
        | 4  | d  | b  |
        | 5  | e  | b  |
        | 6  | e  | f  |
        | 7  | g  | f  |
        | 8  | g  | f  |

    Scenario: max_by and min_by keep the newer row on ties in a growing ROWS frame
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | a  | a  |
        | 2  | b  | b  |
        | 3  | b  | b  |
        | 4  | d  | b  |
        | 5  | e  | b  |
        | 6  | e  | f  |
        | 7  | g  | f  |
        | 8  | g  | f  |

    Scenario: max_by and min_by keep the newer row on ties over the whole partition
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | g  | f  |
        | 2  | g  | f  |
        | 3  | g  | f  |
        | 4  | g  | f  |
        | 5  | g  | f  |
        | 6  | g  | f  |
        | 7  | g  | f  |
        | 8  | g  | f  |

    Scenario: max_by and min_by keep the newer row on ties in ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 0) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | b  | b  |
        | 2  | b  | c  |
        | 3  | b  | c  |

    # TODO: Spark orders -0.0 and 0.0 as equal, so the newer row wins the tie, but Sail orders -0.0
    #   first. See the NaN and zero ordering follow-up (tmp/nan-central-rule-20261107).
    @sail-bug
    Scenario: max_by and min_by treat -0.0 and 0.0 orderings as a tie
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, CAST('-0.0' AS DOUBLE)), (2, CAST('0.0' AS DOUBLE)), (3, CAST('NaN' AS DOUBLE)), (4, CAST('NaN' AS DOUBLE)) AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 1  |
        | 2  | 2  | 2  |
        | 3  | 3  | 2  |
        | 4  | 4  | 2  |

  Rule: Sliding frames

    Scenario: max_by and min_by over ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | a  | a  |
        | 2  | b  | b  |
        | 3  | b  | b  |
        | 4  | d  | b  |
        | 5  | e  | e  |
        | 6  | e  | f  |
        | 7  | g  | f  |
        | 8  | g  | f  |

    Scenario: max_by and min_by over ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx   | mn   |
        | 1  | b    | b    |
        | 2  | d    | d    |
        | 3  | e    | e    |
        | 4  | e    | f    |
        | 5  | g    | f    |
        | 6  | g    | g    |
        | 7  | NULL | NULL |
        | 8  | NULL | NULL |

    Scenario: max_by and min_by over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx   | mn   |
        | 1  | g    | f    |
        | 2  | g    | f    |
        | 3  | g    | f    |
        | 4  | g    | f    |
        | 5  | g    | f    |
        | 6  | g    | f    |
        | 7  | g    | g    |
        | 8  | NULL | NULL |

    Scenario: max_by and min_by over ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx   | mn   |
        | 1  | NULL | NULL |
        | 2  | a    | a    |
        | 3  | b    | b    |
        | 4  | b    | b    |
        | 5  | d    | b    |
        | 6  | e    | e    |
        | 7  | e    | f    |
        | 8  | g    | f    |

    Scenario: max_by and min_by over RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx, min_by(s, k) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | b  | b  |
        | 2  | b  | b  |
        | 3  | d  | b  |
        | 4  | e  | e  |
        | 5  | e  | f  |
        | 6  | g  | f  |
        | 7  | g  | f  |
        | 8  | g  | g  |

    Scenario: max_by and min_by over a partitioned sliding frame
      When query
        """
        SELECT id, max_by(s, k) OVER (PARTITION BY id % 2 ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(s, k) OVER (PARTITION BY id % 2 ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', NULL), (4, 'd', 2), (5, 'e', 2), (6, 'f', 0), (7, 'g', 2), (8, 'h', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | a  | a  |
        | 2  | b  | b  |
        | 3  | a  | a  |
        | 4  | d  | b  |
        | 5  | e  | e  |
        | 6  | d  | f  |
        | 7  | g  | g  |
        | 8  | f  | f  |

    Scenario: max_by and min_by over a sliding frame with a NULL value
      When query
        """
        SELECT id, max_by(s, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(s, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, 'a', 1), (2, CAST(NULL AS STRING), 5), (3, 'c', 0), (4, 'd', NULL) AS t(id, s, k) ORDER BY id
        """
      Then query result ordered
        | id | mx   | mn |
        | 1  | a    | a  |
        | 2  | NULL | a  |
        | 3  | NULL | c  |
        | 4  | c    | c  |

    Scenario: max_by and min_by over a sliding frame with array values and double orderings
      When query
        """
        SELECT id, max_by(v, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(v, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, array(1, 2), CAST(1.5 AS DOUBLE)), (2, array(3), CAST('NaN' AS DOUBLE)), (3, CAST(NULL AS ARRAY<INT>), CAST(-1 AS DOUBLE)) AS t(id, v, k) ORDER BY id
        """
      Then query result ordered
        | id | mx     | mn     |
        | 1  | [1, 2] | [1, 2] |
        | 2  | [3]    | [1, 2] |
        | 3  | [3]    | NULL   |

    Scenario: max_by and min_by over a sliding frame with tied string orderings
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, 'b'), (2, 'a'), (3, 'a'), (4, 'c') AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 1  |
        | 2  | 1  | 2  |
        | 3  | 3  | 3  |
        | 4  | 4  | 3  |

    Scenario: max_by and min_by over a sliding frame with NULL struct fields
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, named_struct('a', 1)), (2, named_struct('a', CAST(NULL AS INT))), (3, named_struct('a', CAST(NULL AS INT))), (4, CAST(NULL AS STRUCT<a: INT>)), (5, named_struct('a', 0)) AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 1  |
        | 2  | 1  | 2  |
        | 3  | 3  | 3  |
        | 4  | 3  | 3  |
        | 5  | 5  | 5  |

    Scenario: max_by and min_by over a sliding frame with NULL array elements
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn FROM VALUES (1, array(1)), (2, array(CAST(NULL AS INT))), (3, array(1, CAST(NULL AS INT))), (4, CAST(NULL AS ARRAY<INT>)), (5, array(0)) AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 2  |
        | 2  | 3  | 2  |
        | 3  | 3  | 2  |
        | 4  | 3  | 5  |
        | 5  | 5  | 5  |

    Scenario: max_by and min_by over a sliding frame with NULL elements in pairs
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mn FROM VALUES (1, array(1, 1)), (2, array(CAST(NULL AS INT), 5)), (3, array(1, CAST(NULL AS INT))), (4, CAST(NULL AS ARRAY<INT>)), (5, array(0, 0)) AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 1  |
        | 2  | 1  | 2  |
        | 3  | 3  | 2  |
        | 4  | 3  | 3  |
        | 5  | 5  | 5  |

    Scenario: max_by and min_by over a sliding frame with NULL fields of structs in arrays
      When query
        """
        SELECT id, max_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx, min_by(id, k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn FROM VALUES (1, array(named_struct('a', 1))), (2, array(named_struct('a', CAST(NULL AS INT)))), (3, array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT)))), (4, CAST(NULL AS ARRAY<STRUCT<a: INT>>)), (5, array(named_struct('a', 0))) AS t(id, k) ORDER BY id
        """
      Then query result ordered
        | id | mx | mn |
        | 1  | 1  | 2  |
        | 2  | 3  | 2  |
        | 3  | 3  | 2  |
        | 4  | 3  | 5  |
        | 5  | 5  | 5  |

    Scenario: max_by over a sliding frame with orderings of other types
      When query
        """
        SELECT id, max_by(id, ks) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ks, max_by(id, kd) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kd, max_by(id, kdt) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kdt, max_by(id, kb) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kb, max_by(id, kbo) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kbo, max_by(id, kts) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kts FROM VALUES (1, 'b', CAST(2.5 AS DECIMAL(10,2)), DATE'2020-01-02', X'02', true, TIMESTAMP'2020-01-02 00:00:00'), (2, 'a', CAST(3.5 AS DECIMAL(10,2)), DATE'2020-01-02', X'03', false, TIMESTAMP'2020-01-03 00:00:00'), (3, 'c', CAST(NULL AS DECIMAL(10,2)), DATE'2020-01-01', X'01', true, CAST(NULL AS TIMESTAMP)), (4, 'c', CAST(3.5 AS DECIMAL(10,2)), DATE'2020-01-03', CAST(NULL AS BINARY), true, TIMESTAMP'2020-01-04 00:00:00') AS t(id, ks, kd, kdt, kb, kbo, kts) ORDER BY id
        """
      Then query result ordered
        | id | ks | kd | kdt | kb | kbo | kts |
        | 1  | 1  | 1  | 1   | 1  | 1   | 1   |
        | 2  | 1  | 2  | 2   | 2  | 1   | 2   |
        | 3  | 3  | 2  | 2   | 2  | 3   | 2   |
        | 4  | 4  | 4  | 4   | 2  | 4   | 4   |

    Scenario: min_by over a sliding frame with orderings of other types
      When query
        """
        SELECT id, min_by(id, ks) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ks, min_by(id, kd) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kd, min_by(id, kdt) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kdt, min_by(id, kb) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kb, min_by(id, kbo) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kbo, min_by(id, kts) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kts FROM VALUES (1, 'b', CAST(2.5 AS DECIMAL(10,2)), DATE'2020-01-02', X'02', true, TIMESTAMP'2020-01-02 00:00:00'), (2, 'a', CAST(3.5 AS DECIMAL(10,2)), DATE'2020-01-02', X'03', false, TIMESTAMP'2020-01-03 00:00:00'), (3, 'c', CAST(NULL AS DECIMAL(10,2)), DATE'2020-01-01', X'01', true, CAST(NULL AS TIMESTAMP)), (4, 'c', CAST(3.5 AS DECIMAL(10,2)), DATE'2020-01-03', CAST(NULL AS BINARY), true, TIMESTAMP'2020-01-04 00:00:00') AS t(id, ks, kd, kdt, kb, kbo, kts) ORDER BY id
        """
      Then query result ordered
        | id | ks | kd | kdt | kb | kbo | kts |
        | 1  | 1  | 1  | 1   | 1  | 1   | 1   |
        | 2  | 2  | 1  | 2   | 1  | 2   | 1   |
        | 3  | 2  | 1  | 3   | 3  | 2   | 1   |
        | 4  | 2  | 4  | 3   | 3  | 2   | 2   |
