@element_at
Feature: element_at / try_element_at over maps and arrays

  # Sail plans `element_at` in `sail-plan/src/function/scalar/collection.rs`.
  # The Map branch lowers to `array_element(map_extract(collection, key), 1)`;
  # the Array branch builds a CASE that raises on index 0 and on out-of-bounds.
  #
  # Out-of-bounds access raises only under ANSI mode; the Array branch reads
  # `plan_config.ansi_mode` so that ANSI=false yields NULL as Spark does. Index 0
  # stays invalid in either mode, and `try_element_at` tolerates out-of-bounds in
  # both.
  #
  # The gaps still tagged `@sail-bug` below share one root: neither branch
  # replicates the validations Spark performs in its ANALYZER (key type, index
  # type, duplicate keys), so DataFusion's coercion converts and returns a VALUE
  # where Spark aborts the query. The empty `map()` literal is separate — it is
  # typed `Map<Null, Null>` and fails to match `map_extract`'s signature, even
  # though an empty map arriving as a column value works fine.

  Rule: Map lookup — hit, miss and NULL

    Scenario: keys resolve per row, missing and NULL keys give NULL
      When query
        """
        SELECT element_at(map('a', '1', 'b', '2', 'c', '3'), c) AS result
        FROM VALUES ('a'), ('b'), ('c'), ('zz'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | 3      |
        | NULL   |
        | NULL   |

    Scenario: a NULL map gives NULL
      When query
        """
        SELECT element_at(CAST(NULL AS MAP<STRING, STRING>), 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a key present with a NULL value is indistinguishable from a miss
      When query
        """
        SELECT element_at(map('a', CAST(NULL AS STRING)), 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: the map may come from a column
      When query
        """
        SELECT element_at(m, 'a') AS result
        FROM VALUES (map('a', '1')), (map('b', '2')), (CAST(NULL AS MAP<STRING, STRING>)) AS t(m)
        """
      Then query result ordered
        | result |
        | 1      |
        | NULL   |
        | NULL   |

  Rule: Map lookup — the empty map literal

    @sail-bug
    Scenario: an empty map returns NULL rather than failing to plan
      When query
        """
        SELECT element_at(map(), 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: try_element_at over an empty map returns NULL
      When query
        """
        SELECT try_element_at(map(), 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Map lookup — the key type must match the map key type

    @sail-bug
    Scenario: an INT key against a STRING-keyed map is rejected
      When query
        """
        SELECT element_at(map('1', 'uno'), 1) AS result
        """
      Then query error should have been "MAP" followed by a value with same key type

    @sail-bug
    Scenario: a DECIMAL key against an INT-keyed map is rejected
      When query
        """
        SELECT element_at(map(1, 'uno'), 1.0) AS result
        """
      Then query error should have been "MAP" followed by a value with same key type

    Scenario: a matching INT key resolves
      When query
        """
        SELECT element_at(map(1, 'uno', 2, 'dos'), 2) AS result
        """
      Then query result
        | result |
        | dos    |

  Rule: Map lookup — duplicate keys in the map literal

    @sail-bug
    Scenario: a duplicated map key aborts the query
      When query
        """
        SELECT element_at(map('a', '1', 'a', '2'), 'a') AS result
        """
      Then query error Duplicate map key

  Rule: Array indexing — 1-based, negative counts from the end

    Scenario: positive and negative indexes resolve per row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), c) AS result
        FROM VALUES (1), (3), (-1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result ordered
        | result |
        | 10     |
        | 30     |
        | 30     |
        | NULL   |

    Scenario: a NULL element is returned as NULL
      When query
        """
        SELECT element_at(array(1, NULL, 3), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: index 0 is always invalid, in every ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), 0) AS result
        """
      Then query error index 0 is invalid

  Rule: Array indexing — out of bounds depends on ANSI mode

    Scenario: out of bounds raises under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT element_at(array(10, 20, 30), 4) AS result
        """
      Then query error out of bounds

    Scenario: out of bounds returns NULL under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), 4) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a negative index out of bounds returns NULL under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), -4) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: indexing an empty array returns NULL under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Array indexing — out of bounds is decided per row, not per batch

    Scenario: distinct valid indexes resolve independently, with no broadcast
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), c) AS result
        FROM VALUES (1), (2), (3), (-1), (-2) AS t(c)
        """
      Then query result ordered
        | result |
        | 10     |
        | 20     |
        | 30     |
        | 30     |
        | 20     |

    Scenario: an out-of-bounds row gives NULL without failing the other rows
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(array(10, 20, 30), c) AS result
        FROM VALUES (1), (4), (-1), (-4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result ordered
        | result |
        | 10     |
        | NULL   |
        | 30     |
        | NULL   |
        | NULL   |

    Scenario: arrays of differing length give NULL where the index does not fit
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(a, 2) AS result
        FROM VALUES (array(1, 2, 3)), (array(9)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(a)
        """
      Then query result ordered
        | result |
        | 2      |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: try_element_at tolerates arrays of differing length
      When query
        """
        SELECT try_element_at(a, 2) AS result
        FROM VALUES (array(1, 2, 3)), (array(9)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(a)
        """
      Then query result ordered
        | result |
        | 2      |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Map lookup — an empty map is fine as a column value

    Scenario: maps of differing size, including an empty one
      When query
        """
        SELECT element_at(m, 'a') AS result
        FROM VALUES (map('a', '1')), (map('a', '2', 'b', '3')), (map('b', '4')), (map()) AS t(m)
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | NULL   |
        | NULL   |

    Scenario: a map built from columns rather than per-row literals
      When query
        """
        SELECT element_at(map_from_arrays(ks, vs), k) AS result
        FROM VALUES (array('a', 'b'), array('1', '2'), 'a'),
                    (array('x'), array('9'), 'x'),
                    (array('p', 'q'), array('7', '8'), 'zz'),
                    (array(), array(), 'a')
        AS t(ks, vs, k)
        """
      Then query result ordered
        | result |
        | 1      |
        | 9      |
        | NULL   |
        | NULL   |

    @sail-bug
    Scenario: duplicate keys arriving from the data abort the query
      When query
        """
        SELECT element_at(str_to_map(s, ',', ':'), 'a') AS result
        FROM VALUES ('a:1,b:2'), ('b:3'), ('a:9,a:8') AS t(s)
        """
      Then query error Duplicate map key

    Scenario: both the map and the key may come from columns
      When query
        """
        SELECT element_at(m, k) AS result
        FROM VALUES (map('a', '1'), 'a'), (map('b', '2'), 'b'), (map('c', '3'), 'zz') AS t(m, k)
        """
      Then query result ordered
        | result |
        | 1      |
        | 2      |
        | NULL   |

  Rule: Array indexing — the index must be INT

    @sail-bug
    Scenario: a BIGINT index is rejected
      When query
        """
        SELECT element_at(array(10, 20), CAST(2 AS BIGINT)) AS result
        """
      Then query error second parameter requires the "INT" type

    @sail-bug
    Scenario: a BIGINT index from a column is rejected too
      When query
        """
        SELECT element_at(array(10, 20, 30), c) AS result
        FROM VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT)) AS t(c)
        """
      Then query error second parameter requires the "INT" type

    @sail-bug
    Scenario: a mismatched key type from a column is rejected too
      When query
        """
        SELECT element_at(map('1', 'uno', '2', 'dos'), c) AS result
        FROM VALUES (1), (2) AS t(c)
        """
      Then query error should have been "MAP" followed by a value with same key type

  Rule: try_element_at tolerates what element_at rejects

    Scenario: out of bounds gives NULL even under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_element_at(array(1, 2), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a negative index out of bounds gives NULL
      When query
        """
        SELECT try_element_at(array(1, 2), -5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: index 0 still raises — try_ does not tolerate it
      When query
        """
        SELECT try_element_at(array(1, 2), 0) AS result
        """
      Then query error index 0 is invalid
