@ceil_floor_preimage
Feature: ceil() / floor() — preimage hook (filter pushdown)

  # SparkFloor implements `fn preimage`: `floor(x) = N` ⟺ `x ∈ [N, N+1)`.
  # This half-open interval matches PreimageResult::Range, so DataFusion's
  # logical simplifier rewrites `WHERE floor(col) = N` into a pure column
  # predicate — unlocking Parquet row-group pruning via min/max stats.
  #
  # SparkCeil does NOT implement preimage: `ceil(x) = N` ⟺ `x ∈ (N-1, N]`
  # is right-closed / left-open and cannot be expressed as [lower, upper).
  # Any rewrite would be unsound. The ceil snapshots below capture the
  # *no-pushdown* state as regression fixtures.

  Rule: Filter pushdown — WHERE floor/ceil(col) OP constant (row results)

    Scenario: WHERE ceil(col) > N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) > 2 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 2.1 |
        | 5.5 |

    Scenario: WHERE floor(col) <= N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) <= 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 0.5 |
        | 1.1 |
        | 1.9 |

    Scenario: WHERE floor(col) = N keeps the right rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.0 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE)),
          (CAST(-0.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) = 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 1.0 |
        | 1.9 |

    Scenario: WHERE floor(col) returns NULL excludes NULL rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST(2.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT count(*) AS c FROM vals WHERE floor(v) IS NOT NULL
        """
      Then query result
        | c |
        | 2 |

    Scenario: WHERE floor(col) != N keeps rows where floor is not N
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.0 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) != 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 0.5 |
        | 2.0 |
        | 2.5 |

  Rule: Adversarial — preimage edge cases

    Scenario: floor with non-integer RHS — filter matches nothing
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 2.5
        """
      Then query result
        | v |

    Scenario: floor equals BIGINT_MAX boundary — no rows match in sample
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 9223372036854775807
        """
      Then query result
        | v |

  Rule: Plan snapshot — filter pushdown on in-memory VALUES

    @sail-only
    Scenario: EXPLAIN WHERE ceil(col) > N
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE ceil(v) > 2
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE floor(col) <= N
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) <= 1
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE floor(col) = N — preimage rewrites to range
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 1
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN floor with non-integer RHS — preimage returns None, no rewrite
      When query
        """
        EXPLAIN SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 2.5
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN floor != N — rewrites to disjunction v < N OR v >= N+1
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) != 1
        """
      Then query plan matches snapshot

  Rule: Plan snapshot — filter pushdown on Parquet (preimage)

    # Baseline: a pure literal predicate that PruningPredicate CAN reason about.
    @sail-only
    Scenario: EXPLAIN SELECT from Parquet with literal filter — baseline for compare
      Given variable location for temporary directory explain_literal_filter
      Given final statement
        """
        DROP TABLE IF EXISTS explain_literal_filter_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_literal_filter_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(10.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN ANALYZE SELECT v FROM explain_literal_filter_parquet WHERE v > 2.0
        """
      Then query plan matches snapshot

    # ceil: no preimage → UDF predicate stays per-row, no pruning_predicate.
    @sail-only
    Scenario: EXPLAIN SELECT from Parquet with ceil filter shows pushdown
      Given variable location for temporary directory explain_ceil_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_ceil_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_ceil_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_ceil_parquet WHERE ceil(v) > 2
        """
      Then query plan matches snapshot

    # floor: has preimage → `floor(v) <= 1` rewrites to `v < 2` → pruning_predicate populated.
    @sail-only
    Scenario: EXPLAIN SELECT from Parquet with floor filter shows pushdown
      Given variable location for temporary directory explain_floor_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_floor_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_floor_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_floor_parquet WHERE floor(v) <= 1
        """
      Then query plan matches snapshot

    # floor equality: `floor(v) = 1` rewrites to `v >= 1 AND v < 2` → pruning_predicate populated.
    @sail-only
    Scenario: EXPLAIN floor filter on Parquet with equality shows preimage pushdown
      Given variable location for temporary directory explain_floor_eq_pushdown
      Given final statement
        """
        DROP TABLE IF EXISTS explain_floor_eq_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_floor_eq_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.0 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_floor_eq_parquet WHERE floor(v) = 1
        """
      Then query plan matches snapshot

    # Non-integer RHS: preimage returns None → no rewrite, UDF stays per-row.
    @sail-only
    Scenario: EXPLAIN floor filter on Parquet with non-integer RHS does not rewrite
      Given variable location for temporary directory explain_floor_nonint
      Given final statement
        """
        DROP TABLE IF EXISTS explain_floor_nonint_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_floor_nonint_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_floor_nonint_parquet WHERE floor(v) = 2.5
        """
      Then query plan matches snapshot

    # floor !=: DataFusion derives complement of Range [N, N+1) → `v < N OR v >= N+1`
    # → pruning_predicate populated with both bounds.
    @sail-only
    Scenario: EXPLAIN floor filter on Parquet with != rewrites to disjunction
      Given variable location for temporary directory explain_floor_neq
      Given final statement
        """
        DROP TABLE IF EXISTS explain_floor_neq_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_floor_neq_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN SELECT v FROM explain_floor_neq_parquet WHERE floor(v) != 1
        """
      Then query plan matches snapshot

    # Mixed literal + UDF: DF derives pruning only from the literal part.
    @sail-only
    Scenario: EXPLAIN combined literal + UDF filter — DF predicate splitting visible
      Given variable location for temporary directory explain_combined_filter
      Given final statement
        """
        DROP TABLE IF EXISTS explain_combined_filter_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_combined_filter_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(10.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        EXPLAIN ANALYZE SELECT v FROM explain_combined_filter_parquet WHERE v > 2.0 AND ceil(v) > 2
        """
      Then query plan matches snapshot

  Rule: Simplify + preimage interaction — ceil(ceil(x)) chains correctly

    Scenario: ceil(ceil(x)) collapses to ceil(x) then preimage applies
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v) WHERE ceil(ceil(v)) <= 2
        """
      Then query plan matches snapshot

    Scenario: floor(floor(x)) collapses to floor(x) then preimage applies
      When query
        """
        EXPLAIN SELECT v FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v) WHERE floor(floor(v)) <= 1
        """
      Then query plan matches snapshot
