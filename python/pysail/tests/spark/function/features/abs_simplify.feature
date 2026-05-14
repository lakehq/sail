@abs_simplify
Feature: abs() — simplify hook

  Rule: simplify rewrite shape
    # Locks the simplify hook's dispatch (Int8/Int16/Int32/Int64/Interval/Duration
    # stay in SparkAbs invoke for ANSI handling; floats/decimals/null delegate
    # to DataFusion's abs). If a future refactor changes either branch, the
    # snapshot diff signals the hook is no longer firing as designed.

    Scenario: EXPLAIN abs INT column keeps spark_abs (ANSI path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-5 AS INT)), (CAST(0 AS INT)), (CAST(5 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs BIGINT column keeps spark_abs (ANSI path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-5 AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(5 AS BIGINT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs DOUBLE column delegates to DataFusion abs
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-1.5 AS DOUBLE)), (CAST(0.0 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs DECIMAL column delegates to DataFusion abs
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-1.50 AS DECIMAL(5,2))), (CAST(1.50 AS DECIMAL(5,2))) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs INTERVAL column keeps spark_abs (kernel path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (INTERVAL '-5' DAY), (INTERVAL '5' DAY) AS t(v)
        """
      Then query plan matches snapshot

  @sail-only
  Rule: cross-nesting with other UDFs
    # Verifies abs simplify/output_ordering composes correctly with other
    # planner hooks already on main (e.g. ceil/floor).

    Scenario: EXPLAIN abs of ceil keeps spark_abs delegation chain consistent
      When query
        """
        EXPLAIN SELECT abs(ceil(v)) FROM VALUES (CAST(-1.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN ceil of abs keeps abs in invoke for integer path
      When query
        """
        EXPLAIN SELECT ceil(abs(v)) FROM VALUES (CAST(-5 AS INT)), (CAST(5 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

  @sail-only
  Rule: constant folding (DataFusion EvaluateScalarsAsConst)
    # Locks DataFusion's general optimizer behavior on constant inputs.
    # These folds happen via DataFusion's EvaluateScalarsAsConst rule
    # (NOT via SparkAbs::simplify) — our hook returns Original for
    # integers but DF then evaluates the constant via invoke_with_args
    # at planning time.

    Scenario: EXPLAIN abs of NULL folds to NULL literal
      When query
        """
        EXPLAIN SELECT abs(NULL) AS result
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs of negative INT literal folds to positive
      When query
        """
        EXPLAIN SELECT abs(CAST(-5 AS INT)) AS result
        """
      Then query plan matches snapshot

  @sail-only
  Rule: idempotence simplify (abs(abs(x)) = abs(x))
    # SparkAbs::simplify detects nested abs calls and collapses them.
    # Uses downcast_ref::<Self>() + ansi_mode check so only same-mode
    # abs chains collapse (e.g. ANSI abs(ANSI abs(x)) = ANSI abs(x)).
    # DataFusion applies simplify bottom-up to a fixed point, so
    # abs(abs(abs(x))) collapses in two passes without special-casing.

    Scenario: EXPLAIN abs(abs(int_col)) collapses to single spark_abs
      When query
        """
        EXPLAIN SELECT abs(abs(v)) FROM VALUES (CAST(-3 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN triple-nested abs collapses to single spark_abs
      When query
        """
        EXPLAIN SELECT abs(abs(abs(v))) FROM VALUES (CAST(-3 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs(abs(double_col)) does NOT collapse (DataFusion abs lacks idempotence)
      # Bottom-up simplification: inner SparkAbs(double) becomes df_abs first.
      # Outer SparkAbs sees df_abs as args[0] — name mismatch ("spark_abs" != "abs"),
      # so idempotence doesn't fire. Then outer type-dispatches Double → df_abs again.
      # Net result: df_abs(df_abs(double)). Would require upstream contribution to
      # DataFusion's abs to implement its own idempotence.
      When query
        """
        EXPLAIN SELECT abs(abs(v)) FROM VALUES (CAST(-1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: abs(abs(x)) returns same as abs(x) (correctness — both ANSI modes)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(abs(v)) AS result
        FROM VALUES (-5), (0), (5), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result
        | result |
        | 5      |
        | 0      |
        | 5      |
        | NULL   |

    Scenario: abs(abs(INT_MIN)) under ANSI=true errors at inner abs
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(abs(CAST(-2147483648 AS INT))) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*
