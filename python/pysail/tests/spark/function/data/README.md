# Scalar-subquery regression corpus

`scalar_subquery.jsonl` preserves the 4,477 original review observations as 3,572
distinct SQL/configuration cases. The follow-up adds 713 distinct controls for
alias/CTE propagation, aggregate defaults, row bounds, shared evaluation, nested
schemas, and the regressions found while comparing candidate implementations.
Each record retains its source IDs. The expanded total is 4,285 cases per mode:
4,279 SQL/settings cases and six executable DataFrame API cases;
the original 3,572-query comparison remains a separate historical denominator.
Both Sail local and local-cluster modes run automatically.

The oracle is Spark 4.2.0, with ANSI mode enabled and UTC unless the record specifies
otherwise. The corpus is version-gated to that release. Results were recollected
from real Spark; a Sail candidate's output is never used as its expected answer.
Known Spark optimizer errors have separate `spark` expectations and a `reason`
explaining the independently derived relational result. Nondeterministic cases use
explicit invariants or permitted result sets instead of comparing sampled numbers.
Statistical assertions retain their false-failure bounds in the corresponding record.

- `expect` is the semantic expectation: collected rows, a specific error pattern,
  or a named invariant. Values are collected strings, with JSON `null` reserved for
  SQL NULL. Row multiplicities are preserved; only outer ORDER BY results are
  compared in order. LTZ timestamp values are normalized to UTC. Year-month
  interval outputs are formatted as STRING after the original query, because
  PySpark cannot collect that interval type directly.
- `spark` supplies an explicit JVM Spark expectation where its behavior differs.
- `sail_bug` marks observed failures separately by execution mode. These tests still
  execute; fixes appear as XPASS. The corpus uses non-strict xfails so independently
  applied fixes do not make this test-only patch incompatible. Remove those marks
  when integrating the fixes so later regressions fail normally.
  A known execution failure uses its exact `sail_execution_error` pattern instead
  of marking the whole test: new errors and newly returned wrong rows fail.
- Newly confirmed pre-PR regressions are strict failures, with their affected modes
  recorded in `pre_pr_regression`; they do not receive a value xfail.
  A candidate also cannot receive an xfail for breaking a case that the reviewed
  PR head already handles correctly, even if that case failed before the PR.
  Newly enabled execution that returns incorrect data is strict too; a former
  planning rejection does not justify a wrong answer.
- An `expect` containing `skip` has no sufficiently established oracle. Its
  reason is explicit, and it is not counted as a passing or expected-failure test.
- `schema` adds a separate public-schema test. The query's values and required
  errors are still tested independently. A `sail_schema_bug` exception applies
  only after validating that actual NULLs are allowed by the public and execution
  schemas, including nested array, struct, and map fields. Execution schemas may
  truthfully be narrower than Spark's public schema. A known unsupported query can
  bypass this check only when its saved `sail_execution_error` diagnostic matches.
  Public-schema exceptions retain only wrong leaf/value pairs already present
  before the PR and match the exact recorded schema; this allows partial repairs
  without hiding a new mismatch. Separately proven inherited unsafe nullability
  uses exact layer/path entries in `sail_nullability_bug`. All new schema and NULL
  safety assertions run before an inherited failure can become XFAIL.
- `setup`, when present, contains statements needed to construct a saved-view
  regression before executing the recorded query.
- `api` replaces `sql` for a typed DataFrame construction in the test helper.
  The four metadata-alias controls use `DataFrame.scalar()`, an outer reference,
  and COALESCE/CASE consumers. Two more controls cover a correlated COUNT over
  one empty grouping set, with and without alias metadata. They preserve the
  actual API recipe because SQL does not express arbitrary alias metadata.
  Results and schemas were verified through the same recipes in real Spark.
  `sail_field_metadata` separately requires Sail to preserve its existing field
  metadata in the public, returned protocol, and Arrow schemas; Spark drops that
  metadata at its scalar boundary. These checks run before any inherited schema
  exception can mark the test as an expected failure.

The three empty-input ROLLUP/CUBE/mixed-grouping-set scalar controls explicitly
expect `0` in Sail and `NULL` in Spark. The grouped branch contributes no rows, but
the empty grand-total grouping contributes `COUNT=0`; the equivalent `UNION ALL`
and isolated empty grouping set both return `0` in real Spark. This exception is
specific to those measured queries, not a blanket grouping-set exemption.

One Spark-only resource skip covers the 40 distinct correlated COUNT projections:
Spark's default test-driver heap can be exhausted by their joins. The complete
result was verified separately in a fresh Spark JVM with a 4 GiB heap; both Sail
modes still execute the query in this corpus.

Run the corpus with the ordinary pytest environment:

```sh
pytest python/pysail/tests/spark/function/test_scalar_subquery_corpus.py
SPARK_REMOTE=local pytest python/pysail/tests/spark/function/test_scalar_subquery_corpus.py
```

No snapshot regeneration is involved. The original historical capture SHA256 is
`d57bc9c78149f5bce62d5089e5e8449d7a57f81a7559e54f03f45fa1e0fa1874`;
the committed 3,572-case executable corpus before this expansion has SHA256
`1899395ca3244478f5481d6dcbb8b9f9ecc36746693acc43a7f4c33af1265417`.
The scalar NULL/multiple-row and COUNT rules were checked against Spark's
`expressions/subquery.scala`, `execution/subquery.scala`, and
`expressions/aggregate/Count.scala`; per-case notes record the remaining intentional
optimizer differences and unresolved expectations.

The three finite `SUM(CAST(global_count IS NULL AS INT))` controls use the same
approved non-null proof as their projection/filter counterparts: each Boolean is
false, so their two-row sum is zero. Their separate Spark expectations retain the
observed runtime CAST error. This extends the existing policy to another consumer;
it does not label Spark's error as an optimizer bug.

Two direct null-check controls include an additional scalar predicate inside the
global COUNT. That predicate does not change the one-row, non-null COUNT proof;
they use the same approved false result while retaining Spark's CAST error as the
separate Spark expectation. Five empty-outer controls reject the eager scalar
cardinality errors found in an alternative candidate; those cases remain strict.

The global-aggregate default controls keep both the plain scalar and explicit
COALESCE/CASE repairs. A non-null semantic result must not let simplification
remove a repair and expose a missing-row NULL from decorrelation. These include
a direct null-check regression on the reviewed head and five rejected-candidate
value regressions; no value xfail may hide a reintroduced failure.

Two volatile aggregate-default controls assert non-null results and evaluation
sharing separately. Matched TRUE guards share one RAND/UUID result; missing
FALSE/NULL keys evaluate non-null defaults per outer row. The test does not pin
random samples or seeded streams. This behavior was checked against Spark's
`optimizer/DecorrelateInnerQuery.scala` and `optimizer/subquery.scala`.

Nine selected volatile-value controls require the actual RAND/UUID/IF result,
so a non-null literal fallback is not misclassified as correct data. Three
repeated-key controls independently require one selected value per correlation
key; making all results non-null by reevaluating the expression per outer row
still fails these tests. Public-schema tests also reject NULL beneath an output
declared non-nullable, even when that query has an older value mismatch.

The one-empty-grouping controls distinguish COUNT from raw scalar output, and
include HAVING, limits, aliases, nullable multiargument COUNT, and empty inputs.
Paired ARRAY/STRUCT/MAP repair controls ensure that declining a nested type rewrite
cannot remove a working COALESCE or CASE repair.

Nested scalar outputs remain nullable even when their parent contains exactly one
empty grouping set. Paired ordinary-aggregate controls distinguish inherited
limitations from wrong newly enabled results, and explicit outer repairs must
remain usable when a proposed grouping rewrite declines the producer.

Four volatile integer-alias controls have an intermittent pre-PR error claiming
that a non-null field contains NULL. Only that independently recorded execution
diagnostic is marked as inherited. A successful execution must still satisfy its
filter/value invariant, so newly returned wrong rows remain strict failures.

Constant HAVING predicates are tested in both true and row-removing forms. A
nonliteral true predicate must not turn a previously working scalar COUNT into
a planning error, while false and NULL predicates must still remove the row.

Volatile HAVING predicates also retain their evaluation sharing: repeated outer
rows with the same correlation key must not receive independently sampled
presence decisions. RAND and UUID predicates pin this invariant for both matched
and missing keys. Always-true and always-false volatile predicates are separate
controls; the inherited missing-key default bug is not mistaken for correct data.
