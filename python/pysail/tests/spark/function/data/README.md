# Scalar-subquery regression corpus

`scalar_subquery.jsonl` preserves the 4,477 recorded cases from the scalar-subquery
review as 3,572 distinct SQL/configuration cases. Each record retains all original
case IDs in `source_ids`; repeated SQL under the same effective settings executes
once per engine mode. Both Sail local and local-cluster modes run automatically.

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
- An `expect` containing `skip` has no sufficiently established oracle. Its
  reason is explicit, and it is not counted as a passing or expected-failure test.
- Schema nullability has separate existing scalar-subquery feature tests. A known
  schema mismatch must not hide a new wrong-row result in this corpus.

One Spark-only resource skip covers the 40 distinct correlated COUNT projections:
Spark's default test-driver heap can be exhausted by their joins. The complete
result was verified separately in a fresh Spark JVM with a 4 GiB heap; both Sail
modes still execute the query in this corpus.

Run the corpus with the ordinary pytest environment:

```sh
pytest python/pysail/tests/spark/function/test_scalar_subquery_corpus.py
SPARK_REMOTE=local pytest python/pysail/tests/spark/function/test_scalar_subquery_corpus.py
```

No snapshot regeneration is involved. The source corpus SHA256 is
`d57bc9c78149f5bce62d5089e5e8449d7a57f81a7559e54f03f45fa1e0fa1874`.
The scalar NULL/multiple-row and COUNT rules were checked against Spark's
`expressions/subquery.scala`, `execution/subquery.scala`, and
`expressions/aggregate/Count.scala`; per-case notes record the remaining intentional
optimizer differences and unresolved expectations.
