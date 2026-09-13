"""The result type of every function, against a recorded Spark 4.2.0 reference.

Why this exists: the arithmetic guards decide by TYPE, so a function whose result type differs
from Spark's puts the same query in a different cell in each engine. That is invisible to
`arithmetic_operand_rejection.feature`, whose operands are all literals -- both engines always
land in the same cell there -- and it is how `DATE + datediff(...)` came to be refused while
Spark answered it.

Measured over 120 composed cells: a composition diverges ONLY where the inner expression's result
type already diverges. So keeping every result type equal to Spark's is what keeps the derived
operands honest, and this test is the guard for that invariant: it fails when a type starts
differing that did not before, and equally when one stops.

The reference records what BOTH engines answer, so the same test runs against either: on Spark it
proves the reference has not rotted, on Sail it pins the known divergences. Regenerate with

    hatch run python3 -m pysail.tests.spark.function.test_result_type_parity          # Sail
    SPARK_REMOTE=local hatch run python3 -m pysail.tests.spark.function.test_result_type_parity

running it once per engine; each run rewrites only its own column of `function_result_types.py`.
"""

from __future__ import annotations

import pathlib
import re

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.function.function_result_types import RESULT_TYPES

REFERENCE = pathlib.Path(__file__).parent / "function_result_types.py"
CATALOGUE = pathlib.Path(__file__).parents[5] / "crates" / "sail-plan" / "data" / "functions" / "scalar"
BATCH = 40
ERROR = "<error>"
# The longest catalogue example worth running as a single expression.
MAX_QUERY_LENGTH = 160
# What the reference currently records; both must go DOWN, never up.
DIVERGENT_TYPES = 38
SAIL_REFUSES = 83


def catalogue_expressions() -> list[str]:
    """Every executable `> SELECT ...` example the function catalogue carries, deduplicated."""
    found = set()
    for path in sorted(CATALOGUE.glob("*.yaml")):
        for match in re.finditer(r">\s*(SELECT [^;]+);", path.read_text()):
            query = " ".join(match.group(1).split())
            if len(query) < MAX_QUERY_LENGTH and "TABLE" not in query.upper():
                found.add(query[len("SELECT ") :].strip())
    return sorted(found)


def measure(spark, expressions: list[str]) -> dict[str, str]:
    """The result type of each expression, batched so the sweep costs tens of queries, not 910.

    A batch is one query with one `typeof` per column; if any expression in it fails, the batch
    is retried one expression at a time so a single failure does not hide the rest.
    """
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.timeType.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    measured: dict[str, str] = {}
    for start in range(0, len(expressions), BATCH):
        chunk = expressions[start : start + BATCH]
        columns = ", ".join(f"typeof({e}) AS c{i}" for i, e in enumerate(chunk))
        try:
            row = spark.sql(f"SELECT {columns}").collect()[0]
            measured.update(dict(zip(chunk, list(row), strict=True)))
        except Exception:  # noqa: BLE001
            for e in chunk:
                try:
                    measured[e] = spark.sql(f"SELECT typeof({e}) AS c").collect()[0][0]
                except Exception:  # noqa: BLE001
                    measured[e] = ERROR
    return measured


def test_the_result_type_of_every_function_matches_the_reference(spark):
    reference = RESULT_TYPES
    engine = "spark" if is_jvm_spark() else "sail"
    expressions = catalogue_expressions()
    assert expressions, "the function catalogue yielded no examples"

    measured = measure(spark, expressions)
    expected = {e: reference[e][engine] for e in expressions if e in reference}
    changed = {e: (expected[e], measured[e]) for e in expected if measured[e] != expected[e]}
    added = [e for e in expressions if e not in reference]

    # A changed type is not automatically a bug -- it is a change nobody recorded. Fixing a
    # divergence trips this too, which is the point: the reference is regenerated deliberately.
    assert not changed, (
        f"{len(changed)} result types changed on {engine}; regenerate the reference if intended:\n"
        + "\n".join(f"  {e}\n      recorded={r!r} now={n!r}" for e, (r, n) in sorted(changed.items())[:20])
    )
    assert not added, f"{len(added)} catalogue examples are not in the reference; regenerate it:\n" + "\n".join(
        f"  {e}" for e in added[:20]
    )


def test_the_recorded_divergences_are_the_known_ones():
    """Two counts, two meanings, and neither may grow silently.

    A type that differs puts a derived operand in a different arithmetic cell in each engine. A
    function Spark answers and Sail does not is the invariant this project cares about most --
    Spark accepts, Sail refuses -- one function short of a working query.
    """
    reference = RESULT_TYPES
    answered = {e: v for e, v in reference.items() if ERROR not in (v["spark"], v["sail"])}
    different_type = [e for e, v in answered.items() if v["spark"] != v["sail"]]
    sail_refuses = [e for e, v in reference.items() if v["sail"] == ERROR and v["spark"] != ERROR]

    assert len(different_type) == DIVERGENT_TYPES, (
        f"{len(different_type)} functions type their result differently from Spark, not "
        f"{DIVERGENT_TYPES}. "
        "A derived operand of one of them lands in a different arithmetic cell in each engine, "
        "which no cell of `arithmetic_operand_rejection.feature` can see."
    )
    # TODO: this number must go DOWN, never up. Each one is a query Spark answers and Sail does
    #   not; triage lives outside the arithmetic work.
    assert len(sail_refuses) == SAIL_REFUSES, (
        f"{len(sail_refuses)} catalogue examples are answered by Spark and refused by Sail, not "
        f"{SAIL_REFUSES}. If it grew, the change under review took a working query away."
    )


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).parents[4]))
    from pysail.testing.spark.session import spark_connect_server, spark_session_factory
    from pysail.tests.spark.function.reference import write_reference

    engine_key = "spark" if is_jvm_spark() else "sail"
    exprs = catalogue_expressions()
    if is_jvm_spark():
        from pyspark.sql import SparkSession

        session = SparkSession.builder.remote("local").getOrCreate()
        result = measure(session, exprs)
    else:
        with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
            result = measure(sessions.create(), exprs)
    data = dict(RESULT_TYPES)
    for expression, value in result.items():
        data.setdefault(expression, {})[engine_key] = value
    write_reference(REFERENCE, "RESULT_TYPES", data)
    print(f"wrote {len(result)} {engine_key} entries to {REFERENCE}")  # noqa: T201
