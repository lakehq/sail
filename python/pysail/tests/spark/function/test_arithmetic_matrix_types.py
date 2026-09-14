"""The RESULT TYPE of every arithmetic cell both engines answer, against a recorded Spark 4.2.0 run.

Why this exists: `arithmetic_operand_rejection.feature` and `arithmetic_operand_resolution.feature`
already pin the VERDICT of all 5760 cells -- who refuses and who answers -- and that is what caught
a 18-cell regression while this PR was being written. But they are blind to the cell where BOTH
engines answer and the TYPES differ: `INTERVAL '1' DAY / 2` came back as a DOUBLE for a long time
and no test could see it, because Sail did answer.

So this is the other half of the photo: 1182 cells, one recorded type per engine. It fails when a
type changes, when a cell that answered starts refusing, and equally when a divergence is fixed --
that last one on purpose, so the reference is regenerated deliberately rather than drifting.

Regenerate with

    hatch run python3 -m pysail.tests.spark.function.test_arithmetic_matrix_types          # Sail
    SPARK_REMOTE=local hatch run python3 -m pysail.tests.spark.function.test_arithmetic_matrix_types

running it once per engine; each run rewrites only its own column.
"""

from __future__ import annotations

import pathlib

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.function.arithmetic_matrix_types import MATRIX_TYPES

REFERENCE = pathlib.Path(__file__).parent / "arithmetic_matrix_types.py"
BATCH = 40
ERROR = "<error>"
# Cells both engines answer with a different type. This must go DOWN, never up: a cell that
# answers with the wrong type is worse than one that refuses.
DIVERGENT_TYPES = 153

# The same 24-token alphabet the two matrix feature files use.
OPERANDS = {
    "tinyint": "CAST(NULL AS TINYINT)",
    "smallint": "CAST(NULL AS SMALLINT)",
    "int": "CAST(NULL AS INT)",
    "bigint": "CAST(NULL AS BIGINT)",
    "float": "CAST(NULL AS FLOAT)",
    "double": "CAST(NULL AS DOUBLE)",
    "decimal": "CAST(NULL AS DECIMAL(10,2))",
    "string": "CAST(NULL AS STRING)",
    "null": "NULL",
    "date": "CAST(NULL AS DATE)",
    "timestamp": "CAST(NULL AS TIMESTAMP)",
    "timestamp_ntz": "CAST(NULL AS TIMESTAMP_NTZ)",
    "time": "CAST(NULL AS TIME(6))",
    "interval_day": "INTERVAL '1' DAY",
    "interval_dts": "INTERVAL '1 2:3:4' DAY TO SECOND",
    "interval_hts": "INTERVAL '2:3:4' HOUR TO SECOND",
    "interval_ym": "INTERVAL '1' YEAR",
    "interval_cal": "make_interval(1,2,3,4,5,6,7)",
    "boolean": "CAST(NULL AS BOOLEAN)",
    "binary": "CAST(NULL AS BINARY)",
    "array": "CAST(NULL AS ARRAY<INT>)",
    "struct": "CAST(NULL AS STRUCT<a:INT>)",
    "map": "CAST(NULL AS MAP<INT,INT>)",
    "variant": "parse_json('1')",
}


def expression(key: str) -> str:
    """The SQL of a reference key, `<ansi>|<op>|<left>|<right>`."""
    _, op, left, right = key.split("|")
    return f"({OPERANDS[left]}) {op} ({OPERANDS[right]})"


def measure(spark, keys: list[str]) -> dict[str, str]:
    """The result type of each cell, batched so the sweep costs tens of queries, not 1182.

    Every cell here is one both engines answered when the reference was taken, so a batch normally
    succeeds; one that does not is retried cell by cell, and a cell that now refuses is recorded as
    an error rather than hiding the rest of its batch.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.timeType.enabled", "true")
    measured: dict[str, str] = {}
    for ansi in ("true", "false"):
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        group = [key for key in keys if key.startswith(f"{ansi}|")]
        for start in range(0, len(group), BATCH):
            chunk = group[start : start + BATCH]
            columns = ", ".join(f"typeof({expression(k)}) AS c{i}" for i, k in enumerate(chunk))
            try:
                row = spark.sql(f"SELECT {columns}").collect()[0]
                measured.update(dict(zip(chunk, list(row), strict=True)))
            except Exception:  # noqa: BLE001
                for key in chunk:
                    try:
                        measured[key] = spark.sql(f"SELECT typeof({expression(key)}) AS c").collect()[0][0]
                    except Exception:  # noqa: BLE001
                        measured[key] = ERROR
    return measured


def test_the_result_type_of_every_answered_cell_matches_the_reference(spark):
    reference = MATRIX_TYPES
    engine = "spark" if is_jvm_spark() else "sail"
    keys = sorted(reference)

    measured = measure(spark, keys)
    changed = {key: (reference[key][engine], measured[key]) for key in keys if measured[key] != reference[key][engine]}

    assert not changed, (
        f"{len(changed)} arithmetic cells changed their result type on {engine}; regenerate the "
        "reference if that was the intent:\n"
        + "\n".join(f"  {key}\n      recorded={was!r} now={now!r}" for key, (was, now) in sorted(changed.items())[:20])
    )


def test_the_recorded_type_divergences_are_the_known_ones():
    """One number, one meaning: how many cells both engines answer with a different type.

    A cell that answers with the wrong type is worse than one that refuses -- the query runs and
    the schema lies -- so this number must go DOWN, never up.
    """
    reference = MATRIX_TYPES
    divergent = [key for key, value in reference.items() if value["spark"] != value["sail"]]

    assert len(divergent) == DIVERGENT_TYPES, (
        f"{len(divergent)} arithmetic cells answer with a type that is not Spark's, not {DIVERGENT_TYPES}."
    )


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).parents[4]))
    from pysail.testing.spark.session import spark_connect_server, spark_session_factory
    from pysail.tests.spark.function.reference import write_reference

    engine_key = "spark" if is_jvm_spark() else "sail"
    data = dict(MATRIX_TYPES)
    if is_jvm_spark():
        from pyspark.sql import SparkSession

        session = SparkSession.builder.remote("local").getOrCreate()
        result = measure(session, sorted(data))
    else:
        with spark_connect_server() as server, spark_session_factory(server.remote) as sessions:
            result = measure(sessions.create(), sorted(data))
    for cell, value in result.items():
        data.setdefault(cell, {})[engine_key] = value
    write_reference(REFERENCE, "MATRIX_TYPES", data)
    print(f"wrote {len(result)} {engine_key} entries to {REFERENCE}")  # noqa: T201
