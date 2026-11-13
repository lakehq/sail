from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.mark.parametrize("explicit_schema", [False, True], ids=["inferred", "explicit"])
@pytest.mark.parametrize("multiple_columns", [False, True], ids=["single-key", "multiple-keys"])
@pytest.mark.parametrize(
    "spellings",
    [
        ("p=keep",),
        ("%70=keep",),
        ("p=%6Beep",),
        ("p=%6beep",),
        ("p=keep", "%70=keep", "p=%6Beep", "p=%6beep"),
    ],
    ids=["canonical", "encoded-name", "encoded-value-upper", "encoded-value-lower", "mixed"],
)
def test_partition_filters_match_decoded_directory_aliases(
    spark, tmp_path, explicit_schema, multiple_columns, spellings
):
    # Spark 4.3 accepts these spellings as the same decoded partition identity.
    path = tmp_path / "data"
    expected = []
    layouts = [(spelling, "keep", "selected") for spelling in spellings] + [("p=other", "other", "selected")]
    if multiple_columns:
        layouts += [("p=keep", "keep", "excluded")]
    for rid, (directory, partition, second) in enumerate(layouts):
        parent = path / directory
        if multiple_columns:
            parent /= f"q={second}"
        parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"rid": pa.array([rid], type=pa.int32())}), parent / f"{rid}.parquet")
        expected.append((rid, partition, second) if multiple_columns else (rid, partition))

    before = {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")}
    reader = spark.read
    if explicit_schema:
        reader = reader.schema("rid INT, p STRING" + (", q STRING" if multiple_columns else ""))
    frame = reader.parquet(str(path))
    columns = ["rid", "p", "q"] if multiple_columns else ["rid", "p"]
    assert Counter(tuple(row) for row in frame.select(*columns).collect()) == Counter(expected)
    predicate = "p = 'keep'" + (" AND q = 'selected'" if multiple_columns else "")
    selected = [row for row in expected if row[1] == "keep" and (not multiple_columns or row[2] == "selected")]
    assert Counter(tuple(row) for row in frame.where(predicate).select(*columns).collect()) == Counter(selected)
    assert {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")} == before
