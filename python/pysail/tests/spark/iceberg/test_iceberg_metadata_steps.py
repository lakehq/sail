import pytest
from pyiceberg.manifest import ManifestContent, PartitionFieldSummary
from pyiceberg.typedef import Record

from pysail.testing.spark.steps.iceberg import _manifest_record_to_dict


@pytest.mark.parametrize(
    ("format_version", "version_fields", "trailing_fields", "expected_version_fields"),
    [
        (1, (), (), {"content": "data", "sequence-number": 0, "min-sequence-number": 0}),
        (
            2,
            (ManifestContent.DELETES, 9, 8),
            (),
            {"content": "deletes", "sequence-number": 9, "min-sequence-number": 8},
        ),
        (
            3,
            (ManifestContent.DATA, 9, 8),
            (100,),
            {"content": "data", "sequence-number": 9, "min-sequence-number": 8, "first-row-id": 100},
        ),
    ],
)
def test_manifest_list_records_use_their_versioned_layout(
    format_version, version_fields, trailing_fields, expected_version_fields
):
    record = Record(
        "manifest.avro",
        42,
        7,
        *version_fields,
        123,
        1,
        2,
        3,
        10,
        20,
        30,
        [PartitionFieldSummary(True, False, b"lo", b"hi")],
        None,
        *trailing_fields,
    )
    assert _manifest_record_to_dict(record, format_version) == {
        "manifest-path": "manifest.avro",
        "manifest-length": 42,
        "partition-spec-id": 7,
        **expected_version_fields,
        "added-snapshot-id": 123,
        "added-files-count": 1,
        "existing-files-count": 2,
        "deleted-files-count": 3,
        "added-rows-count": 10,
        "existing-rows-count": 20,
        "deleted-rows-count": 30,
        "partitions": [{"contains-null": True, "contains-nan": False, "lower-bound": b"lo", "upper-bound": b"hi"}],
        "key-metadata": None,
    }
