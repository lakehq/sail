import sys

import pyarrow as pa
import pytest
from pyspark import Row
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.session import spark_connect_server, spark_session_factory


@pytest.fixture(scope="module")
def array_converter():
    # Load the embedded converter so these tests also work with installed wheels.
    with (
        spark_connect_server(envs={"SAIL_MODE": "local"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        identity = F.udf(lambda value: value, "long", useArrow=False)
        assert spark.range(1).select(identity("id").alias("value")).collect() == [Row(value=0)]
        yield sys.modules["utils.spark"].ArrayConverter


@pytest.fixture(params=[(pa.ListArray, pa.int32()), (pa.LargeListArray, pa.int64())], ids=["list", "large-list"])
def list_layout(request):
    return request.param


@pytest.mark.parametrize(
    ("offset", "length", "expected"),
    [
        (0, 5, [[90, 91], None, [], [7, None], [9]]),
        (1, 4, [None, [], [7, None], [9]]),
        (1, 2, [None, []]),
        (5, 0, []),
    ],
)
def test_array_input_with_retained_null_children(array_converter, list_layout, offset, length, expected):
    array_class, offset_type = list_layout
    offsets = [0, 2, 5, 5, 7, 8]
    children = [90, 91, 100, 101, 102, 7, None, 9]
    nulls = [False, True, False, False, False]
    array = array_class.from_arrays(
        pa.array(offsets, type=offset_type),
        pa.array(children, type=pa.int64()),
        mask=pa.array(nulls),
    ).slice(offset, length)

    # Check physical ingress before passing this exact object to the converter.
    assert array.offset == offset
    assert array.offsets.to_pylist() == offsets[offset : offset + length + 1]
    assert array.values.to_pylist() == children
    assert array.is_null().to_pylist() == nulls[offset : offset + length]
    assert array_converter(array.type).to_pyspark(array) == expected


def test_nested_array_input_slice(array_converter, list_layout):
    array_class, offset_type = list_layout
    inner = array_class.from_arrays(
        pa.array([0, 2, 5, 5, 7, 8], type=offset_type),
        pa.array([90, 91, 100, 101, 102, 7, None, 9], type=pa.int64()),
        mask=pa.array([False, True, False, False, False]),
    )
    outer = array_class.from_arrays(pa.array([0, 1, 3, 5], type=offset_type), inner).slice(1, 2)

    assert outer.offset == 1
    assert outer.offsets.to_pylist() == [1, 3, 5]
    nested = outer.flatten()
    assert nested.offset == 1
    assert nested.offsets.to_pylist() == [2, 5, 5, 7, 8]
    assert nested.values.to_pylist() == [90, 91, 100, 101, 102, 7, None, 9]
    assert nested.is_null().to_pylist() == [True, False, False, False]
    assert array_converter(outer.type).to_pyspark(outer) == [[None, []], [[7, None], [9]]]


def test_array_input_preserves_binary_and_struct_conversion(array_converter):
    data_type = pa.list_(pa.struct([("payload", pa.binary()), ("score", pa.int64())]))
    array = pa.array(
        [
            [{"payload": b"discard", "score": 90}],
            [{"payload": b"a", "score": 1}, {"payload": None, "score": None}],
            [],
        ],
        type=data_type,
    ).slice(1)

    result = array_converter(array.type).to_pyspark(array)
    assert result == [[Row(payload=bytearray(b"a"), score=1), Row(payload=None, score=None)], []]
    assert isinstance(result[0][0], Row)
    assert isinstance(result[0][0].payload, bytearray)
