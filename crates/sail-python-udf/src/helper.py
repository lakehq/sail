from __future__ import annotations

from typing import Any

import pyarrow as pa
from pyspark.sql.pandas.types import DataType, from_arrow_type
from pyspark.sql.types import ArrayType, MapType, StructType

try:
    _PYARROW_LIST_ARRAY_TYPES = (
        pa.ListArray,
        pa.LargeListArray,
        pa.FixedSizeListArray,
        pa.ListViewArray,
        pa.LargeListViewArray,
    )
except AttributeError:
    _PYARROW_LIST_ARRAY_TYPES = (pa.ListArray, pa.LargeListArray, pa.FixedSizeListArray)

try:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType)
except AttributeError:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType)


def arrow_array_to_pyspark(array: pa.Array) -> list[Any]:
    def _convert(data: pa.Array, dt: DataType) -> list[Any]:
        if isinstance(dt, ArrayType):
            if not isinstance(data, _PYARROW_LIST_ARRAY_TYPES):
                msg = f"invalid data type for array: {type(data)}"
                raise TypeError(msg)
            values = _convert(data.flatten(), dt.elementType)
            offsets = data.offsets.to_pylist()
            valid = data.is_valid().to_pylist()
            result = []
            for i in range(len(data)):
                if not valid[i]:
                    result.append(None)
                else:
                    (start, end) = (offsets[i], offsets[i + 1])
                    result.append(values[start:end])
            return result
        elif isinstance(dt, MapType):  # noqa: RET505
            if not isinstance(data, pa.MapArray):
                msg = f"invalid data type for map: {type(data)}"
                raise TypeError(msg)
            keys = _convert(data.keys, dt.keyType)
            values = _convert(data.items, dt.valueType)
            offsets = data.offsets.to_pylist()
            valid = data.is_valid().to_pylist()
            result = []
            for i in range(len(data)):
                if not valid[i]:
                    result.append(None)
                else:
                    (start, end) = (offsets[i], offsets[i + 1])
                    result.append(dict(zip(keys[start:end], values[start:end])))
            return result
        elif isinstance(dt, StructType):
            if not isinstance(data, pa.StructArray):
                msg = f"invalid data type for struct: {type(data)}"
                raise TypeError(msg)
            columns = [_convert(child, field.dataType) for (child, field) in zip(data.flatten(), dt.fields)]
            return [dt.fromInternal(x) for x in zip(*columns)]
        else:
            return [x if x is None else dt.fromInternal(x) for x in data.to_pylist()]

    return _convert(array, from_arrow_type(array.type))


def pyspark_to_arrow_array(obj: list[Any], data_type: pa.DataType) -> pa.Array:
    def _convert(data: list[Any], dt: pa.DataType) -> pa.Array:
        if isinstance(dt, _PYARROW_LIST_TYPES):
            (values, offsets) = [], [0]
            end = 0
            for x in data:
                if x is None:
                    offsets.append(None)
                else:
                    values.extend(x)
                    end += len(x)
                    offsets.append(end)
            return pa.ListArray.from_arrays(
                pa.array(offsets, type=pa.int32()), pa.array(_convert(values, dt.value_type), type=dt.value_type)
            )
        elif isinstance(dt, pa.MapType):  # noqa: RET505
            (keys, values, offsets) = [], [], [0]
            end = 0
            for x in data:
                if x is None:
                    offsets.append(None)
                else:
                    keys.extend(x.keys())
                    values.extend(x.values())
                    end += len(x)
                    offsets.append(end)
            return pa.MapArray.from_arrays(
                pa.array(offsets, type=pa.int32()),
                pa.array(_convert(keys, dt.key_type), type=dt.key_type),
                pa.array(_convert(values, dt.item_type), type=dt.item_type),
            )
        elif isinstance(dt, StructType):
            n = len(dt.fields)
            columns = [[]] * n
            mask = []
            for x in data:
                if x is None:
                    mask.append(True)
                    for i in range(n):
                        columns[i].append(None)
                else:
                    mask.append(False)
                    for i, v in enumerate(x):
                        columns[i].append(v)
            return pa.StructArray.from_arrays(
                [_convert(col, f.type) for col, f in zip(columns, dt.fields)], fields=dt.fields, mask=mask
            )
        elif dt.equals(pa.string()) or dt.equals(pa.large_string()) or dt.equals(pa.string_view()):
            return pa.array([None if x is None else str(x) for x in data], type=dt)
        else:
            converter = from_arrow_type(dt).toInternal
            return pa.array([None if x is None else converter(x) for x in data], type=dt)

    return _convert(obj, data_type)
