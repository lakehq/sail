from __future__ import annotations

import itertools
from typing import Any, Callable, Iterator, Union

import pandas as pd
import pyarrow as pa
from pyspark.sql.pandas.types import _create_converter_from_pandas, _create_converter_to_pandas, from_arrow_type

try:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType)
    _PyArrowListType = Union[pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType]
except AttributeError:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType)
    _PyArrowListType = Union[pa.ListType, pa.LargeListType, pa.FixedSizeListType]

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
    _PYARROW_STRING_TYPE_INSTANCES = (pa.string(), pa.large_string(), pa.string_view())
except AttributeError:
    _PYARROW_STRING_TYPE_INSTANCES = (pa.string(), pa.large_string())

try:
    _PYARROW_BINARY_TYPE_INSTANCES = (pa.binary(), pa.large_binary(), pa.binary_view())
except AttributeError:
    _PYARROW_BINARY_TYPE_INSTANCES = (pa.binary(), pa.large_binary())


class Converter:
    def __init__(self, data_type: pa.DataType):
        self._data_type = data_type

    def to_pyspark(self, data: pa.Array) -> list[Any]:
        raise NotImplementedError

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        raise NotImplementedError


def _get_converter(data_type: pa.DataType) -> Converter:
    if isinstance(data_type, _PYARROW_LIST_TYPES):
        return ArrayConverter(data_type)
    elif isinstance(data_type, pa.MapType):  # noqa: RET505
        return MapConverter(data_type)
    elif isinstance(data_type, pa.StructType):
        return StructConverter(data_type)
    elif any(data_type.equals(x) for x in _PYARROW_STRING_TYPE_INSTANCES):
        return StringConverter(data_type)
    elif any(data_type.equals(x) for x in _PYARROW_BINARY_TYPE_INSTANCES):
        return BinaryConverter(data_type)
    else:
        return PrimitiveConverter(data_type)


class ArrayConverter(Converter):
    def __init__(self, data_type: _PyArrowListType):
        super().__init__(data_type)
        self._value_converter = _get_converter(data_type.value_type)

    def to_pyspark(self, array: pa.Array) -> list[Any]:
        if not isinstance(array, _PYARROW_LIST_ARRAY_TYPES):
            msg = f"invalid data type for array: {type(array)}"
            raise TypeError(msg)
        values = self._value_converter.to_pyspark(array.flatten())
        offsets = array.offsets.to_pylist()
        valid = array.is_valid().to_pylist()
        result = []
        for i in range(len(array)):
            if not valid[i]:
                result.append(None)
            else:
                (start, end) = (offsets[i], offsets[i + 1])
                result.append(values[start:end])
        return result

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        (values, offsets) = [], [0]
        end = 0
        for x in data:
            if x is None:
                offsets.append(None)
            else:
                values.extend(x)
                end += len(x)
                offsets.append(end)
        return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), self._value_converter.from_pyspark(values))


class MapConverter(Converter):
    def __init__(self, data_type: pa.MapType):
        super().__init__(data_type)
        self._key_converter = _get_converter(data_type.key_type)
        self._value_converter = _get_converter(data_type.item_type)

    def to_pyspark(self, array: pa.Array) -> list[Any]:
        if not isinstance(array, pa.MapArray):
            msg = f"invalid data type for map: {type(array)}"
            raise TypeError(msg)
        keys = self._key_converter.to_pyspark(array.keys)
        values = self._value_converter.to_pyspark(array.items)
        offsets = array.offsets.to_pylist()
        valid = array.is_valid().to_pylist()
        result = []
        for i in range(len(array)):
            if not valid[i]:
                result.append(None)
            else:
                (start, end) = (offsets[i], offsets[i + 1])
                result.append(dict(zip(keys[start:end], values[start:end])))
        return result

    def from_pyspark(self, data: list[Any]) -> pa.Array:
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
            self._key_converter.from_pyspark(keys),
            self._value_converter.from_pyspark(values),
        )


class StructConverter(Converter):
    def __init__(self, data_type: pa.StructType):
        super().__init__(data_type)
        try:
            self._fields = data_type.fields
        except AttributeError:
            self._fields = [data_type.field(i) for i in range(data_type.num_fields)]
        self._field_converters = [_get_converter(f.type) for f in self._fields]
        self._spark_data_type = from_arrow_type(data_type)

    def to_pyspark(self, array: pa.Array) -> list[Any]:
        if not isinstance(array, pa.StructArray):
            msg = f"invalid data type for struct: {type(array)}"
            raise TypeError(msg)
        columns = [c.to_pyspark(col) for col, c in zip(array.flatten(), self._field_converters)]
        return [self._spark_data_type.fromInternal(x) for x in zip(*columns)]

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        n = len(self._fields)
        columns = [[] for _ in range(n)]
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
            [c.from_pyspark(col) for col, c in zip(columns, self._field_converters)],
            fields=self._fields,
            mask=pa.array(mask, type=pa.bool_()),
        )


class StringConverter(Converter):
    def to_pyspark(self, array: pa.Array) -> list[Any]:
        return array.to_pylist()

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        return pa.array([None if x is None else str(x) for x in data], type=self._data_type)


class BinaryConverter(Converter):
    def to_pyspark(self, array: pa.Array) -> list[Any]:
        return [None if x is None else bytearray(x) for x in array.to_pylist()]

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        return pa.array([None if x is None else self._to_bytes(x) for x in data], type=self._data_type)

    @staticmethod
    def _to_bytes(data: Any) -> bytes | None:
        if isinstance(data, str):
            return data.encode("utf-8")
        elif isinstance(data, bytes):  # noqa: RET505
            return data
        elif isinstance(data, bytearray):
            return bytes(data)
        else:
            return None


class PrimitiveConverter(Converter):
    def __init__(self, data_type: pa.DataType):
        super().__init__(data_type)
        self._spark_data_type = from_arrow_type(data_type)

    def to_pyspark(self, array: pa.Array) -> list[Any]:
        return [None if x is None else self._spark_data_type.fromInternal(x) for x in array.to_pylist()]

    def from_pyspark(self, data: list[Any]) -> pa.Array:
        return pa.array(
            [None if x is None else self._spark_data_type.fromInternal(x) for x in data], type=self._data_type
        )


ARROW_TO_PANDAS_NULLABLE_TYPES = {
    pa.int8(): pd.Int8Dtype(),
    pa.int16(): pd.Int16Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.int64(): pd.Int64Dtype(),
    pa.uint8(): pd.UInt8Dtype(),
    pa.uint16(): pd.UInt16Dtype(),
    pa.uint32(): pd.UInt32Dtype(),
    pa.uint64(): pd.UInt64Dtype(),
    pa.bool_(): pd.BooleanDtype(),
    pa.float32(): pd.Float32Dtype(),
    pa.float64(): pd.Float64Dtype(),
    pa.string(): pd.StringDtype(),
}


class PySparkBatchUdf:
    def __init__(self, udf: Callable[..., Any], input_types: list[pa.DataType], output_type: pa.DataType):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type
        self._input_converters = [_get_converter(t) for t in input_types]
        self._output_converter = _get_converter(output_type)

    def __call__(self, args: list[pa.Array], num_rows: int) -> pa.Array:
        if len(args) > 0:
            inputs = [c.to_pyspark(a) for a, c in zip(args, self._input_converters)]
            output = list(self._udf(None, zip(*inputs)))
        else:
            output = list(self._udf(None, itertools.repeat((), num_rows)))
        return self._output_converter.from_pyspark(output)


def _array_to_pandas(array: pa.Array) -> pd.Series:
    return array.to_pandas(types_mapper=ARROW_TO_PANDAS_NULLABLE_TYPES.get, split_blocks=True)


class PySparkArrowBatchUdf:
    def __init__(self, udf: Callable[..., Any], input_types: list[pa.DataType], output_type: pa.DataType):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type
        self._input_converters = [
            _create_converter_to_pandas(from_arrow_type(dt), nullable=True, struct_in_pandas="row")
            for dt in input_types
        ]
        self._output_converter = _create_converter_from_pandas(from_arrow_type(output_type), timezone=None)

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(c(_array_to_pandas(a)) for a, c in zip(args, self._input_converters))
        [(output, _output_type)] = list(self._udf(None, (inputs,)))
        return pa.array(self._output_converter(output), type=self._output_type, from_pandas=True)


class PySparkScalarPandasUdf:
    def __init__(self, udf: Callable[..., Any], input_types: list[pa.DataType], output_type: pa.DataType):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(_array_to_pandas(x) for x in args)
        [(output, _output_type)] = list(self._udf(None, (inputs,)))
        return pa.array(output, type=self._output_type, from_pandas=True)


class PySparkScalarPandasIterUdf:
    def __init__(self, udf: Callable[..., Any], input_types: list[pa.DataType], output_type: pa.DataType):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(_array_to_pandas(x) for x in args)
        [(output, _output_type)] = list(self._udf(None, [inputs]))
        return pa.array(output, type=self._output_type, from_pandas=True)


class PySparkGroupAggUdf:
    def __init__(
        self, udf: Callable[..., Any], input_names: list[str], input_types: list[pa.DataType], output_type: pa.DataType
    ):
        self._udf = udf
        self._input_names = input_names
        self._input_types = input_types
        self._output_type = output_type

    def __call__(self, args: list[pa.Array]) -> pa.Array:
        inputs = tuple(_array_to_pandas(x) for x in args)
        for x, name in zip(inputs, self._input_names):
            x.name = name
        [(output, _output_type)] = list(self._udf(None, (inputs,)))
        return pa.array(output, type=self._output_type, from_pandas=True)


def _pandas_to_record_batch(df: pd.DataFrame, schema: pa.Schema, column_match_by_name: bool) -> pa.RecordBatch:  # noqa: FBT001
    if len(df) > 0:
        if not column_match_by_name or all(not isinstance(x, str) for x in df.columns):
            df = df[df.columns[: len(schema.names)]]
            # An exception will be raised if the number of columns does not match the number of fields in the schema.
            df.columns = schema.names
        return pa.RecordBatch.from_pandas(df, schema=schema)
    else:  # noqa: RET505
        return pa.RecordBatch.from_pylist([], schema=schema)


class PySparkGroupMapUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_names: list[str],
        output_schema: pa.Schema,
        column_match_by_name: bool,  # noqa: FBT001
    ):
        self._udf = udf
        self._input_names = input_names
        self._output_schema = output_schema
        self._column_match_by_name = column_match_by_name

    def __call__(self, args: list[pa.Array]) -> pa.RecordBatch:
        inputs = tuple(_array_to_pandas(x) for x in args)
        for x, name in zip(inputs, self._input_names):
            x.name = name
        [[(output, _output_type)]] = list(self._udf(None, (inputs,)))
        return _pandas_to_record_batch(output, self._output_schema, self._column_match_by_name)


class PySparkCoGroupMapUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        output_schema: pa.Schema,
        column_match_by_name: bool,  # noqa: FBT001
    ):
        self._udf = udf
        self._output_schema = output_schema
        self._column_match_by_name = column_match_by_name

    def __call__(self, left: pa.RecordBatch, right: pa.RecordBatch) -> pa.RecordBatch:
        args = (self._convert_input(left), self._convert_input(right))
        [[(output, _output_type)]] = list(self._udf(None, (args,)))
        return _pandas_to_record_batch(output, self._output_schema, self._column_match_by_name)

    @staticmethod
    def _convert_input(batch: pa.RecordBatch) -> list[pd.Series]:
        df = batch.to_pandas(split_blocks=True)
        return [df[c] for c in df.columns]


class PySparkMapPandasIterUdf:
    def __init__(
        self,
        udf: Callable[..., Iterator[pd.DataFrame]],
        output_schema: pa.Schema,
    ):
        self._udf = udf
        self._output_schema = output_schema

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        input_ = map(self._convert_input, args)
        output = self._udf(None, ((x,) for x in input_))
        return map(self._convert_output, (x for x, _ in output))

    @staticmethod
    def _convert_input(batch: pa.RecordBatch) -> pd.DataFrame:
        return batch.to_pandas(split_blocks=True)

    def _convert_output(self, df: pd.DataFrame) -> pa.RecordBatch:
        return _pandas_to_record_batch(df, self._output_schema, True)


class PySparkMapArrowIterUdf:
    def __init__(
        self,
        udf: Callable[..., Iterator[pa.RecordBatch]],
    ):
        self._udf = udf

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        output = self._udf(None, ((x,) for x in args))
        return (x for x, _ in output)


class PySparkTableUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        schema: pa.Schema,
    ):
        self._udf = udf
        self._schema = schema
        fields = [schema.field(i) for i in range(len(schema.names))]
        self._output_converter = StructConverter(pa.struct(fields))

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        [args] = list(args)
        args = args.to_struct_array().flatten()
        outputs = []
        if len(args) > 0:
            inputs = tuple(x.to_pylist() for x in args)
            for x in zip(*inputs):
                for out in self._udf(None, (x,)):
                    outputs.extend(out)
        else:
            for out in self._udf(None, ((),)):
                outputs.extend(out)
        yield pa.RecordBatch.from_struct_array(self._output_converter.from_pyspark(outputs))


class PySparkArrowTableUdf:
    def __init__(
        self,
        udf: Callable[..., Iterator[pa.RecordBatch]],
    ):
        self._udf = udf

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        [args] = list(args)
        args = args.to_struct_array().flatten()
        inputs = tuple(_array_to_pandas(x) for x in args)
        [(output, _output_type)] = list(self._udf(None, (inputs,)))
        yield pa.RecordBatch.from_pandas(output)
