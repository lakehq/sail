from __future__ import annotations

import ctypes
import decimal
import itertools
from typing import Any, Callable, Iterator, Sequence, Union

import pandas as pd
import pyarrow as pa
from pyspark.sql.pandas.serializers import ArrowStreamPandasUDFSerializer, ArrowStreamPandasUDTFSerializer
from pyspark.sql.pandas.types import _create_converter_from_pandas, _create_converter_to_pandas, from_arrow_type
from pyspark.sql.types import Row

_PYARROW_HAS_VIEW_TYPES = all(hasattr(pa, x) for x in ("list_view", "large_list_view", "string_view", "binary_view"))

if _PYARROW_HAS_VIEW_TYPES:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType)
    _PYARROW_LIST_ARRAY_TYPES = (
        pa.ListArray,
        pa.LargeListArray,
        pa.FixedSizeListArray,
        pa.ListViewArray,
        pa.LargeListViewArray,
    )
    _PyArrowListType = Union[pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType]

    def _pyarrow_is_string(t: pa.DataType) -> bool:
        return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)

    def _pyarrow_is_binary(t: pa.DataType) -> bool:
        return pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)
else:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType)
    _PYARROW_LIST_ARRAY_TYPES = (pa.ListArray, pa.LargeListArray, pa.FixedSizeListArray)
    _PyArrowListType = Union[pa.ListType, pa.LargeListType, pa.FixedSizeListType]

    def _pyarrow_is_string(t: pa.DataType) -> bool:
        return pa.types.is_string(t) or pa.types.is_large_string(t)

    def _pyarrow_is_binary(t: pa.DataType) -> bool:
        return pa.types.is_binary(t) or pa.types.is_large_binary(t)


try:
    from itertools import batched
except ImportError:

    def batched(iterable, n):
        it = iter(iterable)
        while chunk := tuple(itertools.islice(it, n)):
            yield chunk


class Converter:
    """
    A converter that converts between PySpark data and Arrow data.
    When matching PySpark data to the Arrow data type, invalid data is converted to null.
    This is the similar behavior in the Scala implementation of PySpark UDF [1].

    * [1] `org.apache.spark.sql.execution.python.EvaluatePython#makeFromJava`
    """

    def __init__(self, data_type: pa.DataType):
        self._data_type = data_type

    def to_pyspark(self, data: pa.Array) -> Sequence[Any]:
        raise NotImplementedError

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        raise NotImplementedError


def _get_converter(t: pa.DataType) -> Converter:
    if pa.types.is_null(t):
        return NullConverter(t)
    if pa.types.is_boolean(t):
        return BooleanConverter(t)
    if pa.types.is_int8(t):
        return IntegerConverter(t, ctypes.c_int8)
    if pa.types.is_int16(t):
        return IntegerConverter(t, ctypes.c_int16)
    if pa.types.is_int32(t):
        return IntegerConverter(t, ctypes.c_int32)
    if pa.types.is_int64(t):
        return IntegerConverter(t, ctypes.c_int64)
    if pa.types.is_uint8(t):
        return IntegerConverter(t, ctypes.c_uint8)
    if pa.types.is_uint16(t):
        return IntegerConverter(t, ctypes.c_uint16)
    if pa.types.is_uint32(t):
        return IntegerConverter(t, ctypes.c_uint32)
    if pa.types.is_uint64(t):
        return IntegerConverter(t, ctypes.c_uint64)
    if pa.types.is_floating(t):
        return FloatConverter(t)
    if pa.types.is_decimal(t):
        return DecimalConverter(t)
    if pa.types.is_time(t) or pa.types.is_timestamp(t) or pa.types.is_date(t) or pa.types.is_duration(t):
        return DefaultConverter(t)
    if _pyarrow_is_string(t):
        return StringConverter(t)
    if _pyarrow_is_binary(t):
        return BinaryConverter(t)
    if isinstance(t, _PYARROW_LIST_TYPES):
        return ArrayConverter(t)
    if isinstance(t, pa.MapType):
        return MapConverter(t)
    if isinstance(t, pa.StructType):
        return StructConverter(t)
    msg = f"unsupported data type: {t}"
    raise ValueError(msg)


def _raise_for_row(data: Any):
    if isinstance(data, Row):
        # Simulate the exception when the JVM receives an invalid row for the data type.
        msg = "net.razorvine.pickle.PickleException: expected zero arguments for construction of ClassDict (for pyspark.sql.types._create_row)."
        raise TypeError(msg)


class DefaultConverter(Converter):
    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        return array.to_pylist()

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        return pa.array(data, type=self._data_type)


class PrimitiveConverter(Converter):
    def __init__(self, data_type: pa.DataType):
        super().__init__(data_type)
        self._spark_data_type = from_arrow_type(data_type)

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        return [None if x is None else self._spark_data_type.fromInternal(x) for x in array.to_pylist()]

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        return pa.array(
            [None if x is None else self._from_python_primitive(self._spark_data_type.toInternal(x)) for x in data],
            type=self._data_type,
        )

    def _from_python_primitive(self, data: Any) -> Any:
        raise NotImplementedError


class NullConverter(PrimitiveConverter):
    def _from_python_primitive(self, data: Any) -> Any:
        _raise_for_row(data)
        return None


class BooleanConverter(PrimitiveConverter):
    def _from_python_primitive(self, data: Any) -> Any:
        _raise_for_row(data)
        if isinstance(data, bool):
            return data
        return None


class IntegerConverter(PrimitiveConverter):
    def __init__(self, data_type: pa.DataType, ctype: Any):
        super().__init__(data_type)
        self._ctype = ctype

    def _from_python_primitive(self, data: Any) -> Any:
        _raise_for_row(data)
        if isinstance(data, int):
            return self._ctype(data).value
        return None


class FloatConverter(PrimitiveConverter):
    def _from_python_primitive(self, data: Any) -> Any:
        _raise_for_row(data)
        if isinstance(data, float):
            return data
        return None


class DecimalConverter(PrimitiveConverter):
    def _from_python_primitive(self, data: Any) -> Any:
        _raise_for_row(data)
        if isinstance(data, decimal.Decimal):
            return data
        return None


def _to_string(data: Any) -> str | None:
    """Converts data to string where the behavior is similar to the Java `Object.toString()` method."""
    _raise_for_row(data)
    if data is None:
        return None
    if isinstance(data, str):
        return data
    if isinstance(data, bool):
        return "true" if data else "false"
    if isinstance(data, (list, tuple)):
        items = ", ".join(_to_string(x) for x in data)
        return f"[{items}]"
    if isinstance(data, dict):
        items = ", ".join(f"{_to_string(k)}={_to_string(v)}" for k, v in data.items())
        return f"{{{items}}}"
    return str(data)


class StringConverter(Converter):
    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        return array.to_pylist()

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        return pa.array([_to_string(x) for x in data], type=self._data_type)


def _to_bytes(data: Any) -> bytes | None:
    _raise_for_row(data)
    if data is None:
        return None
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    return None


class BinaryConverter(Converter):
    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        return [None if x is None else bytearray(x) for x in array.to_pylist()]

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        return pa.array([_to_bytes(x) for x in data], type=self._data_type)


class ArrayConverter(Converter):
    def __init__(self, data_type: _PyArrowListType):
        super().__init__(data_type)
        self._value_converter = _get_converter(data_type.value_type)

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
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

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        (values, offsets) = [], []
        end = 0
        for x in data:
            _raise_for_row(x)
            if x is None or not isinstance(x, (list, tuple)):
                offsets.append(None)
            else:
                offsets.append(end)
                values.extend(x)
                end += len(x)
        offsets.append(end)
        return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), self._value_converter.from_pyspark(values))


class MapConverter(Converter):
    def __init__(self, data_type: pa.MapType):
        super().__init__(data_type)
        self._key_converter = _get_converter(data_type.key_type)
        self._value_converter = _get_converter(data_type.item_type)

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
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

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        (keys, values, offsets) = [], [], []
        end = 0
        for x in data:
            _raise_for_row(x)
            if x is None or not isinstance(x, dict):
                offsets.append(None)
            else:
                offsets.append(end)
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

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        if not isinstance(array, pa.StructArray):
            msg = f"invalid data type for struct: {type(array)}"
            raise TypeError(msg)
        columns = [c.to_pyspark(col) for col, c in zip(array.flatten(), self._field_converters)]
        return [self._spark_data_type.fromInternal(x) for x in zip(*columns)]

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
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
                for i, v in enumerate(self._spark_data_type.toInternal(x)):
                    columns[i].append(v)
        return pa.StructArray.from_arrays(
            [c.from_pyspark(col) for col, c in zip(columns, self._field_converters)],
            fields=self._fields,
            mask=pa.array(mask, type=pa.bool_()),
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
    def __init__(
        self,
        udf: Callable[..., Any],
        input_types: list[pa.DataType],
        output_type: pa.DataType,
        timezone: str,
        safe_check: bool,  # noqa: FBT001
        assign_columns_by_name: bool,  # noqa: FBT001
    ):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=timezone,
            safecheck=safe_check,
            assign_cols_by_name=assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(self._serializer.arrow_to_pandas(x) for x in args)
        [(output, _output_type)] = list(self._udf(None, (inputs,)))
        if isinstance(output, pd.DataFrame):
            return self._serializer._create_struct_array(output, self._output_type)  # noqa: SLF001
        if isinstance(output, pd.Series):
            return self._serializer._create_array(output, self._output_type, arrow_cast=self._serializer._arrow_cast)  # noqa: SLF001
        msg = f"invalid Scalar Pandas UDF output type: {type(output)}"
        raise TypeError(msg)


class PySparkScalarPandasIterUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_types: list[pa.DataType],
        output_type: pa.DataType,
        timezone: str,
        safe_check: bool,  # noqa: FBT001
        assign_columns_by_name: bool,  # noqa: FBT001
    ):
        self._udf = udf
        self._input_types = input_types
        self._output_type = output_type
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=timezone,
            safecheck=safe_check,
            assign_cols_by_name=assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(self._serializer.arrow_to_pandas(x) for x in args)
        [(output, _output_type)] = list(self._udf(None, [inputs]))
        if isinstance(output, pd.DataFrame):
            return self._serializer._create_struct_array(output, self._output_type)  # noqa: SLF001
        if isinstance(output, pd.Series):
            return self._serializer._create_array(output, self._output_type, arrow_cast=self._serializer._arrow_cast)  # noqa: SLF001
        msg = f"invalid Scalar Pandas iter UDF output type: {type(output)}"
        raise TypeError(msg)


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


def _pandas_to_record_batch(df: pd.DataFrame, schema: pa.Schema, *, column_match_by_name: bool) -> pa.RecordBatch:
    if len(df) > 0:
        if not column_match_by_name or all(not isinstance(x, str) for x in df.columns):
            df = df[df.columns[: len(schema.names)]]
            # An exception will be raised if the number of columns does not match the number of fields in the schema.
            df.columns = schema.names
        return pa.RecordBatch.from_pandas(df, schema=schema)
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
        return _pandas_to_record_batch(output, self._output_schema, column_match_by_name=self._column_match_by_name)


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
        return _pandas_to_record_batch(output, self._output_schema, column_match_by_name=self._column_match_by_name)

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
        return _pandas_to_record_batch(df, self._output_schema, column_match_by_name=True)


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
        input_types: list[pa.DataType],
        passthrough_columns: int,
        output_schema: pa.Schema,
        batch_size: int = 1024,
    ):
        self._udf = udf
        self._passthrough_columns = passthrough_columns
        self._batch_size = batch_size
        self._output_schema = output_schema
        self._input_converters = [_get_converter(t) for t in input_types]
        self._output_converter = StructConverter(
            pa.struct([output_schema.field(i) for i in range(len(output_schema.names))])
        )

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for output in batched(self._iter_output_rows(args), self._batch_size):
            yield pa.RecordBatch.from_struct_array(self._output_converter.from_pyspark(output))

    def _iter_input_rows(self, args: Iterator[pa.RecordBatch]) -> Iterator[tuple]:
        for batch in args:
            arrays = batch.to_struct_array().flatten()
            if len(arrays) > 0:
                inputs = tuple(c.to_pyspark(a) for a, c in zip(arrays, self._input_converters))
                yield from zip(*inputs)
            else:
                yield ()

    def _iter_output_rows(self, args: Iterator[pa.RecordBatch]) -> Iterator[list]:
        (rows1, rows2) = itertools.tee(self._iter_input_rows(args))
        inputs = (x[self._passthrough_columns :] for x in rows2)
        outputs = self._udf(None, inputs)
        empty = tuple([None] * len(self._output_schema.names))
        last = tuple([None] * len(self._input_converters))
        for row, it in itertools.zip_longest(rows1, outputs):
            if row is None:
                row = last  # noqa: PLW2901
            passthrough = row[: self._passthrough_columns]
            for out in it:
                if out is None:
                    yield passthrough + empty
                else:
                    yield passthrough + tuple(out)
            last = row


class PySparkArrowTableUdf:
    def __init__(
        self,
        udf: Callable[..., Iterator[pa.RecordBatch]],
        input_names: Sequence[str],
        passthrough_columns: int,
        output_schema: pa.Schema,
        timezone: str,
        safe_check: bool,  # noqa: FBT001
    ):
        self._udf = udf
        self._input_names = input_names
        self._passthrough_columns = passthrough_columns
        self._output_schema = output_schema
        self._output_type = pa.struct([output_schema.field(i) for i in range(len(output_schema.names))])
        self._serializer = ArrowStreamPandasUDTFSerializer(timezone=timezone, safecheck=safe_check)

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for output in self._iter_output(args):
            array = self._serializer._create_struct_array(output, self._output_type)  # noqa: SLF001
            yield pa.RecordBatch.from_struct_array(array)

    def _iter_input(self, args: Iterator[pa.RecordBatch]) -> Iterator[tuple[pd.Series]]:
        for batch in args:
            arrays = batch.to_struct_array().flatten()
            yield tuple(self._serializer.arrow_to_pandas(x) for x in arrays)

    def _iter_output(self, args: Iterator[pa.RecordBatch]) -> Iterator[pd.DataFrame]:
        (batches1, batches2) = itertools.tee(self._iter_input(args))
        inputs = (x[self._passthrough_columns :] for x in batches2)
        outputs = self._udf(None, inputs)
        last = None
        for passthrough, (out, _) in itertools.zip_longest(self._iter_passthrough(batches1), outputs):
            if out is None or len(out) == 0:
                continue
            df = pd.DataFrame(index=out.index)
            if passthrough is None:
                passthrough = last  # noqa: PLW2901
            if passthrough is not None:
                for v, name in zip(passthrough, self._input_names):
                    df[name] = [v] * len(out)
            else:
                for name in self._input_names:
                    df[name] = [None] * len(out)
            for col in out:
                df[col] = out[col]
            yield df
            last = passthrough

    def _iter_passthrough(self, batches: Iterator[tuple[pd.Series]]) -> Iterator[tuple]:
        if self._passthrough_columns > 0:
            for batch in batches:
                yield from zip(*batch[: self._passthrough_columns])
        else:
            for batch in batches:
                if len(batch) > 0:
                    first, *_ = batch
                    for _ in range(len(first)):
                        yield ()
                else:
                    yield ()
