from __future__ import annotations

import ctypes
import decimal
import itertools
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

import pandas as pd
import pyarrow as pa
import pyspark
from pyspark.sql.pandas.serializers import ArrowStreamPandasUDFSerializer, ArrowStreamPandasUDTFSerializer
from pyspark.sql.pandas.types import from_arrow_type
from pyspark.sql.types import ArrayType, MapType, Row, StructField, StructType, UserDefinedType

try:
    from pyspark.sql.types import VariantType
except ImportError:
    VariantType = None

_PYARROW_HAS_VIEW_TYPES = all(hasattr(pa, x) for x in ("list_view", "large_list_view", "string_view", "binary_view"))
_ARROW_EXTENSION_NAME_KEY = b"ARROW:extension:name"
_ARROW_PARQUET_VARIANT_EXTENSION_NAME = "arrow.parquet.variant"
_PYSPARK_VARIANT_METADATA_KEY = b"variant"
_SAIL_SPARK_UDT_METADATA_KEY = b"SAIL::spark::udt"

if _PYARROW_HAS_VIEW_TYPES:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.ListViewType, pa.LargeListViewType)
    _PYARROW_LIST_ARRAY_TYPES = (
        pa.ListArray,
        pa.LargeListArray,
        pa.FixedSizeListArray,
        pa.ListViewArray,
        pa.LargeListViewArray,
    )
    _PyArrowListType = pa.ListType | pa.LargeListType | pa.FixedSizeListType | pa.ListViewType | pa.LargeListViewType

    def _pyarrow_is_string(t: pa.DataType) -> bool:
        return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)

    def _pyarrow_is_binary(t: pa.DataType) -> bool:
        return pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)
else:
    _PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType)
    _PYARROW_LIST_ARRAY_TYPES = (pa.ListArray, pa.LargeListArray, pa.FixedSizeListArray)
    _PyArrowListType = pa.ListType | pa.LargeListType | pa.FixedSizeListType

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


try:
    from pyspark.sql.conversion import (
        ArrowTableToRowsConversion as _ArrowTableToRowsConversion,
    )
    from pyspark.sql.conversion import (
        LocalDataToArrowConversion as _LocalDataToArrowConversion,
    )
except ImportError:
    _ArrowTableToRowsConversion = None
    _LocalDataToArrowConversion = None


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


def _metadata_value(metadata: dict | None, key: bytes) -> bytes | None:
    if metadata is None:
        return None
    return metadata.get(key) or metadata.get(key.decode("utf-8"))


def _field_metadata(field: pa.Field | None) -> dict | None:
    return None if field is None else field.metadata


def _field_type(field_or_type: pa.Field | pa.DataType) -> tuple[pa.Field | None, pa.DataType]:
    if isinstance(field_or_type, pa.Field):
        return field_or_type, field_or_type.type
    return None, field_or_type


def _field_is_variant(field_or_type: pa.Field | pa.DataType) -> bool:
    if VariantType is None:
        return False
    field, data_type = _field_type(field_or_type)
    extension_name = _metadata_value(_field_metadata(field), _ARROW_EXTENSION_NAME_KEY)
    if isinstance(extension_name, bytes):
        extension_name = extension_name.decode("utf-8")
    if extension_name == _ARROW_PARQUET_VARIANT_EXTENSION_NAME:
        return True
    if not isinstance(data_type, pa.StructType):
        return False
    try:
        fields = data_type.fields
    except AttributeError:
        fields = [data_type.field(i) for i in range(data_type.num_fields)]
    return any(
        f.name == "metadata" and _metadata_value(f.metadata, _PYSPARK_VARIANT_METADATA_KEY) in (b"true", "true")
        for f in fields
    ) and any(f.name == "value" for f in fields)


def _list_value_field(data_type: _PyArrowListType) -> pa.Field:
    field = getattr(data_type, "value_field", None)
    if field is not None:
        return field
    return pa.field("element", data_type.value_type, nullable=True)


def _map_key_field(data_type: pa.MapType) -> pa.Field:
    field = getattr(data_type, "key_field", None)
    if field is not None:
        return field
    return pa.field("key", data_type.key_type, nullable=False)


def _map_item_field(data_type: pa.MapType) -> pa.Field:
    field = getattr(data_type, "item_field", None)
    if field is not None:
        return field
    return pa.field("value", data_type.item_type, nullable=True)


def _spark_type_from_field(field_or_type: pa.Field | pa.DataType):
    field, data_type = _field_type(field_or_type)
    metadata = _metadata_value(_field_metadata(field), _SAIL_SPARK_UDT_METADATA_KEY)
    if metadata is not None:
        if isinstance(metadata, bytes):
            metadata = metadata.decode("utf-8")
        udt = json.loads(metadata)
        py_class = udt.get("python_class")
        if py_class is None:
            msg = f"UDT metadata is missing python_class: {udt}"
            raise ValueError(msg)
        value = {
            "type": "udt",
            "pyClass": py_class,
            "sqlType": _spark_type_from_field(data_type).jsonValue(),
        }
        if udt.get("jvm_class") is not None:
            value["class"] = udt["jvm_class"]
        if udt.get("serialized_python_class") is not None:
            value["serializedClass"] = udt["serialized_python_class"]
        return UserDefinedType.fromJson(value)

    if _field_is_variant(field_or_type):
        return VariantType()
    if isinstance(data_type, _PYARROW_LIST_TYPES):
        value_field = _list_value_field(data_type)
        return ArrayType(_spark_type_from_field(value_field), value_field.nullable)
    if isinstance(data_type, pa.MapType):
        value_field = _map_item_field(data_type)
        return MapType(
            _spark_type_from_field(_map_key_field(data_type)),
            _spark_type_from_field(value_field),
            value_field.nullable,
        )
    if isinstance(data_type, pa.StructType):
        try:
            fields = data_type.fields
        except AttributeError:
            fields = [data_type.field(i) for i in range(data_type.num_fields)]
        return StructType([StructField(f.name, _spark_type_from_field(f), f.nullable) for f in fields])
    return from_arrow_type(data_type)


def _field_has_udt(field_or_type: pa.Field | pa.DataType) -> bool:
    field, data_type = _field_type(field_or_type)
    if _metadata_value(_field_metadata(field), _SAIL_SPARK_UDT_METADATA_KEY) is not None:
        return True
    if isinstance(data_type, _PYARROW_LIST_TYPES):
        return _field_has_udt(_list_value_field(data_type))
    if isinstance(data_type, pa.MapType):
        return _field_has_udt(_map_key_field(data_type)) or _field_has_udt(_map_item_field(data_type))
    if isinstance(data_type, pa.StructType):
        try:
            fields = data_type.fields
        except AttributeError:
            fields = [data_type.field(i) for i in range(data_type.num_fields)]
        return any(_field_has_udt(f) for f in fields)
    return False


def _strip_udt_metadata_from_field(field: pa.Field) -> pa.Field:
    metadata = field.metadata
    if metadata is not None:
        metadata = {k: v for k, v in metadata.items() if k != _SAIL_SPARK_UDT_METADATA_KEY}
        if not metadata:
            metadata = None
    return pa.field(
        field.name,
        _strip_udt_metadata_from_type(field.type),
        nullable=field.nullable,
        metadata=metadata,
    )


def _strip_udt_metadata_from_type(data_type: pa.DataType) -> pa.DataType:
    if isinstance(data_type, _PYARROW_LIST_TYPES):
        value_field = _strip_udt_metadata_from_field(_list_value_field(data_type))
        if isinstance(data_type, pa.LargeListType):
            return pa.large_list(value_field)
        if isinstance(data_type, pa.FixedSizeListType):
            return pa.list_(value_field, data_type.list_size)
        if _PYARROW_HAS_VIEW_TYPES and isinstance(data_type, pa.LargeListViewType):
            return pa.large_list_view(value_field)
        if _PYARROW_HAS_VIEW_TYPES and isinstance(data_type, pa.ListViewType):
            return pa.list_view(value_field)
        return pa.list_(value_field)
    if isinstance(data_type, pa.MapType):
        return pa.map_(
            _strip_udt_metadata_from_field(_map_key_field(data_type)),
            _strip_udt_metadata_from_field(_map_item_field(data_type)),
            keys_sorted=data_type.keys_sorted,
        )
    if isinstance(data_type, pa.StructType):
        try:
            fields = data_type.fields
        except AttributeError:
            fields = [data_type.field(i) for i in range(data_type.num_fields)]
        return pa.struct([_strip_udt_metadata_from_field(f) for f in fields])
    return data_type


def _get_converter(t: pa.Field | pa.DataType) -> Converter:
    field, t = _field_type(t)
    if _metadata_value(_field_metadata(field), _SAIL_SPARK_UDT_METADATA_KEY) is not None:
        return UdtConverter(field)
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
        return DateTimeConverter(t)
    if _pyarrow_is_string(t):
        return StringConverter(t)
    if _pyarrow_is_binary(t):
        return BinaryConverter(t)
    if isinstance(t, _PYARROW_LIST_TYPES):
        return ArrayConverter(field or t)
    if isinstance(t, pa.MapType):
        return MapConverter(field or t)
    if isinstance(t, pa.StructType):
        return StructConverter(field or t)
    msg = f"unsupported data type: {t}"
    raise ValueError(msg)


def _raise_for_row(data: Any):
    if isinstance(data, Row):
        # Simulate the exception when the JVM receives an invalid row for the data type.
        msg = "net.razorvine.pickle.PickleException: expected zero arguments for construction of ClassDict (for pyspark.sql.types._create_row)."
        raise TypeError(msg)


class ScalarConverter(Converter):
    def to_pyspark(self, data: pa.Array) -> Sequence[Any]:
        return [self._to_pyspark_value(x) for x in data.to_pylist()]

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        return pa.array([self._from_pyspark_value(x) for x in data], type=self._data_type)

    def _to_pyspark_value(self, data: Any) -> Any:
        raise NotImplementedError

    def _from_pyspark_value(self, data: Any) -> Any:
        raise NotImplementedError


class PrimitiveConverter(ScalarConverter):
    def __init__(self, data_type: pa.DataType):
        super().__init__(data_type)
        self._spark_data_type = from_arrow_type(data_type)

    def _to_pyspark_value(self, data: Any) -> Any:
        # Reference: `pyspark.sql.types._create_row_inbound_converter`
        return self._spark_data_type.fromInternal(data)

    def _from_pyspark_value(self, data: Any) -> Any:
        return self._from_python_primitive(self._spark_data_type.toInternal(data))

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


class DateTimeConverter(ScalarConverter):
    # No conversion is performed since pyarrow already handles conversion for
    # types in the `datetime` module.

    def _to_pyspark_value(self, data: Any) -> Any:
        return data

    def _from_pyspark_value(self, data: Any) -> Any:
        return data


def _to_string(data: Any) -> str | None:
    """Converts data to string where the behavior is similar to the Java `Object.toString()` method."""
    _raise_for_row(data)
    if data is None:
        return None
    if isinstance(data, str):
        return data
    if isinstance(data, bool):
        return "true" if data else "false"
    if isinstance(data, list | tuple):
        items = ", ".join(_to_string(x) for x in data)
        return f"[{items}]"
    if isinstance(data, dict):
        items = ", ".join(f"{_to_string(k)}={_to_string(v)}" for k, v in data.items())
        return f"{{{items}}}"
    return str(data)


class StringConverter(ScalarConverter):
    def _to_pyspark_value(self, data: Any) -> Any:
        return data

    def _from_pyspark_value(self, data: Any) -> Any:
        return _to_string(data)


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


class BinaryConverter(ScalarConverter):
    def _to_pyspark_value(self, data: Any) -> Any:
        return None if data is None else bytearray(data)

    def _from_pyspark_value(self, data: Any) -> Any:
        return _to_bytes(data)


class UdtConverter(Converter):
    def __init__(self, field: pa.Field):
        super().__init__(field.type)
        self._spark_data_type = _spark_type_from_field(field)
        self._sql_converter = _get_converter(field.type)

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        values = self._sql_converter.to_pyspark(array)
        return [None if x is None else self._spark_data_type.fromInternal(x) for x in values]

    def from_pyspark(self, data: Sequence[Any]) -> pa.Array:
        values = [None if x is None else self._spark_data_type.toInternal(x) for x in data]
        return self._sql_converter.from_pyspark(values)


class ArrayConverter(Converter):
    def __init__(self, field_or_type: pa.Field | _PyArrowListType):
        _, data_type = _field_type(field_or_type)
        super().__init__(data_type)
        self._value_converter = _get_converter(_list_value_field(data_type))

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
            if x is None or not isinstance(x, list | tuple):
                offsets.append(None)
            else:
                offsets.append(end)
                values.extend(x)
                end += len(x)
        offsets.append(end)
        return pa.ListArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            self._value_converter.from_pyspark(values),
            type=self._data_type,
        )


class MapConverter(Converter):
    def __init__(self, field_or_type: pa.Field | pa.MapType):
        _, data_type = _field_type(field_or_type)
        super().__init__(data_type)
        self._key_converter = _get_converter(_map_key_field(data_type))
        self._value_converter = _get_converter(_map_item_field(data_type))

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
                result.append(dict(zip(keys[start:end], values[start:end], strict=True)))
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
            type=self._data_type,
        )


class StructConverter(Converter):
    def __init__(self, field_or_type: pa.Field | pa.StructType):
        field, data_type = _field_type(field_or_type)
        super().__init__(data_type)
        try:
            self._fields = data_type.fields
        except AttributeError:
            self._fields = [data_type.field(i) for i in range(data_type.num_fields)]
        self._field_converters = [_get_converter(f) for f in self._fields]
        self._spark_data_type = _spark_type_from_field(field or data_type)

    def to_pyspark(self, array: pa.Array) -> Sequence[Any]:
        if not isinstance(array, pa.StructArray):
            msg = f"invalid data type for struct: {type(array)}"
            raise TypeError(msg)
        columns = [c.to_pyspark(col) for col, c in zip(array.flatten(), self._field_converters, strict=True)]
        return [self._spark_data_type.fromInternal(x) for x in zip(*columns, strict=True)]

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
            [c.from_pyspark(col) for col, c in zip(columns, self._field_converters, strict=True)],
            fields=self._fields,
            mask=pa.array(mask, type=pa.bool_()),
        )


if pyspark.__version__.startswith(("3.", "4.0.")):

    def _arrow_column_to_pandas(column: pa.Array, serializer: ArrowStreamPandasUDFSerializer):
        return serializer.arrow_to_pandas(column)
else:

    def _arrow_column_to_pandas(column: pa.Array, serializer: ArrowStreamPandasUDFSerializer):
        return serializer.arrow_to_pandas(column, 0)


def _pandas_to_arrow_array(
    data,
    data_type: pa.DataType,
    serializer: ArrowStreamPandasUDFSerializer,
    spark_type=None,
) -> pa.Array:
    if serializer._struct_in_pandas == "dict" and pa.types.is_struct(data_type):  # noqa: SLF001
        return serializer._create_struct_array(data, data_type)  # noqa: SLF001
    return serializer._create_array(data, data_type, spark_type=spark_type, arrow_cast=serializer._arrow_cast)  # noqa: SLF001


def _arrow_columns_to_python(
    args: list[pa.Array],
    input_fields: Sequence[pa.Field] | None = None,
    *,
    binary_as_bytes: bool = True,
) -> tuple:
    """Convert Arrow arrays to Python lists with proper type conversion,
    matching PySpark's ArrowBatchUDFSerializer.load_stream.
    Uses ArrowTableToRowsConversion to convert Arrow-native representations
    to Python-native types (e.g. map arrays become dicts, not list-of-tuples)."""
    # `none_on_identity=True` returns None when no conversion is needed for the type,
    # so we can skip the per-element loop and pass the list directly.
    spark_input_types = (
        [_spark_type_from_field(f) for f in input_fields]
        if input_fields is not None
        else [from_arrow_type(a.type) for a in args]
    )
    converters = [
        _ArrowTableToRowsConversion._create_converter(  # noqa: SLF001
            spark_type,
            none_on_identity=True,
            binary_as_bytes=binary_as_bytes,
        )
        for spark_type in spark_input_types
    ]
    return tuple(
        [conv(v) for v in a.to_pylist()] if conv is not None else a.to_pylist()
        for a, conv in zip(args, converters, strict=False)
    )


def _python_values_to_arrow_array(
    data: list,
    arrow_type: pa.DataType,
    spark_type,
    *,
    int_to_decimal_coercion_enabled: bool = False,
    safecheck: bool = False,
) -> pa.Array:
    """Convert a Python list of values to an Arrow array, using
    PySpark's LocalDataToArrowConversion for proper type coercion
    (e.g. int -> Decimal)."""
    # `none_on_identity=True` returns None when no conversion is needed for the type,
    # so we can skip the per-element loop and pass the list directly to `pa.array()`.
    conv = _LocalDataToArrowConversion._create_converter(  # noqa: SLF001
        spark_type,
        none_on_identity=True,
        int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
    )
    converted = [conv(v) for v in data] if conv is not None else data
    try:
        return pa.array(converted, type=arrow_type)
    except pa.lib.ArrowInvalid:
        return pa.array(converted).cast(target_type=arrow_type, safe=safecheck)


def _arrow_array_to_output_type(data, data_type: pa.DataType) -> pa.Array:
    if not isinstance(data, list):
        data = list(data)
    if len(data) == 0:
        return pa.array([], type=data_type)

    struct_arrays = []

    for batch in data:
        if len(data_type.fields) != batch.num_columns:
            error = f"column number doesn't match: expected {len(data_type.fields)}, got {batch.num_columns}"
            raise ValueError(error)

        arrays = []
        names = []

        for i, target_field in enumerate(data_type.fields):
            target_name = target_field.name
            target_type = target_field.type

            source_array = batch[target_name] if target_name in batch.schema.names else batch.column(i)

            if source_array.type != target_type:
                source_array = source_array.cast(target_type)

            arrays.append(source_array)
            names.append(target_name)

        struct_arrays.append(pa.StructArray.from_arrays(arrays, names=names))

    return pa.concat_arrays(struct_arrays)


def _named_arrays_to_pandas(
    data: Sequence[pa.Array], names: Sequence[str], serializer: ArrowStreamPandasUDFSerializer
) -> Sequence[pd.Series]:
    inputs = [_arrow_column_to_pandas(x, serializer) for x in data]
    for x, name in zip(inputs, names, strict=True):
        x.name = name
    return inputs


class PySparkBatchUdf:
    def __init__(self, udf: Callable[..., Any], input_fields: Sequence[pa.Field], output_field: pa.Field):
        self._udf = udf
        self._input_converters = [_get_converter(f) for f in input_fields]
        self._output_converter = _get_converter(_strip_udt_metadata_from_field(output_field))

    def __call__(self, args: list[pa.Array], num_rows: int) -> pa.Array:
        if len(args) > 0:
            inputs = [c.to_pyspark(a) for a, c in zip(args, self._input_converters, strict=True)]
            output = list(self._udf(None, zip(*inputs, strict=True)))
        else:
            output = list(self._udf(None, itertools.repeat((), num_rows)))
        return self._output_converter.from_pyspark(output)


class PySparkArrowBatchUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_fields: Sequence[pa.Field],
        output_field: pa.Field,
        config,
    ):
        self._udf = udf
        self._input_fields = input_fields
        self._input_converters = [_get_converter(f) for f in input_fields]
        self._input_has_udt = [_field_has_udt(f) for f in input_fields]
        self._output_spark_type = _spark_type_from_field(output_field)
        self._use_legacy = config.python_udf_pandas_conversion_enabled or pyspark.__version__.startswith(("3.", "4.0."))
        if self._use_legacy:
            self._serializer = ArrowStreamPandasUDFSerializer(
                timezone=config.session_timezone,
                safecheck=config.arrow_convert_safely,
                assign_cols_by_name=config.assign_columns_by_name,
                df_for_struct=False,
                struct_in_pandas="row",
                ndarray_as_list=True,
                arrow_cast=True,
            )
        else:
            self._binary_as_bytes = config.binary_as_bytes
            self._int_to_decimal_coercion_enabled = config.python_udf_pandas_int_to_decimal_coercion_enabled
            self._safecheck = config.arrow_convert_safely

    def __call__(self, args: list[pa.Array], num_rows: int) -> pa.Array:
        if self._use_legacy:
            return self._call_legacy(args, num_rows)
        return self._call_arrow(args, num_rows)

    def _call_legacy(self, args: list[pa.Array], num_rows: int) -> pa.Array:
        if len(args) == 0:
            inputs = tuple(pd.Series([pyspark._NoValue]).repeat(num_rows) for _ in range(1))  # noqa: SLF001
        else:
            inputs = []
            for array, converter, has_udt in zip(args, self._input_converters, self._input_has_udt, strict=True):
                if has_udt:
                    inputs.append(pd.Series(converter.to_pyspark(array)))
                else:
                    inputs.append(_arrow_column_to_pandas(array, self._serializer))
            inputs = tuple(inputs)
        [result] = list(self._udf(None, (inputs,)))
        output, output_type = result[0], result[1]
        return _pandas_to_arrow_array(output, output_type, self._serializer, self._output_spark_type)

    def _call_arrow(self, args: list[pa.Array], num_rows: int) -> pa.Array:
        # Convert Arrow arrays to Python lists with proper type conversion,
        # matching PySpark's ArrowBatchUDFSerializer.load_stream which calls
        # .to_pylist() with ArrowTableToRowsConversion converters
        # (e.g. map arrays → dicts instead of list-of-tuples).
        inputs = (
            ([pyspark._NoValue] * num_rows,)  # noqa: SLF001
            if len(args) == 0
            else _arrow_columns_to_python(
                args,
                self._input_fields,
                binary_as_bytes=self._binary_as_bytes,
            )
        )
        [result] = list(self._udf(None, (inputs,)))
        output, output_type, spark_return_type = result[0], result[1], result[2]
        spark_return_type = self._output_spark_type or spark_return_type
        if isinstance(output, pa.ChunkedArray):
            output = output.combine_chunks()
        if not isinstance(output, pa.Array):
            output = _python_values_to_arrow_array(
                output,
                output_type,
                spark_return_type,
                int_to_decimal_coercion_enabled=self._int_to_decimal_coercion_enabled,
                safecheck=self._safecheck,
            )
        if output.type != output_type:
            output = output.cast(output_type)
        return output


class PySparkScalarPandasUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        config,
    ):
        self._udf = udf
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(_arrow_column_to_pandas(x, self._serializer) for x in args)
        [(output, output_type)] = list(self._udf(None, (inputs,)))
        return _pandas_to_arrow_array(output, output_type, self._serializer)


class PySparkScalarPandasIterUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        config,
    ):
        self._udf = udf
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(_arrow_column_to_pandas(x, self._serializer) for x in args)
        [(output, output_type)] = list(self._udf(None, [inputs]))
        return _pandas_to_arrow_array(output, output_type, self._serializer)


class PySparkScalarArrowUdf:
    """Arrow-native scalar UDF (eval_type 250).

    The user function receives and returns pyarrow.Array directly.
    No Pandas conversion is performed.
    """

    def __init__(
        self,
        udf: Callable[..., Any],
        _config,
    ):
        self._udf = udf

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(args)
        [(output, output_type)] = list(self._udf(None, (inputs,)))
        if isinstance(output, pa.ChunkedArray):
            output = output.combine_chunks()
        if not isinstance(output, pa.Array):
            msg = f"Arrow UDF (eval_type 250) must return a pyarrow.Array, got {type(output).__name__!r}"
            raise TypeError(msg)
        if output.type != output_type:
            output = output.cast(output_type)
        return output


class PySparkScalarArrowIterUdf:
    """Arrow-native scalar iterator UDF (eval_type 251).

    The user function receives and returns Iterator[pyarrow.Array] directly.
    No Pandas conversion is performed.
    """

    def __init__(
        self,
        udf: Callable[..., Any],
        _config,
    ):
        self._udf = udf

    def __call__(self, args: list[pa.Array], _num_rows: int) -> pa.Array:
        inputs = tuple(args)
        [(output, output_type)] = list(self._udf(None, [inputs]))
        if isinstance(output, pa.ChunkedArray):
            output = output.combine_chunks()
        if not isinstance(output, pa.Array):
            msg = f"Arrow iterator UDF (eval_type 251) must return a pyarrow.Array, got {type(output).__name__!r}"
            raise TypeError(msg)
        if output.type != output_type:
            output = output.cast(output_type)
        return output


class PySparkGroupAggUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_names: Sequence[str],
        config,
    ):
        self._udf = udf
        self._input_names = input_names
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array]) -> pa.Array:
        inputs = _named_arrays_to_pandas(args, self._input_names, self._serializer)
        [(output, output_type)] = list(self._udf(None, (inputs,)))
        return _pandas_to_arrow_array(output, output_type, self._serializer)


class PySparkGroupAggArrowUdf:
    """Arrow-native grouped aggregate UDF (eval_type 252).

    The user function receives pyarrow.Array columns directly and returns
    a scalar. No Pandas conversion is performed.
    """

    def __init__(
        self,
        udf: Callable[..., Any],
        _config,
    ):
        self._udf = udf

    def __call__(self, args: list[pa.Array]) -> pa.Array:
        [(output, output_type)] = list(self._udf(None, (args,)))
        if isinstance(output, pa.ChunkedArray):
            output = output.combine_chunks()
        if not isinstance(output, pa.Array):
            msg = (
                f"Arrow grouped aggregate UDF (eval_type 252) must return a pyarrow.Array, "
                f"got {type(output).__name__!r}"
            )
            raise TypeError(msg)
        if output.type != output_type:
            output = output.cast(output_type)
        return output


class PySparkGroupMapUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_names: Sequence[str],
        is_pandas: bool,  # noqa: FBT001
        config,
    ):
        self._udf = udf
        self._input_names = input_names
        self.is_pandas = is_pandas
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: list[pa.Array]) -> pa.Array:
        if self.is_pandas:
            inputs = _named_arrays_to_pandas(args, self._input_names, self._serializer)
            [[(output, output_type)]] = list(self._udf(None, (inputs,)))
            return _pandas_to_arrow_array(output, output_type, self._serializer)

        inputs = [pa.RecordBatch.from_arrays(args, self._input_names)]
        [(output, output_type)] = list(self._udf(None, (inputs,)))
        return _arrow_array_to_output_type(output, output_type)


class PySparkCoGroupMapUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        left_names: Sequence[str],
        right_names: Sequence[str],
        is_pandas: bool,  # noqa: FBT001
        config,
    ):
        self._udf = udf
        self._left_names = left_names
        self._right_names = right_names
        self.is_pandas = is_pandas
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, left: list[pa.Array], right: list[pa.Array]) -> pa.Array:
        if self.is_pandas:
            inputs = [
                _named_arrays_to_pandas(left, self._left_names, self._serializer),
                _named_arrays_to_pandas(right, self._right_names, self._serializer),
            ]
            [[(output, output_type)]] = list(self._udf(None, (inputs,)))
            return _pandas_to_arrow_array(output, output_type, self._serializer)

        inputs = [
            [pa.RecordBatch.from_arrays(left, self._left_names)],
            [pa.RecordBatch.from_arrays(right, self._right_names)],
        ]
        [(output, output_type)] = list(self._udf(None, (inputs,)))
        return _arrow_array_to_output_type(output, output_type)


class PySparkMapPandasIterUdf:
    def __init__(
        self,
        udf: Callable[..., Iterator[pd.DataFrame]],
        config,
    ):
        self._udf = udf
        self._serializer = ArrowStreamPandasUDFSerializer(
            timezone=config.session_timezone,
            safecheck=config.arrow_convert_safely,
            assign_cols_by_name=config.assign_columns_by_name,
            df_for_struct=True,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
        )

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        output = self._udf(None, ((self._convert_input(x),) for x in args))
        return (self._convert_output(x, t) for x, t in output)

    def _convert_input(self, batch: pa.RecordBatch) -> pd.DataFrame:
        return _arrow_column_to_pandas(batch.to_struct_array(), self._serializer)

    def _convert_output(self, df: pd.DataFrame, data_type: pa.DataType) -> pa.RecordBatch:
        array = _pandas_to_arrow_array(df, data_type, self._serializer)
        return pa.RecordBatch.from_struct_array(array)


class PySparkMapArrowIterUdf:
    def __init__(self, udf: Callable[..., Iterator[pa.RecordBatch]]):
        self._udf = udf

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        output = self._udf(None, ((x,) for x in args))
        return (x for x, _ in output)


class PySparkTableUdf:
    def __init__(
        self,
        udf: Callable[..., Any],
        input_types: Sequence[pa.DataType],
        passthrough_columns: int,
        output_schema: pa.Schema,
        config,
    ):
        self._udf = udf
        self._passthrough_columns = passthrough_columns
        self._batch_size = config.arrow_max_records_per_batch
        if self._batch_size <= 0:
            msg = f"invalid batch size: {self._batch_size}"
            raise ValueError(msg)
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
                inputs = tuple(c.to_pyspark(a) for a, c in zip(arrays, self._input_converters, strict=True))
                yield from zip(*inputs, strict=True)
            else:
                yield ()

    def _iter_output_rows(self, args: Iterator[pa.RecordBatch]) -> Iterator[list]:
        args1, args2 = itertools.tee(args)
        if sum(1 for _ in args2) == 0:
            return
        (rows1, rows2) = itertools.tee(self._iter_input_rows(args1))
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
        input_types: Sequence[pa.DataType],
        passthrough_columns: int,
        output_schema: pa.Schema,
        config,
    ):
        self._udf = udf
        self._input_names = input_names
        self._passthrough_columns = passthrough_columns
        self._output_schema = output_schema
        self._output_type = pa.struct([output_schema.field(i) for i in range(len(output_schema.names))])
        self._use_legacy = config.python_udtf_pandas_conversion_enabled or pyspark.__version__.startswith(
            ("3.", "4.0.")
        )
        if self._use_legacy:
            if pyspark.__version__.startswith(("3.", "4.0.")):
                self._serializer = ArrowStreamPandasUDTFSerializer(
                    timezone=config.session_timezone, safecheck=config.arrow_convert_safely
                )
            else:
                spark_input_types = [from_arrow_type(t) for t in input_types]
                self._serializer = ArrowStreamPandasUDTFSerializer(
                    timezone=config.session_timezone,
                    safecheck=config.arrow_convert_safely,
                    input_types=spark_input_types,
                    int_to_decimal_coercion_enabled=config.python_udf_pandas_int_to_decimal_coercion_enabled,
                )

    def __call__(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        if self._use_legacy:
            return self._call_legacy(args)
        return self._call_arrow(args)

    def _call_arrow(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        first = next(args, None)
        if first is None:
            return
        args = itertools.chain([first], args)
        # Tee: one branch for the mapper (full batches), one for passthrough extraction.
        # The mapper expects full input batches — it handles Python conversion and column
        # selection internally (via converters + args_kwargs_offsets).
        batches_for_passthrough, batches_for_mapper = itertools.tee(args)
        outputs = self._udf(None, batches_for_mapper)
        # Extract passthrough values from raw arrow batches (no pandas)
        columns = (tuple(batch.column(i) for i in range(batch.num_columns)) for batch in batches_for_passthrough)
        last = None
        for passthrough, (out, _) in itertools.zip_longest(self._iter_passthrough(columns), outputs):
            if out is None or out.num_rows == 0:
                continue
            out_batch = out.combine_chunks().to_batches()[0] if isinstance(out, pa.Table) else out
            num_rows = out_batch.num_rows
            if passthrough is None:
                passthrough = last  # noqa: PLW2901
            passthrough_arrays = []
            passthrough_fields = []
            for i, name in enumerate(self._input_names[: self._passthrough_columns]):
                field = self._output_schema.field(self._output_schema.get_field_index(name))
                if passthrough is not None:
                    val = passthrough[i].as_py() if isinstance(passthrough[i], pa.Scalar) else passthrough[i]
                    passthrough_arrays.append(pa.array([val] * num_rows, type=field.type))
                else:
                    passthrough_arrays.append(pa.nulls(num_rows).cast(field.type))
                passthrough_fields.append(field)
            yield pa.RecordBatch.from_arrays(
                passthrough_arrays + [out_batch.column(i) for i in range(out_batch.num_columns)],
                schema=pa.schema(
                    passthrough_fields + [out_batch.schema.field(i) for i in range(out_batch.num_columns)]
                ),
            )
            last = passthrough

    def _call_legacy(self, args: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for output in self._iter_output_legacy(args):
            array = self._serializer._create_struct_array(output, self._output_type)  # noqa: SLF001
            yield pa.RecordBatch.from_struct_array(array)

    def _iter_input_legacy(self, args: Iterator[pa.RecordBatch]) -> Iterator[tuple[pd.Series]]:
        for batch in args:
            arrays = batch.to_struct_array().flatten()
            columns = tuple(_arrow_column_to_pandas(x, self._serializer) for x in arrays)
            if len(columns) == 0 and not pyspark.__version__.startswith(("3.", "4.0.")):
                # PySpark 4.1+ legacy mapper uses len(a[0]) for num_rows.
                # Preserve the row count for 0-column batches (e.g. 0-arg UDTFs).
                yield (pd.Series(range(batch.num_rows)),)
            else:
                yield columns

    def _iter_output_legacy(self, args: Iterator[pa.RecordBatch]) -> Iterator[pd.DataFrame]:
        args1, args2 = itertools.tee(args)
        if sum(1 for _ in args2) == 0:
            return
        (batches1, batches2) = itertools.tee(self._iter_input_legacy(args1))
        if pyspark.__version__.startswith(("3.", "4.0.")):
            # PySpark 3.x/4.0 mapper receives sliced non-passthrough columns.
            inputs = (x[self._passthrough_columns :] for x in batches2)
        else:
            # PySpark 4.1+ mapper receives full tuples and selects columns via args_kwargs_offsets.
            inputs = batches2
        outputs = self._udf(None, inputs)
        last = None
        for passthrough, (out, *_) in itertools.zip_longest(self._iter_passthrough(batches1), outputs):
            if out is None or len(out) == 0:
                continue
            df = pd.DataFrame(index=out.index)
            if passthrough is None:
                passthrough = last  # noqa: PLW2901
            if passthrough is not None:
                for v, name in zip(passthrough, self._input_names, strict=False):
                    df[name] = [v] * len(out)
            else:
                for name in self._input_names:
                    df[name] = [None] * len(out)
            for col in out:
                df[col] = out[col]
            yield df
            last = passthrough

    def _iter_passthrough(self, batches: Iterator[tuple]) -> Iterator[tuple]:
        if self._passthrough_columns > 0:
            for batch in batches:
                yield from zip(*batch[: self._passthrough_columns], strict=True)
        else:
            for batch in batches:
                if len(batch) > 0:
                    first, *_ = batch
                    for _ in range(len(first)):
                        yield ()
                else:
                    yield ()


def analyze_udtf(handler, arguments):
    """
    Call the Python UDTF's ``analyze`` static method to determine the return type.

    Parameters
    ----------
    handler : type
        The UDTF class (already loaded and unpickled).
    arguments : list of tuples (arrow_type, is_constant, value_array, kwarg_name, is_table)
        - arrow_type: a PyArrow data type for this argument
        - is_constant: bool, whether the argument is a constant expression
        - value_array: a single-element PyArrow array with the literal value, or None
        - kwarg_name: str or None, the keyword argument name if applicable
        - is_table: bool, whether the argument is a TABLE argument

    Returns
    -------
    pa.Schema
        A PyArrow schema representing the UDTF's return type.
    """
    import inspect
    from textwrap import dedent

    from pyspark.sql.pandas.types import from_arrow_type, to_arrow_schema
    from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult

    args = []
    kwargs = {}

    for arrow_type, is_constant, value_array, kwarg_name, is_table in arguments:
        spark_type = from_arrow_type(arrow_type)
        value = value_array.to_pylist()[0] if value_array is not None else None
        arg = AnalyzeArgument(
            dataType=spark_type,
            value=value,
            isTable=is_table,
            isConstantExpression=is_constant,
        )
        if kwarg_name is not None:
            kwargs[kwarg_name] = arg
        else:
            args.append(arg)

    udtf_name = getattr(handler, "__name__", "")
    error_prefix = f"Failed to evaluate the user-defined table function '{udtf_name}'"

    def format_error(msg):
        return dedent(msg).replace("\n", " ")

    # Check that the arguments match the analyze() signature before calling it.
    try:
        inspect.signature(handler.analyze).bind(*args, **kwargs)
    except TypeError as e:
        raise TypeError(
            format_error(
                f"""
                {error_prefix} because the function arguments did not match the expected
                signature of the static 'analyze' method ({e}). Please update the query so that
                this table function call provides arguments matching the expected signature, or
                else update the table function so that its static 'analyze' method accepts the
                provided arguments, and then try the query again."""
            )
        ) from e

    result = handler.analyze(*args, **kwargs)

    if not isinstance(result, AnalyzeResult):
        raise TypeError(
            format_error(
                f"""
                {error_prefix} because the static 'analyze' method expects a result of type
                pyspark.sql.udtf.AnalyzeResult, but instead this method returned a value of
                type: {type(result)}"""
            )
        )

    return to_arrow_schema(result.schema)
