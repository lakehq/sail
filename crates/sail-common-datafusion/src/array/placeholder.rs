use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, ArrayData, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BinaryArray,
    BinaryViewArray, BooleanBuilder, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
    LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray, ListArray,
    ListViewArray, MapArray, NullArray, PrimitiveArray, PrimitiveBuilder, RunArray, StringArray,
    StringViewArray, StructArray, UnionArray,
};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type,
    Decimal32Type, Decimal64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    UnionMode,
};
use datafusion_common::internal_err;

/// Create a non-null placeholder array of the specified data type and length.
pub fn placeholder_array(
    data_type: &DataType,
    length: usize,
) -> datafusion_common::Result<ArrayRef> {
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(length))),
        DataType::Boolean => Ok(placeholder_boolean_array(length)),
        DataType::Int8 => Ok(placeholder_primitive_array::<Int8Type>(length)),
        DataType::Int16 => Ok(placeholder_primitive_array::<Int16Type>(length)),
        DataType::Int32 => Ok(placeholder_primitive_array::<Int32Type>(length)),
        DataType::Int64 => Ok(placeholder_primitive_array::<Int64Type>(length)),
        DataType::UInt8 => Ok(placeholder_primitive_array::<UInt8Type>(length)),
        DataType::UInt16 => Ok(placeholder_primitive_array::<UInt16Type>(length)),
        DataType::UInt32 => Ok(placeholder_primitive_array::<UInt32Type>(length)),
        DataType::UInt64 => Ok(placeholder_primitive_array::<UInt64Type>(length)),
        DataType::Float16 => Ok(placeholder_primitive_array::<Float16Type>(length)),
        DataType::Float32 => Ok(placeholder_primitive_array::<Float32Type>(length)),
        DataType::Float64 => Ok(placeholder_primitive_array::<Float64Type>(length)),
        DataType::Timestamp(time_unit, tz) => {
            match time_unit {
                TimeUnit::Second => Ok(placeholder_timestamp_array::<TimestampSecondType>(
                    length,
                    tz.clone(),
                )),
                TimeUnit::Millisecond => Ok(
                    placeholder_timestamp_array::<TimestampMillisecondType>(length, tz.clone()),
                ),
                TimeUnit::Microsecond => Ok(
                    placeholder_timestamp_array::<TimestampMicrosecondType>(length, tz.clone()),
                ),
                TimeUnit::Nanosecond => Ok(placeholder_timestamp_array::<TimestampNanosecondType>(
                    length,
                    tz.clone(),
                )),
            }
        }
        DataType::Date32 => Ok(placeholder_primitive_array::<Date32Type>(length)),
        DataType::Date64 => Ok(placeholder_primitive_array::<Date64Type>(length)),
        DataType::Time32(time_unit) => match time_unit {
            TimeUnit::Second => Ok(placeholder_primitive_array::<Time32SecondType>(length)),
            TimeUnit::Millisecond => {
                Ok(placeholder_primitive_array::<Time32MillisecondType>(length))
            }
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                internal_err!("invalid data type: {data_type:?}")
            }
        },
        DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                internal_err!("invalid data type: {data_type:?}")
            }
            TimeUnit::Microsecond => {
                Ok(placeholder_primitive_array::<Time64MicrosecondType>(length))
            }
            TimeUnit::Nanosecond => Ok(placeholder_primitive_array::<Time64NanosecondType>(length)),
        },
        DataType::Duration(time_unit) => match time_unit {
            TimeUnit::Second => Ok(placeholder_primitive_array::<DurationSecondType>(length)),
            TimeUnit::Millisecond => Ok(placeholder_primitive_array::<DurationMillisecondType>(
                length,
            )),
            TimeUnit::Microsecond => Ok(placeholder_primitive_array::<DurationMicrosecondType>(
                length,
            )),
            TimeUnit::Nanosecond => Ok(placeholder_primitive_array::<DurationNanosecondType>(
                length,
            )),
        },
        DataType::Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                Ok(placeholder_primitive_array::<IntervalYearMonthType>(length))
            }
            IntervalUnit::DayTime => Ok(placeholder_primitive_array::<IntervalDayTimeType>(length)),
            IntervalUnit::MonthDayNano => Ok(
                placeholder_primitive_array::<IntervalMonthDayNanoType>(length),
            ),
        },
        DataType::Binary => {
            let offsets = vec![0i32; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(Vec::<u8>::new()))
                .build()?;
            Ok(Arc::new(BinaryArray::from(array_data)))
        }
        DataType::FixedSizeBinary(size) => {
            let values = vec![0u8; *size as usize * length];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(values))
                .build()?;
            Ok(Arc::new(FixedSizeBinaryArray::from(array_data)))
        }
        DataType::LargeBinary => {
            let offsets = vec![0i64; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(Vec::<u8>::new()))
                .build()?;
            Ok(Arc::new(LargeBinaryArray::from(array_data)))
        }
        DataType::BinaryView => {
            let views = vec![0i128; length];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(views))
                .build()?;
            Ok(Arc::new(BinaryViewArray::from(array_data)))
        }
        DataType::Utf8 => {
            let offsets = vec![0i32; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(Vec::<u8>::new()))
                .build()?;
            Ok(Arc::new(StringArray::from(array_data)))
        }
        DataType::LargeUtf8 => {
            let offsets = vec![0i64; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(Vec::<u8>::new()))
                .build()?;
            Ok(Arc::new(LargeStringArray::from(array_data)))
        }
        DataType::Utf8View => {
            let views = vec![0i128; length];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(views))
                .build()?;
            Ok(Arc::new(StringViewArray::from(array_data)))
        }
        DataType::List(field) => {
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), 0)
            } else {
                placeholder_array(field.data_type(), 0)?
            };
            let offsets = vec![0i32; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(ListArray::from(array_data)))
        }
        DataType::ListView(field) => {
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), 0)
            } else {
                placeholder_array(field.data_type(), 0)?
            };
            let offsets = vec![0i32; length];
            let sizes = vec![0i32; length];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(sizes))
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(ListViewArray::from(array_data)))
        }
        DataType::FixedSizeList(field, list_size) => {
            let child_length = (*list_size as usize) * length;
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), child_length)
            } else {
                placeholder_array(field.data_type(), child_length)?
            };
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(FixedSizeListArray::from(array_data)))
        }
        DataType::LargeList(field) => {
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), 0)
            } else {
                placeholder_array(field.data_type(), 0)?
            };
            let offsets = vec![0i64; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(LargeListArray::from(array_data)))
        }
        DataType::LargeListView(field) => {
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), 0)
            } else {
                placeholder_array(field.data_type(), 0)?
            };
            let offsets = vec![0i64; length];
            let sizes = vec![0i64; length];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_buffer(Buffer::from(sizes))
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(LargeListViewArray::from(array_data)))
        }
        DataType::Struct(fields) => {
            let mut child_data = Vec::new();
            for field in fields {
                let child_array = if field.is_nullable() {
                    new_null_array(field.data_type(), length)
                } else {
                    placeholder_array(field.data_type(), length)?
                };
                child_data.push(child_array.to_data());
            }
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .child_data(child_data)
                .build()?;
            Ok(Arc::new(StructArray::from(array_data)))
        }
        DataType::Union(union_fields, union_mode) => {
            // use first type ID for all entries
            let Some(first_type_id) = union_fields.iter().next().map(|(type_id, _)| type_id) else {
                return internal_err!("union type must have at least one field");
            };
            // create type IDs buffer
            let type_ids: ScalarBuffer<i8> = vec![first_type_id; length].into();

            // create child arrays for each field in the union
            let mut children = Vec::new();
            for (type_id, field) in union_fields.iter() {
                let child_length = match union_mode {
                    UnionMode::Sparse => length,
                    UnionMode::Dense => {
                        if type_id == first_type_id {
                            length
                        } else {
                            0
                        }
                    }
                };
                let child_array = if field.is_nullable() {
                    new_null_array(field.data_type(), child_length)
                } else {
                    placeholder_array(field.data_type(), child_length)?
                };
                children.push(child_array);
            }

            let offsets = match union_mode {
                UnionMode::Dense => Some((0i32..length as i32).collect::<Vec<_>>().into()),
                UnionMode::Sparse => None,
            };

            let union_array =
                UnionArray::try_new(union_fields.clone(), type_ids, offsets, children)?;

            Ok(Arc::new(union_array))
        }
        DataType::Dictionary(key_type, value_type) => {
            // create a minimal values array (single element)
            let values_array = placeholder_array(value_type, 1)?;

            // create keys array with all zeros pointing to the single value
            match key_type.as_ref() {
                DataType::Int8 => {
                    let keys = PrimitiveArray::<Int8Type>::from(vec![0i8; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::Int16 => {
                    let keys = PrimitiveArray::<Int16Type>::from(vec![0i16; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::Int32 => {
                    let keys = PrimitiveArray::<Int32Type>::from(vec![0i32; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::Int64 => {
                    let keys = PrimitiveArray::<Int64Type>::from(vec![0i64; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::UInt8 => {
                    let keys = PrimitiveArray::<UInt8Type>::from(vec![0u8; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::UInt16 => {
                    let keys = PrimitiveArray::<UInt16Type>::from(vec![0u16; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::UInt32 => {
                    let keys = PrimitiveArray::<UInt32Type>::from(vec![0u32; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                DataType::UInt64 => {
                    let keys = PrimitiveArray::<UInt64Type>::from(vec![0u64; length]);
                    let dict_array = DictionaryArray::try_new(keys, values_array)?;
                    Ok(Arc::new(dict_array))
                }
                _ => internal_err!("invalid dictionary key type: {key_type:?}"),
            }
        }
        DataType::Decimal32(_, _) => Ok(placeholder_primitive_array::<Decimal32Type>(length)),
        DataType::Decimal64(_, _) => Ok(placeholder_primitive_array::<Decimal64Type>(length)),
        DataType::Decimal128(_, _) => Ok(placeholder_primitive_array::<Decimal128Type>(length)),
        DataType::Decimal256(_, _) => Ok(placeholder_primitive_array::<Decimal256Type>(length)),
        DataType::Map(field, _) => {
            let child_array = if field.is_nullable() {
                new_null_array(field.data_type(), 0)
            } else {
                placeholder_array(field.data_type(), 0)?
            };
            let offsets = vec![0i32; length + 1];
            let array_data = ArrayData::builder(data_type.clone())
                .len(length)
                .add_buffer(Buffer::from(offsets))
                .add_child_data(child_array.to_data())
                .build()?;
            Ok(Arc::new(MapArray::from(array_data)))
        }
        DataType::RunEndEncoded(run_ends_field, values_field) => {
            // create a minimal values array (single element)
            let values_array = if values_field.is_nullable() {
                new_null_array(values_field.data_type(), 1)
            } else {
                placeholder_array(values_field.data_type(), 1)?
            };

            // create run-end array where all values point to the single run
            match run_ends_field.data_type() {
                DataType::Int16 => {
                    let run_ends = PrimitiveArray::<Int16Type>::from(vec![length as i16]);
                    let run_array = RunArray::try_new(&run_ends, &*values_array)?;
                    Ok(Arc::new(run_array))
                }
                DataType::Int32 => {
                    let run_ends = PrimitiveArray::<Int32Type>::from(vec![length as i32]);
                    let run_array = RunArray::try_new(&run_ends, &*values_array)?;
                    Ok(Arc::new(run_array))
                }
                DataType::Int64 => {
                    let run_ends = PrimitiveArray::<Int64Type>::from(vec![length as i64]);
                    let run_array = RunArray::try_new(&run_ends, &*values_array)?;
                    Ok(Arc::new(run_array))
                }
                _ => internal_err!("invalid run ends type: {:?}", run_ends_field.data_type()),
            }
        }
    }
}

pub fn placeholder_boolean_array(length: usize) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(length);
    for _ in 0..length {
        builder.append_value(false);
    }
    Arc::new(builder.finish())
}

pub fn placeholder_primitive_array<T: ArrowPrimitiveType>(length: usize) -> ArrayRef {
    let mut builder = <PrimitiveBuilder<T>>::with_capacity(length);
    for _ in 0..length {
        builder.append_value(T::Native::ZERO);
    }
    Arc::new(builder.finish())
}

pub fn placeholder_timestamp_array<T: ArrowTimestampType + ArrowPrimitiveType>(
    length: usize,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    let mut builder = <PrimitiveBuilder<T>>::with_capacity(length);
    for _ in 0..length {
        builder.append_value(T::Native::ZERO);
    }
    Arc::new(builder.finish().with_timezone_opt(tz))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::Result;
    use sail_common::spec::{
        SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
        SAIL_MAP_VALUE_FIELD_NAME,
    };

    use super::*;

    fn test_placeholder_array(data_type: DataType, length: usize) -> Result<()> {
        let array = placeholder_array(&data_type, length)?;
        assert_eq!(array.len(), length);
        // The null count is 0 even for `DataType::Null` since
        // `NullArray` contains logical nulls rather than physical nulls.
        assert_eq!(array.null_count(), 0);
        for i in 0..length {
            assert!(array.is_valid(i));
        }
        Ok(())
    }

    #[test]
    fn test_all_placeholder_arrays() -> Result<()> {
        let test_cases = vec![
            (DataType::Null, 10),
            (DataType::Boolean, 10),
            (DataType::Int8, 10),
            (DataType::Int16, 10),
            (DataType::Int32, 10),
            (DataType::Int64, 10),
            (DataType::UInt8, 10),
            (DataType::UInt16, 10),
            (DataType::UInt32, 10),
            (DataType::UInt64, 10),
            (DataType::Float16, 10),
            (DataType::Float32, 10),
            (DataType::Float64, 10),
            (DataType::Timestamp(TimeUnit::Second, None), 10),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                10,
            ),
            (DataType::Timestamp(TimeUnit::Microsecond, None), 10),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("PST"))),
                10,
            ),
            (DataType::Date32, 10),
            (DataType::Date64, 10),
            (DataType::Time32(TimeUnit::Second), 10),
            (DataType::Time32(TimeUnit::Millisecond), 10),
            (DataType::Time64(TimeUnit::Microsecond), 10),
            (DataType::Time64(TimeUnit::Nanosecond), 10),
            (DataType::Duration(TimeUnit::Second), 10),
            (DataType::Duration(TimeUnit::Millisecond), 10),
            (DataType::Duration(TimeUnit::Microsecond), 10),
            (DataType::Duration(TimeUnit::Nanosecond), 10),
            (DataType::Interval(IntervalUnit::YearMonth), 10),
            (DataType::Interval(IntervalUnit::DayTime), 10),
            (DataType::Interval(IntervalUnit::MonthDayNano), 10),
            (DataType::Binary, 10),
            (DataType::FixedSizeBinary(4), 10),
            (DataType::LargeBinary, 10),
            (DataType::BinaryView, 10),
            (DataType::Utf8, 10),
            (DataType::LargeUtf8, 10),
            (DataType::Utf8View, 10),
            (
                DataType::List(Arc::new(Field::new(
                    SAIL_LIST_FIELD_NAME,
                    DataType::Int32,
                    false,
                ))),
                10,
            ),
            (
                DataType::ListView(Arc::new(Field::new(
                    SAIL_LIST_FIELD_NAME,
                    DataType::Int32,
                    false,
                ))),
                10,
            ),
            (
                DataType::FixedSizeList(
                    Arc::new(Field::new(SAIL_LIST_FIELD_NAME, DataType::Int32, false)),
                    3,
                ),
                10,
            ),
            (
                DataType::LargeList(Arc::new(Field::new(
                    SAIL_LIST_FIELD_NAME,
                    DataType::Int32,
                    false,
                ))),
                10,
            ),
            (
                DataType::LargeListView(Arc::new(Field::new(
                    SAIL_LIST_FIELD_NAME,
                    DataType::Int32,
                    false,
                ))),
                10,
            ),
            (
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int32, false),
                        Field::new("b", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                10,
            ),
            (
                DataType::Union(
                    [(0i8, Arc::new(Field::new("a", DataType::Int32, false)))]
                        .into_iter()
                        .collect(),
                    UnionMode::Sparse,
                ),
                10,
            ),
            (
                DataType::Union(
                    [
                        (0i8, Arc::new(Field::new("a", DataType::Int32, false))),
                        (1i8, Arc::new(Field::new("b", DataType::Utf8, false))),
                    ]
                    .into_iter()
                    .collect(),
                    UnionMode::Dense,
                ),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Int32)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Float32)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Boolean)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Float64)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Boolean)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Date32)),
                10,
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Date64)),
                10,
            ),
            (DataType::Decimal128(10, 2), 10),
            (DataType::Decimal256(20, 4), 10),
            (
                DataType::Map(
                    Arc::new(Field::new(
                        SAIL_MAP_FIELD_NAME,
                        DataType::Struct(
                            vec![
                                Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                                Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                10,
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int16, false)),
                    Arc::new(Field::new("values", DataType::Float64, false)),
                ),
                10,
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Utf8, true)),
                ),
                10,
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int64, false)),
                    Arc::new(Field::new("values", DataType::Int32, false)),
                ),
                10,
            ),
        ];
        for (data_type, length) in test_cases {
            test_placeholder_array(data_type, length)?;
        }
        Ok(())
    }
}
