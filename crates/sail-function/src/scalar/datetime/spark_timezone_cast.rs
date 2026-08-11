use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, GenericListViewArray, MapArray,
    PrimitiveArray, StringArray, StructArray,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UnionFields,
};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use super::convert_tz::convert_tz_inner;
use super::spark_timestamp::SparkTimestamp;
use crate::scalar::spark_to_string::spark_to_string_array;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkTimezoneCast {
    signature: Signature,
    target_type: DataType,
    session_timezone: Arc<str>,
    safe: bool,
    struct_by_name: bool,
    case_sensitive: bool,
}

impl SparkTimezoneCast {
    pub fn new(target_type: DataType, session_timezone: Arc<str>, safe: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type: canonicalize_ltz_type(&target_type),
            session_timezone,
            safe,
            struct_by_name: false,
            case_sensitive: false,
        }
    }

    pub fn new_by_name(
        target_type: DataType,
        session_timezone: Arc<str>,
        safe: bool,
        case_sensitive: bool,
    ) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type: canonicalize_ltz_type(&target_type),
            session_timezone,
            safe,
            struct_by_name: true,
            case_sensitive,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    pub fn struct_by_name(&self) -> bool {
        self.struct_by_name
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }
}

fn canonicalize_ltz_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(canonicalize_ltz_type(field.data_type())),
    )
}

fn canonicalize_ltz_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, Some(Arc::from("UTC"))),
        DataType::List(field) => DataType::List(canonicalize_ltz_field(field)),
        DataType::ListView(field) => DataType::ListView(canonicalize_ltz_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(canonicalize_ltz_field(field), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(canonicalize_ltz_field(field)),
        DataType::LargeListView(field) => DataType::LargeListView(canonicalize_ltz_field(field)),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(canonicalize_ltz_field)
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Map(field, sorted) => DataType::Map(canonicalize_ltz_field(field), *sorted),
        DataType::Union(fields, mode) => {
            let type_ids = fields.iter().map(|(type_id, _)| type_id);
            let output_fields = fields
                .iter()
                .map(|(_, field)| canonicalize_ltz_field(field));
            match UnionFields::try_new(type_ids, output_fields) {
                Ok(fields) => DataType::Union(fields, *mode),
                Err(_) => data_type.clone(),
            }
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(canonicalize_ltz_type(key)),
            Box::new(canonicalize_ltz_type(value)),
        ),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            canonicalize_ltz_field(run_ends),
            canonicalize_ltz_field(values),
        ),
        _ => data_type.clone(),
    }
}

impl ScalarUDFImpl for SparkTimezoneCast {
    fn name(&self) -> &str {
        "spark_timezone_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return plan_err!("spark_timezone_cast expects exactly one argument");
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            input.is_nullable()
                || matches!(
                    (input.data_type(), &self.target_type),
                    (
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                        DataType::Timestamp(_, Some(_)),
                    )
                ),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| {
                let [array] = args else {
                    return exec_err!("spark_timezone_cast expects exactly one argument");
                };
                cast_array(
                    array,
                    &self.target_type,
                    &self.session_timezone,
                    self.safe,
                    self.struct_by_name,
                    self.case_sensitive,
                )
            },
            vec![Hint::AcceptsSingular],
        )(args.args.as_slice())
    }
}

fn arrow_cast(array: &ArrayRef, target_type: &DataType, safe: bool) -> Result<ArrayRef> {
    Ok(cast_with_options(
        array.as_ref(),
        target_type,
        &CastOptions {
            safe,
            ..Default::default()
        },
    )?)
}

fn timezone_array(timezone: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![timezone]))
}

fn convert_timezone(
    timestamp: ArrayRef,
    from_timezone: &str,
    to_timezone: &str,
    safe: bool,
) -> Result<ArrayRef> {
    convert_tz_inner(
        &[
            timezone_array(from_timezone),
            timezone_array(to_timezone),
            timestamp,
        ],
        false,
        safe,
    )
}

fn retag_typed<T: ArrowTimestampType>(
    array: &ArrayRef,
    timezone: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let Some(array) = array.as_any().downcast_ref::<PrimitiveArray<T>>() else {
        return exec_err!("expected timestamp array, got {}", array.data_type());
    };
    Ok(Arc::new(array.clone().with_timezone_opt(timezone)))
}

fn retag_timestamp(array: &ArrayRef, timezone: Option<Arc<str>>) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            retag_typed::<TimestampSecondType>(array, timezone)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            retag_typed::<TimestampMillisecondType>(array, timezone)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            retag_typed::<TimestampMicrosecondType>(array, timezone)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            retag_typed::<TimestampNanosecondType>(array, timezone)
        }
        data_type => exec_err!("expected timestamp array, got {data_type}"),
    }
}

fn cast_to_ltz(
    array: &ArrayRef,
    unit: TimeUnit,
    session_timezone: &str,
    safe: bool,
) -> Result<ArrayRef> {
    let timestamp = arrow_cast(array, &DataType::Timestamp(unit, None), safe)?;
    let timestamp = convert_timezone(timestamp, session_timezone, "UTC", safe)?;
    retag_timestamp(&timestamp, Some(Arc::from("UTC")))
}

fn cast_ltz_to_ntz(
    array: &ArrayRef,
    unit: TimeUnit,
    session_timezone: &str,
    safe: bool,
) -> Result<ArrayRef> {
    let timestamp = retag_timestamp(array, None)?;
    let timestamp = arrow_cast(&timestamp, &DataType::Timestamp(unit, None), safe)?;
    convert_timezone(timestamp, "UTC", session_timezone, safe)
}

fn cast_array(
    array: &ArrayRef,
    target_type: &DataType,
    session_timezone: &str,
    safe: bool,
    struct_by_name: bool,
    case_sensitive: bool,
) -> Result<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(Arc::clone(array));
    }

    match (array.data_type(), target_type) {
        (_, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            spark_to_string_array(array.as_ref(), target_type, session_timezone)
        }
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Timestamp(_, Some(_)),
        ) => {
            let timestamp = SparkTimestamp::try_new(Some(Arc::from(session_timezone)), true, safe)?
                .parse_array(array)?;
            cast_array(
                &timestamp,
                target_type,
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )
        }
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
            DataType::Timestamp(unit, Some(_)),
        ) => cast_to_ltz(array, *unit, session_timezone, safe),
        (DataType::Timestamp(_, Some(_)), DataType::Timestamp(unit, Some(timezone))) => {
            // LTZ-to-LTZ preserves the instant. Remove the metadata before
            // changing precision so Arrow never parses Spark-only zone IDs.
            let timestamp = retag_timestamp(array, None)?;
            let timestamp = arrow_cast(&timestamp, &DataType::Timestamp(*unit, None), safe)?;
            retag_timestamp(&timestamp, Some(Arc::clone(timezone)))
        }
        (DataType::Timestamp(_, Some(_)), DataType::Timestamp(unit, None)) => {
            cast_ltz_to_ntz(array, *unit, session_timezone, safe)
        }
        (DataType::Timestamp(source_unit, Some(_)), DataType::Date32 | DataType::Date64) => {
            let timestamp = cast_ltz_to_ntz(array, *source_unit, session_timezone, safe)?;
            arrow_cast(&timestamp, target_type, safe)
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let source = array.as_list::<i32>();
            let values = cast_array(
                source.values(),
                target_field.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            Ok(Arc::new(GenericListArray::<i32>::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(_), DataType::LargeList(target_field)) => {
            let source = array.as_list::<i64>();
            let values = cast_array(
                source.values(),
                target_field.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            Ok(Arc::new(GenericListArray::<i64>::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (DataType::ListView(_), DataType::ListView(target_field)) => {
            let source = array.as_list_view::<i32>();
            let values = cast_array(
                source.values(),
                target_field.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            Ok(Arc::new(GenericListViewArray::<i32>::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                source.sizes().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (DataType::LargeListView(_), DataType::LargeListView(target_field)) => {
            let source = array.as_list_view::<i64>();
            let values = cast_array(
                source.values(),
                target_field.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            Ok(Arc::new(GenericListViewArray::<i64>::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                source.sizes().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        (
            DataType::FixedSizeList(_, source_size),
            DataType::FixedSizeList(target_field, target_size),
        ) => {
            if source_size != target_size {
                return exec_err!(
                    "spark_timezone_cast fixed-size list length mismatch: {source_size} vs {target_size}"
                );
            }
            let source = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_timezone_cast expected FixedSizeListArray".to_string(),
                    )
                })?;
            let values = cast_array(
                source.values(),
                target_field.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(target_field),
                *target_size,
                values,
                source.nulls().cloned(),
            )?))
        }
        (DataType::Map(_, source_sorted), DataType::Map(target_entry, target_sorted)) => {
            if source_sorted != target_sorted {
                return exec_err!(
                    "spark_timezone_cast map sortedness mismatch: {source_sorted} vs {target_sorted}"
                );
            }
            let source = array.as_map();
            let DataType::Struct(target_fields) = target_entry.data_type() else {
                return exec_err!("spark_timezone_cast map entries must be a struct");
            };
            let [target_key, target_value] = target_fields.as_ref() else {
                return exec_err!("spark_timezone_cast map entries must contain key and value");
            };
            let keys = cast_array(
                source.keys(),
                target_key.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            let values = cast_array(
                source.values(),
                target_value.data_type(),
                session_timezone,
                safe,
                struct_by_name,
                case_sensitive,
            )?;
            let entries = StructArray::try_new(
                target_fields.clone(),
                vec![keys, values],
                source.entries().nulls().cloned(),
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(target_entry),
                source.offsets().clone(),
                entries,
                source.nulls().cloned(),
                *target_sorted,
            )?))
        }
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            if source_fields.len() != target_fields.len() {
                return exec_err!(
                    "spark_timezone_cast struct field count mismatch: {} vs {}",
                    source_fields.len(),
                    target_fields.len()
                );
            }
            let source = array.as_struct();
            let columns = if struct_by_name {
                let mut matched = vec![false; source_fields.len()];
                target_fields
                    .iter()
                    .map(|target_field| {
                        let mut matches =
                            source_fields
                                .iter()
                                .enumerate()
                                .filter(|(_, source_field)| {
                                    if case_sensitive {
                                        source_field.name() == target_field.name()
                                    } else {
                                        source_field
                                            .name()
                                            .eq_ignore_ascii_case(target_field.name())
                                    }
                                });
                        let Some((source_index, _)) = matches.next() else {
                            return exec_err!(
                                "spark_timezone_cast missing struct field {}",
                                target_field.name()
                            );
                        };
                        if matches.next().is_some() || matched[source_index] {
                            return exec_err!(
                                "spark_timezone_cast ambiguous struct field {}",
                                target_field.name()
                            );
                        }
                        matched[source_index] = true;
                        cast_array(
                            source.column(source_index),
                            target_field.data_type(),
                            session_timezone,
                            safe,
                            struct_by_name,
                            case_sensitive,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                source
                    .columns()
                    .iter()
                    .zip(target_fields)
                    .map(|(column, target_field)| {
                        cast_array(
                            column,
                            target_field.data_type(),
                            session_timezone,
                            safe,
                            struct_by_name,
                            case_sensitive,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        _ => arrow_cast(array, target_type, safe),
    }
}

/// Cast an Arrow array using Spark's session-zone semantics.
///
/// This is shared by physical writer boundaries that must adapt LTZ and NTZ
/// columns without asking Arrow to parse Spark-only zone identifiers.
pub fn spark_timezone_cast_array(
    array: &ArrayRef,
    target_type: &DataType,
    session_timezone: &str,
    safe: bool,
) -> Result<ArrayRef> {
    cast_array(array, target_type, session_timezone, safe, false, false)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Fields;

    use super::*;

    #[test]
    fn constructor_canonicalizes_nested_ltz_metadata() {
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+01:02:03"))),
            true,
        )));
        let cast = SparkTimezoneCast::new(target, Arc::from("+01:02:03"), false);
        assert_eq!(
            cast.target_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            )))
        );
    }

    #[test]
    fn return_field_marks_only_fallible_top_level_string_casts_nullable() -> Result<()> {
        let udf = SparkTimezoneCast::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            Arc::from("+01:02:03"),
            true,
        );
        let string = Arc::new(Field::new("value", DataType::Utf8, false));
        let string_output = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&string),
            scalar_arguments: &[],
        })?;
        assert!(string_output.is_nullable());

        let ntz = Arc::new(Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));
        let ntz_output = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&ntz),
            scalar_arguments: &[],
        })?;
        assert!(!ntz_output.is_nullable());
        Ok(())
    }

    fn nested_value(array: &ArrayRef) -> ArrayRef {
        match array.data_type() {
            DataType::List(_) => Arc::clone(array.as_list::<i32>().values()),
            DataType::LargeList(_) => Arc::clone(array.as_list::<i64>().values()),
            DataType::ListView(_) => Arc::clone(array.as_list_view::<i32>().values()),
            DataType::LargeListView(_) => Arc::clone(array.as_list_view::<i64>().values()),
            DataType::FixedSizeList(_, _) => Arc::clone(array.as_fixed_size_list().values()),
            DataType::Struct(_) => Arc::clone(array.as_struct().column(0)),
            DataType::Map(_, _) => Arc::clone(array.as_map().values()),
            _ => Arc::clone(array),
        }
    }

    fn nested_timestamp_type(data_type: &DataType) -> DataType {
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        let replace =
            |field: &FieldRef| Arc::new(field.as_ref().clone().with_data_type(timestamp.clone()));
        match data_type {
            DataType::List(field) => DataType::List(replace(field)),
            DataType::LargeList(field) => DataType::LargeList(replace(field)),
            DataType::ListView(field) => DataType::ListView(replace(field)),
            DataType::LargeListView(field) => DataType::LargeListView(replace(field)),
            DataType::FixedSizeList(field, size) => DataType::FixedSizeList(replace(field), *size),
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(replace).collect::<Vec<_>>().into())
            }
            DataType::Map(entries, sorted) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    unreachable!()
                };
                let fields = Fields::from(vec![Arc::clone(&fields[0]), replace(&fields[1])]);
                DataType::Map(
                    Arc::new(
                        entries
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(fields)),
                    ),
                    *sorted,
                )
            }
            _ => timestamp,
        }
    }

    fn assert_nested_string_timestamp_cast(input: ArrayRef) -> Result<()> {
        let timestamp = spark_timezone_cast_array(
            &input,
            &nested_timestamp_type(input.data_type()),
            "+01:02:03",
            false,
        )?;
        assert_eq!(
            nested_value(&timestamp)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            -3_723_000_000
        );

        let string = spark_timezone_cast_array(&timestamp, input.data_type(), "+01:02:03", false)?;
        assert_eq!(
            nested_value(&string).as_string::<i32>().value(0),
            "1970-01-01 00:00:00"
        );
        Ok(())
    }

    #[test]
    fn casts_nested_strings_and_ltz_values_with_session_timezone() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["1970-01-01 00:00:00"]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1]));

        let list: ArrayRef = Arc::new(GenericListArray::<i32>::try_new(
            Arc::clone(&field),
            offsets.clone(),
            Arc::clone(&values),
            None,
        )?);
        let large_list: ArrayRef = Arc::new(GenericListArray::<i64>::try_new(
            Arc::clone(&field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::clone(&values),
            None,
        )?);
        let fixed_size_list: ArrayRef = Arc::new(FixedSizeListArray::try_new(
            Arc::clone(&field),
            1,
            Arc::clone(&values),
            None,
        )?);
        let list_view: ArrayRef = Arc::new(GenericListViewArray::<i32>::try_new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0]),
            ScalarBuffer::from(vec![1]),
            Arc::clone(&values),
            None,
        )?);
        let large_list_view: ArrayRef = Arc::new(GenericListViewArray::<i64>::try_new(
            Arc::clone(&field),
            ScalarBuffer::from(vec![0]),
            ScalarBuffer::from(vec![1]),
            Arc::clone(&values),
            None,
        )?);
        let struct_array: ArrayRef = Arc::new(StructArray::try_new(
            vec![Arc::clone(&field)].into(),
            vec![Arc::clone(&values)],
            None,
        )?);

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries_fields: Fields = vec![Arc::clone(&key_field), Arc::clone(&value_field)].into();
        let entries = StructArray::try_new(
            entries_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["x"])), Arc::clone(&values)],
            None,
        )?;
        let map: ArrayRef = Arc::new(MapArray::try_new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries_fields),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        )?);

        for input in [
            list,
            large_list,
            fixed_size_list,
            list_view,
            large_list_view,
            struct_array,
            map,
        ] {
            assert_nested_string_timestamp_cast(input)?;
        }
        Ok(())
    }

    #[test]
    fn casts_reordered_struct_fields_by_name_for_assignments() -> Result<()> {
        let source_fields: Fields = vec![
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
        ]
        .into();
        let source: ArrayRef = Arc::new(StructArray::try_new(
            source_fields,
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(TimestampMicrosecondArray::from(vec![0])),
            ],
            None,
        )?);
        let target_fields: Fields = vec![
            Arc::new(Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            )),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let target_type = DataType::Struct(target_fields.clone());

        let output = cast_array(&source, &target_type, "+01:02:03", false, true, false)?;
        let output = output.as_struct();
        assert_eq!(output.fields(), &target_fields);
        assert_eq!(
            output
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            -3_723_000_000
        );
        assert_eq!(
            output
                .column(1)
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .value(0),
            7
        );
        Ok(())
    }

    #[test]
    fn casts_map_values_by_name_without_using_arrow_wrapper_names() -> Result<()> {
        let source_value_fields: Fields = vec![
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
        ]
        .into();
        let source_values: ArrayRef = Arc::new(StructArray::try_new(
            source_value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(TimestampMicrosecondArray::from(vec![0])),
            ],
            None,
        )?);
        let source_entry_fields: Fields = vec![
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(Field::new(
                "values",
                DataType::Struct(source_value_fields),
                false,
            )),
        ]
        .into();
        let source_entries = StructArray::try_new(
            source_entry_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["x"])), source_values],
            None,
        )?;
        let source: ArrayRef = Arc::new(MapArray::try_new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(source_entry_fields),
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            source_entries,
            None,
            false,
        )?);

        let target_value_fields: Fields = vec![
            Arc::new(Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            )),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let target_entry_fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new(
                "value",
                DataType::Struct(target_value_fields.clone()),
                false,
            )),
        ]
        .into();
        let target_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(target_entry_fields),
                false,
            )),
            false,
        );

        let output = cast_array(&source, &target_type, "+01:02:03", false, true, false)?;
        assert_eq!(output.data_type(), &target_type);
        let values = output.as_map().values().as_struct();
        assert_eq!(values.fields(), &target_value_fields);
        assert_eq!(
            values
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            -3_723_000_000
        );
        assert_eq!(
            values
                .column(1)
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .value(0),
            7
        );
        Ok(())
    }
}
