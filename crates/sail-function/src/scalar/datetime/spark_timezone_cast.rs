use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, MapArray, PrimitiveArray,
    StringArray, StructArray,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use super::convert_tz::convert_tz_inner;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkTimezoneCast {
    signature: Signature,
    target_type: DataType,
    session_timezone: Arc<str>,
    safe: bool,
}

impl SparkTimezoneCast {
    pub fn new(target_type: DataType, session_timezone: Arc<str>, safe: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
            session_timezone,
            safe,
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
            input.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| {
                let [array] = args else {
                    return exec_err!("spark_timezone_cast expects exactly one argument");
                };
                cast_array(array, &self.target_type, &self.session_timezone, self.safe)
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
    timezone: &Arc<str>,
    safe: bool,
) -> Result<ArrayRef> {
    let timestamp = arrow_cast(array, &DataType::Timestamp(unit, None), safe)?;
    let timestamp = convert_timezone(timestamp, timezone, "UTC", safe)?;
    retag_timestamp(&timestamp, Some(Arc::clone(timezone)))
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
) -> Result<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(Arc::clone(array));
    }

    match (array.data_type(), target_type) {
        (
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
            DataType::Timestamp(unit, Some(timezone)),
        ) => cast_to_ltz(array, *unit, timezone, safe),
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
            )?;
            Ok(Arc::new(GenericListArray::<i64>::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
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
            let entries = Arc::new(source.entries().clone()) as ArrayRef;
            let entries = cast_array(&entries, target_entry.data_type(), session_timezone, safe)?;
            let Some(entries) = entries.as_any().downcast_ref::<StructArray>() else {
                return exec_err!("spark_timezone_cast map entries must be a struct");
            };
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(target_entry),
                source.offsets().clone(),
                entries.clone(),
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
            let columns = source
                .columns()
                .iter()
                .zip(target_fields)
                .map(|(column, target_field)| {
                    cast_array(column, target_field.data_type(), session_timezone, safe)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )?))
        }
        _ => arrow_cast(array, target_type, safe),
    }
}
