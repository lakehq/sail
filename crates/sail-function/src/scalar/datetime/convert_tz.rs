use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, MappedLocalTime, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Int64Array, LargeListArray, ListArray, MapArray,
    StructArray, UInt64Array, new_null_array,
};
use datafusion::arrow::compute::kernels::{cast, numeric, take};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::expr::Cast;
use datafusion_expr::function::Hint;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, ValueOrLambda, Volatility,
};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::utils::make_scalar_function;
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;

use crate::functions_nested_utils::{evaluate_lambdas_until_null, scatter_active_rows};
use crate::scalar::datetime::spark_timestamp::parse_zone_offset;

/// How [`ConvertTz`] converts values, which differs for Spark's casts between TIMESTAMP_NTZ and
/// TIMESTAMP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ConvertTzMode {
    /// The `convert_timezone`, `from_utc_timestamp` and `to_utc_timestamp` functions.
    #[default]
    Function,
    /// A cast, which fails for a value out of range, like Spark.
    Cast,
    /// A `try_cast`, which is NULL for a value out of range.
    TryCast,
}

/// A helper scalar UDF for converting time zones for timestamps.
/// The timestamp must be NTZ timestamp, which should have [`None`] time zone
/// in the Arrow data type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTz {
    /// Whether to use the "classic" algorithm to convert time zone.
    /// The "classic" algorithm is used by the `convert_timezone` function in Spark,
    /// while the "non-classic" algorithm is used by the `from_utc_timestamp` and
    /// `to_utc_timestamp` functions in Spark.
    classic: bool,
    /// Whether NULL rows short-circuit before parsing their time zones.
    null_short_circuit: bool,
    mode: ConvertTzMode,
    /// The time zone that labels the converted values, which makes them TIMESTAMP values.
    output_timezone: Option<Arc<str>>,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
            null_short_circuit: false,
            mode: ConvertTzMode::Function,
            output_timezone: None,
        }
    }

    /// A cast between TIMESTAMP_NTZ and TIMESTAMP, which reads the wall-clock times of the values
    /// in the `to` time zone as wall-clock times in the `from` time zone, like Spark's
    /// `convertTz`. It takes fixed offsets and the full range of microseconds, and a comparison
    /// of it with a constant can be unwrapped.
    pub fn cast(is_try: bool) -> Self {
        Self {
            mode: if is_try {
                ConvertTzMode::TryCast
            } else {
                ConvertTzMode::Cast
            },
            ..Self::new(false)
        }
    }

    pub fn with_output_timezone(mut self, timezone: Option<Arc<str>>) -> Self {
        self.output_timezone = timezone;
        self
    }

    pub fn mode(&self) -> ConvertTzMode {
        self.mode
    }

    pub fn output_timezone(&self) -> Option<&Arc<str>> {
        self.output_timezone.as_ref()
    }

    pub fn with_null_short_circuit(mut self) -> Self {
        self.null_short_circuit = true;
        self
    }

    pub fn classic(&self) -> bool {
        self.classic
    }

    pub fn null_short_circuit(&self) -> bool {
        self.null_short_circuit
    }
}

/// Evaluates `convert_timezone` arguments from left to right and stops at the first NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTzLazy {
    signature: HigherOrderSignature,
    convert_tz: ConvertTz,
}

impl ConvertTzLazy {
    pub fn new(convert_tz: ConvertTz) -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
            convert_tz,
        }
    }

    pub fn classic(&self) -> bool {
        self.convert_tz.classic()
    }

    pub fn null_short_circuit(&self) -> bool {
        self.convert_tz.null_short_circuit()
    }
}

impl HigherOrderUDFImpl for ConvertTzLazy {
    fn name(&self) -> &str {
        "convert_tz_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        check_lazy_convert_tz_args(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_lazy_convert_tz_args(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        ScalarUDFImpl::return_field_from_args(
            &self.convert_tz,
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_lazy_convert_tz_args(&args.args)?;
        let lambdas = args
            .args
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(lambda) => Ok(lambda),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let arg_fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: Add single-pass row-level lambda evaluation so errors follow Spark's row-major
        // order. Replaying batches is not equivalent because it re-evaluates volatile/stateful
        // bodies, which Spark requires to run at most once per row.
        let (values, active_rows) = evaluate_lambdas_until_null(&lambdas, args.number_rows)?;
        if active_rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }

        let value = ScalarUDFImpl::invoke_with_args(
            &self.convert_tz,
            ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: active_rows.len(),
                return_field: Arc::clone(&args.return_field),
                config_options: Arc::clone(&args.config_options),
            },
        )?
        .into_array(active_rows.len())?;

        Ok(ColumnarValue::Array(scatter_active_rows(
            value,
            &active_rows,
            args.number_rows,
        )?))
    }
}

fn check_lazy_convert_tz_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3 {
        return exec_err!(
            "convert_timezone takes 3 internal arguments, got {}",
            args.len()
        );
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
    {
        return exec_err!("convert_timezone expected lambda arguments");
    }
    Ok(())
}

impl ScalarUDFImpl for ConvertTz {
    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, _, ts] = arg_types else {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        match ts {
            DataType::Timestamp(unit, None) => {
                Ok(DataType::Timestamp(*unit, self.output_timezone.clone()))
            }
            // The TIMESTAMP_NTZ values in a nested value are converted.
            _ if has_timestamp_ntz(ts) => {
                Ok(label_timestamp_ntz(ts, self.output_timezone.as_ref()))
            }
            _ => plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.mode != ConvertTzMode::Function {
            return self.invoke_cast(&args.args);
        }
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic, self.null_short_circuit),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }

    /// Spark unwraps a cast in a comparison with a constant when the constant casts back to
    /// itself (`UnwrapCastInBinaryComparison`), which keeps the comparison prunable. A wall-clock
    /// constant repeated in a DST overlap is compared per row instead, since it is more than one
    /// instant.
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        if self.mode == ConvertTzMode::Function {
            return Ok(PreimageResult::None);
        }
        let [from, to, value] = args else {
            return Ok(PreimageResult::None);
        };
        let (Some(Ok(from)), Some(Ok(to))) = (
            literal_string(from).map(parse_cast_zone),
            literal_string(to).map(parse_cast_zone),
        ) else {
            return Ok(PreimageResult::None);
        };
        if !matches!(
            value.get_type(info.schema())?,
            DataType::Timestamp(TimeUnit::Microsecond, None)
        ) {
            return Ok(PreimageResult::None);
        }
        let Some(converted) = literal_timestamp_micros(lit_expr) else {
            return Ok(PreimageResult::None);
        };
        let Some(start) = convert_cast_micros(converted, &to, &from) else {
            return Ok(PreimageResult::None);
        };
        // Spark unwraps a constant whose wall-clock time is repeated in a DST overlap too, which
        // drops the later instants (UnwrapCastInBinaryComparison is not exact there).
        if convert_cast_micros(start, &from, &to) != Some(converted)
            || !is_single_local_time(converted, &to)
        {
            return Ok(PreimageResult::None);
        }
        let Some(end) = start.checked_add(1) else {
            return Ok(PreimageResult::None);
        };
        Ok(PreimageResult::Range {
            expr: value.clone(),
            interval: Box::new(Interval::try_new(
                ScalarValue::TimestampMicrosecond(Some(start), None),
                ScalarValue::TimestampMicrosecond(Some(end), None),
            )?),
        })
    }
}

impl ConvertTz {
    fn invoke_cast(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let [from, to, value] = args else {
            return exec_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        let (from, to) = (cast_zone_argument(from)?, cast_zone_argument(to)?);
        let convert = |array: &ArrayRef| -> Result<ArrayRef> {
            let DataType::Timestamp(unit, None) = array.data_type() else {
                return exec_err!(
                    "invalid timestamp type for `convert_tz`: {:?}",
                    array.data_type()
                );
            };
            let converted = timestamp_to_microseconds(array)?
                .iter()
                .map(|micros| {
                    let Some(micros) = micros else {
                        return Ok(None);
                    };
                    match convert_cast_micros(micros, &from, &to) {
                        Some(converted) => Ok(Some(converted)),
                        None if self.mode == ConvertTzMode::TryCast => Ok(None),
                        None => exec_err!(
                            "[DATETIME_OVERFLOW] long overflow: the timestamp {micros} is out of range in the time zone"
                        ),
                    }
                })
                .collect::<Result<Int64Array>>()?;
            Ok(label_timestamp_array(
                microseconds_to_timestamp(converted, *unit)?,
                self.output_timezone.as_ref(),
            ))
        };
        match value {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(map_timestamp_ntz(array, &convert)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = map_timestamp_ntz(&scalar.to_array()?, &convert)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

const MICROS_PER_SECOND: i64 = 1_000_000;

/// A time zone of a cast, which can be a fixed offset.
#[derive(Debug, Clone, Copy)]
enum CastZone {
    Named(Tz),
    /// The UTC offset in microseconds.
    Fixed(i64),
}

fn parse_cast_zone(timezone: &str) -> Result<CastZone> {
    // Spark's offsets can have seconds, which Chrono's parser ignores.
    if let Some(offset) = parse_zone_offset(timezone) {
        return Ok(CastZone::Fixed(
            i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND,
        ));
    }
    match timezone.parse::<Tz>() {
        // A zone that is always UTC converts without looking up its offsets.
        Ok(tz) if is_fixed_zero_offset_timezone(tz.name()) => Ok(CastZone::Fixed(0)),
        Ok(tz) => Ok(CastZone::Named(tz)),
        Err(_) => match LEGACY_TIMEZONES
            .iter()
            .find(|(legacy, _)| *legacy == timezone)
            .and_then(|(_, tz)| tz.parse::<Tz>().ok())
        {
            Some(tz) => Ok(CastZone::Named(tz)),
            None => exec_err!("[INVALID_TIMEZONE] The timezone: {timezone:?} is invalid."),
        },
    }
}

fn cast_zone_argument(value: &ColumnarValue) -> Result<CastZone> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
            Some(timezone) => parse_cast_zone(timezone),
            None => exec_err!("`convert_tz` expects a time zone but got {scalar:?}"),
        },
        ColumnarValue::Array(_) => exec_err!("`convert_tz` casts expect constant time zones"),
    }
}

fn literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value, _) => value.try_as_str().flatten(),
        _ => None,
    }
}

fn literal_timestamp_micros(expr: &Expr) -> Option<i64> {
    let value = match expr {
        Expr::Literal(value, _) => value.clone(),
        Expr::Cast(Cast { expr, field }) => match expr.as_ref() {
            Expr::Literal(value, _) => value.cast_to(field.data_type()).ok()?,
            _ => return None,
        },
        _ => return None,
    };
    match value {
        ScalarValue::TimestampMicrosecond(Some(micros), _) => Some(micros),
        _ => None,
    }
}

/// The UTC offset of the time zone at an instant in microseconds. Chrono's time zones keep their
/// last offset beyond the dates that chrono represents, so the instant is clamped to them.
fn cast_zone_offset(zone: &CastZone, utc_micros: i64) -> i64 {
    match zone {
        CastZone::Fixed(offset) => *offset,
        CastZone::Named(tz) => {
            let micros = utc_micros.clamp(
                DateTime::<Utc>::MIN_UTC.timestamp_micros(),
                DateTime::<Utc>::MAX_UTC.timestamp_micros(),
            );
            DateTime::<Utc>::from_timestamp_micros(micros).map_or(0, |datetime| {
                i64::from(
                    tz.offset_from_utc_datetime(&datetime.naive_utc())
                        .fix()
                        .local_minus_utc(),
                ) * MICROS_PER_SECOND
            })
        }
    }
}

/// Spark's `convertTz` for casts, which takes the wall-clock time of the instant `micros` in `to`
/// and returns the instant of that wall-clock time in `from`. Returns `None` out of range.
fn convert_cast_micros(micros: i64, from: &CastZone, to: &CastZone) -> Option<i64> {
    let local = micros.checked_add(cast_zone_offset(to, micros))?;
    match from {
        CastZone::Fixed(offset) => local.checked_sub(*offset),
        CastZone::Named(tz) => {
            if let Some(datetime) = DateTime::<Utc>::from_timestamp_micros(local)
                && let Some(instant) = disambiguate_local_datetime(datetime.naive_utc(), tz)
            {
                return Some(instant.timestamp_micros());
            }
            local.checked_sub(cast_zone_offset(from, local))
        }
    }
}

/// Whether the wall-clock time `micros` in the time zone is a single instant.
fn is_single_local_time(micros: i64, zone: &CastZone) -> bool {
    match zone {
        CastZone::Fixed(_) => true,
        CastZone::Named(tz) => {
            DateTime::<Utc>::from_timestamp_micros(micros).is_some_and(|datetime| {
                matches!(
                    datetime.naive_utc().and_local_timezone(*tz),
                    MappedLocalTime::Single(_)
                )
            })
        }
    }
}

/// Labels the timestamps of an array with `timezone`, which keeps their instants.
fn label_timestamp_array(array: ArrayRef, timezone: Option<&Arc<str>>) -> ArrayRef {
    let Some(timezone) = timezone else {
        return array;
    };
    let timezone = Arc::clone(timezone);
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => Arc::new(
            array
                .as_primitive::<TimestampSecondType>()
                .clone()
                .with_timezone(timezone),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
            array
                .as_primitive::<TimestampMillisecondType>()
                .clone()
                .with_timezone(timezone),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone(timezone),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
            array
                .as_primitive::<TimestampNanosecondType>()
                .clone()
                .with_timezone(timezone),
        ),
        _ => array,
    }
}

/// The type with its TIMESTAMP_NTZ values labelled with `timezone`.
fn label_timestamp_ntz(data_type: &DataType, timezone: Option<&Arc<str>>) -> DataType {
    let field = |field: &FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(label_timestamp_ntz(field.data_type(), timezone)),
        )
    };
    match data_type {
        DataType::Timestamp(unit, None) => DataType::Timestamp(*unit, timezone.cloned()),
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        _ => data_type.clone(),
    }
}

/// Whether the type is or has TIMESTAMP_NTZ values.
pub fn has_timestamp_ntz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, None) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => has_timestamp_ntz(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_timestamp_ntz(field.data_type())),
        _ => false,
    }
}

fn with_field_type(field: &FieldRef, array: &ArrayRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(array.data_type().clone()),
    )
}

/// Applies `f` to the TIMESTAMP_NTZ values in a nested array.
fn map_timestamp_ntz(
    array: &ArrayRef,
    f: &dyn Fn(&ArrayRef) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Timestamp(_, None) => f(array)?,
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let values = map_timestamp_ntz(list.values(), f)?;
            Arc::new(ListArray::try_new(
                with_field_type(field, &values),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?)
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            let values = map_timestamp_ntz(list.values(), f)?;
            Arc::new(LargeListArray::try_new(
                with_field_type(field, &values),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let values = map_timestamp_ntz(list.values(), f)?;
            Arc::new(FixedSizeListArray::try_new(
                with_field_type(field, &values),
                *size,
                values,
                list.nulls().cloned(),
            )?)
        }
        DataType::Struct(fields) => {
            let values = array.as_struct();
            let columns = values
                .columns()
                .iter()
                .map(|column| map_timestamp_ntz(column, f))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                fields
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| with_field_type(field, column))
                    .collect(),
                columns,
                values.nulls().cloned(),
            )?)
        }
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let entries = map_timestamp_ntz(&entries, f)?;
            Arc::new(MapArray::try_new(
                with_field_type(field, &entries),
                map.offsets().clone(),
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *sorted,
            )?)
        }
        _ => Arc::clone(array),
    })
}

/// Spark's legacy time zone IDs (`java.time.ZoneId.SHORT_IDS`).
const LEGACY_TIMEZONES: &[(&str, &str)] = &[
    ("ACT", "Australia/Darwin"),
    ("AET", "Australia/Sydney"),
    ("AGT", "America/Argentina/Buenos_Aires"),
    ("ART", "Africa/Cairo"),
    ("AST", "America/Anchorage"),
    ("BET", "America/Sao_Paulo"),
    ("BST", "Asia/Dhaka"),
    ("CAT", "Africa/Harare"),
    ("CNT", "America/St_Johns"),
    ("CST", "America/Chicago"),
    ("CTT", "Asia/Shanghai"),
    ("EAT", "Africa/Addis_Ababa"),
    ("ECT", "Europe/Paris"),
    ("EST", "America/New_York"),
    ("HST", "Pacific/Honolulu"),
    ("IET", "America/Indianapolis"),
    ("IST", "Asia/Calcutta"),
    ("JST", "Asia/Tokyo"),
    ("MIT", "Pacific/Apia"),
    ("MST", "America/Denver"),
    ("NET", "Asia/Yerevan"),
    ("NST", "Pacific/Auckland"),
    ("PLT", "Asia/Karachi"),
    ("PNT", "America/Phoenix"),
    ("PRT", "America/Puerto_Rico"),
    ("PST", "America/Los_Angeles"),
    ("SST", "Pacific/Guadalcanal"),
    ("VST", "Asia/Saigon"),
];

fn convert_tz_inner(
    args: &[ArrayRef],
    classic: bool,
    null_short_circuit: bool,
) -> Result<ArrayRef> {
    let legacy_timezones: HashMap<&str, &str> = LEGACY_TIMEZONES.iter().copied().collect();

    let parse_tz = |input: Option<&str>| {
        input
            .map(|tz_str_opt| {
                let tz_err = |tz_str| {
                    exec_err!(
                        "[INVALID_TIMEZONE] The timezone: {tz_str:?} is invalid. \
        The timezone must be either a region-based zone ID or a zone offset. \
        Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. \
        Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm’ or '(+|-)HH:mm:ss', \
        e.g '-08' , '+01:00' or '-13:33:33', and must be in the range from -18:00 to +18:00. \
        'Z' and 'UTC' are accepted as synonyms for '+00:00'."
                    )
                };

                match tz_str_opt.parse::<Tz>() {
                    Ok(tz) => Ok(Some(tz)),
                    Err(_) => match legacy_timezones.get(tz_str_opt).cloned() {
                        Some(tz_str) => match tz_str.parse::<Tz>() {
                            Ok(tz) => Ok(Some(tz)),
                            Err(_) => tz_err(tz_str),
                        },
                        None => tz_err(tz_str_opt),
                    },
                }
            })
            .transpose()
            .map(|opt| opt.flatten())
    };

    let convert = if classic {
        convert_tz_classic
    } else {
        convert_tz_non_classic
    };

    let from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Option<&str>, Option<&str>)| match inputs {
            (Some(ts_micros), Some(from_tz), Some(to_tz)) => {
                match (parse_tz(Some(from_tz))?, parse_tz(Some(to_tz))?) {
                    (Some(from_tz), Some(to_tz)) => Ok(convert(ts_micros, &from_tz, &to_tz)),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        };

    let eager_from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Result<Option<Tz>>, Result<Option<Tz>>)| match inputs {
            (Some(ts_micros), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
                Ok(convert(ts_micros, &from_tz, &to_tz))
            }
            (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            _ => Ok(None),
        };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;
    let ts_arr = &args[2];

    let results: Int64Array = {
        let (from_tz_strs, to_tz_strs) = match (
            from_tz_strs_arr.as_string_opt::<i32>(),
            to_tz_strs_arr.as_string_opt::<i32>(),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                return exec_err!(
                    "`convert_timezone` first and second arguments must be string literal or array, received {:?}, {:?}",
                    args[0],
                    args[1]
                );
            }
        };

        let arr_lens = args.iter().map(|a| a.len()).collect::<Vec<_>>();
        let max_len = *arr_lens.iter().max().map_or_else(
            || exec_err!("`convert_timezone`: could not get array lengths max"),
            Ok,
        )?;

        let ts_arr = if ts_arr.len() != max_len && ts_arr.len() == 1 {
            let indices = (0..max_len).map(|_| 0u64).collect::<UInt64Array>();
            take::take(&ts_arr, &indices, None)?
        } else {
            ts_arr.clone()
        };

        let micros_arr = timestamp_to_microseconds(&ts_arr)?;

        if null_short_circuit {
            // Time zones are parsed only after all three row values pass null checks.
            let mut from_tzs = from_tz_strs.iter();
            let mut to_tzs = to_tz_strs.iter();

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = from_tzs.next().flatten();
                    let to_tz = to_tzs.next().flatten();

                    micros_arr
                        .iter()
                        .map(|ts| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (true, false) => {
                    let from_tz = from_tzs.next().flatten();
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = to_tzs.next().flatten();

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                    .collect::<Result<Int64Array>>(),
            }
        } else {
            let first = |iter: &mut dyn Iterator<Item = Result<Option<Tz>>>| {
                iter.next().transpose().map(|opt| opt.flatten())
            };
            let mut from_tzs = from_tz_strs.iter().map(parse_tz);
            let mut to_tzs = to_tz_strs.iter().map(parse_tz);

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = first(&mut from_tzs)?;
                    let to_tz = first(&mut to_tzs)?;

                    micros_arr
                        .iter()
                        .map(|ts| eager_from_to_utc_timestamp_func((ts, Ok(from_tz), Ok(to_tz))))
                        .collect::<Result<Int64Array>>()
                }
                (true, false) => {
                    let from_tz = first(&mut from_tzs)?;
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| {
                            eager_from_to_utc_timestamp_func((ts, Ok(from_tz), to_tz))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = first(&mut to_tzs)?;

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| {
                            eager_from_to_utc_timestamp_func((ts, from_tz, Ok(to_tz)))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| {
                        eager_from_to_utc_timestamp_func((ts, from_tz, to_tz))
                    })
                    .collect::<Result<Int64Array>>(),
            }
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    microseconds_to_timestamp(results, time_unit)
}

// TODO: Support Spark's full i64 microsecond range for conversions between
// different zones. Chrono cannot represent every Spark timestamp, so this needs
// wide-range zone-offset and DST arithmetic outside Chrono.
fn disambiguate_local_datetime(local: NaiveDateTime, tz: &Tz) -> Option<DateTime<Tz>> {
    // Handle ambiguous or non-existent local date time
    // in the same way as `java.time.ZonedDateTime#atZone`.
    match local.and_local_timezone(*tz) {
        MappedLocalTime::Single(x) => Some(x),
        MappedLocalTime::Ambiguous(earliest, _latest) => Some(earliest),
        MappedLocalTime::None => GapInfo::new(&local, tz).and_then(|gap| {
            if let (Some((start, _)), Some(end)) = (gap.begin, gap.end) {
                end.checked_add_signed(local - start)
            } else {
                None
            }
        }),
    }
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(ts_micros: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = match DateTime::<Utc>::from_timestamp_micros(ts_micros) {
        Some(datetime) => datetime.naive_utc(),
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let dt = disambiguate_local_datetime(local, from_zone)?;
    Some(
        dt.with_timezone(to_zone)
            .naive_local()
            .and_utc()
            .timestamp_micros(),
    )
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(ts_micros: i64, from_zone: &Tz, to_zone: &Tz) -> Option<i64> {
    let local = match to_zone.timestamp_micros(ts_micros).single() {
        Some(datetime) => datetime.naive_local(),
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let dt = disambiguate_local_datetime(local, from_zone)?;
    Some(dt.timestamp_micros())
}

fn timestamp_to_microseconds(array: &dyn Array) -> Result<Int64Array> {
    let values = cast::cast(array, &DataType::Int64)?;
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))?;
    let scaled = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000_000))?
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000))?
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => return Ok(values.clone()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            numeric::div(values, &Int64Array::new_scalar(1_000))?
        }
        _ => {
            return exec_err!(
                "`convert_timezone`: third argument type must coerce to NTZ timestamp, received {:?}",
                array.data_type()
            );
        }
    };
    scaled
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))
}

fn microseconds_to_timestamp(array: Int64Array, time_unit: TimeUnit) -> Result<ArrayRef> {
    if time_unit == TimeUnit::Microsecond {
        return Ok(cast::cast(&array, &DataType::Timestamp(time_unit, None))?);
    }
    let values = match time_unit {
        TimeUnit::Second => numeric::div(&array, &Int64Array::new_scalar(1_000_000))?,
        TimeUnit::Millisecond => numeric::div(&array, &Int64Array::new_scalar(1_000))?,
        TimeUnit::Microsecond => unreachable!(),
        TimeUnit::Nanosecond => numeric::mul(&array, &Int64Array::new_scalar(1_000))?,
    };
    Ok(cast::cast(&values, &DataType::Timestamp(time_unit, None))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cast_zone_offset_with_seconds() -> Result<()> {
        for zone in ["+05:30:45", "+053045"] {
            assert!(matches!(
                parse_cast_zone(zone)?,
                CastZone::Fixed(19_845_000_000)
            ));
        }
        assert!(matches!(
            parse_cast_zone("+08")?,
            CastZone::Fixed(28_800_000_000)
        ));
        assert!(matches!(
            parse_cast_zone("-0800")?,
            CastZone::Fixed(-28_800_000_000)
        ));
        Ok(())
    }

    #[test]
    fn test_parse_cast_zone_utc_is_fixed() -> Result<()> {
        for name in ["UTC", "Etc/UTC", "GMT", "Zulu"] {
            assert!(matches!(parse_cast_zone(name)?, CastZone::Fixed(0)));
        }
        assert!(matches!(
            parse_cast_zone("America/Los_Angeles")?,
            CastZone::Named(_)
        ));
        Ok(())
    }

    #[test]
    fn test_convert_cast_micros() -> Result<()> {
        let los_angeles = parse_cast_zone("America/Los_Angeles")?;
        let utc = parse_cast_zone("UTC")?;
        let fixed = parse_cast_zone("+05:30")?;
        // 2020-03-08 02:30:00 is in a DST gap and moves forward to 10:30:00 UTC.
        assert_eq!(
            convert_cast_micros(1_583_634_600_000_000, &los_angeles, &utc),
            Some(1_583_663_400_000_000)
        );
        assert_eq!(convert_cast_micros(0, &fixed, &utc), Some(-19_800_000_000));
        // Chrono does not represent these instants, but the zone offsets still apply.
        assert!(convert_cast_micros(i64::MAX - 1, &utc, &los_angeles).is_some());
        assert_eq!(convert_cast_micros(i64::MAX, &utc, &fixed), None);
        // The offset past Chrono's dates is the last one in the time zone data.
        let year_2200 = 7_258_118_400_000_000;
        assert_eq!(
            cast_zone_offset(&los_angeles, i64::MAX),
            cast_zone_offset(&los_angeles, year_2200)
        );
        Ok(())
    }

    #[test]
    fn test_cast_mode_out_of_range() -> Result<()> {
        let args = [
            ColumnarValue::Scalar(ScalarValue::from("UTC")),
            ColumnarValue::Scalar(ScalarValue::from("+05:30")),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(i64::MAX), None)),
        ];
        assert!(ConvertTz::cast(false).invoke_cast(&args).is_err());
        let ColumnarValue::Scalar(value) = ConvertTz::cast(true).invoke_cast(&args)? else {
            return exec_err!("expected a scalar");
        };
        assert!(value.is_null());
        Ok(())
    }
}
