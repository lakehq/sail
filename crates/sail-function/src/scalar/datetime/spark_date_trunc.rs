use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, new_null_array};
use datafusion::arrow::compute::interleave;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::unsupported_data_type_exec_err;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

/// The granularities DataFusion's `date_trunc` understands. Spark returns NULL for anything
/// else, so an unrecognized unit never reaches the inner function.
const GRANULARITIES: [&str; 10] = [
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
];

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }

    /// Spark resolves the unit row by row, so a column of units truncates each row by its own
    /// unit and nullifies only the rows whose unit is NULL or unrecognized. DataFusion's
    /// `date_trunc` demands a scalar unit, so evaluate it once per DISTINCT unit — at most ten,
    /// and one or two in practice — over the whole timestamp array, then assemble the rows.
    /// That keeps the vectorized kernel instead of degrading to a per-row call.
    fn invoke_with_unit_array(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        let (unit, timestamp) = args.two()?;
        let timestamp_field = arg_fields.two()?.1;
        let ColumnarValue::Array(unit) = unit else {
            return internal_err!("`date_trunc` expected an array of units");
        };
        let units = unit_strings(&unit)?;

        // Spark matches the unit case-insensitively, and the planner's `CASE` only rewrites the
        // aliases (`mm`, `yy`, `dd`), so a column still carries the user's own casing.
        let truncated_by_unit = GRANULARITIES
            .iter()
            .filter(|granularity| {
                units
                    .iter()
                    .flatten()
                    .any(|unit| unit.eq_ignore_ascii_case(granularity))
            })
            .map(|granularity| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::from(*granularity)),
                        timestamp.clone(),
                    ],
                    arg_fields: vec![
                        Arc::new(Field::new("granularity", DataType::Utf8, false)),
                        Arc::clone(&timestamp_field),
                    ],
                    number_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                };
                let truncated = self.inner.invoke_with_args(args)?.to_array(number_rows)?;
                Ok((*granularity, truncated))
            })
            .collect::<Result<Vec<_>>>()?;

        // The last source is the NULL row every unrecognized or NULL unit points at.
        let nulls = new_null_array(return_field.data_type(), 1);
        let sources = truncated_by_unit
            .iter()
            .map(|(_, array)| array.as_ref())
            .chain(std::iter::once(nulls.as_ref()))
            .collect::<Vec<_>>();
        let null_source = truncated_by_unit.len();

        let indices = units
            .iter()
            .enumerate()
            .map(|(row, unit)| {
                unit.and_then(|unit| {
                    truncated_by_unit
                        .iter()
                        .position(|(granularity, _)| unit.eq_ignore_ascii_case(granularity))
                })
                .map_or((null_source, 0), |source| (source, row))
            })
            .collect::<Vec<_>>();

        Ok(ColumnarValue::Array(interleave(&sources, &indices)?))
    }
}

/// The unit column reaches the function as any of the string types, since the planner builds it
/// with a `CASE` expression over the user's own column.
fn unit_strings(unit: &ArrayRef) -> Result<Vec<Option<&str>>> {
    let units: Vec<Option<&str>> = match unit.data_type() {
        DataType::Utf8 => unit.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => unit.as_string::<i64>().iter().collect(),
        DataType::Utf8View => unit.as_string_view().iter().collect(),
        other => {
            return Err(unsupported_data_type_exec_err(
                "date_trunc",
                "a string unit",
                other,
            ));
        }
    };
    Ok(units)
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first() {
            Some(ColumnarValue::Array(_)) => self.invoke_with_unit_array(args),
            _ => self.inner.invoke_with_args(args),
        }
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}
