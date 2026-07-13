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
        // Resolve every row's granularity in a single pass. Spark matches the unit
        // case-insensitively, and the planner's `CASE` only rewrites the aliases (`mm`, `yy`,
        // `dd`), so a column still carries the user's own casing. A row whose unit is NULL or
        // unrecognized has no granularity, and Spark nullifies it.
        let granularity_of_row = unit_strings(&unit)?
            .into_iter()
            .map(|unit| {
                unit.and_then(|unit| {
                    GRANULARITIES
                        .iter()
                        .position(|granularity| unit.eq_ignore_ascii_case(granularity))
                })
            })
            .collect::<Vec<_>>();

        // Truncate once per DISTINCT granularity the column actually uses, over the whole
        // timestamp array, and remember which source array each one landed in.
        let mut source_of_granularity = [None; GRANULARITIES.len()];
        let mut truncated = Vec::new();
        for granularity in granularity_of_row.iter().flatten() {
            if source_of_granularity[*granularity].is_some() {
                continue;
            }
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from(GRANULARITIES[*granularity])),
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
            source_of_granularity[*granularity] = Some(truncated.len());
            truncated.push(self.inner.invoke_with_args(args)?.to_array(number_rows)?);
        }

        // The last source is the NULL row every unrecognized or NULL unit points at.
        let nulls = new_null_array(return_field.data_type(), 1);
        let null_source = truncated.len();
        let sources = truncated
            .iter()
            .map(|array| array.as_ref())
            .chain(std::iter::once(nulls.as_ref()))
            .collect::<Vec<_>>();

        let indices = granularity_of_row
            .iter()
            .enumerate()
            .map(|(row, granularity)| {
                granularity
                    .and_then(|granularity| source_of_granularity[granularity])
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

    /// Spark yields NULL for a unit it does not recognize, and for a NULL unit, whereas
    /// DataFusion errors. Resolve that here rather than in the planner, so the behavior does
    /// not depend on whether the unit reached the function as a literal or as a column.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first() {
            Some(ColumnarValue::Array(_)) => return self.invoke_with_unit_array(args),
            Some(ColumnarValue::Scalar(unit)) => {
                let recognized = unit.try_as_str().flatten().is_some_and(|unit| {
                    GRANULARITIES
                        .iter()
                        .any(|granularity| unit.eq_ignore_ascii_case(granularity))
                });
                if !recognized {
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                        args.return_field.data_type(),
                    )?));
                }
            }
            None => {}
        }
        self.inner.invoke_with_args(args)
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
