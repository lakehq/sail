use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{UInt64Array, new_null_array};
use datafusion::arrow::compute::{interleave, take};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;

use crate::scalar::datetime::utils::string_array_iter;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TruncInstant.nullable = true`, unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/datetimeExpressions.scala#L2287>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Array(units) = &args.args[0] else {
            return self.inner.invoke_with_args(args);
        };
        // Keep constant units on the native fast path. Dynamic units share that kernel
        // in batches, rather than constructing and invoking a scalar function per row.
        let mut groups = BTreeMap::<&str, Vec<usize>>::new();
        for (row, unit) in string_array_iter(units.as_ref())?.enumerate() {
            let Some(unit) = unit else { continue };
            if let Some(unit) = [
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
            ]
            .into_iter()
            .find(|candidate| unit.eq_ignore_ascii_case(candidate))
            {
                groups.entry(unit).or_default().push(row);
            }
        }
        let mut results = vec![new_null_array(args.return_type(), 1)];
        let mut positions = vec![(0, 0); units.len()];
        for (unit, rows) in groups {
            let value = match &args.args[1] {
                ColumnarValue::Array(array) if rows.len() != units.len() => {
                    let indices = UInt64Array::from_iter_values(rows.iter().map(|row| *row as u64));
                    ColumnarValue::Array(take(array, &indices, None)?)
                }
                value => value.clone(),
            };
            let result = self
                .inner
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(unit.to_owned()))),
                        value,
                    ],
                    arg_fields: args.arg_fields.clone(),
                    number_rows: rows.len(),
                    return_field: Arc::clone(&args.return_field),
                    config_options: Arc::clone(&args.config_options),
                })?
                .into_array(rows.len())?;
            if rows.len() == units.len() {
                return Ok(ColumnarValue::Array(result));
            }
            for (position, row) in rows.into_iter().enumerate() {
                positions[row] = (results.len(), position);
            }
            results.push(result);
        }
        let arrays = results
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        Ok(ColumnarValue::Array(interleave(&arrays, &positions)?))
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
