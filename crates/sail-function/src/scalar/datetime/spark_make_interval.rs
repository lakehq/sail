use std::any::Any;

use datafusion::arrow::datatypes::{DataType, IntervalUnit};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval as UpstreamMakeInterval;

/// Spark-compatible `make_interval` / `try_make_interval` function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#make_interval>
///
/// Thin wrapper around `datafusion_spark::SparkMakeInterval` that adds a
/// `safe` flag for the `try_make_interval` variant.
/// - `safe = false` → strict `make_interval`: errors propagate (default upstream behavior).
/// - `safe = true`  → `try_make_interval`: errors are caught and turned into NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    inner: UpstreamMakeInterval,
    safe: bool,
}

impl SparkMakeInterval {
    pub fn new(safe: bool) -> Self {
        Self {
            inner: UpstreamMakeInterval::default(),
            safe,
        }
    }

    pub fn safe(&self) -> bool {
        self.safe
    }

    fn null_result(number_rows: usize) -> ColumnarValue {
        if number_rows <= 1 {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(None))
        } else {
            ColumnarValue::Array(datafusion::arrow::array::new_null_array(
                &DataType::Interval(IntervalUnit::MonthDayNano),
                number_rows,
            ))
        }
    }
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.safe {
            "try_make_interval"
        } else {
            self.inner.name()
        }
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Spark requires at least 1 argument. The upstream UDF accepts 0 args
        // (returns zero interval) via TypeSignature::Nullary, but that diverges
        // from Spark; reject explicitly to align with the canonical error.
        if args.args.is_empty() {
            return datafusion_common::exec_err!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{}` requires [1, 2, 3, 4, 5, 6, 7] parameters but the actual number is 0.",
                self.name()
            );
        }
        let number_rows = args.number_rows;
        match self.inner.invoke_with_args(args) {
            Ok(out) => Ok(out),
            Err(_) if self.safe => Ok(Self::null_result(number_rows)),
            Err(e) => Err(e),
        }
    }
}
