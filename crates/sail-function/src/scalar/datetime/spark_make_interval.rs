use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, IntervalMonthDayNanoArray};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval as UpstreamMakeInterval;

/// Thin wrapper around `datafusion_spark::SparkMakeInterval` that adds a
/// `safe` flag for the `try_make_interval` variant.
///
/// All UDF behavior — signature, type coercion, return type, kernel —
/// is delegated to the upstream impl. The only difference is:
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
            let array: IntervalMonthDayNanoArray = (0..number_rows).map(|_| None).collect();
            ColumnarValue::Array(Arc::new(array) as ArrayRef)
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
        let number_rows = args.number_rows;
        match self.inner.invoke_with_args(args) {
            Ok(out) => Ok(out),
            Err(_) if self.safe => Ok(Self::null_result(number_rows)),
            Err(e) => Err(e),
        }
    }
}
