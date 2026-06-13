use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Marker UDF for Spark `session_window(timeColumn, gapDuration)`. Sessions
/// merge across rows, which a scalar UDF cannot express, so the aggregate
/// resolver rewrites this marker into a `SessionWindowNode`. It only carries
/// the `struct<start, end>` return type for analysis; `invoke` always errors.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSessionWindow {
    signature: Signature,
}

impl Default for SparkSessionWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSessionWindow {
    pub fn new() -> Self {
        Self {
            // `any(2)`: the resolver has already cast the arguments (time ->
            // Timestamp(us), gap -> interval); no coercion is needed here.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSessionWindow {
    fn name(&self) -> &str {
        "session_window"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Both struct fields carry the time argument's timezone so that
        // `session_window(ts, ...).start` keeps `ts`'s timezone.
        let tz = match arg_types.first() {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz)) => tz.clone(),
            other => {
                return plan_err!(
                    "session_window expects a Timestamp(Microsecond, *) first argument, got {other:?}"
                );
            }
        };
        let field_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
        // Fields are nullable: a group whose rows are all filtered out (null ts /
        // non-positive gap) contributes no session, matching Spark.
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("start", field_type.clone(), true),
            Field::new("end", field_type, true),
        ])))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Reaching here means the resolver rewrite did not fire (e.g. the
        // marker was used outside a grouping position); fail loudly.
        exec_err!("session_window can only be used as a grouping expression in an aggregation")
    }
}
