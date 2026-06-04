use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_spark::function::math::negative::SparkNegative as DataFusionNegative;

/// Spark unary minus / `negative(x)` that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Under ANSI mode, negating the minimum value of an integral type raises
/// ARITHMETIC_OVERFLOW; otherwise it wraps two's complement (the minimum negates
/// to itself). Decimal/interval overflow always errors.
///
/// Name, signature, return type, and the negate kernel are delegated to upstream
/// `datafusion_spark::SparkNegative`. Its free `spark_negative` helper is private,
/// so the plan-time flag is threaded by overriding
/// `config_options.execution.enable_ansi_mode` before delegating to the upstream
/// `invoke_with_args`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    inner: DataFusionNegative,
    ansi_mode: bool,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkNegative {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            inner: DataFusionNegative::new(),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkNegative {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut config = (*args.config_options).clone();
        config.execution.enable_ansi_mode = self.ansi_mode;
        let args = ScalarFunctionArgs {
            config_options: Arc::new(config),
            ..args
        };
        self.inner.invoke_with_args(args)
    }
}
