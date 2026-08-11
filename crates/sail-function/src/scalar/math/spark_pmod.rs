use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::math::modulus::{SparkPmod as DataFusionPmod, spark_pmod};

/// Spark `pmod(a, b)` (positive modulo) that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Under ANSI mode a zero divisor raises an error; otherwise it returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPmod {
    inner: DataFusionPmod,
    ansi_mode: bool,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkPmod {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            inner: DataFusionPmod::new(),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkPmod {
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

    /// Spark: `Pmod` declares `override def nullable: Boolean = true`
    /// (`arithmetic.scala:1061`) — unconditionally, so it wins over the `BinaryExpression`
    /// arity default (`left || right`), and it is NOT narrowed by ANSI mode: under ANSI a zero
    /// divisor raises instead of returning NULL, but the declared flag stays `true`.
    ///
    /// The output type keeps coming from the wrapped `datafusion-spark` implementation, so the
    /// coercion rules stay in one place.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_pmod(&args.args, self.ansi_mode)
    }
}
