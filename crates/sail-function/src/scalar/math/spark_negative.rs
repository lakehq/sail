use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::math::negative::SparkNegative as DataFusionNegative;

use crate::error::invalid_arg_count_exec_err;

/// `ConfigOptions` snapshots with `execution.enable_ansi_mode` pinned. Negation
/// only reads the ANSI flag, so the two possible snapshots are built once and
/// shared process-wide; each batch clones an `Arc` instead of deep-cloning the
/// session config.
static ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(true));
static NON_ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(false));

fn make_config(ansi_mode: bool) -> Arc<ConfigOptions> {
    let mut config = ConfigOptions::default();
    config.execution.enable_ansi_mode = ansi_mode;
    Arc::new(config)
}

/// Spark unary minus / `negative(x)` that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Name, signature, and the negate kernel are delegated to upstream
/// `datafusion_spark::SparkNegative` (its free `spark_negative` helper is private).
/// We drive the kernel by handing it the matching pinned-ANSI [`ConfigOptions`]
/// snapshot per batch.
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [arg] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        // DataFusion can label an empty scalar subquery non-nullable. Keep this
        // conservative until its scalar-subquery nullability fix is available.
        // FIXME: After upgrading to DataFusion with that fix, replace true with arg.is_nullable().
        Ok(Arc::new(Field::new(self.name(), arg.data_type().clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let config_options = if self.ansi_mode {
            Arc::clone(&ANSI_CONFIG)
        } else {
            Arc::clone(&NON_ANSI_CONFIG)
        };
        let args = ScalarFunctionArgs {
            config_options,
            ..args
        };
        self.inner.invoke_with_args(args)
    }
}
