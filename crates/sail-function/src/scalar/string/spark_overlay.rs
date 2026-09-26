use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

/// DataFusion's vectorized `overlay` kernel with Spark's null-intolerant output field.
///
/// Spark's `Overlay` extends `QuaternaryExpression` and is null-intolerant
/// (`stringExpressions.scala:1039-1089`), while DataFusion conservatively declares its return
/// nullable. Delegating execution retains the kernel and only repairs the analyzed schema.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkOverlay {
    signature: Signature,
    nullable: bool,
}

impl Default for SparkOverlay {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkOverlay {
    pub fn new(nullable: bool) -> Self {
        Self {
            signature: datafusion_functions::core::overlay().signature().clone(),
            nullable,
        }
    }
}

impl ScalarUDFImpl for SparkOverlay {
    fn name(&self) -> &str {
        if self.nullable {
            "spark_overlay_nullable"
        } else {
            "spark_overlay_nonnullable"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if !(3..=4).contains(&args.arg_fields.len()) {
            return exec_err!("spark_overlay expects input, replacement, position [, length]");
        }
        let data_type = datafusion_functions::core::overlay().return_type(
            &args
                .arg_fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Arc::new(Field::new(self.name(), data_type, self.nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion_functions::core::overlay().invoke_with_args(args)
    }
}
