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

    /// Spark: `Pmod.nullable = true`, unconditional (not narrowed by ANSI mode).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1061>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_pmod(&args.args, self.ansi_mode, args.return_type())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::error::ArrowError;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DataFusionError, ScalarValue};

    use super::*;

    fn invoke(ansi_mode: bool, values: [ScalarValue; 2]) -> Result<ScalarValue> {
        let function = SparkPmod::new(ansi_mode);
        let arg_fields = values
            .iter()
            .map(|value| Arc::new(Field::new("arg", value.data_type(), value.is_null())))
            .collect::<Vec<_>>();
        let return_field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[Some(&values[0]), Some(&values[1])],
        })?;
        assert!(return_field.is_nullable());
        let mut config = ConfigOptions::default();
        // Runtime configuration must not override the flag captured by the UDF.
        config.execution.enable_ansi_mode = !ansi_mode;
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: values.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(config),
        })?;
        ScalarValue::try_from_array(&result.into_array(1)?, 0)
    }

    #[test]
    fn planned_decimal_and_null_result_types() -> Result<()> {
        for ansi_mode in [false, true] {
            for (values, expected) in [
                (
                    [
                        ScalarValue::Decimal128(Some(75), 3, 1),
                        ScalarValue::Decimal128(Some(20), 2, 1),
                    ],
                    ScalarValue::Decimal128(Some(15), 2, 1),
                ),
                (
                    [
                        ScalarValue::Decimal128(Some(-755), 4, 2),
                        ScalarValue::Decimal128(Some(20), 2, 1),
                    ],
                    ScalarValue::Decimal128(Some(45), 3, 2),
                ),
                (
                    [ScalarValue::Null, ScalarValue::Decimal128(Some(20), 2, 1)],
                    ScalarValue::Decimal128(None, 2, 1),
                ),
                (
                    [ScalarValue::Decimal128(Some(75), 3, 1), ScalarValue::Null],
                    ScalarValue::Decimal128(None, 3, 1),
                ),
                (
                    [ScalarValue::Null, ScalarValue::Null],
                    ScalarValue::Float64(None),
                ),
            ] {
                assert_eq!(invoke(ansi_mode, values)?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn zero_division_uses_captured_ansi_mode() -> Result<()> {
        for (left, zero, null) in [
            (
                ScalarValue::Int32(Some(7)),
                ScalarValue::Int32(Some(0)),
                ScalarValue::Int32(None),
            ),
            (
                ScalarValue::Float64(Some(7.5)),
                ScalarValue::Float64(Some(-0.0)),
                ScalarValue::Float64(None),
            ),
            (
                ScalarValue::Decimal128(Some(75), 3, 1),
                ScalarValue::Decimal128(Some(0), 2, 1),
                ScalarValue::Decimal128(None, 2, 1),
            ),
        ] {
            assert_eq!(invoke(false, [left.clone(), zero.clone()])?, null);
            assert!(matches!(
                invoke(true, [left.clone(), zero.clone()]),
                Err(DataFusionError::ArrowError(error, _)) if matches!(*error, ArrowError::DivideByZero)
            ));
            assert_eq!(
                invoke(true, [ScalarValue::try_new_null(&left.data_type())?, zero])?,
                null,
            );
        }
        Ok(())
    }
}
