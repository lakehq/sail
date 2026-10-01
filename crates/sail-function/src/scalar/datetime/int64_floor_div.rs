use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// `Int64` floor division by a fixed, positive divisor -- Java/Scala's
/// `Math.floorDiv`, used by `Cast.scala`'s `timestampToLong`/`timeToLong`.
///
/// One opaque UDF call, not a `quotient - adjustment` expression referencing
/// the input twice: DataFusion's CSE optimizer factored that duplication into
/// a shared column on some plan paths but not others, breaking windows like
/// `ORDER BY CAST(ts AS BIGINT) ... RANGE BETWEEN ...`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Int64FloorDiv {
    signature: Signature,
    divisor: i64,
}

impl Int64FloorDiv {
    pub fn new(divisor: i64) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            divisor,
        }
    }

    pub fn divisor(&self) -> i64 {
        self.divisor
    }
}

impl ScalarUDFImpl for Int64FloorDiv {
    fn name(&self) -> &str {
        "int64_floor_div"
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
        let [field] = args.arg_fields else {
            return exec_err!(
                "`{}` function requires 1 argument, got {}",
                self.name(),
                args.arg_fields.len()
            );
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return exec_err!(
                "`{}` function requires 1 argument, got {}",
                self.name(),
                args.len()
            );
        };
        let divisor = self.divisor;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Int64(val)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(val.map(|x| x.div_euclid(divisor))),
            )),
            ColumnarValue::Array(array) => {
                let result: ArrayRef = Arc::new(
                    array
                        .as_primitive::<Int64Type>()
                        .unary::<_, Int64Type>(|x| x.div_euclid(divisor)),
                );
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!("Unsupported arg {other:?} for function {}", self.name()),
        }
    }
}
