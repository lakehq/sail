use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::functions_utils::make_scalar_function;
use crate::scalar::url::parse_url::spark_handled_parse_url;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryParseUrl {
    signature: Signature,
}

impl Default for SparkTryParseUrl {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryParseUrl {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryParseUrl {
    fn name(&self) -> &str {
        "spark_try_parse_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::scalar_url_spark_try_parse_url_typing::spark_try_parse_url_return_type(_arg_types)
    }

    /// Spark: `TryParseUrl` is `RuntimeReplaceable` over `ParseUrl`, whose
    /// `nullable = true` is unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/urlExpressions.scala#L221>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::policies::scalar_url_spark_try_parse_url_typing::spark_try_parse_url_return_field_from_args(self.name(), args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        sail_type_system::policies::scalar_url_spark_try_parse_url_typing::spark_try_parse_url_coerce_types(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_try_parse_url, vec![])(&args)
    }
}

fn spark_try_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
    spark_handled_parse_url(args, |x| match x {
        Err(_) => Ok(None),
        result => result,
    })
}
