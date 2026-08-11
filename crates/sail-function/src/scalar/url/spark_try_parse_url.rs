use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::functions_utils::make_scalar_function;
use crate::scalar::url::parse_url::{ParseUrl, parse_url_return_type, spark_handled_parse_url};

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
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryParseUrl` (`urlExpressions.scala:182-193`) declares no `nullable` of its own,
    /// and neither does the `InheritAnalysisRules` trait it mixes in (`Expression.scala:470`),
    /// so the rule comes from `RuntimeReplaceable` (`Expression.scala:446`):
    /// `replacement.nullable`. Its replacement is `ParseUrl(children, failOnError = false)`
    /// (`:184`), which declares `true` unconditionally (`urlExpressions.scala:221`).
    ///
    /// So both spellings land on the same constant, and unlike the `to_number` pair there is no
    /// try-vs-strict split to encode here.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = parse_url_return_type(self.name(), &arg_types)?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let parse_url: ParseUrl = ParseUrl::new();
        parse_url.coerce_types(arg_types)
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

#[cfg(test)]
mod return_field_tests {
    use std::sync::Arc;

    use datafusion_common::ScalarValue;

    use super::*;

    fn non_nullable(data_type: DataType) -> FieldRef {
        Arc::new(Field::new("c", data_type, false))
    }

    /// Spark declares this function's output nullable regardless of its children, so a
    /// non-nullable argument -- the case where the arity default would say `false` -- must
    /// still come back nullable.
    #[test]
    fn test_non_nullable_arguments_still_yield_a_nullable_field() -> Result<()> {
        let arg_fields = vec![non_nullable(DataType::Utf8), non_nullable(DataType::Utf8)];
        let scalar_arguments: Vec<Option<&ScalarValue>> = vec![None; arg_fields.len()];
        let field = SparkTryParseUrl::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
        Ok(())
    }

    #[test]
    fn test_return_type_is_not_the_source_of_truth() {
        assert!(
            SparkTryParseUrl::new()
                .return_type(&[DataType::Utf8, DataType::Utf8])
                .is_err()
        );
    }
}
