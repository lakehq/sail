use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;

/// DataFusion's `date_trunc`, with Spark's nullability.
///
/// The unit is resolved in the plan builder, which enumerates the units and calls this function
/// with a literal one per branch (see `truncate_by_unit`), so an unrecognized, NULL, or columnar
/// unit never reaches the inner function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    /// Spark truncates a NULL timestamp to NULL, so the result is nullable even when the input is
    /// not, which is what DataFusion would infer.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(input)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}
