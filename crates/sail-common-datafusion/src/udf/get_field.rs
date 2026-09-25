use datafusion::arrow::array::{Array, make_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::functions::core::get_field;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::array::record_batch::cast_array_recursively;

// TODO: Restore nested Parquet projection pruning for this accessor. DataFusion
// recognizes GetFieldFunc only; upstream support must preserve parent validity.
/// Extract one field while preserving the validity of its enclosing struct.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkGetField {
    signature: Signature,
}

impl Default for SparkGetField {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkGetField {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkGetField {
    fn name(&self) -> &str {
        "spark_get_field"
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        get_field().inner().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        get_field().inner().placement(args)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        get_field().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        get_field().return_field_from_args(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        match get_field().inner().simplify(args, info)? {
            ExprSimplifyResult::Simplified(expr) => {
                // Keep constructor-field pruning without letting nested accesses
                // bypass the parent-validity handling in this function.
                let expr = expr
                    .transform_up(|expr| {
                        if let Expr::ScalarFunction(function) = &expr
                            && function.func.inner().is::<GetFieldFunc>()
                        {
                            let mut args = function.args.iter().cloned();
                            let base = args.next().ok_or_else(|| {
                                internal_datafusion_err!("missing get_field input")
                            })?;
                            let expr = args.fold(base, |base, field| {
                                ScalarUDF::from(Self::new()).call(vec![base, field])
                            });
                            return Ok(Transformed::yes(expr));
                        }
                        Ok(Transformed::no(expr))
                    })
                    .data()?;
                Ok(ExprSimplifyResult::Simplified(expr))
            }
            result => Ok(result),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let parent_nulls = match args.args.first() {
            Some(ColumnarValue::Array(array))
                if matches!(array.data_type(), DataType::Struct(_)) =>
            {
                array.nulls().cloned()
            }
            Some(ColumnarValue::Scalar(ScalarValue::Struct(array))) => array.nulls().cloned(),
            _ => None,
        };
        let return_type = args.return_field.data_type().clone();
        let mut value = get_field().inner().invoke_with_args(args)?;
        if let ColumnarValue::Array(array) = &value
            && array.data_type() != &return_type
            && array.data_type().equals_datatype(&return_type)
        {
            // File readers may retain storage metadata on nested fields. Match the
            // planned field metadata without changing names, types, or nullability.
            let actual = Schema::new(vec![Field::new("", array.data_type().clone(), true)]);
            let expected = Schema::new(vec![Field::new("", return_type.clone(), true)]);
            if actual
                .flattened_fields()
                .iter()
                .map(|field| field.name())
                .eq(expected.flattened_fields().iter().map(|field| field.name()))
            {
                value = ColumnarValue::Array(cast_array_recursively(array, &return_type)?);
            }
        }
        // NullArray already represents all NULLs and cannot carry a validity bitmap.
        match (value, parent_nulls) {
            (ColumnarValue::Array(array), Some(parent_nulls)) if !array.data_type().is_null() => {
                // Arrow children may contain valid values underneath a null struct.
                // Spark's GetStructField returns NULL whenever the parent is NULL.
                let nulls = NullBuffer::union(Some(&parent_nulls), array.nulls());
                let data = array.to_data().into_builder().nulls(nulls).build()?;
                Ok(ColumnarValue::Array(make_array(data)))
            }
            (value, _) => Ok(value),
        }
    }
}
