use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::compute::nullif;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions::core::get_field;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::array::record_batch::cast_array_positionally_recursively;

pub mod physical;

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
                array.nulls()
            }
            Some(ColumnarValue::Scalar(ScalarValue::Struct(array))) => array.nulls(),
            _ => None,
        }
        .filter(|nulls| nulls.null_count() > 0)
        .cloned();
        let return_field = args.return_field.clone();
        let return_type = return_field.data_type();
        let mut value = get_field().inner().invoke_with_args(args)?;
        if let ColumnarValue::Array(array) = &value
            && array.data_type() != return_type
            && array.data_type().equals_datatype(return_type)
            && same_field_names(array.data_type(), return_type)
        {
            // File readers may retain storage metadata on nested fields. Match the
            // planned field metadata without changing names, types, or nullability.
            // Names and order already match, so avoid searching by name for each field.
            value = ColumnarValue::Array(cast_array_positionally_recursively(array, return_type)?);
        }
        // NullArray already represents all NULLs and cannot carry a validity bitmap.
        match (value, parent_nulls) {
            (ColumnarValue::Array(array), Some(parent_nulls)) if !array.data_type().is_null() => {
                if array.null_count() == array.len()
                    || array
                        .nulls()
                        .is_some_and(|nulls| nulls.inner().ptr_eq(parent_nulls.inner()))
                {
                    return Ok(ColumnarValue::Array(array));
                }
                // Arrow children may contain valid values underneath a null struct.
                // Spark's GetStructField returns NULL whenever the parent is NULL.
                // The child is already valid Arrow data; replace only its null mask
                // without revalidating variable-width payloads.
                let parent_is_null = BooleanArray::new(!parent_nulls.inner(), None);
                Ok(ColumnarValue::Array(nullif(
                    array.as_ref(),
                    &parent_is_null,
                )?))
            }
            (value, _) => Ok(value),
        }
    }
}

/// Compare nested names without allocating flattened schemas. The caller checks
/// `equals_datatype` first, which checks types and nullability but ignores names.
fn same_field_names(left: &DataType, right: &DataType) -> bool {
    let same_field = |left: &FieldRef, right: &FieldRef| {
        left.name() == right.name() && same_field_names(left.data_type(), right.data_type())
    };
    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) => left
            .iter()
            .zip(right)
            .all(|(left, right)| same_field(left, right)),
        (DataType::Union(left, _), DataType::Union(right, _)) => left
            .iter()
            .zip(right.iter())
            // `equals_datatype` matches unions by ID, regardless of field order.
            .all(|((left_id, left), (right_id, right))| {
                left_id == right_id && same_field(left, right)
            }),
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right))
        | (DataType::FixedSizeList(left, _), DataType::FixedSizeList(right, _))
        | (DataType::Map(left, _), DataType::Map(right, _))
        | (DataType::RunEndEncoded(_, left), DataType::RunEndEncoded(_, right)) => {
            same_field(left, right)
        }
        (DataType::Dictionary(_, left), DataType::Dictionary(_, right)) => {
            same_field_names(left, right)
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, UnionFields, UnionMode};

    use super::*;

    #[test]
    fn metadata_normalization_requires_matching_names_and_union_order() -> Result<()> {
        let nested = |field| {
            DataType::Struct(
                vec![Field::new("items", DataType::List(Arc::new(field)), true)].into(),
            )
        };
        let element = Field::new("element", DataType::Int32, true);
        let expected = nested(element.clone());
        let actual =
            nested(element.with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])));
        assert_ne!(actual, expected);
        assert!(actual.equals_datatype(&expected));
        assert!(same_field_names(&actual, &expected));

        let renamed = nested(Field::new("renamed", DataType::Int32, true));
        assert!(actual.equals_datatype(&renamed));
        assert!(!same_field_names(&actual, &renamed));

        let complex = Field::new("value", actual, true);
        let scalar = Field::new("value", DataType::Int32, true);
        let original = DataType::Union(
            UnionFields::try_new([0, 1], [complex.clone(), scalar.clone()])?,
            UnionMode::Sparse,
        );
        let reordered = DataType::Union(
            UnionFields::try_new([1, 0], [scalar, complex])?,
            UnionMode::Sparse,
        );
        assert!(original.equals_datatype(&reordered));
        assert!(!same_field_names(&original, &reordered));
        Ok(())
    }
}
