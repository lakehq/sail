use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::buffer::NullBuffer;
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

/// Extract a struct field path while preserving every ancestor's validity.
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
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_field().inner().coerce_types(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        get_field().return_field_from_args(args)
    }

    fn simplify(&self, mut args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Keep maps and dictionaries at their native access boundaries.
        let mut flattened = false;
        while let Some(Expr::ScalarFunction(parent)) = args.first()
            && parent.func.inner().is::<Self>()
            && matches!(info.get_data_type(&args[0])?, DataType::Struct(_))
            && matches!(info.get_data_type(&parent.args[0])?, DataType::Struct(_))
        {
            let mut path = parent.args.clone();
            path.extend(args.into_iter().skip(1));
            args = path;
            flattened = true;
        }
        match get_field().inner().simplify(args, info)? {
            ExprSimplifyResult::Simplified(expr) => {
                // Keep constructor-field pruning without letting nested accesses
                // bypass the parent-validity handling in this function.
                let expr = expr
                    .transform_up(|expr| {
                        if let Expr::ScalarFunction(function) = &expr
                            && function.func.inner().is::<GetFieldFunc>()
                        {
                            let expr = ScalarUDF::from(Self::new()).call(function.args.clone());
                            return Ok(Transformed::yes(expr));
                        }
                        Ok(Transformed::no(expr))
                    })
                    .data()?;
                Ok(ExprSimplifyResult::Simplified(expr))
            }
            ExprSimplifyResult::Original(args) if flattened => Ok(ExprSimplifyResult::Simplified(
                ScalarUDF::from(Self::new()).call(args),
            )),
            result => Ok(result),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let struct_array = match args.args.first() {
            Some(ColumnarValue::Array(array))
                if matches!(array.data_type(), DataType::Struct(_)) =>
            {
                Some(Arc::clone(array))
            }
            Some(ColumnarValue::Scalar(ScalarValue::Struct(array))) => {
                Some(Arc::clone(array) as ArrayRef)
            }
            _ => None,
        };
        if let Some(array) = struct_array {
            let path = args.args[1..].iter().map(|name| match name {
                ColumnarValue::Scalar(name) => name
                    .try_as_str()
                    .flatten()
                    .ok_or_else(|| internal_datafusion_err!("invalid struct field name")),
                _ => Err(internal_datafusion_err!("struct field name must be scalar")),
            });
            return extract_struct_field(array, path, args.return_type());
        }
        let return_field = args.return_field.clone();
        let value = get_field().inner().invoke_with_args(args)?;
        finish_field(value, return_field.data_type(), None)
    }
}

/// Walk a struct path without rebuilding intermediate arrays, then mask its leaf once.
pub(super) fn extract_struct_field<'a>(
    array: ArrayRef,
    names: impl Iterator<Item = Result<&'a str>>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    let mut value = &array;
    let mut ancestors = Vec::with_capacity(names.size_hint().0);
    for name in names {
        let name = name?;
        let parent = datafusion_common::cast::as_struct_array(value.as_ref())?;
        if let Some(nulls) = parent.nulls().filter(|nulls| nulls.null_count() > 0) {
            ancestors.push(nulls);
        }
        value = parent
            .column_by_name(name)
            .ok_or_else(|| internal_datafusion_err!("field {name} is missing"))?;
    }
    if let Some(nulls) = value.nulls() {
        ancestors.retain(|parent| !same_null_buffer(nulls, parent));
    }
    // Equal masks are common in nested data. Avoid allocating and recounting
    // their union, including when the leaf has additional or different nulls.
    ancestors.dedup_by(|left, right| same_null_buffer(left, right));
    let nulls = match ancestors.as_slice() {
        [] => None,
        [nulls] => Some((*nulls).clone()),
        _ => NullBuffer::union_many(ancestors.into_iter().map(Some)),
    };
    finish_field(ColumnarValue::Array(Arc::clone(value)), return_type, nulls)
}

/// A fast sufficient equality check. Matching byte spans imply equal logical
/// slices only when their offsets and lengths agree. Different unused bits may
/// miss this shortcut, in which case callers still use Arrow's logical kernels.
fn same_null_buffer(left: &NullBuffer, right: &NullBuffer) -> bool {
    if left.inner().ptr_eq(right.inner()) {
        return true;
    }
    if left.null_count() != right.null_count()
        || left.len() != right.len()
        || left.offset() != right.offset()
    {
        return false;
    }
    // Slices can retain a much larger allocation. Compare only the bytes
    // containing this slice, rather than scanning the entire backing buffer.
    let start = left.offset() / 8;
    let end = (left.offset() + left.len()).div_ceil(8);
    left.validity()[start..end] == right.validity()[start..end]
}

/// Normalize the selected field and apply its ancestors' validity once.
pub(super) fn finish_field(
    mut value: ColumnarValue,
    return_type: &DataType,
    parent_nulls: Option<NullBuffer>,
) -> Result<ColumnarValue> {
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
                || array.nulls().is_some_and(|nulls| {
                    same_null_buffer(nulls, &parent_nulls)
                        || (nulls.null_count() >= parent_nulls.null_count()
                            && nulls.contains(&parent_nulls))
                })
            {
                // Readers can produce separate buffers for the same validity,
                // or a child mask that already covers all parent nulls.
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

    use datafusion::arrow::buffer::{BooleanBuffer, Buffer};
    use datafusion::arrow::datatypes::{Field, UnionFields, UnionMode};

    use super::*;

    #[test]
    fn mask_shortcut_respects_bit_slice_boundaries() {
        let mask = |offset, len| {
            NullBuffer::new(BooleanBuffer::new(
                Buffer::from(vec![0b0101_u8]),
                offset,
                len,
            ))
        };
        let original = mask(0, 2);
        let shifted = mask(1, 2);
        // Equal bytes and null counts do not imply equal logical positions.
        assert_eq!(original.validity(), shifted.validity());
        assert_eq!(original.null_count(), shifted.null_count());
        assert_ne!(original, shifted);
        assert!(!same_null_buffer(&original, &shifted));
        assert!(same_null_buffer(&original, &mask(0, 2)));
        assert!(!same_null_buffer(&original, &mask(0, 3)));
        let with_unused_bytes =
            NullBuffer::new(BooleanBuffer::new(Buffer::from(vec![0b0101_u8, 0]), 0, 2));
        assert!(same_null_buffer(&original, &with_unused_bytes));
    }

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
