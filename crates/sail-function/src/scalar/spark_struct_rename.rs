use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

/// Spark-compatible "rename struct fields by position".
///
/// DataFusion only allows casting between structs that share at least one
/// field name. Spark allows positional casts between structs of matching arity
/// even when no names overlap (e.g. `CAST(struct(x, x) AS struct<f1:T, f2:T>)`).
///
/// This UDF takes a single input column and a target type (struct or
/// list-of-struct, possibly nested) and returns the same underlying values with
/// the field names rewritten positionally to match the target. It is a
/// metadata-only operation when the leaf types already match; if leaf types
/// differ, callers should wrap the result in a regular CAST after the rename.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkStructRename {
    signature: Signature,
    target_type: DataType,
}

impl SparkStructRename {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }
}

impl ScalarUDFImpl for SparkStructRename {
    fn name(&self) -> &str {
        "spark_struct_rename"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("spark_struct_rename expects exactly one argument");
        }
        make_scalar_function(|arrays| rename_positionally(&arrays[0], &self.target_type))(
            &args.args,
        )
    }
}

/// Recursively rebuild `arr` so that any `Struct` field names match those in
/// `target_type` positionally. Leaf data types must already match.
fn rename_positionally(arr: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    match (arr.data_type(), target_type) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields)) => {
            if src_fields.len() != tgt_fields.len() {
                return exec_err!(
                    "spark_struct_rename: struct field count mismatch: {} vs {}",
                    src_fields.len(),
                    tgt_fields.len()
                );
            }
            let src = arr.as_struct();
            let new_columns: Vec<ArrayRef> = src
                .columns()
                .iter()
                .zip(tgt_fields.iter())
                .map(|(c, tgt)| rename_positionally(c, tgt.data_type()))
                .collect::<Result<_>>()?;
            let new_fields: Fields = tgt_fields
                .iter()
                .zip(new_columns.iter())
                .map(|(target_field, column)| {
                    Arc::new(
                        target_field
                            .as_ref()
                            .clone()
                            .with_data_type(column.data_type().clone()),
                    )
                })
                .collect();
            let renamed = StructArray::try_new(new_fields, new_columns, src.nulls().cloned())?;
            Ok(Arc::new(renamed))
        }
        (DataType::List(_), DataType::List(tgt_field)) => {
            let src = arr.as_list::<i32>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                tgt_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_values.data_type().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListArray::<i32>::try_new(
                new_field,
                src.offsets().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::LargeList(_), DataType::LargeList(tgt_field)) => {
            let src = arr.as_list::<i64>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                tgt_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_values.data_type().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListArray::<i64>::try_new(
                new_field,
                src.offsets().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::ListView(_), DataType::ListView(tgt_field)) => {
            let src = arr.as_list_view::<i32>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                tgt_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_values.data_type().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListViewArray::<i32>::try_new(
                new_field,
                src.offsets().clone(),
                src.sizes().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::LargeListView(_), DataType::LargeListView(tgt_field)) => {
            let src = arr.as_list_view::<i64>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                tgt_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_values.data_type().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListViewArray::<i64>::try_new(
                new_field,
                src.offsets().clone(),
                src.sizes().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::FixedSizeList(_, src_size), DataType::FixedSizeList(tgt_field, tgt_size)) => {
            if src_size != tgt_size {
                return exec_err!(
                    "spark_struct_rename: fixed-size list length mismatch: {} vs {}",
                    src_size,
                    tgt_size
                );
            }
            let src = arr
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_struct_rename: expected FixedSizeListArray".to_string(),
                    )
                })?;
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                tgt_field
                    .as_ref()
                    .clone()
                    .with_data_type(new_values.data_type().clone()),
            );
            let renamed = FixedSizeListArray::try_new(
                new_field,
                *tgt_size,
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::Map(_, src_sorted), DataType::Map(tgt_entry, tgt_sorted)) => {
            if src_sorted != tgt_sorted {
                return exec_err!(
                    "spark_struct_rename: map sortedness mismatch: {} vs {}",
                    src_sorted,
                    tgt_sorted
                );
            }
            // Maps are list-of-struct under the hood; recurse into the entries struct.
            let src = arr.as_map();
            let entries = src.entries();
            let new_entries_arr = rename_positionally(
                &(Arc::new(entries.clone()) as ArrayRef),
                tgt_entry.data_type(),
            )?;
            let new_entries_struct = new_entries_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_struct_rename: map entries must be Struct".to_string(),
                    )
                })?;
            let new_entry_field = Arc::new(
                tgt_entry
                    .as_ref()
                    .clone()
                    .with_data_type(new_entries_struct.data_type().clone()),
            );
            let renamed = datafusion::arrow::array::MapArray::try_new(
                new_entry_field,
                src.offsets().clone(),
                new_entries_struct.clone(),
                src.nulls().cloned(),
                *tgt_sorted,
            )?;
            Ok(Arc::new(renamed))
        }
        (source_type, target_type) if source_type == target_type => Ok(Arc::clone(arr)),
        (source_type, target_type) => {
            exec_err!("spark_struct_rename: leaf type mismatch: {source_type} vs {target_type}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{GenericListViewArray, Int32Array, OffsetSizeTrait};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_rename_positionally_uses_target_field_definition() -> Result<()> {
        let source_field = Arc::new(Field::new("source", DataType::Int32, false));
        let source = Arc::new(StructArray::try_new(
            Fields::from(vec![source_field]),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        )?) as ArrayRef;
        let target_type = DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "target",
            DataType::Int32,
            true,
        ))]));

        let renamed = rename_positionally(&source, &target_type)?;

        assert_eq!(renamed.data_type(), &target_type);
        Ok(())
    }

    #[test]
    fn test_rename_positionally_rejects_leaf_type_changes() {
        let source = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        assert!(rename_positionally(&source, &DataType::Int64).is_err());
    }

    fn check_list_view_fields<O: OffsetSizeTrait>() -> Result<()> {
        let source = GenericListViewArray::<O>::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            vec![4, 1, 0, 2, 1].into_iter().map(O::usize_as).collect(),
            vec![1, 3, 0, 2, 2].into_iter().map(O::usize_as).collect(),
            Arc::new(Int32Array::from(vec![
                Some(10),
                Some(20),
                None,
                Some(40),
                Some(50),
            ])),
            Some(NullBuffer::from(vec![true, true, true, false, true])),
        )?
        .slice(1, 4);
        let element = Arc::new(
            Field::new("element", DataType::Int32, true)
                .with_metadata(HashMap::from([("logical_type".into(), "element".into())])),
        );
        let target = if O::IS_LARGE {
            DataType::LargeListView(element)
        } else {
            DataType::ListView(element)
        };
        let input: ArrayRef = Arc::new(source.clone());
        let renamed = rename_positionally(&input, &target)?;
        let view = renamed.as_list_view::<O>();
        assert_eq!(view.data_type(), &target);
        assert_eq!(view.offsets(), source.offsets());
        assert_eq!(view.sizes(), source.sizes());
        assert_eq!(view.nulls(), source.nulls());
        assert!(Arc::ptr_eq(view.values(), source.values()));

        // Exercise the scalar cast path that loses the requested child metadata.
        let udf = SparkStructRename::new(target.clone());
        for row in 0..source.len() {
            let scalar = ScalarValue::try_from_array(&source, row)?.cast_to(&target)?;
            let input_field = Arc::new(Field::new("input", scalar.data_type(), scalar.is_null()));
            let return_field = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&input_field)],
                scalar_arguments: &[Some(&scalar)],
            })?;
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(scalar)],
                arg_fields: vec![input_field],
                number_rows: 1,
                return_field,
                config_options: Default::default(),
            })?;
            assert!(matches!(result, ColumnarValue::Scalar(_)));
            assert_eq!(result.data_type(), target);
            assert_eq!(
                result.into_array(1)?.as_ref(),
                renamed.slice(row, 1).as_ref()
            );
        }
        Ok(())
    }

    #[test]
    fn test_list_view_field_restoration() -> Result<()> {
        check_list_view_fields::<i32>()
    }

    #[test]
    fn test_large_list_view_field_restoration() -> Result<()> {
        check_list_view_fields::<i64>()
    }
}
