use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, StructArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

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
/// An optional boolean literal prepares hidden nested storage for a subsequent
/// strict cast. Ordinary rename callers do not request this extra work.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkStructRename {
    signature: Signature,
    target_type: DataType,
}

impl SparkStructRename {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (input, prepare_for_cast) = match args.args.as_slice() {
            [input] => (input, false),
            [
                input,
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(prepare))),
            ] => (input, *prepare),
            _ => {
                return exec_err!(
                    "spark_struct_rename expects an input and an optional boolean literal"
                );
            }
        };
        let arr = ColumnarValue::values_to_arrays(std::slice::from_ref(input))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "spark_struct_rename: missing input array".to_string(),
                )
            })?;
        let renamed = rename_positionally(&arr, &self.target_type, prepare_for_cast)?;
        Ok(ColumnarValue::Array(renamed))
    }
}

/// Recursively rebuild `arr` so that any `Struct` field names match those in
/// `target_type` positionally. Leaf data types must already match.
fn rename_positionally(
    arr: &ArrayRef,
    target_type: &DataType,
    prepare_for_cast: bool,
) -> Result<ArrayRef> {
    // Null indices omit hidden list/map ranges as well as struct children.
    // Rebuild only nullable nested storage, and only when a cast requests it.
    let visible;
    let arr = if prepare_for_cast && arr.null_count() > 0 && arr.data_type().is_nested() {
        let indices =
            UInt64Array::from_iter((0..arr.len()).map(|i| arr.is_valid(i).then_some(i as u64)));
        visible = datafusion::arrow::compute::take(arr.as_ref(), &indices, None)?;
        &visible
    } else {
        arr
    };
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
                .map(|(c, tgt)| rename_positionally(c, tgt.data_type(), prepare_for_cast))
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
            let new_values =
                rename_positionally(src.values(), tgt_field.data_type(), prepare_for_cast)?;
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
            let new_values =
                rename_positionally(src.values(), tgt_field.data_type(), prepare_for_cast)?;
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
            let new_values =
                rename_positionally(src.values(), tgt_field.data_type(), prepare_for_cast)?;
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
                prepare_for_cast,
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
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn test_rename_positionally_masks_hidden_nonnullable_children() -> Result<()> {
        use datafusion::arrow::array::StringArray;
        let source: ArrayRef = Arc::new(StructArray::try_new(
            vec![Arc::new(Field::new("a", DataType::Utf8, false))].into(),
            vec![Arc::new(StringArray::from(vec!["bad", "7", "bad"]))],
            Some(vec![false, true, false].into()),
        )?);
        let source = source.slice(1, 2);
        let target = DataType::Struct(vec![Field::new("x", DataType::Utf8, false)].into());
        let unprepared = rename_positionally(&source, &target, false)?;
        assert!(unprepared.as_struct().column(0).is_valid(1));
        let renamed = rename_positionally(&source, &target, true)?;
        assert_eq!(renamed.data_type(), &target);
        assert_eq!(renamed.nulls(), source.nulls());
        let child = renamed.as_struct().column(0);
        assert!(child.is_valid(0));
        assert!(child.is_null(1));
        let converted = datafusion::arrow::compute::cast_with_options(
            child,
            &DataType::Int32,
            &datafusion::arrow::compute::CastOptions {
                safe: false,
                ..Default::default()
            },
        )?;
        assert_eq!(
            converted
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .value(0),
            7
        );
        Ok(())
    }

    #[test]
    fn test_cast_preparation_omits_hidden_list_and_map_values() -> Result<()> {
        use datafusion::arrow::array::{ListArray, MapArray, StringArray};
        use datafusion::arrow::buffer::OffsetBuffer;
        let values: ArrayRef = Arc::new(StringArray::from(vec!["bad", "7", "bad"]));
        let leaf = Arc::new(Field::new("item", DataType::Utf8, true));
        let list: ArrayRef = Arc::new(ListArray::try_new(
            leaf,
            OffsetBuffer::from_lengths([1, 1, 1]),
            values.clone(),
            None,
        )?);
        let entries = StructArray::try_new(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
            vec![Arc::new(StringArray::from(vec!["k"; 3])), values],
            None,
        )?;
        let map: ArrayRef = Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1, 1, 1]),
            entries,
            None,
            false,
        )?);
        for child in [list, map] {
            let source: ArrayRef = Arc::new(StructArray::try_new(
                vec![Field::new("a", child.data_type().clone(), false)].into(),
                vec![child],
                Some(vec![false, true, false].into()),
            )?);
            let source = source.slice(1, 2);
            for prepare in [false, true] {
                let renamed = rename_positionally(&source, source.data_type(), prepare)?;
                assert_eq!(renamed.nulls(), source.nulls());
                let child = renamed.as_struct().column(0);
                let values = match child.data_type() {
                    DataType::List(_) => child.as_list::<i32>().values(),
                    DataType::Map(_, _) => child.as_map().values(),
                    _ => return exec_err!("expected list or map"),
                };
                let converted = datafusion::arrow::compute::cast_with_options(
                    values,
                    &DataType::Int32,
                    &datafusion::arrow::compute::CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                );
                assert_eq!(converted.is_ok(), prepare);
                if prepare {
                    assert_eq!(
                        converted?
                            .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                            .values()
                            .as_ref(),
                        &[7]
                    );
                }
            }
        }
        Ok(())
    }

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

        let renamed = rename_positionally(&source, &target_type, false)?;

        assert_eq!(renamed.data_type(), &target_type);
        Ok(())
    }

    #[test]
    fn test_rename_positionally_rejects_leaf_type_changes() {
        let source = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        assert!(rename_positionally(&source, &DataType::Int64, false).is_err());
    }
}
