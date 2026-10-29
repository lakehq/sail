use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, MapArray, StructArray, make_array};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::normalize_float_zero;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Normalize signed-zero comparison keys without changing selected raw values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NormalizeFloatKeys {
    signature: Signature,
}

impl Default for NormalizeFloatKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl NormalizeFloatKeys {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    pub fn has_float(data_type: &DataType) -> bool {
        match data_type {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) => Self::has_float(field.data_type()),
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) => fields
                    .get(1)
                    .is_some_and(|value| Self::has_float(value.data_type())),
                _ => false,
            },
            DataType::Struct(fields) => fields
                .iter()
                .any(|field| Self::has_float(field.data_type())),
            DataType::Union(fields, _) => fields
                .iter()
                .any(|(_, field)| Self::has_float(field.data_type())),
            DataType::Dictionary(_, value) => Self::has_float(value),
            DataType::RunEndEncoded(_, value) => Self::has_float(value.data_type()),
            _ => false,
        }
    }
}

impl ScalarUDFImpl for NormalizeFloatKeys {
    fn name(&self) -> &str {
        "normalize_float_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args preserves the comparison key's field")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields {
            [field] => Ok(Arc::clone(field)),
            _ => internal_err!("normalize_float_keys expects one argument"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("normalize_float_keys expects one argument");
        };
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(normalize(array)?)),
            ColumnarValue::Scalar(value) => {
                let array = normalize(&value.to_array()?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
    }
}

fn normalize(array: &ArrayRef) -> Result<ArrayRef> {
    if !NormalizeFloatKeys::has_float(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    if array.data_type().is_floating() {
        return Ok(normalize_float_zero(array));
    }
    if let DataType::Map(field, sorted) = array.data_type() {
        // Spark's recursive normalization preserves map keys and normalizes values.
        let map = array.as_map();
        let values = normalize(map.values())?;
        if Arc::ptr_eq(map.values(), &values) {
            return Ok(Arc::clone(array));
        }
        let entries = StructArray::try_new(
            map.entries().fields().clone(),
            vec![Arc::clone(map.keys()), values],
            map.entries().nulls().cloned(),
        )?;
        return Ok(Arc::new(MapArray::try_new(
            Arc::clone(field),
            map.offsets().clone(),
            entries,
            map.nulls().cloned(),
            *sorted,
        )?));
    }
    // Retain the parent's offsets, validity, fields and metadata. Only changed
    // floating leaf buffers are replaced; null lists and sliced arrays stay intact.
    let data = array.to_data();
    let mut changed = false;
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            let child = make_array(child.clone());
            let normalized = normalize(&child)?;
            changed |= !Arc::ptr_eq(&child, &normalized);
            Ok(normalized.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        Ok(make_array(
            data.into_builder().child_data(children).build()?,
        ))
    } else {
        Ok(Arc::clone(array))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::builder::{Float64Builder, MapBuilder};
    use arrow::array::{Array, AsArray, Float32Array, Float64Array, ListArray, StructArray};
    use arrow::datatypes::{Field, Float32Type, Float64Type};

    use super::*;

    #[test]
    fn normalizes_zeros_without_changing_nan_bits_or_nulls() -> Result<()> {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            Some(f64::from_bits(0xfff8_0000_0000_0042)),
            None,
        ]));
        let result = normalize(&values)?;
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0).to_bits(), 0);
        assert_eq!(result.value(1).to_bits(), 0);
        assert_eq!(result.value(2).to_bits(), 0x7ff0_0000_0000_0001);
        assert_eq!(result.value(3).to_bits(), 0xfff8_0000_0000_0042);
        assert!(result.is_null(4));
        let single: ArrayRef =
            Arc::new(Float32Array::from(vec![-0.0, f32::from_bits(0x7f80_0001)]));
        let result = normalize(&single)?;
        assert_eq!(result.as_primitive::<Float32Type>().value(0).to_bits(), 0);
        assert_eq!(
            result.as_primitive::<Float32Type>().value(1).to_bits(),
            0x7f80_0001
        );
        Ok(())
    }

    #[test]
    fn canonical_comparison_keys_leave_raw_values_unchanged() -> Result<()> {
        let bits = [
            0x7ff0_0000_0000_0001,
            0xfff8_0000_0000_0042,
            (-0.0_f64).to_bits(),
        ];
        let raw: ArrayRef = Arc::new(Float64Array::from(
            bits.iter().copied().map(f64::from_bits).collect::<Vec<_>>(),
        ));
        let canonical = normalize(&raw)?;
        let canonical = canonical.as_primitive::<Float64Type>();
        assert_eq!(canonical.value(0).to_bits(), bits[0]);
        assert_eq!(canonical.value(1).to_bits(), bits[1]);
        assert_eq!(canonical.value(2).to_bits(), 0);
        for (index, bits) in bits.into_iter().enumerate() {
            assert_eq!(
                raw.as_primitive::<Float64Type>().value(index).to_bits(),
                bits
            );
        }
        Ok(())
    }

    #[test]
    fn preserves_sliced_nested_fields_and_validity() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(9.0)]),
            Some(vec![
                Some(-0.0),
                None,
                Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            ]),
            None,
            Some(vec![]),
        ])
        .slice(1, 3);
        let field = Arc::new(
            Field::new("values", list.data_type().clone(), true)
                .with_metadata(HashMap::from([("kept".into(), "yes".into())])),
        );
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![field].into(),
            vec![Arc::new(list)],
            None,
        ));
        let result = normalize(&input)?;
        assert_eq!(result.data_type(), input.data_type());
        let original = input.as_struct().column(0).as_list::<i32>();
        let list = result.as_struct().column(0).as_list::<i32>();
        assert_eq!(list.value_offsets(), original.value_offsets());
        assert_eq!(list.nulls(), original.nulls());
        let first = list.value(0);
        let first = first.as_primitive::<Float64Type>();
        assert_eq!(first.value(0).to_bits(), 0);
        assert!(first.is_null(1));
        assert_eq!(first.value(2).to_bits(), 0x7ff0_0000_0000_0001);
        assert!(list.is_null(1));
        assert!(list.value(2).is_empty());
        Ok(())
    }

    #[test]
    fn preserves_map_keys_while_normalizing_values() -> Result<()> {
        let mut builder = MapBuilder::new(None, Float64Builder::new(), Float64Builder::new());
        builder.keys().append_value(-0.0);
        builder.values().append_value(-0.0);
        builder.append(true)?;
        builder.append(false)?;
        let input: ArrayRef = Arc::new(builder.finish());
        let result = normalize(&input)?;
        let map = result.as_map();
        assert_eq!(result.data_type(), input.data_type());
        assert_eq!(
            map.keys().as_primitive::<Float64Type>().value(0).to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            map.values()
                .as_primitive::<Float64Type>()
                .value(0)
                .to_bits(),
            0
        );
        assert!(map.is_null(1));
        Ok(())
    }

    #[test]
    fn retains_scalar_nullability_and_field_metadata() -> Result<()> {
        let function = NormalizeFloatKeys::new();
        for nullable in [false, true] {
            let field = Arc::new(
                Field::new("key", DataType::Float64, nullable)
                    .with_metadata(HashMap::from([("comment".into(), "kept".into())])),
            );
            let fields = [Arc::clone(&field)];
            let output = function.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None],
            })?;
            assert_eq!(output, field);
            let value = if nullable { None } else { Some(-0.0) };
            let result = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Float64(value))],
                arg_fields: fields.to_vec(),
                number_rows: 1,
                return_field: output,
                config_options: Arc::default(),
            })?;
            let ColumnarValue::Scalar(ScalarValue::Float64(result)) = result else {
                return internal_err!("expected a Float64 scalar");
            };
            assert_eq!(result.map(f64::to_bits), value.map(|_| 0));
        }
        Ok(())
    }

    #[test]
    fn keeps_unchanged_arrays_shared() -> Result<()> {
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(0.0), Some(1.0)]),
            None,
        ]));
        assert!(Arc::ptr_eq(&input, &normalize(&input)?));
        Ok(())
    }
}
