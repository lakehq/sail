//! UUID identity and validated write conversion. UUIDs are not arbitrary fixed[16] fields.
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    make_array, new_null_array,
};
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

pub const UUID_METADATA_KEY: &str = "iceberg.type";

pub fn is_uuid_field(field: &Field) -> bool {
    field.data_type() == &DataType::FixedSizeBinary(16)
        && field.metadata().get(UUID_METADATA_KEY).map(String::as_str) == Some("uuid")
}

pub fn contains_uuid(field: &Field) -> bool {
    is_uuid_field(field)
        || match field.data_type() {
            DataType::Struct(fields) => fields.iter().any(|field| contains_uuid(field)),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => contains_uuid(field),
            _ => false,
        }
}

/// Replace only UUID leaves, retaining input shape, names, and unrelated data types.
/// Schema evolution and ordinary casts remain the responsibility of the caller.
pub fn uuid_input_field(source: &Field, target: &Field) -> Field {
    if is_uuid_field(target) || (source.data_type() == &DataType::Null && contains_uuid(target)) {
        let mut metadata = source.metadata().clone();
        metadata.extend(target.metadata().clone());
        return source
            .clone()
            .with_data_type(target.data_type().clone())
            .with_metadata(metadata);
    }
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source), DataType::Struct(target)) => DataType::Struct(
            source
                .iter()
                .map(|field| {
                    target
                        .iter()
                        .find(|target| target.name() == field.name())
                        .map(|target| Arc::new(uuid_input_field(field, target)))
                        .unwrap_or_else(|| field.clone())
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        (DataType::List(source), DataType::List(target) | DataType::LargeList(target)) => {
            DataType::List(Arc::new(uuid_input_field(source, target)))
        }
        (DataType::LargeList(source), DataType::List(target) | DataType::LargeList(target)) => {
            DataType::LargeList(Arc::new(uuid_input_field(source, target)))
        }
        (DataType::FixedSizeList(source, size), DataType::FixedSizeList(target, _)) => {
            DataType::FixedSizeList(Arc::new(uuid_input_field(source, target)), *size)
        }
        (DataType::Map(source, sorted), DataType::Map(target, _)) => {
            DataType::Map(Arc::new(uuid_input_field(source, target)), *sorted)
        }
        _ => source.data_type().clone(),
    };
    source.clone().with_data_type(data_type)
}

/// TRY_CAST can introduce nulls at UUID leaves, not at their containing structs
/// or map entries. Use the same field transformation for planning and execution.
fn uuid_cast_field(source: &Field, target: &Field, safe: bool) -> Result<Field> {
    let field = uuid_input_field(source, target);
    if safe {
        nullable_uuid_field(source, &field)
    } else {
        Ok(field)
    }
}

fn nullable_uuid_field(source: &Field, field: &Field) -> Result<Field> {
    if is_uuid_field(field) {
        let fallible = !matches!(
            source.data_type(),
            DataType::Null | DataType::FixedSizeBinary(16)
        );
        return Ok(field.clone().with_nullable(field.is_nullable() || fallible));
    }
    let data_type = match (source.data_type(), field.data_type()) {
        (DataType::Struct(source), DataType::Struct(fields)) => DataType::Struct(
            source
                .iter()
                .zip(fields)
                .map(|(source, field)| nullable_uuid_field(source, field).map(Arc::new))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        (DataType::List(source), DataType::List(field)) => {
            DataType::List(Arc::new(nullable_uuid_field(source, field)?))
        }
        (DataType::LargeList(source), DataType::LargeList(field)) => {
            DataType::LargeList(Arc::new(nullable_uuid_field(source, field)?))
        }
        (DataType::FixedSizeList(source, _), DataType::FixedSizeList(field, size)) => {
            DataType::FixedSizeList(Arc::new(nullable_uuid_field(source, field)?), *size)
        }
        (DataType::Map(source, _), DataType::Map(entries, sorted)) => {
            let entries = nullable_uuid_field(source, entries)?;
            if let DataType::Struct(fields) = entries.data_type()
                && fields[0].is_nullable()
            {
                // Like Spark TRY_CAST, do not allow a fallible conversion to
                // introduce null map keys. Map entries themselves stay non-null.
                return exec_err!("TRY_CAST cannot introduce null UUID map keys");
            }
            DataType::Map(Arc::new(entries), *sorted)
        }
        _ => field.data_type().clone(),
    };
    Ok(field.clone().with_data_type(data_type))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UuidCast {
    signature: Signature,
    target: FieldRef,
    safe: bool,
}

impl UuidCast {
    pub fn new(target: FieldRef, safe: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target,
            safe,
        }
    }

    pub fn target(&self) -> &FieldRef {
        &self.target
    }
    pub fn safe(&self) -> bool {
        self.safe
    }
}

impl ScalarUDFImpl for UuidCast {
    fn name(&self) -> &str {
        "uuid_cast"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [source] = args else {
            return exec_err!("uuid_cast expects one argument");
        };
        Ok(uuid_cast_field(
            &Field::new("", source.clone(), true),
            &self.target,
            self.safe,
        )?
        .data_type()
        .clone())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return exec_err!("uuid_cast expects one argument");
        };
        Ok(Arc::new(
            uuid_cast_field(source, &self.target, self.safe)?
                .with_nullable(source.is_nullable() || self.safe),
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [source] = args.args.as_slice() else {
            return exec_err!("uuid_cast expects one argument");
        };
        let scalar = matches!(source, ColumnarValue::Scalar(_));
        let array = source.to_array(if scalar { 1 } else { args.number_rows })?;
        let output = convert_uuid_array(&array, &self.target, self.safe)?;
        if scalar {
            Ok(ColumnarValue::Scalar(
                crate::utils::scalar::scalar_from_array(&output, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn parse_values<'a>(values: impl Iterator<Item = Option<&'a str>>, safe: bool) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        match value {
            None => builder.append_null(),
            Some(value) => {
                // Accept the standard hyphenated spelling, case-insensitively. Do not
                // accept compact/URN/braced forms or silently truncate UTF-8 bytes.
                let parsed = (value.len() == 36)
                    .then(|| uuid::Uuid::parse_str(value))
                    .transpose();
                match parsed {
                    Ok(Some(uuid)) => builder.append_value(uuid.as_bytes())?,
                    _ if safe => builder.append_null(),
                    _ => return exec_err!("Invalid UUID value: {value:?}"),
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn convert_uuid_leaf(array: &ArrayRef, safe: bool) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Null || array.null_count() == array.len() {
        return Ok(new_null_array(&DataType::FixedSizeBinary(16), array.len()));
    }
    match array.data_type() {
        DataType::Utf8 => parse_values(as_string_array(array)?.iter(), safe),
        DataType::LargeUtf8 => parse_values(as_large_string_array(array)?.iter(), safe),
        DataType::Utf8View => parse_values(as_string_view_array(array)?.iter(), safe),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(16) => {
            Ok(cast_with_options(
                array,
                &DataType::FixedSizeBinary(16),
                &CastOptions {
                    safe,
                    ..Default::default()
                },
            )?)
        }
        DataType::BinaryView => convert_uuid_leaf(&cast(array, &DataType::Binary)?, safe),
        other => exec_err!("Cannot convert {other} to UUID"),
    }
}

fn convert_uuid_array(array: &ArrayRef, target: &Field, safe: bool) -> Result<ArrayRef> {
    let source = Field::new("", array.data_type().clone(), true);
    let output = uuid_cast_field(&source, target, safe)?;
    convert_visible_uuid_array(array, &output, safe, &vec![true; array.len()])
}

fn convert_visible_uuid_array(
    array: &ArrayRef,
    output: &Field,
    safe: bool,
    visible: &[bool],
) -> Result<ArrayRef> {
    if is_uuid_field(output) {
        if visible.iter().all(|visible| *visible) {
            return convert_uuid_leaf(array, safe);
        }
        let hidden = BooleanArray::from(visible.iter().map(|v| !v).collect::<Vec<_>>());
        let masked = arrow::compute::nullif(array.as_ref(), &hidden)?;
        let converted = convert_uuid_leaf(&masked, safe)?;
        // Hidden values must not be validated. Use a non-null placeholder there
        // so non-nullable list elements/map keys remain valid Arrow storage.
        // Parent validity and offsets (and all visible leaf nulls) are unchanged.
        return Ok(arrow::compute::kernels::zip::zip(
            &hidden,
            &FixedSizeBinaryArray::new_scalar([0; 16]),
            &converted.as_ref(),
        )?);
    }
    if array.data_type() == &DataType::Null {
        return Ok(new_null_array(output.data_type(), array.len()));
    }
    let fields: Vec<FieldRef> = match output.data_type() {
        DataType::Struct(fields) => fields.iter().cloned().collect(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field.clone()],
        _ => return Ok(array.clone()),
    };
    let data = array.to_data();
    let visible = visible
        .iter()
        .enumerate()
        .map(|(i, visible)| *visible && array.is_valid(i))
        .collect::<Vec<_>>();
    let child_visible = match array.data_type() {
        DataType::Struct(_) => visible,
        DataType::List(_) => visible_child_ranges(
            &visible,
            array
                .as_list::<i32>()
                .value_offsets()
                .iter()
                .map(|x| *x as usize),
            data.child_data()[0].len(),
        ),
        DataType::LargeList(_) => visible_child_ranges(
            &visible,
            array
                .as_list::<i64>()
                .value_offsets()
                .iter()
                .map(|x| *x as usize),
            data.child_data()[0].len(),
        ),
        DataType::Map(_, _) => visible_child_ranges(
            &visible,
            array.as_map().value_offsets().iter().map(|x| *x as usize),
            data.child_data()[0].len(),
        ),
        DataType::FixedSizeList(_, size) => visible_child_ranges(
            &visible,
            (0..=array.len()).map(|i| i * *size as usize),
            data.child_data()[0].len(),
        ),
        _ => unreachable!("nested UUID storage"),
    };
    let children = data
        .child_data()
        .iter()
        .zip(&fields)
        .map(|(child, field)| {
            convert_visible_uuid_array(&make_array(child.clone()), field, safe, &child_visible)
                .map(|a| a.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(output.data_type().clone())
            .child_data(children)
            .build()?,
    ))
}

fn visible_child_ranges(
    visible: &[bool],
    mut offsets: impl Iterator<Item = usize>,
    child_len: usize,
) -> Vec<bool> {
    let mut children = vec![false; child_len];
    let mut start = offsets.next().unwrap_or(0);
    for (visible, end) in visible.iter().zip(offsets) {
        if *visible {
            children[start..end].fill(true);
        }
        start = end;
    }
    children
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        BinaryArray, FixedSizeListArray, LargeListArray, LargeStringArray, ListArray, MapArray,
        StringArray, StringViewArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion_common::ScalarValue;

    use super::*;

    fn uuid_field() -> Field {
        Field::new("u", DataType::FixedSizeBinary(16), true)
            .with_metadata([(UUID_METADATA_KEY.to_string(), "uuid".to_string())].into())
    }

    fn nested_cases(values: ArrayRef, nulls: Option<NullBuffer>) -> Result<Vec<(ArrayRef, Field)>> {
        let leaf = Arc::new(Field::new(
            "u",
            values.data_type().clone(),
            values.is_nullable(),
        ));
        let uuid = Arc::new(uuid_field().with_nullable(leaf.is_nullable()));
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len()));
        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let entries = StructArray::try_new(
            vec![key.clone(), leaf.clone()].into(),
            vec![
                Arc::new(StringArray::from(vec!["k"; values.len()])),
                values.clone(),
            ],
            None,
        )?;
        let target_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, uuid.clone()].into()),
            false,
        ));
        let cases: Vec<(ArrayRef, DataType)> = vec![
            (
                Arc::new(StructArray::try_new(
                    vec![leaf.clone()].into(),
                    vec![values.clone()],
                    nulls.clone(),
                )?),
                DataType::Struct(vec![uuid.clone()].into()),
            ),
            (
                Arc::new(ListArray::try_new(
                    leaf.clone(),
                    offsets.clone(),
                    values.clone(),
                    nulls.clone(),
                )?),
                DataType::List(uuid.clone()),
            ),
            (
                Arc::new(LargeListArray::try_new(
                    leaf.clone(),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len())),
                    values.clone(),
                    nulls.clone(),
                )?),
                DataType::LargeList(uuid.clone()),
            ),
            (
                Arc::new(FixedSizeListArray::try_new(leaf, 1, values, nulls.clone())?),
                DataType::FixedSizeList(uuid, 1),
            ),
            (
                Arc::new(MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    offsets,
                    entries,
                    nulls,
                    false,
                )?),
                DataType::Map(target_entries, false),
            ),
        ];
        Ok(cases
            .into_iter()
            .map(|(array, target)| (array, Field::new("container", target, true)))
            .collect())
    }

    fn leaf_at(array: &ArrayRef, index: usize) -> ArrayRef {
        match array.data_type() {
            DataType::Struct(_) => array.as_struct().column(0).slice(index, 1),
            DataType::List(_) => array.as_list::<i32>().value(index),
            DataType::LargeList(_) => array.as_list::<i64>().value(index),
            DataType::FixedSizeList(_, _) => array.as_fixed_size_list().value(index),
            DataType::Map(_, _) => array.as_map().value(index).column(1).clone(),
            _ => unreachable!("test container"),
        }
    }

    #[test]
    fn nested_uuid_conversion_ignores_hidden_values() -> Result<()> {
        // The slice hides both a prefix and a suffix; its second (null) parent
        // still references invalid, non-nullable child storage.
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            "bad",
            "00000000-0000-0000-0000-000000000001",
            "bad",
            "bad",
        ]));
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            &b"bad"[..],
            &[1; 16][..],
            &b"bad"[..],
            &b"bad"[..],
        ]));
        for values in [strings, binary] {
            let expected = if values.data_type() == &DataType::Utf8 {
                let mut bytes = vec![0; 16];
                bytes[15] = 1;
                bytes
            } else {
                vec![1; 16]
            };
            for (input, target) in nested_cases(values, Some(vec![true, true, false, true].into()))?
            {
                assert!(convert_uuid_array(&input, &target, false).is_err());
                let slice = input.slice(1, 2);
                let output = convert_uuid_array(&slice, &target, false)?;
                output.to_data().validate_full()?;
                assert_eq!(output.len(), 2);
                assert_eq!(output.nulls(), slice.nulls());
                assert_eq!(output.data_type(), target.data_type());
                assert_eq!(
                    ScalarValue::try_from_array(&leaf_at(&output, 0), 0)?,
                    ScalarValue::FixedSizeBinary(16, Some(expected.clone())),
                );
                // Propagate visibility through a nullable struct into each
                // container (including map entries and fixed-size lists).
                let parent: ArrayRef = Arc::new(StructArray::try_new(
                    vec![Arc::new(Field::new(
                        "container",
                        input.data_type().clone(),
                        false,
                    ))]
                    .into(),
                    vec![input.slice(0, 1)],
                    Some(vec![false].into()),
                )?);
                let parent_target =
                    Field::new("s", DataType::Struct(vec![Arc::new(target)].into()), true);
                let output = convert_uuid_array(&parent, &parent_target, false)?;
                output.to_data().validate_full()?;
                assert!(output.is_null(0));
            }
        }
        Ok(())
    }

    #[test]
    fn try_cast_nested_uuid_nullability_matches_declared_field() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "bad",
            "00000000-0000-0000-0000-000000000001",
        ]));
        for (input, target) in nested_cases(values, None)? {
            let udf = UuidCast::new(Arc::new(target.clone()), true);
            let source = Arc::new(Field::new("source", input.data_type().clone(), false));
            let declared = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &[source],
                scalar_arguments: &[None],
            })?;
            let output = convert_uuid_array(&input, &target, true)?;
            output.to_data().validate_full()?;
            assert_eq!(output.data_type(), declared.data_type());
            assert_eq!(
                udf.return_type(&[input.data_type().clone()])?,
                *output.data_type()
            );
            assert_eq!(output.null_count(), 0);
            assert!(leaf_at(&output, 0).is_null(0));
            assert!(leaf_at(&output, 1).is_valid(0));
            // Non-null map entries and keys must not be relaxed along with values.
            if let DataType::Map(entries, _) = output.data_type() {
                assert!(!entries.is_nullable());
                let DataType::Struct(fields) = entries.data_type() else {
                    unreachable!()
                };
                assert!(!fields[0].is_nullable());
                assert!(fields[1].is_nullable());
            }
        }
        Ok(())
    }

    #[test]
    fn nested_uuid_conversion_preserves_visible_nulls_and_empty_arrays() -> Result<()> {
        for values in [
            Arc::new(StringArray::from(vec![
                None,
                Some("00000000-0000-0000-0000-000000000001"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
        ] {
            for (input, target) in nested_cases(values, None)? {
                for safe in [false, true] {
                    let output = convert_uuid_array(&input, &target, safe)?;
                    output.to_data().validate_full()?;
                    assert_eq!(output.len(), input.len());
                    assert_eq!(output.null_count(), 0);
                    if !output.is_empty() {
                        assert!(leaf_at(&output, 0).is_null(0));
                        assert!(leaf_at(&output, 1).is_valid(0));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn uuid_map_keys_remain_nonnullable() -> Result<()> {
        let source_key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = Arc::new(Field::new("value", DataType::Utf8, false));
        let entries = StructArray::try_new(
            vec![source_key, value.clone()].into(),
            vec![
                Arc::new(StringArray::from(vec![
                    "bad",
                    "00000000-0000-0000-0000-000000000001",
                    "bad",
                ])),
                Arc::new(StringArray::from(vec!["v"; 3])),
            ],
            None,
        )?;
        let input: ArrayRef = Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1, 1, 1]),
            entries,
            Some(vec![true, true, false].into()),
            false,
        )?);
        let input = input.slice(1, 2);
        let target = Field::new(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(uuid_field().with_name("key").with_nullable(false)),
                            value,
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        let output = convert_uuid_array(&input, &target, false)?;
        output.to_data().validate_full()?;
        assert!(output.is_null(1));
        assert_eq!(output.as_map().keys().null_count(), 0);
        assert!(convert_uuid_array(&input, &target, true).is_err());
        // Fixed[16] keys (with or without a UUID marker) cannot become null.
        for input in [
            output.clone(),
            output.slice(0, 0),
            new_null_array(output.data_type(), 1),
        ] {
            let converted = convert_uuid_array(&input, &target, true)?;
            assert_eq!(converted.data_type(), output.data_type());
            assert_eq!(converted.len(), input.len());
            assert_eq!(converted.null_count(), input.null_count());
            for index in 0..input.len() {
                assert_eq!(converted.is_null(index), input.is_null(index));
            }
            converted.to_data().validate_full()?;
            assert_eq!(
                UuidCast::new(Arc::new(target.clone()), true)
                    .return_type(&[input.data_type().clone()])?,
                *converted.data_type()
            );
        }
        assert!(
            UuidCast::new(Arc::new(target), true)
                .return_type(&[input.data_type().clone()])
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn uuid_scalar_containers_preserve_fields() -> Result<()> {
        for value in [
            Some("00000000-0000-0000-0000-000000000001"),
            Some("bad"),
            None,
        ] {
            for safe in [false, true] {
                if value == Some("bad") && !safe {
                    continue;
                }
                for nulls in [None, Some(NullBuffer::new_null(1))] {
                    let values: ArrayRef = Arc::new(StringArray::from(vec![value]));
                    for (input, target) in nested_cases(values, nulls)? {
                        let converted = convert_uuid_array(&input, &target, safe)?;
                        let scalar = crate::utils::scalar::scalar_from_array(&converted, 0)?;
                        assert_eq!(scalar.data_type(), *converted.data_type());
                        assert_eq!(scalar.to_array_of_size(1)?.to_data(), converted.to_data());
                        // Lists inside structs/maps and other lists must retain the same
                        // child metadata, not just the outer container's field.
                        for (nested, _) in nested_cases(converted.clone(), None)? {
                            let scalar = crate::utils::scalar::scalar_from_array(&nested, 0)?;
                            assert_eq!(scalar.data_type(), *nested.data_type());
                            assert_eq!(scalar.to_array_of_size(1)?.to_data(), nested.to_data());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn uuid_try_cast_preserves_infallible_leaf_metadata() -> Result<()> {
        let source = Field::new("key", DataType::FixedSizeBinary(16), false)
            .with_metadata([("custom".to_string(), "kept".to_string())].into());
        let target = uuid_field().with_name("key");
        let output = uuid_cast_field(&source, &target, true)?;
        assert!(!output.is_nullable());
        assert!(is_uuid_field(&output));
        assert_eq!(
            output.metadata().get("custom"),
            source.metadata().get("custom")
        );
        Ok(())
    }

    #[test]
    fn uuid_identity_requires_metadata() {
        assert!(is_uuid_field(&uuid_field()));
        assert!(!is_uuid_field(&Field::new(
            "f",
            DataType::FixedSizeBinary(16),
            true
        )));
        assert!(!is_uuid_field(
            &uuid_field().with_data_type(DataType::Binary)
        ));
    }

    #[test]
    fn uuid_strings_validate_all_arrow_representations() -> Result<()> {
        let values = vec![
            Some("6F1C2D3E-4B5A-4C6D-8E7F-0123456789AB"),
            None,
            Some("bad"),
        ];
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        for input in inputs {
            assert!(convert_uuid_array(&input, &uuid_field(), false).is_err());
            let result = convert_uuid_array(&input, &uuid_field(), true)?;
            let result = result
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!("expected UUID storage")
                })?;
            assert_eq!(
                result.value(0),
                &[
                    0x6f, 0x1c, 0x2d, 0x3e, 0x4b, 0x5a, 0x4c, 0x6d, 0x8e, 0x7f, 0x01, 0x23, 0x45,
                    0x67, 0x89, 0xab
                ]
            );
            assert!(result.is_null(1));
            assert!(result.is_null(2));
        }
        Ok(())
    }

    #[test]
    fn uuid_binary_length_is_validated() -> Result<()> {
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"short"[..])]));
        assert!(convert_uuid_array(&array, &uuid_field(), false).is_err());
        assert!(convert_uuid_array(&array, &uuid_field(), true)?.is_null(0));
        Ok(())
    }

    #[test]
    fn null_struct_does_not_parse_hidden_strings() -> Result<()> {
        let source = Arc::new(Field::new("u", DataType::Utf8, true));
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![source].into(),
            vec![Arc::new(StringArray::from(vec![
                "00000000-0000-0000-0000-000000000000",
                "bad",
            ]))],
            Some(vec![true, false].into()),
        ));
        let target = Field::new(
            "s",
            DataType::Struct(vec![Arc::new(uuid_field())].into()),
            true,
        );
        let result = convert_uuid_array(&input, &target, false)?;
        assert!(result.is_null(1));
        assert_eq!(result.data_type(), target.data_type());
        Ok(())
    }
}
