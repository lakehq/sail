// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common_union.rs

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, NullArray, StringArray,
    UnionArray,
};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::arrow::error::ArrowError;
use datafusion::common::ScalarValue;

pub fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

/// Extract nested JSON from a `JsonUnion` `UnionArray`
///
/// # Arguments
/// * `array` - The `UnionArray` to extract the nested JSON from
/// * `object_lookup` - If `true`, extract from the "object" member of the union,
///   otherwise extract from the "array" member
pub(crate) fn nested_json_array(array: &ArrayRef, object_lookup: bool) -> Option<&StringArray> {
    nested_json_array_ref(array, object_lookup).map(AsArray::as_string)
}

pub(crate) fn nested_json_array_ref(array: &ArrayRef, object_lookup: bool) -> Option<&ArrayRef> {
    let union_array: &UnionArray = array.as_any().downcast_ref::<UnionArray>()?;
    let type_id = if object_lookup {
        TYPE_ID_OBJECT
    } else {
        TYPE_ID_ARRAY
    };
    Some(union_array.child(type_id))
}

/// Extract a JSON string from a `JsonUnion` scalar
pub(crate) fn json_from_union_scalar<'a>(
    type_id_value: Option<&'a (i8, Box<ScalarValue>)>,
    fields: &UnionFields,
) -> Option<&'a str> {
    if let Some((type_id, value)) = type_id_value {
        // we only want to take the ScalarValue string if the type_id indicates the value represents nested JSON
        if fields == &union_fields() && (*type_id == TYPE_ID_ARRAY || *type_id == TYPE_ID_OBJECT) {
            if let ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) =
                value.as_ref()
            {
                return s.as_deref();
            }
        }
    }
    None
}

pub static JSON_UNION_DATA_TYPE: LazyLock<DataType> = LazyLock::new(JsonUnion::data_type);

#[derive(Debug)]
pub(crate) struct JsonUnion {
    bools: Vec<Option<bool>>,
    ints: Vec<Option<i64>>,
    floats: Vec<Option<f64>>,
    strings: Vec<Option<String>>,
    arrays: Vec<Option<String>>,
    objects: Vec<Option<String>>,
    type_ids: Vec<i8>,
    index: usize,
    length: usize,
}

impl JsonUnion {
    pub fn new(length: usize) -> Self {
        Self {
            bools: vec![None; length],
            ints: vec![None; length],
            floats: vec![None; length],
            strings: vec![None; length],
            arrays: vec![None; length],
            objects: vec![None; length],
            type_ids: vec![TYPE_ID_NULL; length],
            index: 0,
            length,
        }
    }

    pub fn data_type() -> DataType {
        DataType::Union(union_fields(), UnionMode::Sparse)
    }

    pub fn push(&mut self, field: JsonUnionField) {
        self.type_ids[self.index] = field.type_id();
        match field {
            JsonUnionField::JsonNull => (),
            JsonUnionField::Bool(value) => self.bools[self.index] = Some(value),
            JsonUnionField::Int(value) => self.ints[self.index] = Some(value),
            JsonUnionField::Float(value) => self.floats[self.index] = Some(value),
            JsonUnionField::Str(value) => self.strings[self.index] = Some(value),
            JsonUnionField::Array(value) => self.arrays[self.index] = Some(value),
            JsonUnionField::Object(value) => self.objects[self.index] = Some(value),
        }
        self.index += 1;
        debug_assert!(self.index <= self.length);
    }

    pub fn push_none(&mut self) {
        self.index += 1;
        debug_assert!(self.index <= self.length);
    }
}

/// So we can do `collect::<JsonUnion>()`
impl FromIterator<Option<JsonUnionField>> for JsonUnion {
    fn from_iter<I: IntoIterator<Item = Option<JsonUnionField>>>(iter: I) -> Self {
        let inner = iter.into_iter();
        let (lower, upper) = inner.size_hint();
        let mut union = Self::new(upper.unwrap_or(lower));

        for opt_field in inner {
            if let Some(union_field) = opt_field {
                union.push(union_field);
            } else {
                union.push_none();
            }
        }
        union
    }
}

impl TryFrom<JsonUnion> for UnionArray {
    type Error = ArrowError;

    fn try_from(value: JsonUnion) -> Result<Self, Self::Error> {
        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(NullArray::new(value.length)),
            Arc::new(BooleanArray::from(value.bools)),
            Arc::new(Int64Array::from(value.ints)),
            Arc::new(Float64Array::from(value.floats)),
            Arc::new(StringArray::from(value.strings)),
            Arc::new(StringArray::from(value.arrays)),
            Arc::new(StringArray::from(value.objects)),
        ];
        UnionArray::try_new(
            union_fields(),
            Buffer::from_vec(value.type_ids).into(),
            None,
            children,
        )
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum JsonUnionField {
    JsonNull,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(String),
    Object(String),
}

pub(crate) const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            let json_metadata: HashMap<String, String> =
                HashMap::from_iter(vec![("is_json".to_string(), "true".to_string())]);
            UnionFields::from_iter([
                (
                    TYPE_ID_NULL,
                    Arc::new(Field::new("null", DataType::Null, true)),
                ),
                (
                    TYPE_ID_BOOL,
                    Arc::new(Field::new("bool", DataType::Boolean, false)),
                ),
                (
                    TYPE_ID_INT,
                    Arc::new(Field::new("int", DataType::Int64, false)),
                ),
                (
                    TYPE_ID_FLOAT,
                    Arc::new(Field::new("float", DataType::Float64, false)),
                ),
                (
                    TYPE_ID_STR,
                    Arc::new(Field::new("str", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_ARRAY,
                    Arc::new(
                        Field::new("array", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
                (
                    TYPE_ID_OBJECT,
                    Arc::new(
                        Field::new("object", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
            ])
        })
        .clone()
}

impl JsonUnionField {
    fn type_id(&self) -> i8 {
        match self {
            Self::JsonNull => TYPE_ID_NULL,
            Self::Bool(_) => TYPE_ID_BOOL,
            Self::Int(_) => TYPE_ID_INT,
            Self::Float(_) => TYPE_ID_FLOAT,
            Self::Str(_) => TYPE_ID_STR,
            Self::Array(_) => TYPE_ID_ARRAY,
            Self::Object(_) => TYPE_ID_OBJECT,
        }
    }

    #[allow(dead_code)]
    pub fn scalar_value(f: Option<Self>) -> ScalarValue {
        ScalarValue::Union(
            f.map(|f| (f.type_id(), Box::new(f.into()))),
            union_fields(),
            UnionMode::Sparse,
        )
    }
}

impl From<JsonUnionField> for ScalarValue {
    fn from(value: JsonUnionField) -> Self {
        match value {
            JsonUnionField::JsonNull => Self::Null,
            JsonUnionField::Bool(b) => Self::Boolean(Some(b)),
            JsonUnionField::Int(i) => Self::Int64(Some(i)),
            JsonUnionField::Float(f) => Self::Float64(Some(f)),
            JsonUnionField::Str(s) | JsonUnionField::Array(s) | JsonUnionField::Object(s) => {
                Self::Utf8(Some(s))
            }
        }
    }
}

pub struct JsonUnionEncoder {
    boolean: BooleanArray,
    int: Int64Array,
    float: Float64Array,
    string: StringArray,
    array: StringArray,
    object: StringArray,
    type_ids: ScalarBuffer<i8>,
}

impl JsonUnionEncoder {
    #[must_use]
    pub fn from_union(union: UnionArray) -> Option<Self> {
        if is_json_union(union.data_type()) {
            let (_, type_ids, _, c) = union.into_parts();
            Some(Self {
                boolean: c[1].as_boolean().clone(),
                int: c[2].as_primitive().clone(),
                float: c[3].as_primitive().clone(),
                string: c[4].as_string().clone(),
                array: c[5].as_string().clone(),
                object: c[6].as_string().clone(),
                type_ids,
            })
        } else {
            None
        }
    }

    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.type_ids.len()
    }

    /// Get the encodable value for a given index
    ///
    /// # Panics
    ///
    /// Panics if the idx is outside the union values or an invalid type id exists in the union.
    #[must_use]
    pub fn get_value(&self, idx: usize) -> JsonUnionValue<'_> {
        let type_id = self.type_ids[idx];
        match type_id {
            TYPE_ID_NULL => JsonUnionValue::JsonNull,
            TYPE_ID_BOOL => JsonUnionValue::Bool(self.boolean.value(idx)),
            TYPE_ID_INT => JsonUnionValue::Int(self.int.value(idx)),
            TYPE_ID_FLOAT => JsonUnionValue::Float(self.float.value(idx)),
            TYPE_ID_STR => JsonUnionValue::Str(self.string.value(idx)),
            TYPE_ID_ARRAY => JsonUnionValue::Array(self.array.value(idx)),
            TYPE_ID_OBJECT => JsonUnionValue::Object(self.object.value(idx)),
            _ => unreachable!("Invalid type_id: {type_id}, not a valid JSON type"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum JsonUnionValue<'a> {
    JsonNull,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    Array(&'a str),
    Object(&'a str),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod test {
    use super::*;

    #[test]
    fn test_json_union() {
        let json_union = JsonUnion::from_iter(vec![
            Some(JsonUnionField::JsonNull),
            Some(JsonUnionField::Bool(true)),
            Some(JsonUnionField::Bool(false)),
            Some(JsonUnionField::Int(42)),
            Some(JsonUnionField::Float(42.0)),
            Some(JsonUnionField::Str("foo".to_string())),
            Some(JsonUnionField::Array("[42]".to_string())),
            Some(JsonUnionField::Object(r#"{"foo": 42}"#.to_string())),
            None,
        ]);

        let union_array = UnionArray::try_from(json_union).unwrap();
        let encoder = JsonUnionEncoder::from_union(union_array).unwrap();

        let values_after: Vec<_> = (0..encoder.len())
            .map(|idx| encoder.get_value(idx))
            .collect();
        assert_eq!(
            values_after,
            vec![
                JsonUnionValue::JsonNull,
                JsonUnionValue::Bool(true),
                JsonUnionValue::Bool(false),
                JsonUnionValue::Int(42),
                JsonUnionValue::Float(42.0),
                JsonUnionValue::Str("foo"),
                JsonUnionValue::Array("[42]"),
                JsonUnionValue::Object(r#"{"foo": 42}"#),
                JsonUnionValue::JsonNull,
            ]
        );
    }
}
