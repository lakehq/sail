// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common.rs

use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::arrow::array::{
    downcast_array, AnyDictionaryArray, Array, ArrayAccessor, ArrayRef, AsArray, DictionaryArray,
    LargeStringArray, PrimitiveArray, PrimitiveBuilder, RunArray, StringArray, StringViewArray,
};
use datafusion::arrow::compute::kernels::cast;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Int64Type, UInt64Type};
use datafusion_common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use jiter::{Jiter, JiterError, Peek};

use crate::scalar::json::common_union::{
    is_json_union, json_from_union_scalar, nested_json_array, nested_json_array_ref, TYPE_ID_NULL,
};

/// General implementation of `ScalarUDFImpl::return_type`.
///
/// # Arguments
///
/// * `args` - The arguments to the function
/// * `fn_name` - The name of the function
/// * `value_type` - The general return type of the function, might be wrapped in a dictionary depending
///   on the first argument
pub fn return_type_check(
    args: &[DataType],
    fn_name: &str,
    value_type: DataType,
) -> Result<DataType> {
    let Some(first) = args.first() else {
        return plan_err!("The '{fn_name}' function requires one or more arguments.");
    };
    let first_dict_key_type = dict_key_type(first);
    if !(is_str(first) || is_json_union(first) || first_dict_key_type.is_some()) {
        // if !matches!(first, DataType::Utf8 | DataType::LargeUtf8) {
        return plan_err!("Unexpected argument type to '{fn_name}' at position 1, expected a string, got {first:?}.");
    }
    args.iter().skip(1).enumerate().try_for_each(|(index, arg)| {
        if is_str(arg) || is_int(arg) || dict_key_type(arg).is_some() {
            Ok(())
        } else {
            plan_err!(
                "Unexpected argument type to '{fn_name}' at position {}, expected string or int, got {arg:?}.",
                index + 2
            )
        }
    })?;
    if first_dict_key_type.is_some() && !value_type.is_primitive() {
        Ok(DataType::Dictionary(
            Box::new(DataType::Int64),
            Box::new(value_type),
        ))
    } else {
        Ok(value_type)
    }
}

fn is_str(d: &DataType) -> bool {
    matches!(d, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn is_int(d: &DataType) -> bool {
    // TODO we should support more types of int, but that's a longer task
    matches!(d, DataType::UInt64 | DataType::Int64)
}

fn dict_key_type(d: &DataType) -> Option<DataType> {
    if let DataType::Dictionary(key, value) = d {
        if is_str(value) || is_json_union(value) {
            return Some(*key.clone());
        }
    }
    None
}

#[derive(Debug)]
pub enum JsonPath<'s> {
    Key(&'s str),
    Index(usize),
    None,
}

impl<'a> From<&'a str> for JsonPath<'a> {
    fn from(key: &'a str) -> Self {
        JsonPath::Key(key)
    }
}

impl From<u64> for JsonPath<'_> {
    fn from(index: u64) -> Self {
        match usize::try_from(index) {
            Ok(i) => Self::Index(i),
            Err(_) => Self::None,
        }
    }
}

impl From<i64> for JsonPath<'_> {
    fn from(index: i64) -> Self {
        match usize::try_from(index) {
            Ok(i) => Self::Index(i),
            Err(_) => Self::None,
        }
    }
}

#[derive(Debug)]
enum JsonPathArgs<'a> {
    Array(&'a ArrayRef),
    Scalars(Vec<JsonPath<'a>>),
}

impl<'s> JsonPathArgs<'s> {
    fn extract_path(path_args: &'s [ColumnarValue]) -> Result<Self> {
        // If there is a single argument as an array, we know how to handle it
        if let Some((ColumnarValue::Array(array), &[])) = path_args.split_first() {
            return Ok(Self::Array(array));
        }

        path_args
            .iter()
            .enumerate()
            .map(|(pos, arg)| match arg {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)),
                ) => Ok(JsonPath::Key(s)),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => Ok((*i).into()),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Ok((*i).into()),
                ColumnarValue::Scalar(
                    ScalarValue::Null
                    | ScalarValue::Utf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::UInt64(None)
                    | ScalarValue::Int64(None),
                ) => Ok(JsonPath::None),
                ColumnarValue::Array(_) => {
                    // if there was a single arg, which is an array, handled above in the
                    // split_first case. So this is multiple args of which one is an array
                    exec_err!("More than 1 path element is not supported when querying JSON using an array.")
                }
                ColumnarValue::Scalar(arg) => exec_err!(
                    "Unexpected argument type at position {}, expected string or int, got {arg:?}.",
                    pos + 1
                ),
            })
            .collect::<Result<_>>()
            .map(JsonPathArgs::Scalars)
    }
}

pub trait InvokeResult {
    type Item;
    type Builder;

    // Whether the return type should is allowed to be a dictionary
    const ACCEPT_DICT_RETURN: bool;

    fn builder(capacity: usize) -> Self::Builder;
    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>);
    fn finish(builder: Self::Builder) -> Result<ArrayRef>;

    /// Convert a single value to a scalar
    fn scalar(value: Option<Self::Item>) -> ScalarValue;
}

pub fn invoke<R: InvokeResult>(
    args: &[ColumnarValue],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<R::Item, GetError>,
) -> Result<ColumnarValue> {
    let Some((json_arg, path_args)) = args.split_first() else {
        return exec_err!("expected at least one argument");
    };

    let path = JsonPathArgs::extract_path(path_args)?;
    match (json_arg, path) {
        (ColumnarValue::Array(json_array), JsonPathArgs::Array(path_array)) => {
            invoke_array_array::<R>(json_array, path_array, jiter_find).map(ColumnarValue::Array)
        }
        (ColumnarValue::Array(json_array), JsonPathArgs::Scalars(path)) => {
            invoke_array_scalars::<R>(json_array, &path, jiter_find).map(ColumnarValue::Array)
        }
        (ColumnarValue::Scalar(s), JsonPathArgs::Array(path_array)) => {
            invoke_scalar_array::<R>(s, path_array, jiter_find)
        }
        (ColumnarValue::Scalar(s), JsonPathArgs::Scalars(path)) => {
            invoke_scalar_scalars(s, &path, jiter_find, R::scalar)
        }
    }
}

fn invoke_array_array<R: InvokeResult>(
    json_array: &ArrayRef,
    path_array: &ArrayRef,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<R::Item, GetError>,
) -> Result<ArrayRef> {
    match json_array.data_type() {
        // for string dictionaries, cast dictionary keys to larger types to avoid generic explosion
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            let json_array = cast_to_large_dictionary(json_array.as_any_dictionary())?;
            let output = zip_apply::<R>(
                json_array.downcast_dict::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal(
                        "dictionary value type should be StringArray".to_string(),
                    )
                })?,
                path_array,
                jiter_find,
            )?;
            if R::ACCEPT_DICT_RETURN {
                // ensure return is a dictionary to satisfy the declaration above in return_type_check
                Ok(Arc::new(wrap_as_large_dictionary(&json_array, output)?))
            } else {
                Ok(output)
            }
        }
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::LargeUtf8 => {
            let json_array = cast_to_large_dictionary(json_array.as_any_dictionary())?;
            let output = zip_apply::<R>(
                json_array
                    .downcast_dict::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "dictionary value type should be LargeStringArray".to_string(),
                        )
                    })?,
                path_array,
                jiter_find,
            )?;
            if R::ACCEPT_DICT_RETURN {
                // ensure return is a dictionary to satisfy the declaration above in return_type_check
                Ok(Arc::new(wrap_as_large_dictionary(&json_array, output)?))
            } else {
                Ok(output)
            }
        }
        other_dict_type @ DataType::Dictionary(_, _) => {
            // Horrible case: dict containing union as input with array for paths, figure
            // out from the path type which union members we should access, repack the
            // dictionary and then recurse.
            if let Some(child_array) = nested_json_array_ref(
                json_array.as_any_dictionary().values(),
                is_object_lookup_array(path_array.data_type()),
            ) {
                invoke_array_array::<R>(
                    &(Arc::new(
                        json_array
                            .as_any_dictionary()
                            .with_values(child_array.clone()),
                    ) as _),
                    path_array,
                    jiter_find,
                )
            } else {
                exec_err!("unexpected json array type {:?}", other_dict_type)
            }
        }
        DataType::Utf8 => zip_apply::<R>(json_array.as_string::<i32>(), path_array, jiter_find),
        DataType::LargeUtf8 => {
            zip_apply::<R>(json_array.as_string::<i64>(), path_array, jiter_find)
        }
        DataType::Utf8View => zip_apply::<R>(json_array.as_string_view(), path_array, jiter_find),
        other => {
            if let Some(string_array) =
                nested_json_array(json_array, is_object_lookup_array(path_array.data_type()))
            {
                zip_apply::<R>(string_array, path_array, jiter_find)
            } else {
                exec_err!("unexpected json array type {:?}", other)
            }
        }
    }
}

/// Transform keys that may be pointing to values with nulls to nulls themselves.
/// keys = `[0, 1, 2, 3]`, values = `[null, "a", null, "b"]`
/// into
/// keys = `[null, 0, null, 1]`, values = `["a", "b"]`
///
/// Arrow / `DataFusion` assumes that dictionary values do not contain nulls, nulls are encoded by the keys.
/// Not following this invariant causes invalid dictionary arrays to be built later on inside of `DataFusion`
/// when arrays are concacted and such.
fn remap_dictionary_key_nulls(
    keys: PrimitiveArray<Int64Type>,
    values: ArrayRef,
) -> DictionaryArray<Int64Type> {
    // fast path: no nulls in values
    if values.null_count() == 0 {
        return DictionaryArray::new(keys, values);
    }

    let mut new_keys_builder = PrimitiveBuilder::<Int64Type>::new();

    for key in &keys {
        match key {
            Some(k) if values.is_null(k.as_usize()) => new_keys_builder.append_null(),
            Some(k) => new_keys_builder.append_value(k),
            None => new_keys_builder.append_null(),
        }
    }

    let new_keys = new_keys_builder.finish();
    DictionaryArray::new(new_keys, values)
}

fn invoke_array_scalars<R: InvokeResult>(
    json_array: &ArrayRef,
    path: &[JsonPath],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<R::Item, GetError>,
) -> Result<ArrayRef> {
    #[allow(clippy::needless_pass_by_value)] // ArrayAccessor is implemented on references
    fn inner<'j, R: InvokeResult>(
        json_array: impl ArrayAccessor<Item = &'j str>,
        path: &[JsonPath],
        jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<R::Item, GetError>,
    ) -> Result<ArrayRef> {
        let mut builder = R::builder(json_array.len());
        for i in 0..json_array.len() {
            let opt_json = if json_array.is_null(i) {
                None
            } else {
                Some(json_array.value(i))
            };
            let opt_value = jiter_find(opt_json, path).ok();
            R::append_value(&mut builder, opt_value);
        }
        R::finish(builder)
    }

    match json_array.data_type() {
        DataType::Dictionary(_, _) => {
            let json_array = json_array.as_any_dictionary();
            let values = invoke_array_scalars::<R>(json_array.values(), path, jiter_find)?;
            return if R::ACCEPT_DICT_RETURN {
                // make the keys into i64 to avoid generic bloat here
                let mut keys: PrimitiveArray<Int64Type> =
                    downcast_array(&cast(json_array.keys(), &DataType::Int64)?);
                if is_json_union(values.data_type()) {
                    // JSON union: post-process the array to set keys to null where the union member is null
                    let type_ids = values.as_union().type_ids();
                    keys = mask_dictionary_keys(&keys, type_ids);
                }
                Ok(Arc::new(remap_dictionary_key_nulls(keys, values)))
            } else {
                // this is what cast would do under the hood to unpack a dictionary into an array of its values
                Ok(take(&values, json_array.keys(), None)?)
            };
        }
        DataType::Utf8 => inner::<R>(json_array.as_string::<i32>(), path, jiter_find),
        DataType::LargeUtf8 => inner::<R>(json_array.as_string::<i64>(), path, jiter_find),
        DataType::Utf8View => inner::<R>(json_array.as_string_view(), path, jiter_find),
        other => {
            if let Some(string_array) = nested_json_array(json_array, is_object_lookup(path)) {
                inner::<R>(string_array, path, jiter_find)
            } else {
                exec_err!("unexpected json array type {:?}", other)
            }
        }
    }
}

fn invoke_scalar_array<R: InvokeResult>(
    scalar: &ScalarValue,
    path_array: &ArrayRef,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<R::Item, GetError>,
) -> Result<ColumnarValue> {
    let s = extract_json_scalar(scalar)?;
    let arr = s.map_or_else(
        || StringArray::new_null(1),
        |s| StringArray::new_scalar(s).into_inner(),
    );

    // TODO: possible optimization here if path_array is a dictionary; can apply against the
    // dictionary values directly for less work
    zip_apply::<R>(
        RunArray::try_new(
            &PrimitiveArray::<Int64Type>::new_scalar(i64::try_from(path_array.len()).map_err(
                |_| DataFusionError::Internal("path_array length out of i64 range".to_string()),
            )?)
            .into_inner(),
            &arr,
        )?
        .downcast::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("RunArray should contain StringArray".to_string())
        })?,
        path_array,
        jiter_find,
    )
    // FIXME edge cases where scalar is wrapped in a dictionary, should return a dictionary?
    .map(ColumnarValue::Array)
}

fn invoke_scalar_scalars<I>(
    scalar: &ScalarValue,
    path: &[JsonPath],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
) -> Result<ColumnarValue> {
    let s = extract_json_scalar(scalar)?;
    let v = jiter_find(s, path).ok();
    // FIXME edge cases where scalar is wrapped in a dictionary, should return a dictionary?
    Ok(ColumnarValue::Scalar(to_scalar(v)))
}

fn zip_apply<'a, R: InvokeResult>(
    json_array: impl ArrayAccessor<Item = &'a str>,
    path_array: &ArrayRef,
    jiter_find: impl Fn(Option<&'a str>, &[JsonPath]) -> Result<R::Item, GetError>,
) -> Result<ArrayRef> {
    fn get_array_values<'j, 'p, P: Into<JsonPath<'p>>>(
        j: &impl ArrayAccessor<Item = &'j str>,
        p: &impl ArrayAccessor<Item = P>,
        index: usize,
    ) -> Option<(Option<&'j str>, JsonPath<'p>)> {
        let path = if p.is_null(index) {
            return None;
        } else {
            p.value(index).into()
        };

        let json = if j.is_null(index) {
            None
        } else {
            Some(j.value(index))
        };

        Some((json, path))
    }

    #[allow(clippy::needless_pass_by_value)] // ArrayAccessor is implemented on references
    fn inner<'a, 'p, P: Into<JsonPath<'p>>, R: InvokeResult>(
        json_array: impl ArrayAccessor<Item = &'a str>,
        path_array: impl ArrayAccessor<Item = P>,
        jiter_find: impl Fn(Option<&'a str>, &[JsonPath]) -> Result<R::Item, GetError>,
    ) -> Result<ArrayRef> {
        let mut builder = R::builder(json_array.len());
        for i in 0..json_array.len() {
            if let Some((opt_json, path)) = get_array_values(&json_array, &path_array, i) {
                let value = jiter_find(opt_json, &[path]).ok();
                R::append_value(&mut builder, value);
            } else {
                R::append_value(&mut builder, None);
            }
        }
        R::finish(builder)
    }

    match path_array.data_type() {
        // for string dictionaries, cast dictionary keys to larger types to avoid generic explosion
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            let path_array = cast_to_large_dictionary(path_array.as_any_dictionary())?;
            inner::<_, R>(
                json_array,
                path_array.downcast_dict::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal(
                        "dictionary value type should be StringArray".to_string(),
                    )
                })?,
                jiter_find,
            )
        }
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::LargeUtf8 => {
            let path_array = cast_to_large_dictionary(path_array.as_any_dictionary())?;
            inner::<_, R>(
                json_array,
                path_array
                    .downcast_dict::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "dictionary value type should be LargeStringArray".to_string(),
                        )
                    })?,
                jiter_find,
            )
        }
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8View => {
            let path_array = cast_to_large_dictionary(path_array.as_any_dictionary())?;
            inner::<_, R>(
                json_array,
                path_array
                    .downcast_dict::<StringViewArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "dictionary value type should be StringViewArray".to_string(),
                        )
                    })?,
                jiter_find,
            )
        }
        // for integer dictionaries, cast them directly to the inner type because it basically costs
        // the same as building a new key array anyway
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Int64 => {
            inner::<_, R>(
                json_array,
                cast(path_array, &DataType::Int64)?.as_primitive::<Int64Type>(),
                jiter_find,
            )
        }
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::UInt64 => {
            inner::<_, R>(
                json_array,
                cast(path_array, &DataType::UInt64)?.as_primitive::<UInt64Type>(),
                jiter_find,
            )
        }
        // for basic types, just consume directly
        DataType::Utf8 => inner::<_, R>(json_array, path_array.as_string::<i32>(), jiter_find),
        DataType::LargeUtf8 => inner::<_, R>(json_array, path_array.as_string::<i64>(), jiter_find),
        DataType::Utf8View => inner::<_, R>(json_array, path_array.as_string_view(), jiter_find),
        DataType::Int64 => inner::<_, R>(
            json_array,
            path_array.as_primitive::<Int64Type>(),
            jiter_find,
        ),
        DataType::UInt64 => inner::<_, R>(
            json_array,
            path_array.as_primitive::<UInt64Type>(),
            jiter_find,
        ),
        other => {
            exec_err!(
                "unexpected second argument type, expected string or int array, got {:?}",
                other
            )
        }
    }
}

fn extract_json_scalar(scalar: &ScalarValue) -> Result<Option<&str>> {
    match scalar {
        ScalarValue::Dictionary(_, b) => extract_json_scalar(b.as_ref()),
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => {
            Ok(s.as_deref())
        }
        ScalarValue::Union(type_id_value, union_fields, _) => {
            Ok(json_from_union_scalar(type_id_value.as_ref(), union_fields))
        }
        _ => {
            exec_err!("unexpected first argument type, expected string or JSON union")
        }
    }
}

fn is_object_lookup(path: &[JsonPath]) -> bool {
    if let Some(first) = path.first() {
        matches!(first, JsonPath::Key(_))
    } else {
        false
    }
}

fn is_object_lookup_array(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, value_type) => is_object_lookup_array(value_type),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        _ => false,
    }
}

/// Cast an array to a dictionary with i64 indices.
///
/// According to <https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout> the
/// recommendation is to avoid unsigned indices due to technologies like the JVM making it harder to
/// support unsigned integers.
///
/// So we'll just use i64 as the largest signed integer type.
fn cast_to_large_dictionary(
    dict_array: &dyn AnyDictionaryArray,
) -> Result<DictionaryArray<Int64Type>> {
    let keys = downcast_array(&cast(dict_array.keys(), &DataType::Int64)?);
    Ok(DictionaryArray::<Int64Type>::new(
        keys,
        dict_array.values().clone(),
    ))
}

/// Wrap an array as a dictionary with i64 indices.
fn wrap_as_large_dictionary(
    original: &dyn AnyDictionaryArray,
    new_values: ArrayRef,
) -> Result<DictionaryArray<Int64Type>> {
    assert_eq!(original.keys().len(), new_values.len());
    let mut keys = PrimitiveArray::from_iter_values(
        0i64..original.keys().len().try_into().map_err(|_| {
            DataFusionError::Internal("dictionary keys length should fit in i64 range".to_string())
        })?,
    );
    if is_json_union(new_values.data_type()) {
        // JSON union: post-process the array to set keys to null where the union member is null
        let type_ids = new_values.as_union().type_ids();
        keys = mask_dictionary_keys(&keys, type_ids);
    }
    Ok(DictionaryArray::new(keys, new_values))
}

pub fn jiter_json_find<'j>(
    opt_json: Option<&'j str>,
    path: &[JsonPath],
) -> Option<(Jiter<'j>, Peek)> {
    let json_str = opt_json?;
    let mut jiter = Jiter::new(json_str.as_bytes());
    let mut peek = jiter.peek().ok()?;
    for element in path {
        match element {
            JsonPath::Key(key) if peek == Peek::Object => {
                let mut next_key = jiter.known_object().ok()??;

                while next_key != *key {
                    jiter.next_skip().ok()?;
                    next_key = jiter.next_key().ok()??;
                }

                peek = jiter.peek().ok()?;
            }
            JsonPath::Index(index) if peek == Peek::Array => {
                let mut array_item = jiter.known_array().ok()??;

                for _ in 0..*index {
                    jiter.known_skip(array_item).ok()?;
                    array_item = jiter.array_step().ok()??;
                }

                peek = array_item;
            }
            _ => {
                return None;
            }
        }
    }
    Some((jiter, peek))
}

macro_rules! get_err {
    () => {
        Err(GetError)
    };
}
pub(crate) use get_err;

pub struct GetError;

impl From<JiterError> for GetError {
    fn from(_: JiterError) -> Self {
        GetError
    }
}

impl From<Utf8Error> for GetError {
    fn from(_: Utf8Error) -> Self {
        GetError
    }
}

/// Set keys to null where the union member is null.
///
/// This is a workaround to <https://github.com/apache/arrow-rs/issues/6017#issuecomment-2352756753>
/// - i.e. that dictionary null is most reliably done if the keys are null.
///
/// That said, doing this might also be an optimization for cases like null-checking without needing
/// to check the value union array.
fn mask_dictionary_keys(
    keys: &PrimitiveArray<Int64Type>,
    type_ids: &[i8],
) -> PrimitiveArray<Int64Type> {
    let mut null_mask = vec![true; keys.len()];
    for (i, k) in keys.iter().enumerate() {
        match k {
            // if the key is non-null and value is non-null, don't mask it out
            Some(k) if type_ids[k.as_usize()] != TYPE_ID_NULL => {}
            // i.e. key is null or value is null here
            _ => null_mask[i] = false,
        }
    }
    PrimitiveArray::new(keys.values().clone(), Some(null_mask.into()))
}
