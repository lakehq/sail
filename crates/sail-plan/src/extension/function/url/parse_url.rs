use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, LargeStringArray, StringArray, StringArrayType};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use url::Url;

#[derive(Debug)]
pub struct ParseUrl {
    signature: Signature,
}

impl Default for ParseUrl {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseUrl {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8View,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8View,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8View,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                    ]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                        DataType::Utf8View,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
    fn parse(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
        Url::parse(value)
            .map_err(|e| exec_datafusion_err!("{e:?}"))
            .map(|url| match part {
                "HOST" => url.host_str().map(String::from),
                "PATH" => Some(url.path().to_string()),
                "QUERY" => match key {
                    None => url.query().map(String::from),
                    Some(key) => url
                        .query_pairs()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.into_owned()),
                },
                "REF" => url.fragment().map(String::from),
                "PROTOCOL" => Some(url.scheme().to_string()),
                "FILE" => {
                    let path = url.path();
                    match url.query() {
                        Some(query) => Some(format!("{path}?{query}")),
                        None => Some(path.to_string()),
                    }
                }
                "AUTHORITY" => Some(url.authority().to_string()),
                "USERINFO" => {
                    let username = url.username();
                    if username.is_empty() {
                        return None;
                    }
                    match url.password() {
                        Some(password) => Some(format!("{username}:{password}")),
                        None => Some(username.to_string()),
                    }
                }
                _ => None,
            })
    }
}

impl ScalarUDFImpl for ParseUrl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parse_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "{} expects 2 or 3 arguments, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => plan_err!("1st argument should be String, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() < 2 || args.len() > 3 {
            return exec_err!(
                "{} expects 2 or 3 arguments, but got {}",
                self.name(),
                args.len()
            );
        }

        spark_parse_url(&args[..])
    }
}

fn spark_parse_url(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [url, part, ..] = args else {
        return exec_err!(
            "`parse_url` function requires at least 2 arguments, got {}",
            args.len()
        );
    };

    let key_to_extract = args.get(2);
    match key_to_extract {
        None => match (url, part) {
            (ColumnarValue::Scalar(scalar_url), ColumnarValue::Scalar(scalar_part)) => match (scalar_url, scalar_part) {
                (
                    ScalarValue::Utf8(Some(url))
                    | ScalarValue::LargeUtf8(Some(url))
                    | ScalarValue::Utf8View(Some(url)),
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                ) => {
                    let result = ParseUrl::parse(url, part, None)?;
                    match scalar_url {
                        ScalarValue::LargeUtf8(Some(_)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
                        }
                        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result))),
                    }
                }
                _ => {
                    exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}")
                }
            },

            (ColumnarValue::Array(array_url), ColumnarValue::Scalar(scalar_part)) => match (array_url.data_type(), scalar_part) {
                (
                    DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                ) => match array_url.data_type() {
                    DataType::Utf8 => process_parse_url_array::<_, StringArray>(as_string_array(&array_url)?, part, None),
                    DataType::Utf8View => process_parse_url_array::<_, StringArray>(as_string_view_array(&array_url)?, part, None),
                    DataType::LargeUtf8 => process_parse_url_array::<_, LargeStringArray>(as_large_string_array(&array_url)?, part, None),
                    other => {
                        exec_err!("Spark `decode` function: First arg must be BINARY, got {other:?}")
                    }
                }
                _ => exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}"),
            }

            (ColumnarValue::Array(array_url), ColumnarValue::Array(array_part)) => {
                match (array_url.data_type(), array_part.data_type()) {
                    (DataType::Utf8, DataType::Utf8) => process_parse_arrays::<_, _, StringArray>(
                        as_string_array(&array_url)?,
                        as_string_array(&array_part)?,
                        None
                    ),
                    (DataType::Utf8, DataType::Utf8View) => process_parse_arrays::<_, _, StringArray>(
                        as_string_array(&array_url)?,
                        as_string_view_array(&array_part)?,
                        None
                    ),
                    (DataType::Utf8, DataType::LargeUtf8) => process_parse_arrays::<_, _, LargeStringArray>(
                        as_string_array(&array_url)?,
                        as_large_string_array(&array_part)?,
                        None
                    ),
                    (DataType::Utf8View, DataType::Utf8) => process_parse_arrays::<_, _, StringArray>(
                        as_string_view_array(&array_url)?,
                        as_string_array(&array_part)?,
                        None
                    ),
                    (DataType::Utf8View, DataType::Utf8View) => process_parse_arrays::<_, _, StringArray>(
                        as_string_view_array(&array_url)?,
                        as_string_view_array(&array_part)?,
                        None
                    ),
                    (DataType::Utf8View, DataType::LargeUtf8) => process_parse_arrays::<_, _, LargeStringArray>(
                        as_string_view_array(&array_url)?,
                        as_large_string_array(&array_part)?,
                        None
                    ),
                    (DataType::LargeUtf8, DataType::Utf8) => process_parse_arrays::<_, _, LargeStringArray>(
                        as_large_string_array(&array_url)?,
                        as_string_array(&array_part)?,
                        None
                    ),
                    (DataType::LargeUtf8, DataType::Utf8View) => process_parse_arrays::<_, _, LargeStringArray>(
                        as_large_string_array(&array_url)?,
                        as_string_view_array(&array_part)?,
                        None
                    ),
                    (DataType::LargeUtf8, DataType::LargeUtf8) => process_parse_arrays::<_, _, LargeStringArray>(
                        as_large_string_array(&array_url)?,
                        as_large_string_array(&array_part)?,
                        None
                    ),
                    _ => exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}"),
                }
            }
            _ => exec_err!("`parse_url` function: Unsupported"),
        },
        Some(key) => match (url, part, key) {
            (
                ColumnarValue::Scalar(scalar_url),
                ColumnarValue::Scalar(scalar_part),
                ColumnarValue::Scalar(scalar_key),
            ) => match (scalar_url, scalar_part, scalar_key) {
                (
                    ScalarValue::Utf8(Some(url))
                    | ScalarValue::LargeUtf8(Some(url))
                    | ScalarValue::Utf8View(Some(url)),
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key),
                ) => {
                    let result = ParseUrl::parse(url, part, key.as_deref())?;
                    match scalar_url {
                        ScalarValue::LargeUtf8(Some(_)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
                        }
                        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result))),
                    }
                }
                _ => {
                    exec_err!("`parse_url` function: Args must be STRING type, got {args:?}")
                }
            },
            (
                ColumnarValue::Array(array_url),
                ColumnarValue::Scalar(scalar_part),
                ColumnarValue::Scalar(scalar_key),
            ) => match (array_url.data_type(), scalar_part, scalar_key) {
                (
                    DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key),
                ) => {
                    match array_url.data_type() {
                        DataType::Utf8 => process_parse_url_array::<_, StringArray>(as_string_array(&array_url)?, part, key.as_deref()),
                        DataType::Utf8View => process_parse_url_array::<_, StringArray>(as_string_view_array(&array_url)?, part, key.as_deref()),
                        DataType::LargeUtf8 => process_parse_url_array::<_, LargeStringArray>(as_large_string_array(&array_url)?, part, key.as_deref()),
                        other => exec_err!("`parse_url` function: Args must be STRING type, got {other:?}"),
                    }
                }
                _ => exec_err!("`parse_url` function: Args must be STRING type, got {args:?}"),
            },
            (
                ColumnarValue::Array(array_url),
                ColumnarValue::Array(array_part),
                ColumnarValue::Scalar(scalar_key),
            ) => match (array_url.data_type(), array_part.data_type(), scalar_key) {
                (
                    DataType::Utf8,
                    DataType::Utf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, StringArray>(
                    as_string_array(&array_url)?,
                    as_string_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::Utf8,
                    DataType::Utf8View,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, StringArray>(
                    as_string_array(&array_url)?,
                    as_string_view_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, LargeStringArray>(
                    as_string_array(&array_url)?,
                    as_large_string_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::Utf8View,
                    DataType::Utf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, StringArray>(
                    as_string_view_array(&array_url)?,
                    as_string_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::Utf8View,
                    DataType::Utf8View,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, StringArray>(
                    as_string_view_array(&array_url)?,
                    as_string_view_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::Utf8View,
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, LargeStringArray>(
                    as_string_view_array(&array_url)?,
                    as_large_string_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::LargeUtf8,
                    DataType::Utf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    as_string_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::LargeUtf8,
                    DataType::Utf8View,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    as_string_view_array(&array_part)?,
                    key.as_deref()
                ),
                (
                    DataType::LargeUtf8,
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(key)
                    | ScalarValue::LargeUtf8(key)
                    | ScalarValue::Utf8View(key)
                ) => process_parse_arrays::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    as_large_string_array(&array_part)?,
                    key.as_deref()
                ),
                _ => exec_err!("`parse_url` function: Args must be STRING type, got {args:?}"),
            },
            (
                ColumnarValue::Array(array_url),
                ColumnarValue::Scalar(scalar_part),
                ColumnarValue::Array(array_key),
            ) => match (array_url.data_type(), scalar_part, array_key.data_type()) {
                (
                    DataType::Utf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8
                ) => process_parse_arrays_part_scalar::<_, _, StringArray>(
                    as_string_array(&array_url)?,
                    part,
                    as_string_array(&array_key)?
                ),
                (
                    DataType::Utf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8View,
                ) => process_parse_arrays_part_scalar::<_, _, StringArray>(
                    as_string_array(&array_url)?,
                    part,
                    as_string_view_array(&array_key)?,
                ),
                (
                    DataType::Utf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::LargeUtf8,
                ) => process_parse_arrays_part_scalar::<_, _, LargeStringArray>(
                    as_string_array(&array_url)?,
                    part,
                    as_large_string_array(&array_key)?,
                ),
                (
                    DataType::Utf8View,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8,
                ) => process_parse_arrays_part_scalar::<_, _, StringArray>(
                    as_string_view_array(&array_url)?,
                    part,
                    as_string_array(&array_key)?
                ),
                (
                    DataType::Utf8View,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8View,
                ) => process_parse_arrays_part_scalar::<_, _, StringArray>(
                    as_string_view_array(&array_url)?,
                    part,
                    as_string_view_array(&array_key)?
                ),
                (
                    DataType::Utf8View,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::LargeUtf8
                ) => process_parse_arrays_part_scalar::<_, _, LargeStringArray>(
                    as_string_view_array(&array_url)?,
                    part,
                    as_large_string_array(&array_key)?,
                ),
                (
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8
                ) => process_parse_arrays_part_scalar::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    part,
                    as_string_array(&array_key)?,
                ),
                (
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::Utf8View
                ) => process_parse_arrays_part_scalar::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    part,
                    as_string_view_array(&array_key)?,
                ),
                (
                    DataType::LargeUtf8,
                    ScalarValue::Utf8(Some(part))
                    | ScalarValue::LargeUtf8(Some(part))
                    | ScalarValue::Utf8View(Some(part)),
                    DataType::LargeUtf8,
                ) => process_parse_arrays_part_scalar::<_, _, LargeStringArray>(
                    as_large_string_array(&array_url)?,
                    part,
                    as_large_string_array(&array_key)?,
                ),
                _ => exec_err!("`parse_url` function: Args must be STRING type, got {args:?}"),
            },
            (
                ColumnarValue::Array(array_url),
                ColumnarValue::Array(array_part),
                ColumnarValue::Array(array_key),
            ) => match (array_url.data_type(), array_part.data_type(), array_key.data_type()) {
                (DataType::Utf8, DataType::Utf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_array(&array_url)?, as_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8, DataType::Utf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_array(&array_url)?, as_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8, DataType::Utf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_array(&array_url)?, as_string_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::Utf8, DataType::Utf8View, DataType::Utf8) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_array(&array_url)?, as_string_view_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8, DataType::Utf8View, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_array(&array_url)?, as_string_view_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_array(&array_url)?, as_string_view_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_array(&array_url)?, as_large_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_array(&array_url)?, as_large_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_array(&array_url)?, as_large_string_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_view_array(&array_url)?, as_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_view_array(&array_url)?, as_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_view_array(&array_url)?, as_string_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8View, DataType::Utf8) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_view_array(&array_url)?, as_string_view_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, StringArray>(as_string_view_array(&array_url)?, as_string_view_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8View, DataType::Utf8View, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_view_array(&array_url)?, as_string_view_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_view_array(&array_url)?, as_large_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_view_array(&array_url)?, as_large_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::Utf8View, DataType::LargeUtf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_string_view_array(&array_url)?, as_large_string_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_view_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_view_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::LargeUtf8, DataType::Utf8View, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_string_view_array(&array_part)?, as_large_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_large_string_array(&array_part)?, as_string_array(&array_key)?),
                (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8View) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_large_string_array(&array_part)?, as_string_view_array(&array_key)?),
                (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => process_parse_arrays_all::<_, _, _, LargeStringArray>(as_large_string_array(&array_url)?, as_large_string_array(&array_part)?, as_large_string_array(&array_key)?),
                _ => exec_err!("`parse_url` function: Args must be STRING type, got {args:?}"),
            },
            _ => exec_err!("`parse_url` function: Unsupported"),
        },
    }
}

fn process_parse_url_array<'a, A, T>(
    url_array: &'a A,
    part: &str,
    key: Option<&str>,
) -> Result<ColumnarValue>
where
    &'a A: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .map(|x| {
            x.map(|x| ParseUrl::parse(x, part, key))
                .transpose()
                .map(Option::flatten)
        })
        .collect::<Result<T>>()
        .map(|array| ColumnarValue::Array(Arc::new(array)))
}

fn process_parse_arrays<'a, A, B, T>(
    url_array: &'a A,
    part_array: &'a B,
    key: Option<&str>,
) -> Result<ColumnarValue>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .zip(part_array.iter())
        .map(|(url, part)| {
            if let (Some(url), Some(part)) = (url, part) {
                ParseUrl::parse(url, part, key)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| ColumnarValue::Array(Arc::new(array)))
}

fn process_parse_arrays_part_scalar<'a, A, B, T>(
    url_array: &'a A,
    part: &str,
    key_array: &'a B,
) -> Result<ColumnarValue>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .zip(key_array.iter())
        .map(|(url, key)| {
            if let (Some(url), key) = (url, key) {
                ParseUrl::parse(url, part, key)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| ColumnarValue::Array(Arc::new(array)))
}

fn process_parse_arrays_all<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
) -> Result<ColumnarValue>
where
    &'a A: StringArrayType<'a>,
    &'a B: StringArrayType<'a>,
    &'a C: StringArrayType<'a>,
    T: Array + FromIterator<Option<String>> + 'static,
{
    url_array
        .iter()
        .zip(part_array.iter())
        .zip(key_array.iter())
        .map(|((url, part), key)| {
            if let (Some(url), Some(part), key) = (url, part, key) {
                ParseUrl::parse(url, part, key)
            } else {
                Ok(None)
            }
        })
        .collect::<Result<T>>()
        .map(|array| ColumnarValue::Array(Arc::new(array)))
}
