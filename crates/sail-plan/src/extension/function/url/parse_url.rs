use std::any::Any;
use std::sync::Arc;

use crate::extension::function::functions_utils::make_scalar_function;
use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringBuilder, LargeStringArray, StringArray, StringArrayType,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result};
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
                vec![TypeSignature::String(2), TypeSignature::String(3)],
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

        make_scalar_function(spark_parse_url, vec![])(&args)
    }
}

fn spark_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`parse_url` expects 2 or 3 arguments, but got {}",
            args.len()
        );
    }

    let url = &args[0];
    let part = &args[1];

    let result = if args.len() == 3 {
        let key = &args[2];

        match (url.data_type(), part.data_type(), key.data_type()) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                )
            }
            _ => exec_err!(
                "`parse_url` function: URL must be STRING type, got {:?}",
                args[0].data_type()
            ),
        }
    } else {
        // The key parameter is missing, assume all values are null
        // Build null string array
        let mut builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        for _ in 0..args[0].len() {
            builder.append_null();
        }
        let key = builder.finish();

        match (url.data_type(), part.data_type()) {
            (DataType::Utf8, DataType::Utf8) => process_parse_url::<_, _, _, StringArray>(
                as_string_array(url)?,
                as_string_array(part)?,
                &key
            ),
            (DataType::Utf8, DataType::Utf8View) => process_parse_url::<_, _, _, StringArray>(
                as_string_array(url)?,
                as_string_view_array(part)?,
                &key
            ),
            (DataType::Utf8, DataType::LargeUtf8) => process_parse_url::<_, _, _, LargeStringArray>(
                as_string_array(url)?,
                as_large_string_array(part)?,
                &key
            ),
            (DataType::Utf8View, DataType::Utf8) => process_parse_url::<_, _, _, StringArray>(
                as_string_view_array(url)?,
                as_string_array(part)?,
                &key
            ),
            (DataType::Utf8View, DataType::Utf8View) => process_parse_url::<_, _, _, StringArray>(
                as_string_view_array(url)?,
                as_string_view_array(part)?,
                &key
            ),
            (DataType::Utf8View, DataType::LargeUtf8) => process_parse_url::<_, _, _, LargeStringArray>(
                as_string_view_array(url)?,
                as_large_string_array(part)?,
                &key
            ),
            (DataType::LargeUtf8, DataType::Utf8) => process_parse_url::<_, _, _, LargeStringArray>(
                as_large_string_array(url)?,
                as_string_array(part)?,
                &key
            ),
            (DataType::LargeUtf8, DataType::Utf8View) => process_parse_url::<_, _, _, LargeStringArray>(
                as_large_string_array(url)?,
                as_string_view_array(part)?,
                &key
            ),
            (DataType::LargeUtf8, DataType::LargeUtf8) => process_parse_url::<_, _, _, LargeStringArray>(
                as_large_string_array(url)?,
                as_large_string_array(part)?,
                &key
            ),
            _ => exec_err!("Spark `decode` function: First arg must be BINARY and second arg must be STRING type, got {args:?}"),
        }
    };
    result
}

fn process_parse_url<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
) -> Result<ArrayRef>
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
        .map(|array| Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use datafusion_common::Result;

    use super::*;

    #[test]
    fn test_parse_url() -> Result<()> {
        let url = Arc::new(StringArray::from(vec![
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            Some("http://userinfo@spark.apache.org/path?query=1#Ref"),
            None,
        ]));
        let part = Arc::new(StringArray::from(vec![
            Some("HOST"),
            Some("PATH"),
            Some("QUERY"),
            Some("REF"),
            Some("PROTOCOL"),
            Some("FILE"),
            Some("AUTHORITY"),
            Some("USERINFO"),
            None,
        ]));
        let expected = StringArray::from(vec![
            Some("spark.apache.org"),
            Some("/path"),
            Some("query=1"),
            Some("Ref"),
            Some("http"),
            Some("/path?query=1"),
            Some("userinfo@spark.apache.org"),
            Some("userinfo"),
            None,
        ]);

        let result = spark_parse_url(&[url.clone(), part.clone()])?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);

        Ok(())
    }
}
