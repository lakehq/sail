use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericStringBuilder, LargeStringArray, StringArray, StringArrayType,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use url::Url;

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseUrl {
    safe: bool,
    signature: Signature,
}

impl Default for ParseUrl {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ParseUrl {
    pub fn new(safe: bool) -> Self {
        Self {
            safe,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn safe(&self) -> bool {
        self.safe
    }
    /// Parses a URL and extracts the specified component.
    ///
    /// This function takes a URL string and extracts different parts of it based on the
    /// `part` parameter. For query parameters, an optional `key` can be specified to
    /// extract a specific query parameter value.
    ///
    /// # Arguments
    ///
    /// * `value` - The URL string to parse
    /// * `part` - The component of the URL to extract. Valid values are:
    ///   - `"HOST"` - The hostname (e.g., "example.com")
    ///   - `"PATH"` - The path portion (e.g., "/path/to/resource")
    ///   - `"QUERY"` - The query string or a specific query parameter
    ///   - `"REF"` - The fragment/anchor (the part after #)
    ///   - `"PROTOCOL"` - The URL scheme (e.g., "https", "http")
    ///   - `"FILE"` - The path with query string (e.g., "/path?query=value")
    ///   - `"AUTHORITY"` - The authority component (host:port)
    ///   - `"USERINFO"` - The user information (username:password)
    /// * `key` - Optional parameter used only with `"QUERY"`. When provided, extracts
    ///   the value of the specific query parameter with this key name.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(String))` - The extracted URL component as a string
    /// * `Ok(None)` - If the requested component doesn't exist or is empty
    /// * `Err(DataFusionError)` - If the URL is malformed and cannot be parsed
    ///
    fn parse(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
        match Url::parse(value) {
            Ok(url) => Ok(match part {
                "HOST" => url.host_str().map(String::from),
                "PATH" => {
                    let path = url.path().to_string();
                    // Spark: "https://example.com" → empty PATH, "https://example.com/" → "/"
                    // The url crate always returns "/" when there is no path, so we use
                    // has_explicit_path to distinguish "http://ex.com" from "http://ex.com/".
                    let path = if path == "/" && !has_explicit_path(value) {
                        "".to_string()
                    } else {
                        path
                    };
                    Some(path)
                }
                "QUERY" => match key {
                    None => url.query().map(String::from),
                    Some(key) => {
                        // Spark doesn't decode percent-encoding in query values.
                        // Use raw query string parsing instead of url.query_pairs()
                        // which auto-decodes.
                        url.query().and_then(|q| {
                            q.split('&')
                                .filter_map(|pair| pair.split_once('='))
                                .find(|(k, _)| *k == key)
                                .map(|(_, v)| v.to_string())
                        })
                    }
                },
                "REF" => url.fragment().map(String::from),
                "PROTOCOL" => Some(url.scheme().to_string()),
                "FILE" => {
                    let path = url.path();
                    let path = if path == "/" && !has_explicit_path(value) {
                        ""
                    } else {
                        path
                    };
                    match url.query() {
                        Some(query) => Some(format!("{path}?{query}")),
                        None => Some(path.to_string()),
                    }
                }
                "AUTHORITY" => {
                    // Build authority manually to preserve explicit port
                    // (url crate strips default ports like 443 for https)
                    let mut auth = String::new();
                    let username = url.username();
                    if !username.is_empty() {
                        auth.push_str(username);
                        if let Some(password) = url.password() {
                            auth.push(':');
                            auth.push_str(password);
                        }
                        auth.push('@');
                    }
                    if let Some(host) = url.host_str() {
                        auth.push_str(host);
                    }
                    // Spark preserves the port even if it's the default for the scheme.
                    // Re-parse from the original string to get the explicit port.
                    if let Some(port_str) = extract_explicit_port(value) {
                        auth.push(':');
                        auth.push_str(port_str);
                    } else if let Some(port) = url.port() {
                        auth.push(':');
                        auth.push_str(&port.to_string());
                    }
                    Some(auth)
                }
                "USERINFO" => {
                    let username = url.username();
                    if username.is_empty() {
                        return Ok(None);
                    }
                    match url.password() {
                        Some(password) => Some(format!("{username}:{password}")),
                        None => Some(username.to_string()),
                    }
                }
                _ => None,
            }),
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                // Spark's java.net.URI treats schemeless strings as relative URIs.
                // Parse the components manually: path?query#fragment
                let (without_fragment, fragment) = match value.find('#') {
                    Some(i) => (&value[..i], Some(&value[i + 1..])),
                    None => (value, None),
                };
                let (path, query) = match without_fragment.find('?') {
                    Some(i) => (&without_fragment[..i], Some(&without_fragment[i + 1..])),
                    None => (without_fragment, None),
                };
                Ok(match part {
                    "PATH" => Some(path.to_string()),
                    "QUERY" => match key {
                        None => query.map(String::from),
                        Some(key) => query.and_then(|q| {
                            q.split('&')
                                .filter_map(|pair| pair.split_once('='))
                                .find(|(k, _)| *k == key)
                                .map(|(_, v)| v.to_string())
                        }),
                    },
                    "REF" => fragment.map(String::from),
                    "FILE" => {
                        let file = match query {
                            Some(q) => format!("{path}?{q}"),
                            None => path.to_string(),
                        };
                        Some(file)
                    }
                    _ => None,
                })
            }
            Err(e) => Err(exec_datafusion_err!("{e:?}")),
        }
    }
}

impl ScalarUDFImpl for ParseUrl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.safe {
            "try_parse_url"
        } else {
            "parse_url"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // The return type should match the largest size datatype
        match arg_types.len() {
            2 | 3 if arg_types.iter().all(is_string_type) => {
                if arg_types
                    .iter()
                    .any(|arg| matches!(arg, DataType::LargeUtf8))
                {
                    Ok(DataType::LargeUtf8)
                } else {
                    Ok(DataType::Utf8)
                }
            }
            2 | 3 => plan_err!(
                "`{}` expects STRING arguments, got {:?}",
                &self.name(),
                arg_types
            ),
            _ => plan_err!(
                "`{}` expects 2 or 3 arguments, got {}",
                &self.name(),
                arg_types.len()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            2 | 3
                if arg_types
                    .iter()
                    .all(|dt| is_string_type(dt) || dt == &DataType::Null) =>
            {
                Ok(arg_types
                    .iter()
                    .map(|dt| {
                        if matches!(dt, DataType::Null) {
                            DataType::Utf8
                        } else {
                            dt.clone()
                        }
                    })
                    .collect())
            }
            2 | 3 => plan_err!(
                "`{}` expects STRING arguments, got {:?}",
                &self.name(),
                arg_types
            ),
            _ => plan_err!(
                "`{}` expects 2 or 3 arguments, got {}",
                &self.name(),
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let safe = self.safe;
        let name = self.name().to_string();
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(move |a| spark_parse_url_impl(a, safe, &name), vec![])(&args)
    }
}

/// Returns true if the URL has an explicit path segment (a `/` immediately after the authority).
/// Distinguishes `http://ex.com` (no path, url crate returns "/") from `http://ex.com/`
/// and `http://ex.com/?` (both have explicit "/").
fn has_explicit_path(url: &str) -> bool {
    let after_scheme = match url.find("://") {
        Some(i) => i + 3,
        None => return false,
    };
    let after_authority = url[after_scheme..]
        .find(['/', '?', '#'])
        .map(|i| after_scheme + i)
        .unwrap_or(url.len());
    url[after_authority..].starts_with('/')
}

/// Extract the explicit port string from a raw URL, even if it's a default port.
/// Returns None if no port is specified in the URL string.
fn extract_explicit_port(url: &str) -> Option<&str> {
    // Find authority section: after "://" and before first "/" or "?" or "#" or end
    let after_scheme = url.find("://").map(|i| i + 3)?;
    let authority_end = url[after_scheme..]
        .find(['/', '?', '#'])
        .map(|i| after_scheme + i)
        .unwrap_or(url.len());
    let authority = &url[after_scheme..authority_end];
    // Strip userinfo if present
    let host_port = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    // Find last colon that's part of port (not IPv6)
    if host_port.starts_with('[') {
        // IPv6: [::1]:port
        host_port.rfind("]:").map(|i| &host_port[i + 2..])
    } else {
        host_port.rfind(':').map(|i| &host_port[i + 1..])
    }
}

fn is_string_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}

fn spark_parse_url_impl(args: &[ArrayRef], safe: bool, name: &str) -> Result<ArrayRef> {
    if safe {
        spark_handled_parse_url(name, args, |x| match x {
            Err(_) => Ok(None),
            result => result,
        })
    } else {
        spark_handled_parse_url(name, args, |x| x)
    }
}

fn spark_handled_parse_url(
    name: &str,
    args: &[ArrayRef],
    handler_err: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`{}` expects 2 or 3 arguments, but got {}",
            name,
            args.len()
        );
    }
    // Required arguments
    let url = &args[0];
    let part = &args[1];

    let result = if args.len() == 3 {
        // In this case, the 'key' argument is passed
        let key = &args[2];

        // Handle all 27 combinations - 3 arguments, each argument can have 3 different data types
        // The result data type would be LargeStringArray if there is any argument with LargeUtf8 data type
        // Else the StringArray would be returned
        match (url.data_type(), part.data_type(), key.data_type()) {
            (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, StringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_string_view_array(key)?,
                    handler_err,
                    true,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    as_large_string_array(key)?,
                    handler_err,
                    true,
                )
            }
            _ => exec_err!("`{}` expects STRING arguments, got {:?}", name, args),
        }
    } else {
        // The 'key' argument is omitted, assume all values are null
        // Create 'null' string array for 'key' argument
        let mut builder: GenericStringBuilder<i32> =
            GenericStringBuilder::with_capacity(args[0].len(), 0);
        for _ in 0..args[0].len() {
            builder.append_null();
        }
        let key = builder.finish();

        // Handle 9 combinations - 2 arguments, each argument can have 3 different data types
        // The result data type would be LargeStringArray if there is any argument with LargeUtf8 data type
        // Else the StringArray would be returned
        match (url.data_type(), part.data_type()) {
            (DataType::Utf8, DataType::Utf8) => process_parse_url::<_, _, _, StringArray>(
                as_string_array(url)?,
                as_string_array(part)?,
                &key,
                handler_err,
                false,
            ),
            (DataType::Utf8, DataType::Utf8View) => process_parse_url::<_, _, _, StringArray>(
                as_string_array(url)?,
                as_string_view_array(part)?,
                &key,
                handler_err,
                false,
            ),
            (DataType::Utf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_array(url)?,
                    as_large_string_array(part)?,
                    &key,
                    handler_err,
                    false,
                )
            }
            (DataType::Utf8View, DataType::Utf8) => process_parse_url::<_, _, _, StringArray>(
                as_string_view_array(url)?,
                as_string_array(part)?,
                &key,
                handler_err,
                false,
            ),
            (DataType::Utf8View, DataType::Utf8View) => process_parse_url::<_, _, _, StringArray>(
                as_string_view_array(url)?,
                as_string_view_array(part)?,
                &key,
                handler_err,
                false,
            ),
            (DataType::Utf8View, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_string_view_array(url)?,
                    as_large_string_array(part)?,
                    &key,
                    handler_err,
                    false,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_array(part)?,
                    &key,
                    handler_err,
                    false,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_string_view_array(part)?,
                    &key,
                    handler_err,
                    false,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                process_parse_url::<_, _, _, LargeStringArray>(
                    as_large_string_array(url)?,
                    as_large_string_array(part)?,
                    &key,
                    handler_err,
                    false,
                )
            }
            _ => exec_err!("`{}` expects STRING arguments, got {:?}", name, args),
        }
    };
    result
}

fn process_parse_url<'a, A, B, C, T>(
    url_array: &'a A,
    part_array: &'a B,
    key_array: &'a C,
    handle: impl Fn(Result<Option<String>>) -> Result<Option<String>>,
    has_key_arg: bool,
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
            // Spark: if 3-arg mode and key is NULL, return NULL
            if has_key_arg && key.is_none() {
                return Ok(None);
            }
            if let (Some(url), Some(part)) = (url, part) {
                handle(ParseUrl::parse(url, part, key))
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

        let result = spark_parse_url_impl(&[url.clone(), part.clone()], false, "parse_url")?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn test_parse_url_file_no_path() -> Result<()> {
        // Spark returns "" for FILE when URL has no explicit path, "?" when it has trailing "?",
        // and "/?query" when URL has explicit "/" before "?".
        let urls = Arc::new(StringArray::from(vec![
            Some("http://ex.com"),       // no path, no query
            Some("http://ex.com?foo=1"), // no explicit path, query only
            Some("http://ex.com/"),      // explicit "/" path, no query
            Some("http://ex.com/?"),     // explicit "/" path, trailing "?"
            Some("http://ex.com/?q=1"),  // explicit "/" path, with query
        ]));
        let parts = Arc::new(StringArray::from(vec![
            Some("FILE"),
            Some("FILE"),
            Some("FILE"),
            Some("FILE"),
            Some("FILE"),
        ]));
        let expected = StringArray::from(vec![
            Some(""),
            Some("?foo=1"),
            Some("/"),
            Some("/?"),
            Some("/?q=1"),
        ]);

        let result = spark_parse_url_impl(&[urls, parts], false, "parse_url")?;
        let result = as_string_array(&result)?;
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn test_has_explicit_path() {
        assert!(!has_explicit_path("http://ex.com"));
        assert!(!has_explicit_path("http://ex.com?foo=1"));
        assert!(!has_explicit_path("http://ex.com#frag"));
        assert!(has_explicit_path("http://ex.com/"));
        assert!(has_explicit_path("http://ex.com/?"));
        assert!(has_explicit_path("http://ex.com/?q=1"));
        assert!(has_explicit_path("http://ex.com/path"));
    }
}
