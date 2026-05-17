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

    fn parse(value: &str, part: &str, key: Option<&str>) -> Result<Option<String>> {
        match Url::parse(value) {
            Ok(url) => Ok(match part {
                "HOST" => {
                    // java.net.URI returns null for percent-encoded or non-ASCII (IDN)
                    // hostnames. The WHATWG url crate decodes %-sequences and converts
                    // IDN to Punycode, so we check the raw string instead.
                    if host_is_percent_encoded(value) || host_has_non_ascii(value) {
                        None
                    } else {
                        url.host_str().map(String::from)
                    }
                }
                "PATH" => {
                    // java.net.URI returns null for PATH on opaque URIs; we match that.
                    if is_opaque_uri(&url) {
                        return Ok(None);
                    }
                    // Extract from the raw string: the url crate percent-encodes non-ASCII
                    // characters in the path, but java.net.URI preserves them verbatim.
                    let path = extract_raw_path_from(value);
                    // Spark: "https://example.com" → empty PATH, "https://example.com/" → "/"
                    // The url crate always returns "/" when there is no path, so we use
                    // has_explicit_path to distinguish "http://ex.com" from "http://ex.com/".
                    let path = if path == "/" && !has_explicit_path(value) {
                        "".to_string()
                    } else {
                        path.to_string()
                    };
                    Some(path)
                }
                "QUERY" => {
                    // Opaque URIs (e.g. "mailto:user@example.com?subject=x") have no
                    // QUERY component in java.net.URI — return NULL to match Spark.
                    if is_opaque_uri(&url) {
                        return Ok(None);
                    }
                    // Extract from raw string to preserve non-ASCII and avoid auto-decoding.
                    let raw_query = extract_raw_query_from(value);
                    match key {
                        None => raw_query.map(String::from),
                        Some(key) => raw_query.and_then(|q| {
                            q.split('&')
                                .filter_map(|pair| pair.split_once('='))
                                .find(|(k, _)| *k == key)
                                .map(|(_, v)| v.to_string())
                        }),
                    }
                }
                // Extract fragment from raw string; url crate percent-encodes non-ASCII.
                "REF" => extract_raw_fragment_from(value).map(String::from),
                "PROTOCOL" => Some(url.scheme().to_string()),
                "FILE" => {
                    // Opaque URIs have no FILE component in java.net.URI — return NULL.
                    if is_opaque_uri(&url) {
                        return Ok(None);
                    }
                    let path = extract_raw_path_from(value);
                    let path = if path == "/" && !has_explicit_path(value) {
                        ""
                    } else {
                        path
                    };
                    match extract_raw_query_from(value) {
                        Some(query) => Some(format!("{path}?{query}")),
                        None => Some(path.to_string()),
                    }
                }
                "AUTHORITY" => {
                    // Extract directly from the raw string: avoids Punycode conversion for
                    // IDN hosts and percent-encoding of non-ASCII userinfo. The raw authority
                    // naturally includes any explicitly-specified port (even default ports),
                    // which matches java.net.URI semantics.
                    extract_raw_authority_from(value)
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                }
                "USERINFO" => {
                    // Extract from raw string to preserve non-ASCII characters.
                    extract_raw_userinfo_from(value)
                        .filter(|s| !s.is_empty())
                        .map(String::from)
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

/// Returns true if the URL is an opaque URI — scheme-specific part without a hierarchical path.
/// Examples: `mailto:user@example.com`, `tel:+1-816-555-1212`, `urn:isbn:...`
/// java.net.URI returns null for PATH, QUERY, FILE, AUTHORITY on opaque URIs.
fn is_opaque_uri(url: &url::Url) -> bool {
    !url.has_authority() && !url.path().starts_with('/')
}

/// Returns the byte range of the host portion within a URL string.
/// Accounts for optional userinfo (before `@`) and stops at `/`, `?`, `#`, or `:` (port).
/// For IPv6 addresses (`[::1]`), the closing `]` is the host end so `:` inside brackets
/// is skipped — making this safe to use for percent-encoding and ASCII checks.
fn raw_host_range(url: &str) -> Option<(usize, usize)> {
    let after_scheme = url.find("://").map(|i| i + 3)?;
    let host_start = url[after_scheme..]
        .find('@')
        .map(|i| after_scheme + i + 1)
        .unwrap_or(after_scheme);
    let host_end = if url[host_start..].starts_with('[') {
        // IPv6 literal: scan to the closing ']'
        url[host_start..]
            .find(']')
            .map(|i| host_start + i + 1)
            .unwrap_or(url.len())
    } else {
        url[host_start..]
            .find(['/', '?', '#', ':'])
            .map(|i| host_start + i)
            .unwrap_or(url.len())
    };
    Some((host_start, host_end))
}

/// Returns true if the host portion of the URL contains a percent-encoded character.
/// java.net.URI returns null for `getHost()` when the host is percent-encoded
/// (e.g. "http://ex%61mple.com"), while the WHATWG url crate decodes it.
fn host_is_percent_encoded(url: &str) -> bool {
    raw_host_range(url)
        .map(|(s, e)| url[s..e].contains('%'))
        .unwrap_or(false)
}

/// Returns true if the host portion of the URL contains non-ASCII characters (IDN hostname).
/// java.net.URI returns null for `getHost()` on non-ASCII (non-Punycode) hostnames.
fn host_has_non_ascii(url: &str) -> bool {
    raw_host_range(url)
        .map(|(s, e)| !url[s..e].is_ascii())
        .unwrap_or(false)
}

/// Extracts the raw (un-encoded) path from a hierarchical URL string.
/// Returns an empty slice when there is no path segment.
fn extract_raw_path_from(url_str: &str) -> &str {
    let path_start = if let Some(i) = url_str.find("://") {
        // URL with authority (scheme://host/path): path starts after authority.
        let after_scheme = i + 3;
        url_str[after_scheme..]
            .find(['/', '?', '#'])
            .map(|j| after_scheme + j)
            .unwrap_or(url_str.len())
    } else if let Some(i) = url_str.find(':') {
        // Authority-less URL (scheme:/path or scheme:opaque): path starts after ':'.
        // Opaque URIs are guarded by is_opaque_uri() before this helper is called.
        i + 1
    } else {
        return "";
    };
    let path_end = url_str[path_start..]
        .find(['?', '#'])
        .map(|i| path_start + i)
        .unwrap_or(url_str.len());
    &url_str[path_start..path_end]
}

/// Extracts the raw query string (the part after `?` and before `#`) from a URL string.
/// Returns `None` when no `?` is present before any `#`.
fn extract_raw_query_from(url_str: &str) -> Option<&str> {
    let frag_start = url_str.find('#').unwrap_or(url_str.len());
    let q_pos = url_str[..frag_start].find('?')?;
    Some(&url_str[q_pos + 1..frag_start])
}

/// Extracts the raw fragment (the part after `#`) from a URL string.
fn extract_raw_fragment_from(url_str: &str) -> Option<&str> {
    url_str.find('#').map(|i| &url_str[i + 1..])
}

/// Extracts the raw authority (everything between `://` and the first `/`, `?`, or `#`).
fn extract_raw_authority_from(url_str: &str) -> Option<&str> {
    let after_scheme = url_str.find("://").map(|i| i + 3)?;
    let auth_end = url_str[after_scheme..]
        .find(['/', '?', '#'])
        .map(|i| after_scheme + i)
        .unwrap_or(url_str.len());
    let auth = &url_str[after_scheme..auth_end];
    if auth.is_empty() {
        None
    } else {
        Some(auth)
    }
}

/// Extracts the raw userinfo (everything before the last `@` in the authority).
fn extract_raw_userinfo_from(url_str: &str) -> Option<&str> {
    let after_scheme = url_str.find("://").map(|i| i + 3)?;
    let auth_end = url_str[after_scheme..]
        .find(['/', '?', '#'])
        .map(|i| after_scheme + i)
        .unwrap_or(url_str.len());
    let authority = &url_str[after_scheme..auth_end];
    authority.rfind('@').map(|i| &authority[..i])
}

/// Returns true if the URL has an explicit path segment (a `/` immediately after the authority).
/// Distinguishes `http://ex.com` (no path, url crate returns "/") from `http://ex.com/`
/// and `http://ex.com/?` (both have explicit "/").
/// Authority-less URLs (e.g. `file:/path`, `custom:/root`) always have an explicit path.
fn has_explicit_path(url: &str) -> bool {
    match url.find("://") {
        Some(i) => {
            let after_scheme = i + 3;
            let after_authority = url[after_scheme..]
                .find(['/', '?', '#'])
                .map(|j| after_scheme + j)
                .unwrap_or(url.len());
            url[after_authority..].starts_with('/')
        }
        // No authority section — the path after the colon is always explicit.
        None => true,
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
        // Authority-less URLs always have an explicit path
        assert!(has_explicit_path("file:/"));
        assert!(has_explicit_path("file:/etc/hosts"));
        assert!(has_explicit_path("custom:/root/path"));
    }
}
