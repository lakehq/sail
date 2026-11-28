// SPDX-License-Identifier: Apache-2.0

use url::Url;

/// Parse an absolute directory location without glob expansion or filesystem probes.
/// Only the path gets a trailing separator; URL authority, escapes, query and fragment stay intact.
pub fn parse_absolute_directory_url(path: &str) -> Option<Url> {
    let mut url = parse_absolute_url(path).or_else(|| file_url_from_absolute_path(path))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Some(url)
}

/// Parse an absolute URL, reading a URL without an authority as Hadoop does.
///
/// The URL parser reads `file:x` as `file:///x` and `http:/h/x` as `http://h/x`.
/// These are not absolute, and neither is a bare scheme such as `file:`.
/// Any other `scheme:/path` has an empty authority, as in `scheme:///path`.
pub fn parse_absolute_url(raw: &str) -> Option<Url> {
    if let Some((scheme, rest)) = raw.split_once(':')
        && scheme.len() > 1
    {
        if rest.is_empty() {
            return None;
        }
        if scheme.eq_ignore_ascii_case("file") {
            if !rest.starts_with('/') && windows_drive_path_to_file_url(rest).is_none() {
                return None;
            }
        } else if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") {
            if rest.strip_prefix("//").is_none_or(|host| host.is_empty() || host.starts_with('/')) {
                return None;
            }
        } else if rest.starts_with('/') && !rest.starts_with("//") {
            return Url::parse(&format!("{scheme}://{rest}")).ok();
        }
    }
    Url::parse(raw).ok().filter(|url| url.scheme().len() > 1)
}

pub fn file_url_from_absolute_path(path: &str) -> Option<Url> {
    if std::path::Path::new(path).is_absolute() {
        return Url::from_file_path(path).ok();
    }
    windows_drive_path_to_file_url(path)
}

fn windows_drive_path_to_file_url(path: &str) -> Option<Url> {
    let bytes = path.as_bytes();
    if bytes.len() < 3
        || !bytes[0].is_ascii_alphabetic()
        || bytes[1] != b':'
        || !matches!(bytes[2], b'/' | b'\\')
    {
        return None;
    }

    let path = path.replace('\\', "/");
    Url::parse(&format!("file:///{path}")).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_without_an_absolute_path_is_not_absolute() {
        for input in [
            "file:x",
            "file:rel/x",
            "file:./x",
            "file:",
            "FILE:x",
            r"file:\x",
            "hdfs:",
            "http:/h/x",
            "http:///h/x",
            "http:h/x",
            "https:/h/x",
            "http://",
        ] {
            assert_eq!(parse_absolute_url(input), None, "{input}");
            assert_eq!(parse_absolute_directory_url(input), None, "{input}");
        }
    }

    #[test]
    fn test_absolute_url() {
        for (input, expected) in [
            ("file:/x", "file:///x"),
            ("file:///x", "file:///x"),
            ("FILE:/x", "file:///x"),
            ("file:C:/x", "file:///C:/x"),
            ("file://localhost/x", "file:///x"),
            ("s3a:/b/k", "s3a:///b/k"),
            ("s3a:///b/k", "s3a:///b/k"),
            ("hdfs:/x", "hdfs:///x"),
            ("s3://b/k", "s3://b/k"),
            ("http://h/x", "http://h/x"),
        ] {
            let url = parse_absolute_url(input).map(String::from);
            assert_eq!(url.as_deref(), Some(expected), "{input}");
            let directory = parse_absolute_directory_url(input).map(String::from);
            assert_eq!(directory, Some(format!("{expected}/")), "{input}");
        }
    }
}
