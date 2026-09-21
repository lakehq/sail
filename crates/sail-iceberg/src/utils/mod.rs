// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod conversions;
pub mod metadata;
pub mod partition_transform;
pub mod snapshot_id;
pub mod timestamp;
pub mod transform;

use std::path::Path;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use object_store::path::Path as ObjectPath;
use url::Url;

pub const WRITE_RELATIVE_PROP: &str = "sail.iceberg.write_relative_paths";

pub enum WritePathMode {
    Absolute,
    Relative,
}

impl WritePathMode {
    pub fn from_properties(props: Option<&std::collections::HashMap<String, String>>) -> Self {
        if let Some(p) = props
            && let Some(v) = p.get(WRITE_RELATIVE_PROP)
            && (v.eq_ignore_ascii_case("true") || v == "1")
        {
            return WritePathMode::Relative;
        }
        WritePathMode::Absolute
    }
}

pub fn join_table_location(table_url: &Url, rel: &str, mode: &WritePathMode) -> Result<String> {
    match mode {
        WritePathMode::Absolute => Ok(format!("{}{rel}", url_to_location(table_url)?)),
        WritePathMode::Relative => Ok(rel.to_string()),
    }
}

pub fn parse_absolute_url(raw: &str) -> Option<Url> {
    Url::parse(raw).ok().filter(|url| url.scheme().len() > 1)
}

pub fn file_url_from_absolute_path(path: &str) -> Option<Url> {
    if Path::new(path).is_absolute() {
        return Url::from_file_path(path).ok();
    }
    windows_drive_path_to_file_url(path)
}

fn is_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn windows_drive_path_to_file_url(path: &str) -> Option<Url> {
    if !is_windows_drive_path(path) {
        return None;
    }

    let path = path.replace('\\', "/");
    let mut url = Url::parse("file:///").ok()?;
    url.path_segments_mut().ok()?.extend(path.split('/'));
    Some(url)
}

/// Resolve a normalized table or writer URL, undoing its URI encoding once.
pub fn url_to_object_path(url: &Url) -> Result<ObjectPath> {
    if cfg!(windows)
        && url.scheme() == "file"
        && let Ok(path) = url.to_file_path()
    {
        return ObjectPath::parse(path.to_string_lossy().replace('\\', "/"))
            .map_err(|error| DataFusionError::External(Box::new(error)));
    }
    ObjectPath::from_url_path(url.path())
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

/// Iceberg locations carry physical path characters, including literal percent escapes.
pub fn location_to_object_path(location: &str) -> Result<ObjectPath> {
    if let Some(path) = absolute_location_to_object_path(location)? {
        return Ok(path);
    }
    ObjectPath::parse(location.replace('\\', "/"))
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

pub(crate) fn absolute_location_to_object_path(location: &str) -> Result<Option<ObjectPath>> {
    let path = if Path::new(location).is_absolute() || is_windows_drive_path(location) {
        location
    } else if let Some((_, path)) = location
        .split_once(':')
        .filter(|_| parse_absolute_url(location).is_some())
    {
        if let Some(authority_and_path) = path.strip_prefix("//") {
            authority_and_path
                .find('/')
                .map_or("", |start| &authority_and_path[start..])
        } else {
            path
        }
    } else {
        return Ok(None);
    };
    ObjectPath::parse(path.replace('\\', "/"))
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

/// Convert an internal URL to the path representation persisted in Iceberg metadata.
pub fn url_to_location(url: &Url) -> Result<String> {
    let path = url_to_object_path(url)?;
    let mut location = format!("{}/{}", &url[..url::Position::BeforePath], path);
    if url.path().ends_with('/') && !location.ends_with('/') {
        location.push('/');
    }
    Ok(location)
}

pub fn get_object_store_from_context(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn get_object_store_from_session(
    session: &dyn Session,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    session
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_urls_and_metadata_locations_resolve_the_same_physical_path()
    -> Result<(), Box<dyn std::error::Error>> {
        for scheme in ["file", "s3"] {
            let authority = if scheme == "file" { "" } else { "bucket" };
            let url = Url::parse(&format!(
                "{scheme}://{authority}/C:/table%20%2520+%23%3F%E4%B8%AD/"
            ))?;
            let path = ObjectPath::parse("C:/table %20+#?中")?;
            assert_eq!(url_to_object_path(&url)?, path);
            let location = url_to_location(&url)?;
            assert_eq!(
                location,
                format!("{scheme}://{authority}/C:/table %20+#?中/")
            );
            assert_eq!(location_to_object_path(&location)?, path);
        }
        Ok(())
    }

    #[test]
    fn windows_paths_encode_literal_percent_when_converted_to_urls()
    -> Result<(), Box<dyn std::error::Error>> {
        let url = file_url_from_absolute_path("C:\\table %20\\data")
            .ok_or("expected Windows file URL")?;
        assert_eq!(url.as_str(), "file:///C:/table%20%2520/data");
        assert_eq!(url_to_object_path(&url)?.as_ref(), "C:/table %20/data");
        Ok(())
    }
}
