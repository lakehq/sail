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

use std::sync::Arc;

use datafusion::common::{plan_err, DataFusionError, Result};
use url::Url;

pub async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    log::trace!("Finding latest metadata file");
    let version_hint_path =
        ObjectPath::parse(format!("{}metadata/version-hint.text", table_url.path()).as_str())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut hinted_version: Option<i32> = None;
    let mut hinted_filename: Option<String> = None;
    if let Ok(version_hint_data) = object_store.get(&version_hint_path).await {
        if let Ok(version_hint_bytes) = version_hint_data.bytes().await {
            if let Ok(version_hint) = String::from_utf8(version_hint_bytes.to_vec()) {
                let content = version_hint.trim();
                if let Ok(version) = content.parse::<i32>() {
                    log::trace!("Using numeric version hint: {}", version);
                    hinted_version = Some(version);
                } else {
                    let fname = if content.ends_with(".metadata.json") {
                        content.to_string()
                    } else {
                        format!("{}.metadata.json", content)
                    };
                    log::trace!("Using filename version hint: {}", fname);
                    hinted_filename = Some(fname);
                }
            }
        }
    }

    log::trace!("Listing metadata directory");
    let metadata_prefix = ObjectPath::parse(format!("{}metadata/", table_url.path()).as_str())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let objects = object_store.list(Some(&metadata_prefix));

    let metadata_files: Result<Vec<_>, _> = objects
        .try_filter_map(|obj| async move {
            let path_str = obj.location.to_string();
            if path_str.ends_with(".metadata.json") {
                if let Some(filename) = path_str.split('/').next_back() {
                    if let Some(version_part) = filename.split('-').next() {
                        if let Ok(version) = version_part.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                    if let Some(version_str) = filename
                        .strip_prefix('v')
                        .and_then(|s| s.strip_suffix(".metadata.json"))
                    {
                        if let Ok(version) = version_str.parse::<i32>() {
                            return Ok(Some((version, path_str, obj.last_modified)));
                        }
                    }
                }
            }
            Ok(None)
        })
        .try_collect()
        .await;

    match metadata_files {
        Ok(mut files) => {
            log::trace!("find_latest_metadata_file: found files: {:?}", &files);
            files.sort_by_key(|(version, _, _)| *version);

            if let Some(fname) = hinted_filename {
                if let Some((version, path, _)) =
                    files.iter().rev().find(|(_, p, _)| p.ends_with(&fname))
                {
                    log::trace!(
                        "find_latest_metadata_file: selected by filename hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            } else if let Some(hint) = hinted_version {
                if let Some((version, path, _)) = files.iter().rev().find(|(v, _, _)| *v == hint) {
                    log::trace!(
                        "find_latest_metadata_file: selected by numeric hint version {} path={}",
                        version,
                        &path
                    );
                    return Ok(path.clone());
                }
            }

            if let Some((version, latest_file, _)) = files.last() {
                log::trace!(
                    "find_latest_metadata_file: selected version {} path={}",
                    version,
                    &latest_file
                );
                Ok(latest_file.clone())
            } else {
                plan_err!("No metadata files found in table location: {}", table_url)
            }
        }
        Err(e) => {
            plan_err!("Failed to list metadata directory: {}", e)
        }
    }
}
