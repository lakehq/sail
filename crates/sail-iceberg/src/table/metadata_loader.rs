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

use std::io::{self, Read};
use std::sync::Arc;

use datafusion::common::{plan_err, Result};
use flate2::read::GzDecoder;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataFileCodec {
    None,
    Gzip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MetadataFileName {
    pub version: i32,
    pub codec: MetadataFileCodec,
}

fn metadata_file_stem(file_name: &str) -> Option<(&str, MetadataFileCodec)> {
    if let Some(stem) = file_name.strip_suffix(".metadata.json.gz") {
        return Some((stem, MetadataFileCodec::Gzip));
    }

    let stem = file_name.strip_suffix(".metadata.json")?;
    if let Some(stem) = stem.strip_suffix(".gz") {
        Some((stem, MetadataFileCodec::Gzip))
    } else {
        Some((stem, MetadataFileCodec::None))
    }
}

pub(crate) fn parse_metadata_file_name(file_name: &str) -> Option<MetadataFileName> {
    let (stem, codec) = metadata_file_stem(file_name)?;
    let version = if let Some(version) = stem.strip_prefix('v') {
        version.parse::<i32>().ok()?
    } else {
        stem.split_once('-')?.0.parse::<i32>().ok()?
    };

    Some(MetadataFileName { version, codec })
}

pub(crate) fn metadata_file_version_from_path(path: &str) -> Option<i32> {
    path.rsplit('/')
        .next()
        .and_then(parse_metadata_file_name)
        .map(|file| file.version)
}

fn metadata_file_codec_from_path(path: &str) -> Option<MetadataFileCodec> {
    path.rsplit('/')
        .next()
        .and_then(parse_metadata_file_name)
        .map(|file| file.codec)
}

pub(crate) fn decode_metadata_file(path: &str, data: &[u8]) -> io::Result<Vec<u8>> {
    match metadata_file_codec_from_path(path) {
        Some(MetadataFileCodec::Gzip) => {
            let mut decoder = GzDecoder::new(data);
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded)?;
            Ok(decoded)
        }
        Some(MetadataFileCodec::None) | None => Ok(data.to_vec()),
    }
}

pub async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    log::trace!("Finding latest metadata file");
    let base_path = crate::utils::url_to_object_path(table_url)?;
    let version_hint_path = base_path.clone().join("metadata").join("version-hint.text");
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
                    let fname = if parse_metadata_file_name(content).is_some() {
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
    let metadata_prefix = base_path.join("metadata");

    let objects = object_store.list(Some(&metadata_prefix));

    let metadata_files: Result<Vec<_>, _> = objects
        .try_filter_map(|obj| async move {
            let path_str = obj.location.to_string();
            if let Some(filename) = path_str.split('/').next_back() {
                if let Some(metadata_file) = parse_metadata_file_name(filename) {
                    return Ok(Some((metadata_file.version, path_str, obj.last_modified)));
                }
            }
            Ok(None)
        })
        .try_collect()
        .await;

    match metadata_files {
        Ok(mut files) => {
            log::trace!("find_latest_metadata_file: found files: {:?}", &files);
            files.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.2.cmp(&right.2))
                    .then_with(|| left.1.cmp(&right.1))
            });

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

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use flate2::write::GzEncoder;
    use flate2::Compression;

    use super::{
        decode_metadata_file, parse_metadata_file_name, MetadataFileCodec, MetadataFileName,
    };

    #[test]
    fn parses_metadata_file_names() {
        assert_eq!(
            parse_metadata_file_name("v1.metadata.json"),
            Some(MetadataFileName {
                version: 1,
                codec: MetadataFileCodec::None,
            })
        );
        assert_eq!(
            parse_metadata_file_name("v2.metadata.json.gz"),
            Some(MetadataFileName {
                version: 2,
                codec: MetadataFileCodec::Gzip,
            })
        );
        assert_eq!(
            parse_metadata_file_name("v3.gz.metadata.json"),
            Some(MetadataFileName {
                version: 3,
                codec: MetadataFileCodec::Gzip,
            })
        );
        assert_eq!(
            parse_metadata_file_name("00004-9441e604-b3c2-498a-a45a-6320e8ab9006.metadata.json"),
            Some(MetadataFileName {
                version: 4,
                codec: MetadataFileCodec::None,
            })
        );
        assert_eq!(
            parse_metadata_file_name("00005-9441e604-b3c2-498a-a45a-6320e8ab9006.metadata.json.gz"),
            Some(MetadataFileName {
                version: 5,
                codec: MetadataFileCodec::Gzip,
            })
        );
        assert_eq!(
            parse_metadata_file_name("00006-9441e604-b3c2-498a-a45a-6320e8ab9006.gz.metadata.json"),
            Some(MetadataFileName {
                version: 6,
                codec: MetadataFileCodec::Gzip,
            })
        );
        assert_eq!(parse_metadata_file_name("1.metadata.json"), None);
    }

    #[test]
    fn decodes_gzip_metadata_files() -> io::Result<()> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(br#"{"format-version":2}"#)?;
        let encoded = encoder.finish()?;

        assert_eq!(
            decode_metadata_file("metadata/v1.metadata.json.gz", &encoded)?,
            br#"{"format-version":2}"#.to_vec()
        );
        assert_eq!(
            decode_metadata_file("metadata/v1.metadata.json", br#"{"format-version":2}"#)?,
            br#"{"format-version":2}"#.to_vec()
        );

        Ok(())
    }
}
