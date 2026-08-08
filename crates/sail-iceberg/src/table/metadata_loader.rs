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

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result, plan_err};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use url::Url;

const METADATA_COMPRESSION_PROPERTY: &str = "write.metadata.compression-codec";
const METADATA_COMPRESSION_NONE: &str = "none";
const METADATA_COMPRESSION_GZIP: &str = "gzip";
const VERSION_HINT_PATH: &str = "metadata/version-hint.text";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataFileCodec {
    None,
    Gzip,
}

impl MetadataFileCodec {
    fn from_property_value(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            METADATA_COMPRESSION_NONE => Ok(Self::None),
            METADATA_COMPRESSION_GZIP => Ok(Self::Gzip),
            other => plan_err!("Unsupported Iceberg metadata compression codec: {other}"),
        }
    }

    fn file_extension(self) -> &'static str {
        match self {
            Self::None => ".metadata.json",
            Self::Gzip => ".gz.metadata.json",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MetadataFileName {
    pub version: i32,
    pub codec: MetadataFileCodec,
}

#[derive(Clone, Debug)]
struct MetadataCandidate {
    version: i32,
    path: ObjectPath,
    last_modified: chrono::DateTime<chrono::Utc>,
}

impl MetadataCandidate {
    fn is_newer_than(&self, other: &Self) -> bool {
        (self.version, &self.last_modified, self.path.as_ref())
            > (other.version, &other.last_modified, other.path.as_ref())
    }
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

pub(crate) fn metadata_location_to_object_path(metadata_location: &str) -> Result<ObjectPath> {
    match crate::utils::parse_absolute_url(metadata_location) {
        Some(url) => crate::utils::url_to_object_path(&url),
        None => ObjectPath::parse(metadata_location.trim_start_matches('/'))
            .map_err(|e| DataFusionError::External(Box::new(e))),
    }
}

pub(crate) fn metadata_location_to_object_path_string(metadata_location: &str) -> Result<String> {
    Ok(metadata_location_to_object_path(metadata_location)?.to_string())
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

pub(crate) fn encode_metadata_file(path: &str, data: &[u8]) -> io::Result<Vec<u8>> {
    match metadata_file_codec_from_path(path) {
        Some(MetadataFileCodec::Gzip) => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(data)?;
            encoder.finish()
        }
        Some(MetadataFileCodec::None) | None => Ok(data.to_vec()),
    }
}

pub(crate) fn metadata_file_extension_from_properties(
    properties: &HashMap<String, String>,
) -> Result<&'static str> {
    let codec = properties
        .get(METADATA_COMPRESSION_PROPERTY)
        .map(String::as_str)
        .unwrap_or(METADATA_COMPRESSION_NONE);
    Ok(MetadataFileCodec::from_property_value(codec)?.file_extension())
}

/// Writes the non-authoritative Iceberg metadata version hint.
///
/// This is best-effort: failures are logged because the hint is only an optional
/// lookup optimization.
pub(crate) async fn write_version_hint(
    object_store: &Arc<dyn object_store::ObjectStore>,
    version_hint: &str,
) {
    let hint_path = ObjectPath::from(VERSION_HINT_PATH);
    if let Err(error) = object_store
        .put(
            &hint_path,
            object_store::PutPayload::from(Bytes::copy_from_slice(version_hint.as_bytes())),
        )
        .await
    {
        log::warn!("Failed to update Iceberg version hint: {error}");
    }
}

pub(crate) async fn load_metadata_file_bytes(
    object_store: &Arc<dyn object_store::ObjectStore>,
    metadata_location: &str,
) -> Result<Vec<u8>> {
    let metadata_path = metadata_location_to_object_path(metadata_location)?;
    let metadata_data = object_store
        .get(&metadata_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    decode_metadata_file(metadata_location, &metadata_data)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub async fn find_latest_metadata_file(
    object_store: &Arc<dyn object_store::ObjectStore>,
    table_url: &Url,
) -> Result<String> {
    log::trace!("Finding latest metadata file");
    let base_path = crate::utils::url_to_object_path(table_url)?;
    let version_hint_path = base_path.clone().join("metadata").join("version-hint.text");
    let mut hinted_version: Option<i32> = None;
    let mut hinted_filename: Option<String> = None;
    if let Ok(version_hint_data) = object_store.get(&version_hint_path).await
        && let Ok(version_hint_bytes) = version_hint_data.bytes().await
        && let Ok(version_hint) = String::from_utf8(version_hint_bytes.to_vec())
    {
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

    log::trace!("Listing metadata directory");
    let metadata_prefix = base_path.join("metadata");

    let mut latest = None;
    let mut hinted = None;
    let objects = object_store.list(Some(&metadata_prefix));
    let result = if hinted_filename.is_none() && hinted_version.is_none() {
        visit_metadata_candidates(objects, |candidate| {
            update_latest_candidate(&mut latest, candidate);
        })
        .await
    } else {
        visit_metadata_candidates(objects, |candidate| {
            let matches_hint = hinted_filename
                .as_deref()
                .is_some_and(|filename| candidate.path.as_ref().ends_with(filename))
                || hinted_version.is_some_and(|version| candidate.version == version);
            if matches_hint {
                update_latest_candidate(&mut hinted, candidate.clone());
            }
            update_latest_candidate(&mut latest, candidate);
        })
        .await
    }
    .map_err(|error| DataFusionError::Plan(format!("Failed to list metadata directory: {error}")));
    result?;

    if let Some(candidate) = hinted {
        log::trace!(
            "find_latest_metadata_file: selected hinted version {} path={}",
            candidate.version,
            candidate.path
        );
        return Ok(candidate.path.to_string());
    }

    if let Some(candidate) = latest {
        log::trace!(
            "find_latest_metadata_file: selected version {} path={}",
            candidate.version,
            candidate.path
        );
        Ok(candidate.path.to_string())
    } else {
        plan_err!("No metadata files found in table location: {}", table_url)
    }
}

async fn visit_metadata_candidates(
    mut objects: futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>,
    mut visit: impl FnMut(MetadataCandidate),
) -> object_store::Result<()> {
    use futures::TryStreamExt;

    while let Some(object) = objects.try_next().await? {
        let Some(metadata_file) = object
            .location
            .filename()
            .and_then(parse_metadata_file_name)
        else {
            continue;
        };
        visit(MetadataCandidate {
            version: metadata_file.version,
            path: object.location,
            last_modified: object.last_modified,
        });
    }
    Ok(())
}

fn update_latest_candidate(current: &mut Option<MetadataCandidate>, candidate: MetadataCandidate) {
    if current
        .as_ref()
        .is_none_or(|current| candidate.is_newer_than(current))
    {
        *current = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{self, Write};
    use std::sync::Arc;

    use bytes::Bytes;
    use datafusion::common::Result;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use futures::executor::block_on;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use url::Url;

    use super::{
        MetadataFileCodec, MetadataFileName, decode_metadata_file, encode_metadata_file,
        find_latest_metadata_file, metadata_file_extension_from_properties,
        metadata_location_to_object_path, parse_metadata_file_name,
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
    #[expect(clippy::expect_used)]
    fn discovers_hinted_metadata_and_falls_back_to_latest() {
        block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            for path in [
                "events/metadata/00001-first.metadata.json",
                "events/metadata/00003-latest.metadata.json",
            ] {
                store
                    .put(&Path::from(path), PutPayload::from(Bytes::new()))
                    .await
                    .expect("seed metadata file");
            }
            let hint_path = Path::from("events/metadata/version-hint.text");
            let table_url = Url::parse("memory://warehouse/events").expect("table URL");

            store
                .put(&hint_path, PutPayload::from(Bytes::from_static(b"1")))
                .await
                .expect("seed numeric hint");
            assert_eq!(
                find_latest_metadata_file(&store, &table_url)
                    .await
                    .expect("discover numeric hint"),
                "events/metadata/00001-first.metadata.json"
            );

            store
                .put(&hint_path, PutPayload::from(Bytes::from_static(b"2")))
                .await
                .expect("seed missing hint");
            assert_eq!(
                find_latest_metadata_file(&store, &table_url)
                    .await
                    .expect("fall back to latest metadata"),
                "events/metadata/00003-latest.metadata.json"
            );

            store
                .put(
                    &hint_path,
                    PutPayload::from(Bytes::from_static(b"00001-first.metadata.json")),
                )
                .await
                .expect("seed filename hint");
            assert_eq!(
                find_latest_metadata_file(&store, &table_url)
                    .await
                    .expect("discover filename hint"),
                "events/metadata/00001-first.metadata.json"
            );
        });
    }

    #[test]
    fn parses_windows_drive_metadata_locations_as_object_paths() -> Result<()> {
        assert_eq!(
            metadata_location_to_object_path(
                "C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json",
            )?
            .as_ref(),
            "C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json"
        );
        assert_eq!(
            metadata_location_to_object_path(
                "file:///C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json",
            )?
            .as_ref(),
            "C:/Users/runneradmin/AppData/Local/Temp/iceberg_table/metadata/v1.metadata.json"
        );
        Ok(())
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

    #[test]
    fn encodes_gzip_metadata_files() -> io::Result<()> {
        let encoded =
            encode_metadata_file("metadata/v1.gz.metadata.json", br#"{"format-version":2}"#)?;
        assert_ne!(encoded, br#"{"format-version":2}"#);
        assert_eq!(
            decode_metadata_file("metadata/v1.gz.metadata.json", &encoded)?,
            br#"{"format-version":2}"#.to_vec()
        );
        assert_eq!(
            encode_metadata_file("metadata/v1.metadata.json", br#"{"format-version":2}"#)?,
            br#"{"format-version":2}"#.to_vec()
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn chooses_metadata_file_extension_from_properties() {
        assert_eq!(
            metadata_file_extension_from_properties(&HashMap::new()).unwrap(),
            ".metadata.json"
        );

        let mut properties = HashMap::new();
        properties.insert(
            "write.metadata.compression-codec".to_string(),
            "gzip".to_string(),
        );
        assert_eq!(
            metadata_file_extension_from_properties(&properties).unwrap(),
            ".gz.metadata.json"
        );

        properties.insert(
            "write.metadata.compression-codec".to_string(),
            "zstd".to_string(),
        );
        assert!(metadata_file_extension_from_properties(&properties).is_err());
    }
}
