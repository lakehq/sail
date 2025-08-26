use std::fmt;
use std::sync::Arc;

use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::log_segment::{ListedLogFiles, LogSegment};
use delta_kernel::path::ParsedLogPath;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::FileMeta;
use deltalake::DeltaTableConfig;
use itertools::Itertools;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use url::Url;

use super::{EagerSnapshot, Snapshot};

impl Serialize for Snapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ascending_commit_files = self
            .inner
            .log_segment()
            .ascending_commit_files
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let ascending_compaction_files = self
            .inner
            .log_segment()
            .ascending_compaction_files
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let checkpoint_parts = self
            .inner
            .log_segment()
            .checkpoint_parts
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let latest_crc_file = self
            .inner
            .log_segment()
            .latest_crc_file
            .as_ref()
            .map(|f| FileMetaSerde::from(&f.location));

        let mut seq = serializer.serialize_seq(None)?;

        seq.serialize_element(&self.version())?;
        seq.serialize_element(&self.table_url)?;
        seq.serialize_element(&self.protocol())?;
        seq.serialize_element(&self.metadata())?;
        seq.serialize_element(&ascending_commit_files)?;
        seq.serialize_element(&ascending_compaction_files)?;
        seq.serialize_element(&checkpoint_parts)?;
        seq.serialize_element(&latest_crc_file)?;

        seq.serialize_element(&self.config)?;

        seq.end()
    }
}

struct SnapshotVisitor;

#[derive(Serialize, Deserialize)]
struct FileMetaSerde {
    /// The fully qualified path to the object
    pub location: Url,
    /// The last modified time as milliseconds since unix epoch
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: u64,
}

impl FileMetaSerde {
    fn into_kernel(self) -> FileMeta {
        FileMeta {
            location: self.location,
            last_modified: self.last_modified,
            size: self.size,
        }
    }
}

impl From<&FileMeta> for FileMetaSerde {
    fn from(file_meta: &FileMeta) -> Self {
        FileMetaSerde {
            location: file_meta.location.clone(),
            last_modified: file_meta.last_modified,
            size: file_meta.size,
        }
    }
}

impl<'de> Visitor<'de> for SnapshotVisitor {
    type Value = Snapshot;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct Snapshot")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Snapshot, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let version: i64 = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let table_url: Url = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let protocol: Protocol = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
        let metadata: Metadata = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(3, &self))?;
        let ascending_commit_files: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(4, &self))?;
        let ascending_compaction_files: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(5, &self))?;
        let checkpoint_parts: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(6, &self))?;
        let latest_crc_file: Option<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(7, &self))?;
        let config: DeltaTableConfig = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(8, &self))?;

        let ascending_commit_files = ascending_commit_files
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let ascending_compaction_files = ascending_compaction_files
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let checkpoint_parts = checkpoint_parts
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let latest_crc_file = latest_crc_file
            .map(|meta| ParsedLogPath::try_from(meta.into_kernel()).map_err(de::Error::custom))
            .transpose()?
            .flatten();

        let listed_log_files = ListedLogFiles::new(
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
        );

        let log_root = if !table_url.path().ends_with("/") {
            let mut aux = table_url.clone();
            aux.set_path(&format!("{}/", table_url.path()));
            aux.join("_delta_log/").map_err(de::Error::custom)?
        } else {
            table_url.join("_delta_log/").map_err(de::Error::custom)?
        };

        let log_segment = LogSegment::try_new(listed_log_files, log_root, Some(version as u64))
            .map_err(de::Error::custom)?;

        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url.clone(), version as u64)
                .map_err(de::Error::custom)?;

        let snapshot = KernelSnapshot::new(log_segment, table_configuration);
        let schema = snapshot
            .metadata()
            .parse_schema()
            .map_err(de::Error::custom)?;

        Ok(Snapshot {
            inner: Arc::new(snapshot),
            table_url,
            schema: Arc::new(schema),
            config,
        })
    }
}

impl<'de> Deserialize<'de> for Snapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(SnapshotVisitor)
    }
}

impl Serialize for EagerSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.snapshot)?;

        let mut buffer = vec![];
        let mut writer = FileWriter::try_new(&mut buffer, self.files.schema().as_ref())
            .map_err(serde::ser::Error::custom)?;
        writer
            .write(&self.files)
            .map_err(serde::ser::Error::custom)?;
        writer.finish().map_err(serde::ser::Error::custom)?;
        let data = writer.into_inner().map_err(serde::ser::Error::custom)?;
        seq.serialize_element(&data)?;

        seq.end()
    }
}

// Deserialize the eager snapshot
struct EagerSnapshotVisitor;

impl<'de> Visitor<'de> for EagerSnapshotVisitor {
    type Value = EagerSnapshot;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct EagerSnapshot")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<EagerSnapshot, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let snapshot = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;

        let Some(elem) = seq.next_element::<Vec<u8>>()? else {
            return Err(de::Error::invalid_length(3, &self));
        };

        let mut reader = FileReader::try_new(std::io::Cursor::new(elem), None)
            .map_err(|e| de::Error::custom(format!("failed to read ipc record batch: {e}")))?;
        let files = reader
            .next()
            .ok_or(de::Error::custom("missing ipc data"))?
            .map_err(|e| de::Error::custom(format!("failed to read ipc record batch: {e}")))?;

        Ok(EagerSnapshot { snapshot, files })
    }
}

impl<'de> Deserialize<'de> for EagerSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<EagerSnapshot, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(EagerSnapshotVisitor)
    }
}
