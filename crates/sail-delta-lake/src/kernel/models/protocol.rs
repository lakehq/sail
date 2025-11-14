#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::iter;

use delta_kernel::actions::Protocol;
use delta_kernel::schema::StructType;
use delta_kernel::table_features::TableFeature;
use deltalake::errors::{DeltaResult, DeltaTableError};
use deltalake::kernel::StructTypeExt as _;
use deltalake::table::config::TableProperty;
use serde::{Deserialize, Serialize};

use super::contains_timestampntz;

pub trait ProtocolExt {
    fn reader_features_set(&self) -> Option<HashSet<TableFeature>>;
    fn writer_features_set(&self) -> Option<HashSet<TableFeature>>;
    fn append_reader_features(self, reader_features: &[TableFeature]) -> DeltaResult<Protocol>;
    fn append_writer_features(self, writer_features: &[TableFeature]) -> DeltaResult<Protocol>;
    fn move_table_properties_into_features(
        self,
        configuration: &HashMap<String, String>,
    ) -> DeltaResult<Protocol>;
    fn apply_column_metadata_to_protocol(self, schema: &StructType) -> DeltaResult<Protocol>;
    fn apply_properties_to_protocol(
        self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Protocol>;
}

impl ProtocolExt for Protocol {
    fn reader_features_set(&self) -> Option<HashSet<TableFeature>> {
        self.reader_features()
            .map(|features| features.iter().cloned().collect())
    }

    fn writer_features_set(&self) -> Option<HashSet<TableFeature>> {
        self.writer_features()
            .map(|features| features.iter().cloned().collect())
    }

    fn append_reader_features(self, reader_features: &[TableFeature]) -> DeltaResult<Protocol> {
        let mut inner = ProtocolInner::from_kernel(&self)?;
        inner = inner.append_reader_features(reader_features.iter().cloned());
        inner.as_kernel()
    }

    fn append_writer_features(self, writer_features: &[TableFeature]) -> DeltaResult<Protocol> {
        let mut inner = ProtocolInner::from_kernel(&self)?;
        inner = inner.append_writer_features(writer_features.iter().cloned());
        inner.as_kernel()
    }

    fn move_table_properties_into_features(
        self,
        configuration: &HashMap<String, String>,
    ) -> DeltaResult<Protocol> {
        let inner =
            ProtocolInner::from_kernel(&self)?.move_table_properties_into_features(configuration);
        inner.as_kernel()
    }

    fn apply_column_metadata_to_protocol(self, schema: &StructType) -> DeltaResult<Protocol> {
        let inner = ProtocolInner::from_kernel(&self)?.apply_column_metadata_to_protocol(schema)?;
        inner.as_kernel()
    }

    fn apply_properties_to_protocol(
        self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Protocol> {
        let inner = ProtocolInner::from_kernel(&self)?
            .apply_properties_to_protocol(new_properties, raise_if_not_exists)?;
        inner.as_kernel()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ProtocolInner {
    min_reader_version: i32,
    min_writer_version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<HashSet<TableFeature>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<HashSet<TableFeature>>,
}

impl Default for ProtocolInner {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        }
    }
}

impl ProtocolInner {
    fn new(min_reader_version: i32, min_writer_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features: None,
            writer_features: None,
        }
    }

    fn from_kernel(value: &Protocol) -> DeltaResult<Self> {
        let json = serde_json::to_value(value)?;
        Ok(serde_json::from_value(json)?)
    }

    fn as_kernel(&self) -> DeltaResult<Protocol> {
        let json = serde_json::to_value(self)?;
        Ok(serde_json::from_value(json)?)
    }

    fn append_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = TableFeature>,
    ) -> Self {
        let new_features: HashSet<_> = reader_features.into_iter().collect();
        if new_features.is_empty() {
            return self;
        }
        self.min_reader_version = self.min_reader_version.max(3);
        match self.reader_features.as_mut() {
            Some(existing) => existing.extend(new_features),
            None => self.reader_features = Some(new_features),
        };
        self
    }

    fn append_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = TableFeature>,
    ) -> Self {
        let new_features: HashSet<_> = writer_features.into_iter().collect();
        if new_features.is_empty() {
            return self;
        }
        self.min_writer_version = self.min_writer_version.max(7);
        match self.writer_features.as_mut() {
            Some(existing) => existing.extend(new_features),
            None => self.writer_features = Some(new_features),
        };
        self
    }

    fn move_table_properties_into_features(
        mut self,
        configuration: &HashMap<String, String>,
    ) -> Self {
        fn parse_bool(value: &str) -> bool {
            value.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v)
        }

        if self.min_writer_version >= 7 {
            let mut converted_writer_features: HashSet<TableFeature> = configuration
                .iter()
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableChangeDataFeed" if parse_bool(value) => {
                        Some(TableFeature::ChangeDataFeed)
                    }
                    "delta.appendOnly" if parse_bool(value) => Some(TableFeature::AppendOnly),
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(TableFeature::DeletionVectors)
                    }
                    "delta.enableRowTracking" if parse_bool(value) => {
                        Some(TableFeature::RowTracking)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(TableFeature::V2Checkpoint),
                    _ => None,
                })
                .collect();

            if configuration
                .keys()
                .any(|key| key.starts_with("delta.constraints."))
            {
                converted_writer_features.insert(TableFeature::CheckConstraints);
            }

            match self.writer_features.as_mut() {
                Some(features) => features.extend(converted_writer_features),
                None => self.writer_features = Some(converted_writer_features),
            }
        }

        if self.min_reader_version >= 3 {
            let converted_reader_features: HashSet<TableFeature> = configuration
                .iter()
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(TableFeature::DeletionVectors)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(TableFeature::V2Checkpoint),
                    _ => None,
                })
                .collect();

            match self.reader_features.as_mut() {
                Some(features) => features.extend(converted_reader_features),
                None => self.reader_features = Some(converted_reader_features),
            }
        }
        self
    }

    fn apply_column_metadata_to_protocol(mut self, schema: &StructType) -> DeltaResult<Self> {
        let generated_cols = schema.get_generated_columns()?;
        let invariants = schema.get_invariants()?;
        let has_timestamp_ntz = contains_timestampntz(schema.fields());

        if has_timestamp_ntz {
            self = self.enable_timestamp_ntz();
        }

        if !generated_cols.is_empty() {
            self = self.enable_generated_columns();
        }

        if !invariants.is_empty() {
            self = self.enable_invariants();
        }

        Ok(self)
    }

    fn apply_properties_to_protocol(
        mut self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Self> {
        let mut parsed_properties: HashMap<TableProperty, String> = HashMap::new();

        for (key, value) in new_properties {
            if let Ok(parsed) = key.parse::<TableProperty>() {
                parsed_properties.insert(parsed, value.to_string());
            } else if raise_if_not_exists {
                return Err(DeltaTableError::generic(format!(
                    "Error parsing property '{key}':'{value}'",
                )));
            }
        }

        if let Some(min_reader_version) = parsed_properties.get(&TableProperty::MinReaderVersion) {
            match min_reader_version.parse::<i32>() {
                Ok(version @ 1..=3) => {
                    if version > self.min_reader_version {
                        self.min_reader_version = version;
                    }
                }
                _ => {
                    return Err(DeltaTableError::generic(format!(
                        "delta.minReaderVersion = '{min_reader_version}' is invalid, valid values are ['1','2','3']"
                    )));
                }
            }
        }

        if let Some(min_writer_version) = parsed_properties.get(&TableProperty::MinWriterVersion) {
            match min_writer_version.parse::<i32>() {
                Ok(version @ 2..=7) => {
                    if version > self.min_writer_version {
                        self.min_writer_version = version;
                    }
                }
                _ => {
                    return Err(DeltaTableError::generic(format!(
                        "delta.minWriterVersion = '{min_writer_version}' is invalid, valid values are ['2','3','4','5','6','7']"
                    )));
                }
            }
        }

        if let Some(enable_cdf) = parsed_properties.get(&TableProperty::EnableChangeDataFeed) {
            match enable_cdf.to_ascii_lowercase().parse::<bool>() {
                Ok(true) => {
                    if self.min_writer_version >= 7 {
                        self =
                            self.append_writer_features(iter::once(TableFeature::ChangeDataFeed));
                    } else if self.min_writer_version <= 3 {
                        self.min_writer_version = 4;
                    }
                }
                Ok(false) => {}
                Err(_) => {
                    return Err(DeltaTableError::generic(format!(
                        "delta.enableChangeDataFeed = '{enable_cdf}' is invalid, valid values are ['true']"
                    )));
                }
            }
        }

        if let Some(enable_dv) = parsed_properties.get(&TableProperty::EnableDeletionVectors) {
            match enable_dv.to_ascii_lowercase().parse::<bool>() {
                Ok(true) => {
                    let writer_features = self.writer_features.get_or_insert_with(HashSet::new);
                    writer_features.insert(TableFeature::DeletionVectors);

                    let reader_features = self.reader_features.get_or_insert_with(HashSet::new);
                    reader_features.insert(TableFeature::DeletionVectors);

                    self.min_reader_version = self.min_reader_version.max(3);
                    self.min_writer_version = self.min_writer_version.max(7);
                }
                Ok(false) => {}
                Err(_) => {
                    return Err(DeltaTableError::generic(format!(
                        "delta.enableDeletionVectors = '{enable_dv}' is invalid, valid values are ['true']"
                    )));
                }
            }
        }

        Ok(self)
    }

    fn enable_timestamp_ntz(mut self) -> Self {
        self = self.append_reader_features(iter::once(TableFeature::TimestampWithoutTimezone));
        self = self.append_writer_features(iter::once(TableFeature::TimestampWithoutTimezone));
        self
    }

    fn enable_generated_columns(mut self) -> Self {
        if self.min_writer_version < 4 {
            self.min_writer_version = 4;
        }
        if self.min_writer_version >= 7 {
            self = self.append_writer_features(iter::once(TableFeature::GeneratedColumns));
        }
        self
    }

    fn enable_invariants(mut self) -> Self {
        if self.min_writer_version >= 7 {
            self = self.append_writer_features(iter::once(TableFeature::Invariants));
        }
        self
    }
}
