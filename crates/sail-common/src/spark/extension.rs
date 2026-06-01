use arrow_schema::extension::{
    ExtensionType, EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkIntervalMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_field: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_field: Option<i32>,
}

impl SparkIntervalMetadata {
    pub fn new(start_field: Option<i32>, end_field: Option<i32>) -> Self {
        Self {
            start_field,
            end_field,
        }
    }

    pub fn arrow_metadata(&self, extension_type_name: &str) -> Vec<(String, String)> {
        vec![
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                extension_type_name.to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                self.serialize_metadata(),
            ),
        ]
    }

    fn serialize_metadata(&self) -> String {
        match (self.start_field, self.end_field) {
            (Some(start), Some(end)) => format!(r#"{{"startField":{start},"endField":{end}}}"#),
            (Some(start), None) => format!(r#"{{"startField":{start}}}"#),
            (None, Some(end)) => format!(r#"{{"endField":{end}}}"#),
            (None, None) => "{}".to_string(),
        }
    }

    fn validate_range(&self, name: &str, min: i32, max: i32) -> Result<(), ArrowError> {
        for (field_name, value) in [
            ("startField", self.start_field),
            ("endField", self.end_field),
        ] {
            if let Some(value) = value {
                if !(min..=max).contains(&value) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "{name} {field_name} must be between {min} and {max}, found {value}"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkDayTimeIntervalType {
    pub metadata: SparkIntervalMetadata,
}

impl SparkDayTimeIntervalType {
    pub const NAME: &'static str = "sail.spark.dayTime.interval";
}

impl ExtensionType for SparkDayTimeIntervalType {
    const NAME: &'static str = SparkDayTimeIntervalType::NAME;

    type Metadata = SparkIntervalMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(self.metadata.serialize_metadata())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        match metadata {
            None => Ok(SparkIntervalMetadata::default()),
            Some(s) if s.trim().is_empty() => Ok(SparkIntervalMetadata::default()),
            Some(s) => serde_json::from_str::<SparkIntervalMetadata>(s)
                .map_err(|e| ArrowError::JsonError(e.to_string())),
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Duration(TimeUnit::Microsecond) => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "{name} data type mismatch, expected duration[us] storage type, found {data_type}",
                name = Self::NAME
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let ext = Self { metadata };
        ext.supports_data_type(data_type)?;
        ext.metadata.validate_range(Self::NAME, 0, 3)?;
        Ok(ext)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkYearMonthIntervalType {
    pub metadata: SparkIntervalMetadata,
}

impl SparkYearMonthIntervalType {
    pub const NAME: &'static str = "sail.spark.yearMonth.interval";
}

impl ExtensionType for SparkYearMonthIntervalType {
    const NAME: &'static str = SparkYearMonthIntervalType::NAME;

    type Metadata = SparkIntervalMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        Some(self.metadata.serialize_metadata())
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        match metadata {
            None => Ok(SparkIntervalMetadata::default()),
            Some(s) if s.trim().is_empty() => Ok(SparkIntervalMetadata::default()),
            Some(s) => serde_json::from_str::<SparkIntervalMetadata>(s)
                .map_err(|e| ArrowError::JsonError(e.to_string())),
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "{name} data type mismatch, expected year-month interval storage type, found {data_type}",
                name = Self::NAME
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let ext = Self { metadata };
        ext.supports_data_type(data_type)?;
        ext.metadata.validate_range(Self::NAME, 0, 1)?;
        Ok(ext)
    }
}
