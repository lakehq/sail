use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType};
use serde::{Deserialize, Serialize};

use crate::geoarrow::projjson;

/// Raw GeoArrow extension metadata deserialized from JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GeoArrowMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edges: Option<GeoArrowEdges>,
    /// CRS can be a string ("OGC:CRS84", "EPSG:3857") or a PROJJSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crs: Option<GeoArrowCrs>,
}

/// Valid edge interpolation values per the GeoArrow spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GeoArrowEdges {
    Spherical,
    Vincenty,
    Thomas,
    Andoyer,
    Karney,
}

/// A CRS value as used in GeoArrow extension metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GeoArrowCrs {
    AuthorityCode(String),
    ProjJson(projjson::Crs),
}

impl GeoArrowCrs {
    pub fn authority_code(&self) -> String {
        match self {
            GeoArrowCrs::AuthorityCode(s) => s.clone(),
            GeoArrowCrs::ProjJson(crs) => crs.id.authority_code(),
        }
    }
}

/// GeoArrow `geoarrow.wkb` extension type for WKB-encoded geometries/geographies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeoArrowWkbType {
    pub metadata: GeoArrowMetadata,
}

impl GeoArrowWkbType {
    pub const NAME: &'static str = "geoarrow.wkb";
}

impl ExtensionType for GeoArrowWkbType {
    const NAME: &'static str = GeoArrowWkbType::NAME;

    type Metadata = GeoArrowMetadata;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn serialize_metadata(&self) -> Option<String> {
        let value = serde_json::to_value(self.metadata()).ok()?;
        serde_json::to_string(&value).ok()
    }

    fn deserialize_metadata(metadata: Option<&str>) -> Result<Self::Metadata, ArrowError> {
        match metadata {
            None => Ok(GeoArrowMetadata::default()),
            Some(s) if s.trim().is_empty() => Ok(GeoArrowMetadata::default()),
            Some(s) => serde_json::from_str::<GeoArrowMetadata>(s)
                .map_err(|e| ArrowError::JsonError(e.to_string())),
        }
    }

    fn supports_data_type(&self, data_type: &DataType) -> Result<(), ArrowError> {
        match data_type {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok(()),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "{name} data type mismatch, expected binary storage type, found {data_type}",
                name = Self::NAME
            ))),
        }
    }

    fn try_new(data_type: &DataType, metadata: Self::Metadata) -> Result<Self, ArrowError> {
        let ext = Self { metadata };
        ext.supports_data_type(data_type)?;
        Ok(ext)
    }
}
