use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

use super::{CatalogPartitionField, CatalogTableSort, LakehouseExecutionContext};

/// A staged catalog definition, not a data-overwrite option. It contains no live reservation
/// or credentials and may cross the physical-plan codec boundary.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct TableReplacement {
    pub catalog_table: Vec<String>,
    pub authority: ReplacementAuthority,
    pub require_existing: bool,
    pub existing: bool,
    pub columns: Vec<(String, DataType, bool)>,
    pub location: String,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_by: Vec<CatalogTableSort>,
    /// Original catalog bookkeeping at staging; replaced with authoritative effective
    /// properties by format preparation before the definition is published.
    pub properties: Vec<(String, String)>,
    /// Only properties explicitly supplied by this replacement request.
    pub property_updates: Vec<(String, String)>,
    pub is_external: bool,
}

/// Publication authority captured on the driver before any replacement data is written.
/// REST metadata is format JSON only, never the load response's access configuration.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "authority", rename_all = "kebab-case")]
pub enum ReplacementAuthority {
    Path {
        catalog_generation: u64,
        database_generation: u64,
        table_generation: u64,
    },
    IcebergRestExisting {
        metadata_json: String,
        metadata_location: String,
        execution: LakehouseExecutionContext,
    },
    IcebergRestStagedCreate {
        metadata_json: String,
        execution: LakehouseExecutionContext,
    },
}

impl ReplacementAuthority {
    pub fn rest_metadata(&self) -> Option<&str> {
        match self {
            Self::Path { .. } => None,
            Self::IcebergRestExisting { metadata_json, .. }
            | Self::IcebergRestStagedCreate { metadata_json, .. } => Some(metadata_json),
        }
    }

    pub fn execution(&self) -> Option<&LakehouseExecutionContext> {
        match self {
            Self::Path { .. } => None,
            Self::IcebergRestExisting { execution, .. }
            | Self::IcebergRestStagedCreate { execution, .. } => Some(execution),
        }
    }
}
