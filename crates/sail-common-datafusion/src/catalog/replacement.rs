use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

use super::{CatalogPartitionField, CatalogTableSort};

/// A staged catalog definition, not a data-overwrite option. It contains no live reservation
/// or credentials and may cross the physical-plan codec boundary.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct TableReplacement {
    pub catalog_table: Vec<String>,
    pub catalog_generation: u64,
    pub database_generation: u64,
    pub table_generation: u64,
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
