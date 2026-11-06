use crate::extension::SessionExtension;

/// Session-level Parquet read settings that have no DataFusion counterpart.
#[derive(Debug, Default)]
pub struct ParquetReadConfig {
    trust_sorting_columns: bool,
}

impl ParquetReadConfig {
    pub fn new(trust_sorting_columns: bool) -> Self {
        Self {
            trust_sorting_columns,
        }
    }

    pub fn trust_sorting_columns(&self) -> bool {
        self.trust_sorting_columns
    }
}

impl SessionExtension for ParquetReadConfig {
    fn name() -> &'static str {
        "ParquetReadConfig"
    }
}
