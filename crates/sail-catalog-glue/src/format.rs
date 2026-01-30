/// Storage format detection for Glue tables.
pub struct GlueStorageFormat;

impl GlueStorageFormat {
    /// Attempts to detect the format from Glue storage descriptor properties.
    pub fn detect_format_from_serde(serde_library: Option<&str>) -> String {
        match serde_library {
            Some(lib) if lib.contains("parquet") || lib.contains("Parquet") => {
                "parquet".to_string()
            }
            Some(lib) if lib.contains("orc") || lib.contains("Orc") => "orc".to_string(),
            Some(lib) if lib.contains("avro") || lib.contains("Avro") => "avro".to_string(),
            Some(lib) if lib.contains("json") || lib.contains("Json") => "json".to_string(),
            Some(lib) if lib.contains("CSV") || lib.contains("csv") => "csv".to_string(),
            Some(lib) if lib.contains("LazySimpleSerde") => "textfile".to_string(),
            _ => "unknown".to_string(),
        }
    }
}
