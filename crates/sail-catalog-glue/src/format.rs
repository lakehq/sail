use sail_catalog::error::{CatalogError, CatalogResult};

/// Storage format information for Glue tables.
///
/// Maps user-friendly format names to Hive/Hadoop Java class names for InputFormat,
/// OutputFormat, and SerDe (Serializer/Deserializer).
///
/// See: <https://github.com/aws/aws-cdk/blob/main/packages/%40aws-cdk/aws-glue-alpha/lib/data-format.ts>
#[derive(Debug, Clone)]
pub struct GlueStorageFormat {
    pub input_format: &'static str,
    pub output_format: &'static str,
    pub serde_library: &'static str,
}

impl GlueStorageFormat {
    /// Returns the Glue storage format configuration for the given format name.
    pub fn from_format(format: &str) -> CatalogResult<Self> {
        let format_lower = format.trim().to_lowercase();

        match format_lower.as_str() {
            "parquet" => Ok(Self::parquet()),
            "csv" | "textfile" => Ok(Self::csv()),
            "json" => Ok(Self::json()),
            "orc" => Ok(Self::orc()),
            "avro" => Ok(Self::avro()),
            _ => Err(CatalogError::NotSupported(format!(
                "Storage format '{format}' is not supported by Glue catalog"
            ))),
        }
    }

    /// Parquet format configuration.
    pub fn parquet() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde_library: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        }
    }

    /// CSV format configuration.
    pub fn csv() -> Self {
        Self {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_library: "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        }
    }

    /// JSON format configuration.
    pub fn json() -> Self {
        Self {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_library: "org.openx.data.jsonserde.JsonSerDe",
        }
    }

    /// ORC format configuration.
    pub fn orc() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            serde_library: "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        }
    }

    /// Avro format configuration.
    pub fn avro() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            serde_library: "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        }
    }

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
