use crate::error::{CatalogError, CatalogResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HiveStorageFormat {
    pub input_format: &'static str,
    pub output_format: &'static str,
    pub serde_library: &'static str,
}

impl HiveStorageFormat {
    pub fn from_format(format: &str) -> CatalogResult<Self> {
        match format.trim().to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::parquet()),
            "csv" => Ok(Self::csv()),
            "textfile" => Ok(Self::textfile()),
            "json" => Ok(Self::json()),
            "orc" => Ok(Self::orc()),
            "avro" => Ok(Self::avro()),
            other => Err(CatalogError::NotSupported(format!(
                "Storage format '{other}' is not supported by Hive-style catalogs"
            ))),
        }
    }

    pub fn parquet() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde_library: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        }
    }

    pub fn csv() -> Self {
        Self {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_library: "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        }
    }

    pub fn textfile() -> Self {
        Self {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_library: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        }
    }

    pub fn json() -> Self {
        Self {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_library: "org.openx.data.jsonserde.JsonSerDe",
        }
    }

    pub fn orc() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            serde_library: "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        }
    }

    pub fn avro() -> Self {
        Self {
            input_format: "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            serde_library: "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HiveCatalogFormat {
    pub logical_format: &'static str,
    pub storage_format: HiveStorageFormat,
}

impl HiveCatalogFormat {
    pub fn from_format(format: &str) -> CatalogResult<Self> {
        match format.trim().to_ascii_lowercase().as_str() {
            "delta" | "deltalake" => Ok(Self {
                logical_format: "delta",
                storage_format: HiveStorageFormat::parquet(),
            }),
            "parquet" => Ok(Self {
                logical_format: "parquet",
                storage_format: HiveStorageFormat::parquet(),
            }),
            "csv" => Ok(Self {
                logical_format: "csv",
                storage_format: HiveStorageFormat::csv(),
            }),
            "textfile" => Ok(Self {
                logical_format: "textfile",
                storage_format: HiveStorageFormat::textfile(),
            }),
            "json" => Ok(Self {
                logical_format: "json",
                storage_format: HiveStorageFormat::json(),
            }),
            "orc" => Ok(Self {
                logical_format: "orc",
                storage_format: HiveStorageFormat::orc(),
            }),
            "avro" => Ok(Self {
                logical_format: "avro",
                storage_format: HiveStorageFormat::avro(),
            }),
            other => Err(CatalogError::NotSupported(format!(
                "Storage format '{other}' is not supported by Hive-style catalogs"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveDetectedFormat {
    Parquet,
    Csv,
    TextFile,
    Json,
    Orc,
    Avro,
    Unknown,
}

impl HiveDetectedFormat {
    pub fn detect(
        serde_library: Option<&str>,
        input_format: Option<&str>,
        output_format: Option<&str>,
    ) -> Self {
        let serde_library = serde_library.unwrap_or_default();
        let input_format = input_format.unwrap_or_default();
        let output_format = output_format.unwrap_or_default();
        let haystack = format!("{serde_library} {input_format} {output_format}");

        if haystack.contains("Parquet") || haystack.contains("parquet") {
            Self::Parquet
        } else if haystack.contains("JsonSerDe") || haystack.contains("json") {
            Self::Json
        } else if haystack.contains("Orc") || haystack.contains("orc") {
            Self::Orc
        } else if haystack.contains("Avro") || haystack.contains("avro") {
            Self::Avro
        } else if haystack.contains("OpenCSVSerde") {
            Self::Csv
        } else if haystack.contains("LazySimpleSerDe")
            || haystack.contains("LazySimpleSerde")
            || haystack.contains("TextInputFormat")
        {
            Self::TextFile
        } else {
            Self::Unknown
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::TextFile => "textfile",
            Self::Json => "json",
            Self::Orc => "orc",
            Self::Avro => "avro",
            Self::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use super::{HiveCatalogFormat, HiveDetectedFormat, HiveStorageFormat};

    #[test]
    fn test_textfile_storage_format_uses_lazy_simple_serde() {
        let format = HiveStorageFormat::textfile();
        assert_eq!(
            format.input_format,
            "org.apache.hadoop.mapred.TextInputFormat"
        );
        assert_eq!(
            format.output_format,
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        );
        assert_eq!(
            format.serde_library,
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        );
    }

    #[test]
    fn test_detect_textfile_from_lazy_simple_serde() {
        let detected = HiveDetectedFormat::detect(
            Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            Some("org.apache.hadoop.mapred.TextInputFormat"),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
        );
        assert_eq!(detected, HiveDetectedFormat::TextFile);
    }

    #[test]
    fn test_detect_csv_from_open_csv_serde() {
        let detected = HiveDetectedFormat::detect(
            Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
            Some("org.apache.hadoop.mapred.TextInputFormat"),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
        );
        assert_eq!(detected, HiveDetectedFormat::Csv);
    }

    #[test]
    fn test_detect_json_before_text_fallback() {
        let detected = HiveDetectedFormat::detect(
            Some("org.openx.data.jsonserde.JsonSerDe"),
            Some("org.apache.hadoop.mapred.TextInputFormat"),
            Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
        );
        assert_eq!(detected, HiveDetectedFormat::Json);
    }

    #[test]
    fn test_catalog_format_maps_delta_to_parquet_storage() {
        let format = HiveCatalogFormat::from_format("deltalake").unwrap();
        assert_eq!(format.logical_format, "delta");
        assert_eq!(format.storage_format, HiveStorageFormat::parquet());
    }

    #[test]
    fn test_catalog_format_maps_textfile_to_textfile_storage() {
        let format = HiveCatalogFormat::from_format("textfile").unwrap();
        assert_eq!(format.logical_format, "textfile");
        assert_eq!(format.storage_format, HiveStorageFormat::textfile());
    }
}
