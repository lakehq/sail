use std::str::FromStr;

use datafusion_common::config::JsonOptions;
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::gen::{
    JsonReadOptions, JsonReadPartialOptions, JsonWriteOptions, JsonWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};

impl JsonReadOptions {
    pub fn into_table_options(self) -> DataSourceResult<JsonOptions> {
        let JsonReadOptions {
            schema_infer_max_records,
            compression,
        } = self;
        let compression = FileCompressionType::from_str(&compression)
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: e.to_string(),
            })?
            .into();
        Ok(JsonOptions {
            schema_infer_max_rec: Some(schema_infer_max_records),
            compression,
            ..JsonOptions::default()
        })
    }
}

impl JsonWriteOptions {
    pub fn into_table_options(self) -> DataSourceResult<JsonOptions> {
        let JsonWriteOptions { compression } = self;
        let compression = FileCompressionType::from_str(&compression)
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: e.to_string(),
            })?
            .into();
        Ok(JsonOptions {
            compression,
            ..JsonOptions::default()
        })
    }
}

pub fn resolve_json_read_options(options: Vec<OptionLayer>) -> DataSourceResult<JsonOptions> {
    let mut partial = JsonReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()?.into_table_options()
}

pub fn resolve_json_write_options(options: Vec<OptionLayer>) -> DataSourceResult<JsonOptions> {
    let mut partial = JsonWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()?.into_table_options()
}

#[cfg(test)]
mod tests {
    use datafusion_common::parsers::CompressionTypeVariant;
    use sail_common_datafusion::datasource::OptionLayer;

    use crate::formats::json::options::{resolve_json_read_options, resolve_json_write_options};

    fn option_list(items: &[(&str, &str)]) -> OptionLayer {
        OptionLayer::OptionList {
            items: items
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_resolve_json_read_options() -> datafusion_common::Result<()> {
        let kv = option_list(&[
            ("schema_infer_max_records", "100"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_json_read_options(vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> datafusion_common::Result<()> {
        let kv = option_list(&[("compression", "bzip2")]);
        let options = resolve_json_write_options(vec![kv])
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
