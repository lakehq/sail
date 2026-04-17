use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::JsonOptions;
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::gen::{
    JsonReadOptions, JsonReadPartialOptions, JsonWriteOptions, JsonWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};

impl BuildPartialOptions<JsonReadPartialOptions> for JsonOptions {
    fn build_partial_options(self) -> DataSourceResult<JsonReadPartialOptions> {
        Ok(JsonReadPartialOptions {
            schema_infer_max_records: self.schema_infer_max_rec,
            compression: Some(self.compression.to_string()),
        })
    }
}

impl BuildPartialOptions<JsonWritePartialOptions> for JsonOptions {
    fn build_partial_options(self) -> DataSourceResult<JsonWritePartialOptions> {
        Ok(JsonWritePartialOptions {
            compression: Some(self.compression.to_string()),
        })
    }
}

impl JsonReadOptions {
    pub fn into_table_options(self) -> DataSourceResult<JsonOptions> {
        let JsonReadOptions {
            schema_infer_max_records,
            compression,
        } = self;
        let compression = FileCompressionType::from_str(&compression)
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: compression.to_string(),
                cause: Some(e.to_string()),
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
                value: compression.to_string(),
                cause: Some(e.to_string()),
            })?
            .into();
        Ok(JsonOptions {
            compression,
            ..JsonOptions::default()
        })
    }
}

pub fn resolve_json_read_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<JsonReadOptions> {
    let mut partial = JsonReadPartialOptions::initialize();
    partial.merge(ctx.default_table_options().json.build_partial_options()?);
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_json_write_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<JsonWriteOptions> {
    let mut partial = JsonWritePartialOptions::initialize();
    partial.merge(ctx.default_table_options().json.build_partial_options()?);
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::json::options::{resolve_json_read_options, resolve_json_write_options};
    use crate::options::option_list;

    #[test]
    fn test_resolve_json_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[
            ("schema_infer_max_records", "100"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_json_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[("compression", "bzip2")]);
        let options = resolve_json_write_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
