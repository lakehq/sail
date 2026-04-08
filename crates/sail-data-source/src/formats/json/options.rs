use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::JsonOptions;
use datafusion_common::Result;
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::OptionLayer;

use crate::options::gen::{
    JsonReadOptions, JsonReadPartialOptions, JsonWriteOptions, JsonWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};

fn apply_json_read_options(from: JsonReadOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonReadOptions {
        schema_infer_max_records,
        compression,
    } = from;
    to.schema_infer_max_rec = Some(schema_infer_max_records);
    to.compression = FileCompressionType::from_str(&compression)?.into();
    Ok(())
}

fn apply_json_write_options(from: JsonWriteOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonWriteOptions { compression } = from;
    to.compression = FileCompressionType::from_str(&compression)?.into();
    Ok(())
}

pub fn resolve_json_read_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> Result<JsonOptions> {
    let mut partial = JsonReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut json_options = ctx.default_table_options().json;
    apply_json_read_options(opts, &mut json_options)?;
    Ok(json_options)
}

pub fn resolve_json_write_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> Result<JsonOptions> {
    let mut partial = JsonWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut json_options = ctx.default_table_options().json;
    apply_json_write_options(opts, &mut json_options)?;
    Ok(json_options)
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::json::options::{resolve_json_read_options, resolve_json_write_options};
    use crate::options::build_option_layer;

    #[test]
    fn test_resolve_json_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_option_layer(&[
            ("schema_infer_max_records", "100"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_json_read_options(&state, vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }

    #[test]
    fn test_resolve_json_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_option_layer(&[("compression", "bzip2")]);
        let options = resolve_json_write_options(&state, vec![kv])?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
