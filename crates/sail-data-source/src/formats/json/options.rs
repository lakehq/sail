use std::collections::HashMap;
use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::JsonOptions;
use datafusion_common::Result;
use datafusion_datasource::file_compression_type::FileCompressionType;

use crate::options::{load_default_options, load_options, JsonReadOptions, JsonWriteOptions};

fn apply_json_read_options(from: JsonReadOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonReadOptions {
        schema_infer_max_records,
        compression,
    } = from;
    if let Some(v) = schema_infer_max_records {
        to.schema_infer_max_rec = Some(v);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

fn apply_json_write_options(from: JsonWriteOptions, to: &mut JsonOptions) -> Result<()> {
    let JsonWriteOptions { compression } = from;
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

pub fn resolve_json_read_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> Result<JsonOptions> {
    let mut json_options = ctx.default_table_options().json;
    apply_json_read_options(load_default_options()?, &mut json_options)?;
    for opt in options {
        apply_json_read_options(load_options(opt)?, &mut json_options)?;
    }
    Ok(json_options)
}

pub fn resolve_json_write_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> Result<JsonOptions> {
    let mut json_options = ctx.default_table_options().json;
    apply_json_write_options(load_default_options()?, &mut json_options)?;
    for opt in options {
        apply_json_write_options(load_options(opt)?, &mut json_options)?;
    }
    Ok(json_options)
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::json::options::{resolve_json_read_options, resolve_json_write_options};
    use crate::options::build_options;

    #[test]
    fn test_resolve_json_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_options(&[
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

        let kv = build_options(&[("compression", "bzip2")]);
        let options = resolve_json_write_options(&state, vec![kv])?;
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
