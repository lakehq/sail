use datafusion_common::parsers::CompressionTypeVariant;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::formats::text::TableTextOptions;
use crate::options::gen::{
    TextReadOptions, TextReadPartialOptions, TextWriteOptions, TextWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};
use crate::utils::char_to_u8;

impl TextReadOptions {
    pub fn into_table_options(self) -> DataSourceResult<TableTextOptions> {
        let TextReadOptions {
            whole_text,
            line_sep,
        } = self;
        // Validate that line_sep (if set) is a valid ASCII byte character
        if let Some(c) = line_sep {
            char_to_u8(c, "line_sep").map_err(|e| DataSourceError::InvalidOption {
                key: "line_sep".to_string(),
                value: c.to_string(),
                cause: Some(e.to_string()),
            })?;
        }
        Ok(TableTextOptions {
            whole_text,
            line_sep,
            ..TableTextOptions::default()
        })
    }
}

impl TextWriteOptions {
    pub fn into_table_options(self) -> DataSourceResult<TableTextOptions> {
        let TextWriteOptions {
            line_sep,
            compression,
        } = self;
        char_to_u8(line_sep, "line_sep").map_err(|e| DataSourceError::InvalidOption {
            key: "line_sep".to_string(),
            value: line_sep.to_string(),
            cause: Some(e.to_string()),
        })?;
        let compression_str = if compression.to_uppercase() == "NONE" {
            "UNCOMPRESSED"
        } else {
            compression.as_str()
        };
        let compression = compression_str
            .parse::<CompressionTypeVariant>()
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: compression.to_string(),
                cause: Some(e.to_string()),
            })?;
        Ok(TableTextOptions {
            line_sep: Some(line_sep),
            compression,
            ..TableTextOptions::default()
        })
    }
}

pub fn resolve_text_read_options(options: Vec<OptionLayer>) -> DataSourceResult<TextReadOptions> {
    let mut partial = TextReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_text_write_options(options: Vec<OptionLayer>) -> DataSourceResult<TextWriteOptions> {
    let mut partial = TextWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

#[cfg(test)]
mod tests {
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::text::options::{resolve_text_read_options, resolve_text_write_options};
    use crate::options::option_list;

    #[test]
    fn test_resolve_text_read_options() -> datafusion_common::Result<()> {
        let kv = option_list(&[]);
        let options = resolve_text_read_options(vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert!(!options.whole_text);
        assert_eq!(options.line_sep, None);
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = option_list(&[("whole_text", "true"), ("line_sep", "\r")]);
        let options = resolve_text_read_options(vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert!(options.whole_text);
        assert_eq!(options.line_sep, Some('\r'));

        Ok(())
    }

    #[test]
    fn test_resolve_text_write_options() -> datafusion_common::Result<()> {
        let kv = option_list(&[]);
        let options = resolve_text_write_options(vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.line_sep, Some('\n'));
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = option_list(&[("line_sep", "\r"), ("compression", "bzip2")]);
        let options = resolve_text_write_options(vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.line_sep, Some('\r'));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
