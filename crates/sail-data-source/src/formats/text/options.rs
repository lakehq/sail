use datafusion_common::parsers::CompressionTypeVariant;
use sail_common_datafusion::datasource::OptionLayer;

use crate::formats::text::TableTextOptions;
use crate::options::gen::{
    TextReadOptions, TextReadPartialOptions, TextWriteOptions, TextWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};
use crate::utils::char_to_u8;

fn apply_text_read_options(
    from: TextReadOptions,
    to: &mut TableTextOptions,
) -> datafusion_common::Result<()> {
    let TextReadOptions {
        whole_text,
        line_sep,
    } = from;
    to.whole_text = whole_text;
    if let Some(line_sep) = line_sep {
        let _ = char_to_u8(line_sep, "line_sep")?;
        to.line_sep = Some(line_sep);
    }
    Ok(())
}

fn apply_text_write_options(
    from: TextWriteOptions,
    to: &mut TableTextOptions,
) -> datafusion_common::Result<()> {
    let TextWriteOptions {
        line_sep,
        compression,
    } = from;
    // Use provided line_sep, or default to '\n' if not set
    let line_sep = line_sep.unwrap_or('\n');
    let _ = char_to_u8(line_sep, "line_sep")?;
    to.line_sep = Some(line_sep);
    let compression = if compression.to_uppercase() == "NONE" {
        "UNCOMPRESSED".to_string()
    } else {
        compression
    };
    to.compression = std::str::FromStr::from_str(compression.as_str())?;
    Ok(())
}

pub fn resolve_text_read_options(
    options: Vec<OptionLayer>,
) -> datafusion_common::Result<TableTextOptions> {
    let mut partial = TextReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut text_options = TableTextOptions::default();
    apply_text_read_options(opts, &mut text_options)?;
    Ok(text_options)
}

pub fn resolve_text_write_options(
    options: Vec<OptionLayer>,
) -> datafusion_common::Result<TableTextOptions> {
    let mut partial = TextWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut text_options = TableTextOptions::default();
    apply_text_write_options(opts, &mut text_options)?;
    Ok(text_options)
}

#[cfg(test)]
mod tests {
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::text::options::{resolve_text_read_options, resolve_text_write_options};
    use crate::options::build_option_layer;

    #[test]
    fn test_resolve_text_read_options() -> datafusion_common::Result<()> {
        let kv = build_option_layer(&[]);
        let options = resolve_text_read_options(vec![kv])?;
        assert!(!options.whole_text);
        assert_eq!(options.line_sep, None);
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = build_option_layer(&[
            ("whole_text", "true"),
            ("line_sep", "\r"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_text_read_options(vec![kv])?;
        assert!(options.whole_text);
        assert_eq!(options.line_sep, Some('\r'));
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        Ok(())
    }

    #[test]
    fn test_resolve_text_write_options() -> datafusion_common::Result<()> {
        let kv = build_option_layer(&[]);
        let options = resolve_text_write_options(vec![kv])?;
        assert_eq!(options.line_sep, Some('\n'));
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = build_option_layer(&[("line_sep", "\r"), ("compression", "bzip2")]);
        let options = resolve_text_write_options(vec![kv])?;
        assert_eq!(options.line_sep, Some('\r'));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
