use std::collections::HashMap;
use std::str::FromStr;

use datafusion_common::parsers::CompressionTypeVariant;

use crate::formats::text::TableTextOptions;
use crate::options::{load_default_options, load_options, TextReadOptions, TextWriteOptions};
use crate::utils::char_to_u8;

fn apply_text_read_options(
    from: TextReadOptions,
    to: &mut TableTextOptions,
) -> datafusion_common::Result<()> {
    if let Some(whole_text) = from.whole_text {
        to.whole_text = whole_text;
    }
    if let Some(Some(line_sep)) = from.line_sep {
        let _ = char_to_u8(line_sep, "line_sep")?;
        to.line_sep = Some(line_sep);
    }
    Ok(())
}

fn apply_text_write_options(
    from: TextWriteOptions,
    to: &mut TableTextOptions,
) -> datafusion_common::Result<()> {
    if let Some(line_sep) = from.line_sep {
        let _ = char_to_u8(line_sep, "line_sep")?;
        to.line_sep = Some(line_sep);
    }
    if let Some(compression) = from.compression {
        let compression = if compression.to_uppercase() == "NONE" {
            "UNCOMPRESSED".to_string()
        } else {
            compression
        };
        to.compression = CompressionTypeVariant::from_str(compression.as_str())?;
    }
    Ok(())
}

pub fn resolve_text_read_options(
    options: Vec<HashMap<String, String>>,
) -> datafusion_common::Result<TableTextOptions> {
    let mut text_options = TableTextOptions::default();
    apply_text_read_options(load_default_options()?, &mut text_options)?;
    for opt in options {
        apply_text_read_options(load_options(opt)?, &mut text_options)?;
    }
    Ok(text_options)
}

pub fn resolve_text_write_options(
    options: Vec<HashMap<String, String>>,
) -> datafusion_common::Result<TableTextOptions> {
    let mut text_options = TableTextOptions::default();
    apply_text_write_options(load_default_options()?, &mut text_options)?;
    for opt in options {
        apply_text_write_options(load_options(opt)?, &mut text_options)?;
    }
    Ok(text_options)
}

#[cfg(test)]
mod tests {
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::text::options::{resolve_text_read_options, resolve_text_write_options};
    use crate::options::build_options;

    #[test]
    fn test_resolve_text_read_options() -> datafusion_common::Result<()> {
        let kv = build_options(&[]);
        let options = resolve_text_read_options(vec![kv])?;
        assert!(!options.whole_text);
        assert_eq!(options.line_sep, None);
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = build_options(&[
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
        let kv = build_options(&[]);
        let options = resolve_text_write_options(vec![kv])?;
        assert_eq!(options.line_sep, Some('\n'));
        assert_eq!(options.compression, CompressionTypeVariant::UNCOMPRESSED);

        let kv = build_options(&[("line_sep", "\r"), ("compression", "bzip2")]);
        let options = resolve_text_write_options(vec![kv])?;
        assert_eq!(options.line_sep, Some('\r'));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
