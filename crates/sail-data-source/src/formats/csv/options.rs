use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::CsvOptions;
use datafusion_common::plan_err;
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::OptionLayer;

use crate::options::gen::{
    CsvReadOptions, CsvReadPartialOptions, CsvWriteOptions, CsvWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};
use crate::utils::char_to_u8;

fn apply_csv_read_options(
    from: CsvReadOptions,
    to: &mut CsvOptions,
) -> datafusion_common::Result<()> {
    let CsvReadOptions {
        delimiter,
        quote,
        escape,
        comment,
        header,
        null_value,
        null_regex,
        line_sep,
        infer_schema,
        schema_infer_max_records,
        multi_line,
        compression,
        allow_truncated_rows,
    } = from;
    let null_regex = match (null_value.as_str(), null_regex.as_str()) {
        (nv, nr) if !nv.is_empty() && !nr.is_empty() => {
            return plan_err!("CSV `null_value` and `null_regex` cannot be both set");
        }
        (nv, _) if !nv.is_empty() => {
            // Convert null value to regex by escaping special characters
            Some(regex::escape(nv))
        }
        (_, nr) if !nr.is_empty() => Some(nr.to_string()),
        _ => None,
    };
    if let Some(null_regex) = null_regex {
        to.null_regex = Some(null_regex);
    }
    to.delimiter = char_to_u8(delimiter, "delimiter")?;
    // TODO: support no quote
    if let Some(quote) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(escape) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    // TODO: support no comment
    if let Some(comment) = comment {
        to.comment = Some(char_to_u8(comment, "comment")?);
    }
    to.has_header = Some(header);
    if let Some(sep) = line_sep {
        to.terminator = Some(char_to_u8(sep, "line_sep")?);
    }
    to.schema_infer_max_rec = Some(schema_infer_max_records);
    to.compression = FileCompressionType::from_str(&compression)?.into();
    to.newlines_in_values = Some(multi_line);
    to.truncated_rows = Some(allow_truncated_rows);
    if !infer_schema {
        to.schema_infer_max_rec = Some(0);
    }
    Ok(())
}

fn apply_csv_write_options(
    from: CsvWriteOptions,
    to: &mut CsvOptions,
) -> datafusion_common::Result<()> {
    let CsvWriteOptions {
        delimiter,
        quote,
        escape,
        escape_quotes,
        header,
        null_value,
        compression,
    } = from;
    to.delimiter = char_to_u8(delimiter, "delimiter")?;
    // TODO: support no quote
    if let Some(quote) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(escape) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    to.double_quote = Some(escape_quotes);
    to.has_header = Some(header);
    to.null_value = Some(null_value);
    to.compression = FileCompressionType::from_str(&compression)?.into();
    Ok(())
}

pub fn resolve_csv_read_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> datafusion_common::Result<CsvOptions> {
    let mut partial = CsvReadPartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut csv_options = ctx.default_table_options().csv;
    apply_csv_read_options(opts, &mut csv_options)?;
    Ok(csv_options)
}

pub fn resolve_csv_write_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> datafusion_common::Result<CsvOptions> {
    let mut partial = CsvWritePartialOptions::initialize();
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    let opts = partial.finalize()?;
    let mut csv_options = ctx.default_table_options().csv;
    apply_csv_write_options(opts, &mut csv_options)?;
    Ok(csv_options)
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::csv::options::{resolve_csv_read_options, resolve_csv_write_options};
    use crate::options::build_option_layer;

    #[test]
    fn test_resolve_csv_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_option_layer(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.comment, Some(b'^'));
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, None); // This is for the writer
        assert_eq!(options.null_regex, Some("MEOW".to_string())); // null_value
        assert_eq!(options.terminator, Some(b'@')); // line_sep
        assert_eq!(options.schema_infer_max_rec, Some(100));
        assert_eq!(options.newlines_in_values, Some(true)); // multi_line
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        // When inferSchema is false, schema_infer_max_rec should be set to 0
        let kv = build_option_layer(&[("inferSchema", "false")]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        // When inferSchema is true (or not set), schema_infer_max_rec should keep its value
        let kv = build_option_layer(&[("inferSchema", "true")]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(1000));

        // When infer_schema is false with snake_case key
        let kv = build_option_layer(&[("infer_schema", "false")]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        // infer_schema=false should override explicit schema_infer_max_records
        let kv = build_option_layer(&[
            ("inferSchema", "false"),
            ("schema_infer_max_records", "500"),
        ]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        let kv = build_option_layer(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("null_regex", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        // null_value and null_regex cannot both be set
        let result = resolve_csv_read_options(&state, vec![kv]);
        assert!(result.is_err());

        let kv = build_option_layer(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("comment", "^"),
            ("header", "true"),
            ("null_regex", "MEOW"),
            ("line_sep", "@"),
            ("schema_infer_max_records", "100"),
            ("multi_line", "true"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_csv_read_options(&state, vec![kv])?;
        assert_eq!(options.null_value, None); // This is for the writer
        assert_eq!(options.null_regex, Some("MEOW".to_string())); // null_regex

        Ok(())
    }

    #[test]
    fn test_resolve_csv_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_option_layer(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("escape_quotes", "true"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_csv_write_options(&state, vec![kv])?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.double_quote, Some(true)); // escape_quotes
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, Some("MEOW".to_string()));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
