use std::collections::HashMap;
use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::CsvOptions;
use datafusion_common::plan_err;
use datafusion_datasource::file_compression_type::FileCompressionType;

use crate::options::{load_default_options, load_options, CsvReadOptions, CsvWriteOptions};
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
        schema_infer_max_records,
        multi_line,
        compression,
        allow_truncated_rows,
        infer_schema: _,
    } = from;
    let null_regex = match (null_value, null_regex) {
        (Some(null_value), Some(null_regex))
            if !null_value.is_empty() && !null_regex.is_empty() =>
        {
            return plan_err!("CSV `null_value` and `null_regex` cannot be both set");
        }
        (Some(null_value), _) if !null_value.is_empty() => {
            // Convert null value to regex by escaping special characters
            Some(regex::escape(&null_value))
        }
        (_, Some(null_regex)) if !null_regex.is_empty() => Some(null_regex),
        _ => None,
    };
    if let Some(null_regex) = null_regex {
        to.null_regex = Some(null_regex);
    }
    if let Some(delimiter) = delimiter {
        to.delimiter = char_to_u8(delimiter, "delimiter")?;
    }
    // TODO: support no quote
    if let Some(Some(quote)) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(Some(escape)) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    // TODO: support no comment
    if let Some(Some(comment)) = comment {
        to.comment = Some(char_to_u8(comment, "comment")?);
    }
    if let Some(header) = header {
        to.has_header = Some(header);
    }
    if let Some(Some(sep)) = line_sep {
        to.terminator = Some(char_to_u8(sep, "line_sep")?);
    }
    if let Some(n) = schema_infer_max_records {
        to.schema_infer_max_rec = Some(n);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    if let Some(multi_line) = multi_line {
        to.newlines_in_values = Some(multi_line);
    }
    if let Some(allow_truncated_rows) = allow_truncated_rows {
        to.truncated_rows = Some(allow_truncated_rows);
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
    if let Some(delimiter) = delimiter {
        to.delimiter = char_to_u8(delimiter, "delimiter")?;
    }
    // TODO: support no quote
    if let Some(Some(quote)) = quote {
        to.quote = char_to_u8(quote, "quote")?;
    }
    // TODO: support no escape
    if let Some(Some(escape)) = escape {
        to.escape = Some(char_to_u8(escape, "escape")?);
    }
    if let Some(escape_quotes) = escape_quotes {
        to.double_quote = Some(escape_quotes);
    }
    if let Some(header) = header {
        to.has_header = Some(header);
    }
    if let Some(null_value) = null_value {
        to.null_value = Some(null_value);
    }
    if let Some(compression) = compression {
        to.compression = FileCompressionType::from_str(&compression)?.into();
    }
    Ok(())
}

pub fn resolve_csv_read_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> datafusion_common::Result<CsvOptions> {
    let mut csv_options = ctx.default_table_options().csv;
    apply_csv_read_options(load_default_options()?, &mut csv_options)?;
    for opt in options {
        apply_csv_read_options(load_options(opt)?, &mut csv_options)?;
    }
    Ok(csv_options)
}

pub fn resolve_csv_write_options(
    ctx: &dyn Session,
    options: Vec<HashMap<String, String>>,
) -> datafusion_common::Result<CsvOptions> {
    let mut csv_options = ctx.default_table_options().csv;
    apply_csv_write_options(load_default_options()?, &mut csv_options)?;
    for opt in options {
        apply_csv_write_options(load_options(opt)?, &mut csv_options)?;
    }
    Ok(csv_options)
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::csv::options::{resolve_csv_read_options, resolve_csv_write_options};
    use crate::options::build_options;

    #[test]
    fn test_resolve_csv_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = build_options(&[
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

        let kv = build_options(&[
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

        let kv = build_options(&[
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

        let kv = build_options(&[
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
