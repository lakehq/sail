use std::str::FromStr;

use datafusion::catalog::Session;
use datafusion_common::config::CsvOptions;
use datafusion_datasource::file_compression_type::FileCompressionType;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::{DataSourceError, DataSourceResult};
use crate::options::gen::{
    CsvReadOptions, CsvReadPartialOptions, CsvWriteOptions, CsvWritePartialOptions,
};
use crate::options::{BuildPartialOptions, PartialOptions};
use crate::utils::char_to_u8;

impl BuildPartialOptions<CsvReadPartialOptions> for CsvOptions {
    fn build_partial_options(self) -> DataSourceResult<CsvReadPartialOptions> {
        Ok(CsvReadPartialOptions {
            delimiter: Some(self.delimiter as char),
            quote: Some(Some(self.quote as char)),
            escape: Some(self.escape.map(|b| b as char)),
            comment: Some(self.comment.map(|b| b as char)),
            header: self.has_header,
            null_value: self.null_value,
            null_regex: self.null_regex,
            line_sep: Some(self.terminator.map(|b| b as char)),
            infer_schema: None,
            schema_infer_max_records: self.schema_infer_max_rec,
            multi_line: self.newlines_in_values,
            compression: Some(self.compression.to_string()),
            allow_truncated_rows: self.truncated_rows,
        })
    }
}

impl BuildPartialOptions<CsvWritePartialOptions> for CsvOptions {
    fn build_partial_options(self) -> DataSourceResult<CsvWritePartialOptions> {
        Ok(CsvWritePartialOptions {
            delimiter: Some(self.delimiter as char),
            quote: Some(Some(self.quote as char)),
            escape: Some(self.escape.map(|b| b as char)),
            header: self.has_header,
            null_value: self.null_value,
            escape_quotes: self.double_quote,
            compression: Some(self.compression.to_string()),
        })
    }
}

impl CsvReadOptions {
    pub fn into_table_options(self) -> DataSourceResult<CsvOptions> {
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
        } = self;
        let null_regex = match (null_value.as_str(), null_regex.as_str()) {
            (nv, nr) if !nv.is_empty() && !nr.is_empty() => {
                return Err(DataSourceError::InvalidOption {
                    key: "null_value/null_regex".to_string(),
                    value: "CSV `null_value` and `null_regex` cannot be both set".to_string(),
                    cause: None,
                });
            }
            (nv, _) if !nv.is_empty() => Some(regex::escape(nv)),
            (_, nr) if !nr.is_empty() => Some(nr.to_string()),
            _ => None,
        };
        let delimiter =
            char_to_u8(delimiter, "delimiter").map_err(|e| DataSourceError::InvalidOption {
                key: "delimiter".to_string(),
                value: delimiter.to_string(),
                cause: Some(e.to_string()),
            })?;
        let quote = quote
            .map(|c| {
                char_to_u8(c, "quote").map_err(|e| DataSourceError::InvalidOption {
                    key: "quote".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?
            .unwrap_or(CsvOptions::default().quote);
        let escape = escape
            .map(|c| {
                char_to_u8(c, "escape").map_err(|e| DataSourceError::InvalidOption {
                    key: "escape".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?;
        let comment = comment
            .map(|c| {
                char_to_u8(c, "comment").map_err(|e| DataSourceError::InvalidOption {
                    key: "comment".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?;
        let terminator = line_sep
            .map(|c| {
                char_to_u8(c, "line_sep").map_err(|e| DataSourceError::InvalidOption {
                    key: "line_sep".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?;
        let schema_infer_max_rec = if infer_schema {
            Some(schema_infer_max_records)
        } else {
            Some(0)
        };
        let compression = FileCompressionType::from_str(&compression)
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: compression.to_string(),
                cause: Some(e.to_string()),
            })?
            .into();
        Ok(CsvOptions {
            has_header: Some(header),
            delimiter,
            quote,
            terminator,
            escape,
            comment,
            null_regex,
            schema_infer_max_rec,
            compression,
            newlines_in_values: Some(multi_line),
            truncated_rows: Some(allow_truncated_rows),
            ..CsvOptions::default()
        })
    }
}

impl CsvWriteOptions {
    pub fn into_table_options(self) -> DataSourceResult<CsvOptions> {
        let CsvWriteOptions {
            delimiter,
            quote,
            escape,
            header,
            null_value,
            escape_quotes,
            compression,
        } = self;
        let delimiter =
            char_to_u8(delimiter, "delimiter").map_err(|e| DataSourceError::InvalidOption {
                key: "delimiter".to_string(),
                value: delimiter.to_string(),
                cause: Some(e.to_string()),
            })?;
        let quote = quote
            .map(|c| {
                char_to_u8(c, "quote").map_err(|e| DataSourceError::InvalidOption {
                    key: "quote".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?
            .unwrap_or(CsvOptions::default().quote);
        let escape = escape
            .map(|c| {
                char_to_u8(c, "escape").map_err(|e| DataSourceError::InvalidOption {
                    key: "escape".to_string(),
                    value: c.to_string(),
                    cause: Some(e.to_string()),
                })
            })
            .transpose()?;
        let null_value = if null_value.is_empty() {
            None
        } else {
            Some(null_value)
        };
        let compression = FileCompressionType::from_str(&compression)
            .map_err(|e| DataSourceError::InvalidOption {
                key: "compression".to_string(),
                value: compression.to_string(),
                cause: Some(e.to_string()),
            })?
            .into();
        Ok(CsvOptions {
            has_header: Some(header),
            delimiter,
            quote,
            escape,
            null_value,
            double_quote: Some(escape_quotes),
            compression,
            ..CsvOptions::default()
        })
    }
}

pub fn resolve_csv_read_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<CsvReadOptions> {
    let mut partial = CsvReadPartialOptions::initialize();
    partial.merge(ctx.default_table_options().csv.build_partial_options()?);
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

pub fn resolve_csv_write_options(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
) -> DataSourceResult<CsvWriteOptions> {
    let mut partial = CsvWritePartialOptions::initialize();
    partial.merge(ctx.default_table_options().csv.build_partial_options()?);
    for layer in options {
        partial.merge(layer.build_partial_options()?);
    }
    partial.finalize()
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion_common::parsers::CompressionTypeVariant;

    use crate::formats::csv::options::{resolve_csv_read_options, resolve_csv_write_options};
    use crate::options::option_list;

    #[test]
    fn test_resolve_csv_read_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[
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
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.comment, Some(b'^'));
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, None);
        assert_eq!(options.null_regex, Some("MEOW".to_string()));
        assert_eq!(options.terminator, Some(b'@'));
        // `inferSchema` defaults to `false` (Spark parity), which collapses
        // `schema_infer_max_rec` to `Some(0)` regardless of the user-supplied
        // `schema_infer_max_records` value above.
        assert_eq!(options.schema_infer_max_rec, Some(0));
        assert_eq!(options.newlines_in_values, Some(true));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        let kv = option_list(&[("inferSchema", "false")]);
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        let kv = option_list(&[("inferSchema", "true")]);
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(1000));

        let kv = option_list(&[("infer_schema", "false")]);
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        let kv = option_list(&[
            ("inferSchema", "false"),
            ("schema_infer_max_records", "500"),
        ]);
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.schema_infer_max_rec, Some(0));

        let kv = option_list(&[("null_value", "MEOW"), ("null_regex", "MEOW")]);
        let result =
            resolve_csv_read_options(&state, vec![kv]).and_then(|o| o.into_table_options());
        assert!(result.is_err());

        let kv = option_list(&[("null_regex", "MEOW")]);
        let options = resolve_csv_read_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.null_value, None);
        assert_eq!(options.null_regex, Some("MEOW".to_string()));

        Ok(())
    }

    #[test]
    fn test_resolve_csv_write_options() -> datafusion_common::Result<()> {
        let ctx = SessionContext::default();
        let state = ctx.state();

        let kv = option_list(&[
            ("delimiter", "!"),
            ("quote", "("),
            ("escape", "*"),
            ("escape_quotes", "true"),
            ("header", "true"),
            ("null_value", "MEOW"),
            ("compression", "bzip2"),
        ]);
        let options = resolve_csv_write_options(&state, vec![kv])
            .and_then(|o| o.into_table_options())
            .map_err(datafusion_common::DataFusionError::from)?;
        assert_eq!(options.delimiter, b'!');
        assert_eq!(options.quote, b'(');
        assert_eq!(options.escape, Some(b'*'));
        assert_eq!(options.double_quote, Some(true));
        assert_eq!(options.has_header, Some(true));
        assert_eq!(options.null_value, Some("MEOW".to_string()));
        assert_eq!(options.compression, CompressionTypeVariant::BZIP2);

        Ok(())
    }
}
