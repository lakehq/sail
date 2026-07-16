use datafusion::arrow::array::{Array, MapArray, StringArray};
use datafusion::error::Result;
use datafusion_common::exec_err;
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

/// Which CSV expression function is reading the options.
///
/// Spark validates almost every option identically in all three, but not quite: only the writer
/// rejects an empty `lineSep`, and only `from_csv` rejects the `DROPMALFORMED` parse mode.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CsvFunction {
    From,
    To,
    SchemaOf,
}

impl CsvFunction {
    fn name(self) -> &'static str {
        match self {
            Self::From => "from_csv",
            Self::To => "to_csv",
            Self::SchemaOf => "schema_of_csv",
        }
    }
}

/// The boolean options that Spark's `CSVOptions` parses with `getBool`.
const BOOL_OPTIONS: [&str; 7] = [
    "header",
    "inferSchema",
    "ignoreLeadingWhiteSpace",
    "ignoreTrailingWhiteSpace",
    "escapeQuotes",
    "quoteAll",
    "enforceSchema",
];

/// Spark reads `multiLine` with Scala's `String.toBoolean` rather than `CSVOptions.getBool`. Both
/// accept exactly `true` and `false` case-insensitively, but they report different errors.
const MULTI_LINE_OPTION: &str = "multiLine";

/// Options Spark reads with `CSVOptions.getChar`. An empty value means "no character" and is
/// accepted; anything longer than one character is rejected.
const SINGLE_CHAR_OPTIONS: [&str; 4] = ["quote", "escape", "comment", "charToEscapeQuoteEscaping"];

/// The field separator, which may be several characters long but not empty.
const SEP_OPTIONS: [&str; 2] = ["sep", "delimiter"];

const INT_OPTIONS: [&str; 2] = ["maxColumns", "maxCharsPerColumn"];
const SAMPLING_RATIO_OPTION: &str = "samplingRatio";
const UNESCAPED_QUOTE_HANDLING_OPTION: &str = "unescapedQuoteHandling";
const UNESCAPED_QUOTE_HANDLING_VALUES: [&str; 5] = [
    "STOP_AT_CLOSING_QUOTE",
    "BACK_TO_DELIMITER",
    "STOP_AT_DELIMITER",
    "SKIP_VALUE",
    "RAISE_ERROR",
];
const LINE_SEP_OPTION: &str = "lineSep";
const MODE_OPTION: &str = "mode";
const DROP_MALFORMED_MODE: &str = "DROPMALFORMED";

fn entry_columns(map: &MapArray) -> Option<(&StringArray, &StringArray)> {
    let keys = map
        .entries()
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)?
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = map
        .entries()
        .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)?
        .as_any()
        .downcast_ref::<StringArray>()?;
    Some((keys, values))
}

/// Returns the value of the `key` option, or `None` when the map does not carry it.
///
/// Spark reads CSV options through a case-insensitive map, so `SEP` selects the same option as
/// `sep`. A null value yields `None` rather than an empty string, though [`validate_options`]
/// rejects the call before that can be observed.
pub(super) fn find_option<'a>(map: &'a MapArray, key: &str) -> Option<&'a str> {
    let (keys, values) = entry_columns(map)?;
    keys.iter()
        .zip(values.iter())
        .find_map(|(entry_key, entry_value)| match entry_key {
            Some(entry_key) if entry_key.eq_ignore_ascii_case(key) => entry_value,
            _ => None,
        })
}

/// Rejects an invalid value for any CSV option present in `map`.
///
/// Spark builds `CSVOptions` eagerly, so every option is parsed as soon as the map is read, even
/// by a function that never uses the resulting value: `from_csv` rejects a bad `quoteAll` and
/// `to_csv` rejects a bad `header`. Unknown keys are ignored, also matching Spark, which only
/// looks up the options it knows and never validates the rest.
///
/// A NULL value is the exception to that: Spark fails the call for any NULL, even under a key it
/// does not know, so it is a property of the map rather than of the individual option.
pub(super) fn validate_options(map: &MapArray, function: CsvFunction) -> Result<()> {
    if map.is_empty() {
        return Ok(());
    }
    let Some((keys, values)) = entry_columns(map) else {
        return Ok(());
    };
    for (key, value) in keys.iter().zip(values.iter()) {
        let (Some(key), Some(value)) = (key, value) else {
            return exec_err!(
                "Failed preparing of the function `{}` for call. Please, double check function's arguments.",
                function.name()
            );
        };
        validate_option(key, value, function)?;
    }
    Ok(())
}

/// Spark reads options through a case-insensitive map but always reports the canonical option
/// name back to the user, never the key as it was written.
fn validate_option(key: &str, value: &str, function: CsvFunction) -> Result<()> {
    if let Some(option) = BOOL_OPTIONS.iter().find(|x| key.eq_ignore_ascii_case(x)) {
        parse_bool_option(Some(value), option, false)?;
    } else if key.eq_ignore_ascii_case(MULTI_LINE_OPTION) {
        validate_multi_line_option(value)?;
    } else if let Some(option) = SINGLE_CHAR_OPTIONS
        .iter()
        .find(|x| key.eq_ignore_ascii_case(x))
    {
        // Spark measures the value with Java's `String.length`, which counts UTF-16 code units:
        // an emoji is two units and is rejected, while `ñ` is one and is accepted.
        if value.encode_utf16().count() > 1 {
            return exec_err!("{option} cannot be more than one character.");
        }
    } else if SEP_OPTIONS.iter().any(|x| key.eq_ignore_ascii_case(x)) {
        if value.is_empty() {
            return exec_err!("Delimiter cannot be empty");
        }
    } else if let Some(option) = INT_OPTIONS.iter().find(|x| key.eq_ignore_ascii_case(x)) {
        if value.parse::<i32>().is_err() {
            return exec_err!("{option} should be an integer. Found {value}.");
        }
    } else if key.eq_ignore_ascii_case(SAMPLING_RATIO_OPTION) {
        if value.parse::<f64>().is_err() {
            return exec_err!("For input string: \"{value}\"");
        }
    } else if key.eq_ignore_ascii_case(UNESCAPED_QUOTE_HANDLING_OPTION) {
        if !UNESCAPED_QUOTE_HANDLING_VALUES
            .iter()
            .any(|x| x.eq_ignore_ascii_case(value))
        {
            // Spark upper-cases the value before the lookup, so it surfaces the failure of the
            // underlying parser's `valueOf` with the upper-cased name.
            let value = value.to_uppercase();
            return exec_err!(
                "No enum constant com.univocity.parsers.csv.UnescapedQuoteHandling.{value}"
            );
        }
    } else if key.eq_ignore_ascii_case(LINE_SEP_OPTION) {
        if function == CsvFunction::To && value.is_empty() {
            return exec_err!("requirement failed: '{LINE_SEP_OPTION}' cannot be an empty string.");
        }
    } else if key.eq_ignore_ascii_case(MODE_OPTION) {
        // An unrecognized mode is not an error: Spark warns and falls back to PERMISSIVE.
        if function == CsvFunction::From && value.eq_ignore_ascii_case(DROP_MALFORMED_MODE) {
            return exec_err!(
                "The function `from_csv` doesn't support the {DROP_MALFORMED_MODE} mode. Acceptable modes are PERMISSIVE and FAILFAST."
            );
        }
    }
    Ok(())
}

/// Parses a boolean CSV option the way Spark's `CSVOptions.getBool` does: case-insensitively,
/// accepting only `true` and `false`, with no surrounding whitespace tolerated.
pub(super) fn parse_bool_option(
    value: Option<&str>,
    option_name: &str,
    default: bool,
) -> Result<bool> {
    match value {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => exec_err!("{option_name} flag can be true or false."),
    }
}

/// Validates `multiLine`, which Spark reads with Scala's `String.toBoolean`. The accepted values
/// are the same as [`parse_bool_option`], but the error quotes the offending value instead of
/// naming the option. No CSV expression function honors the flag, so only the value is checked.
fn validate_multi_line_option(value: &str) -> Result<()> {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        Ok(())
    } else {
        exec_err!("For input string: \"{value}\"")
    }
}
