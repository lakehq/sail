use std::ops::Range;

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

/// The boolean options that Spark's `CSVOptions` parses with `getBool`, which reports
/// `<option> flag can be true or false.` on a non-boolean value.
const BOOL_OPTIONS: [&str; 9] = [
    "header",
    "inferSchema",
    "ignoreLeadingWhiteSpace",
    "ignoreTrailingWhiteSpace",
    "escapeQuotes",
    "quoteAll",
    "enforceSchema",
    "preferDate",
    "columnPruning",
];

/// Boolean options Spark reads with Scala's `String.toBoolean` instead of `getBool`, so a bad
/// value is reported as `For input string: "<value>"`. `multiLine` is one of these; the rest are
/// inherited from the file-source options and validated just as eagerly.
const TO_BOOLEAN_OPTIONS: [&str; 4] = [
    "multiLine",
    "enableDateTimeParsingFallback",
    "ignoreCorruptFiles",
    "ignoreMissingFiles",
];

/// Options Spark reads with `CSVOptions.getChar`. An empty value means "no character" and is
/// accepted; anything longer than one character is rejected.
const SINGLE_CHAR_OPTIONS: [&str; 4] = ["quote", "escape", "comment", "charToEscapeQuoteEscaping"];

/// Integer options Spark reads with `getInt`, reporting `<option> should be an integer. Found <v>.`.
const INT_OPTIONS: [&str; 2] = ["maxColumns", "maxCharsPerColumn"];
/// Integer options Spark reads with a bare `.toInt`, reporting `For input string: "<value>"`.
const TO_INT_OPTIONS: [&str; 1] = ["inputBufferSize"];
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

/// The key and value columns of the options map, together with the entry range of its first row.
///
/// The options argument is one map for the whole batch, but `make_scalar_function` pads a scalar
/// argument to one value per row, so the flattened entries repeat that map once per row. Only the
/// first row is read: the rest are copies, and walking them would validate the same option once
/// per row of the batch.
fn first_row_entries(map: &MapArray) -> Option<(&StringArray, &StringArray, Range<usize>)> {
    if map.is_empty() || map.is_null(0) {
        return None;
    }
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
    let offsets = map.value_offsets();
    let start = *offsets.first()? as usize;
    let end = *offsets.get(1)? as usize;
    Some((keys, values, start..end))
}

/// Returns the effective value of the `key` option, or `None` when the map does not carry it.
///
/// Spark reads CSV options through a `CaseInsensitiveMap`, so `SEP` selects the same option as
/// `sep` and a later case-variant of a key SHADOWS an earlier one — the LAST match wins. A null
/// value yields `None` rather than an empty string.
pub(super) fn find_option<'a>(map: &'a MapArray, key: &str) -> Option<&'a str> {
    let (keys, values, entries) = first_row_entries(map)?;
    keys.iter()
        .zip(values.iter())
        .take(entries.end)
        .skip(entries.start)
        .filter_map(|(entry_key, entry_value)| match entry_key {
            Some(entry_key) if entry_key.eq_ignore_ascii_case(key) => Some(entry_value),
            _ => None,
        })
        .next_back()
        .flatten()
}

/// Rejects a structurally invalid options map — a NULL key or value — the way Spark does during
/// map conversion, before any input row is evaluated.
///
/// This is the ONLY part of option handling that is eager: Spark fails the call for a NULL entry
/// even when the input is NULL and even under a key it does not know, because the failure happens
/// while building the options, not while parsing a row. Value validation, by contrast, is lazy
/// (see [`validate_options`]).
pub(super) fn reject_null_entries(map: &MapArray, function: CsvFunction) -> Result<()> {
    let Some((keys, values, entries)) = first_row_entries(map) else {
        return Ok(());
    };
    for (key, value) in keys
        .iter()
        .zip(values.iter())
        .take(entries.end)
        .skip(entries.start)
    {
        if key.is_none() || value.is_none() {
            return exec_err!(
                "Failed preparing of the function `{}` for call. Please, double check function's arguments.",
                function.name()
            );
        }
    }
    Ok(())
}

/// Rejects an invalid VALUE for any CSV option present in `map`.
///
/// Read-driven, matching Spark: each canonical option is validated by READING its effective value
/// from the case-insensitive map (last case-variant wins, via [`find_option`]), NOT by scanning raw
/// entries. A shadowed value — an earlier case-variant, or the losing side of an alias such as
/// `delimiter` when `sep` is present — is never read and therefore never validated.
///
/// Spark builds `CSVOptions` eagerly relative to other options — so `from_csv` rejects a bad
/// `quoteAll` and `to_csv` rejects a bad `header` — but LAZILY relative to the input: a bad value is
/// not seen when every input row is NULL. Callers therefore invoke this only when the input has at
/// least one non-null row; the structural NULL-entry check ([`reject_null_entries`]) runs
/// unconditionally instead. Unknown keys are ignored, also matching Spark.
pub(super) fn validate_options(map: &MapArray, function: CsvFunction) -> Result<()> {
    for option in BOOL_OPTIONS {
        if let Some(value) = find_option(map, option) {
            parse_bool_option(Some(value), option, false)?;
        }
    }
    // Read with Scala's `String.toBoolean`, which quotes the value instead of naming the option.
    for option in TO_BOOLEAN_OPTIONS {
        if let Some(value) = find_option(map, option)
            && !fold_eq(value, "true")
            && !fold_eq(value, "false")
        {
            return exec_err!("For input string: \"{value}\"");
        }
    }
    // Spark measures the value with Java's `String.length`, which counts UTF-16 code units:
    // an emoji is two units and is rejected, while `ñ` is one and is accepted.
    for option in SINGLE_CHAR_OPTIONS {
        if let Some(value) = find_option(map, option)
            && value.encode_utf16().count() > 1
        {
            return exec_err!("{option} cannot be more than one character.");
        }
    }
    // Spark reads `sep`, falling back to `delimiter`; only the value it would read is validated.
    if let Some(value) = find_option(map, "sep").or_else(|| find_option(map, "delimiter"))
        && value.is_empty()
    {
        return exec_err!("Delimiter cannot be empty");
    }
    for option in INT_OPTIONS {
        if let Some(value) = find_option(map, option)
            && value.parse::<i32>().is_err()
        {
            return exec_err!("{option} should be an integer. Found {value}.");
        }
    }
    // `inputBufferSize` (`.toInt`) and `samplingRatio` (`.toDouble`) both report the raw value.
    for option in TO_INT_OPTIONS {
        if let Some(value) = find_option(map, option)
            && value.parse::<i32>().is_err()
        {
            return exec_err!("For input string: \"{value}\"");
        }
    }
    if let Some(value) = find_option(map, SAMPLING_RATIO_OPTION)
        && value.parse::<f64>().is_err()
    {
        return exec_err!("For input string: \"{value}\"");
    }
    if let Some(value) = find_option(map, UNESCAPED_QUOTE_HANDLING_OPTION)
        && !UNESCAPED_QUOTE_HANDLING_VALUES
            .iter()
            .any(|x| fold_eq(value, x))
    {
        // Spark upper-cases the value before the lookup, so it surfaces the failure of the
        // underlying parser's `valueOf` with the upper-cased name.
        let value = value.to_uppercase();
        return exec_err!(
            "No enum constant com.univocity.parsers.csv.UnescapedQuoteHandling.{value}"
        );
    }
    // The writer requires at most one Unicode character, which Spark measures as at most two UTF-16
    // code units — so `ab` (two units) is accepted but `abc` (three) is not. The reader ignores it.
    if function == CsvFunction::To
        && let Some(value) = find_option(map, LINE_SEP_OPTION)
    {
        if value.is_empty() {
            return exec_err!("requirement failed: '{LINE_SEP_OPTION}' cannot be an empty string.");
        }
        if value.encode_utf16().count() > 2 {
            return exec_err!(
                "requirement failed: '{LINE_SEP_OPTION}' can contain only 1 character."
            );
        }
    }
    // An unrecognized mode is not an error: Spark warns and falls back to PERMISSIVE. Only
    // `from_csv` rejects `DROPMALFORMED`.
    if function == CsvFunction::From
        && let Some(value) = find_option(map, MODE_OPTION)
        && fold_eq(value, DROP_MALFORMED_MODE)
    {
        return exec_err!(
            "The function `from_csv` doesn't support the {DROP_MALFORMED_MODE} mode. Acceptable modes are PERMISSIVE and FAILFAST."
        );
    }
    Ok(())
}

/// Case-insensitive comparison matching Java's `String.equalsIgnoreCase`, which folds non-ASCII
/// letters too — Spark accepts `multiLine='falſe'` because `ſ` (U+017F) upper-cases to `S`. The
/// ASCII fast path keeps the common value allocation-free; only a non-ASCII value pays the fold.
fn fold_eq(value: &str, target: &str) -> bool {
    value.eq_ignore_ascii_case(target)
        || (!value.is_ascii() && value.to_uppercase() == target.to_uppercase())
}

/// Parses a boolean CSV option the way Spark's `CSVOptions.getBool` does: it lower-cases with
/// `Locale.ROOT` and compares to `true`/`false`. That is ASCII-only folding — `ſ` (U+017F) does
/// NOT lower-case to `s`, so `header='falſe'` is rejected, unlike the `toBoolean` options, which
/// upper-case-fold and accept it. `eq_ignore_ascii_case`, not [`fold_eq`], matches this.
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
