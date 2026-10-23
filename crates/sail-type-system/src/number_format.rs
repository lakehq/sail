//! Shared SQL number-format parsing and decimal precision inference.

use datafusion_common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use regex::{Captures, Regex};

static FORMAT_REGEX: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    #[expect(clippy::unwrap_used)]
        // `numbers` uses `*` (not `+`) so "no integer part" formats like `.99`
        // are accepted; the fully-empty case (no digits anywhere) is rejected
        // explicitly in `RegexSpec::try_from(&Captures)`.
        Regex::new(r"^(?<sign_left>MI|S)?(?<currency_left>L|\$)?(?<numbers>[09G,]*)(?<dot>[.D])?(?<decimals>[09]+)?(?<currency_right>L|\$)?(?<sign_right>PR|MI|S)?$")
            .map_err(|e| exec_datafusion_err!("Failed to compile regex: {e}"))
            .unwrap()
});

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NumberComponents {
    pub numbers: String,
    pub decimals: Option<String>,
    pub precision: u8,
    pub scale: i8,
}

impl TryFrom<&RegexSpec> for NumberComponents {
    type Error = DataFusionError;

    fn try_from(format_spec: &RegexSpec) -> Result<Self, Self::Error> {
        let numbers: String = format_spec.numbers.replace(",", "").replace("G", "");
        let decimals: Option<String> = format_spec.decimals.clone();
        let scale = decimals.as_ref().map_or(0, |d| d.len()) as i8;
        let precision = numbers.len() as u8 + scale as u8;

        Ok(Self {
            numbers,
            decimals,
            precision,
            scale,
        })
    }
}

impl TryFrom<&Captures<'_>> for NumberComponents {
    type Error = DataFusionError;

    fn try_from(captures: &Captures<'_>) -> Result<Self, Self::Error> {
        // This is semantically incorrect, because captures can be from a different regex than the format pattern.
        // And RegexSpec is a struct assumed to be used with the format pattern.
        // However, in this case, we will use just numbers and decimals parts. So, it's ok.
        // Just keep in mind this in the future.
        let spec = RegexSpec::try_from(captures)?;
        let spec: NumberComponents = NumberComponents::try_from(&spec)?;
        Ok(spec)
    }
}

macro_rules! get_opt_capture_group {
    ($captures:expr_2021, $group_name:expr_2021) => {
        $captures
            .name($group_name)
            .map(|m| m.as_str().trim().to_string())
    };
}

macro_rules! get_capture_group {
    ($captures:expr_2021, $group_name:expr_2021) => {
        get_opt_capture_group!($captures, $group_name).ok_or_else(|| {
            exec_datafusion_err!(
                "Missing '{}' group in the format regex or not well formed.",
                $group_name
            )
        })
    };
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexSpec {
    pub left_sign: Option<String>,
    pub currency_left: Option<String>,
    pub numbers: String,
    pub dot: Option<String>,
    pub decimals: Option<String>,
    pub currency_right: Option<String>,
    pub right_sign: Option<String>,
}
impl TryFrom<&str> for RegexSpec {
    type Error = DataFusionError;

    fn try_from(format: &str) -> Result<Self, Self::Error> {
        // Spark uppercases the format before parsing, making the format tokens
        // (G, D, L, MI, PR, S) case-insensitive (e.g. `9g999` == `9G999`).
        let format = format.to_uppercase();
        let captures: Captures = match_regex(&format, &FORMAT_REGEX)?;
        RegexSpec::try_from(&captures)
    }
}

impl TryFrom<&Captures<'_>> for RegexSpec {
    type Error = DataFusionError;

    fn try_from(captures: &Captures<'_>) -> Result<Self, Self::Error> {
        let numbers: String = get_capture_group!(captures, "numbers")?;
        let decimals: Option<String> = get_opt_capture_group!(captures, "decimals");

        // Spark rejects formats with no digit at all (`S`, `.`, `$`, …).
        // `numbers` may be empty (e.g. `.99`), but then `decimals` must be present.
        if numbers.is_empty() && decimals.is_none() {
            return exec_err!(
                "The format is invalid. The format string requires at least one number digit."
            );
        }

        // Thousands separators (`,` / `G`) must have digits on both sides
        // (`INVALID_FORMAT.CONT_THOUSANDS_SEPS`): reject `,999`, `999,`, `9,,9`.
        validate_thousands_separator_positions(&numbers)?;

        // Spark: currency characters must appear BEFORE digits. `$999` is valid;
        // `999$` raises `INVALID_FORMAT.CUR_MUST_BEFORE_DIGIT`. The FORMAT_REGEX
        // accepts a trailing currency because the grammar is shared with other
        // SQL dialects, so reject it explicitly here for Spark compatibility.
        let currency_right: Option<String> = get_opt_capture_group!(captures, "currency_right");
        if currency_right.is_some() {
            return exec_err!(
                "The format is invalid. Currency characters must appear before digits in the number format."
            );
        }

        Ok(Self {
            left_sign: get_opt_capture_group!(captures, "sign_left"),
            currency_left: get_opt_capture_group!(captures, "currency_left"),
            numbers,
            dot: get_opt_capture_group!(captures, "dot"),
            decimals,
            currency_right,
            right_sign: get_opt_capture_group!(captures, "sign_right"),
        })
    }
}

/// Reject thousands-separator positions that Spark classifies as
/// `INVALID_FORMAT.CONT_THOUSANDS_SEPS` — separators (`,` or `G`) must have
/// digits on both sides. Rejects leading (`,999`), trailing (`999,`), and
/// consecutive (`9,,9`) separators.
fn validate_thousands_separator_positions(numbers: &str) -> Result<()> {
    let bytes = numbers.as_bytes();
    if bytes.is_empty() {
        return Ok(());
    }
    let is_sep = |b: u8| b == b',' || b == b'G';
    if is_sep(bytes[0]) || is_sep(bytes[bytes.len() - 1]) {
        return exec_err!(
            "The format is invalid: '{numbers}'. Thousands separators (, or G) must have digits on both sides."
        );
    }
    for &[left, right] in bytes.array_windows::<2>() {
        if is_sep(left) && is_sep(right) {
            return exec_err!(
                "The format is invalid: '{numbers}'. Thousands separators (, or G) must have digits between them."
            );
        }
    }
    Ok(())
}

pub fn match_regex<'a>(value: &'a str, regex: &Regex) -> Result<Captures<'a>> {
    // Check if the format matches the regex pattern
    if regex.is_match(value) {
        regex
            .captures(value)
            .ok_or_else(|| exec_datafusion_err!("Value '{value}' does not match the format"))
    } else {
        exec_err!("String '{value}' does not match the expected regex pattern.")
    }
}
