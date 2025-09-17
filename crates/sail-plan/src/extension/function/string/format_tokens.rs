use datafusion_common::Result;

use crate::extension::function::string::spark_to_number::RegexSpec;

/// Spark-style format tokens
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatToken {
    Zero,           // '0' required digit
    Nine,           // '9' optional digit
    DecimalPoint,   // '.' or 'D'
    GroupSeparator, // ',' or 'G'
    SignLeading,    // 'S' or '-' leading
    SignTrailing,   // 'S' or '-' trailing
    Currency,       // '$'
    Literal(char),  // any literal character
}

/// Parses a format string into a vector of `FormatToken`s for `format_number`.
///
/// This function reads each character of the format string and converts it into
/// a `FormatToken`. Supported characters include:
/// - '0' → `FormatToken::Zero` (required digit)
/// - '9' → `FormatToken::Nine` (optional digit)
/// - '.' → `FormatToken::DecimalPoint`
/// - ',' → `FormatToken::GroupSeparator`
/// - 'S' → `FormatToken::SignTrailing` (can be extended to SignLeading)
/// - '$' → `FormatToken::Currency`
/// - Any other character → `FormatToken::Literal(c)`
///
/// # Parameters
/// - `fmt`: The format string to parse (e.g., `"999,999.99S"`).
///
/// # Returns
/// `Result<Vec<FormatToken>>` containing the parsed tokens or an error if parsing fails.
///
/// # Example
/// ```
/// let fmt = "999,999.99S";
/// let tokens = parse_format_string(fmt).unwrap();
/// assert_eq!(tokens.len(), 11);
/// ```
pub fn tokenize_format(fmt: &str) -> Result<Vec<FormatToken>> {
    // Validate using RegexSpec from spark_to_number
    let _ = RegexSpec::try_from(fmt)?;

    let mut tokens = Vec::new();
    let chars: Vec<char> = fmt.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        match c {
            '0' => tokens.push(FormatToken::Zero),
            '9' => tokens.push(FormatToken::Nine),
            '.' | 'D' => tokens.push(FormatToken::DecimalPoint),
            ',' | 'G' => tokens.push(FormatToken::GroupSeparator),
            'S' | '-' => {
                if i == 0 {
                    tokens.push(FormatToken::SignLeading);
                } else {
                    tokens.push(FormatToken::SignTrailing);
                }
            }
            '$' => tokens.push(FormatToken::Currency),
            other => tokens.push(FormatToken::Literal(other)),
        }
    }
    Ok(tokens)
}
