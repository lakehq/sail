use std::collections::HashMap;

use datafusion_common::Result;

mod formatting;
mod locale;
mod parser;
mod parsing;
mod pattern;

pub use formatting::{DateTimeFormatInput, TimePrecision, TimeZoneDisplay, TimestampKind};
pub use parsing::ParsedDateTime;
pub use pattern::{DateTimeFormat, LocaleSpec, ResolverStyle};

pub(crate) fn cached_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
    parse: impl FnOnce(&str) -> Result<DateTimeFormat>,
) -> Result<&'a DateTimeFormat> {
    if cache.contains_key(pattern) {
        return Ok(&cache[pattern]);
    }
    cache.insert(pattern.to_owned(), parse(pattern)?);
    Ok(&cache[pattern])
}
