use chrono::format::{parse_and_remainder, Item, Numeric, Pad, Parsed};
use chrono::{NaiveDate, ParseError};
use datafusion_common::{exec_datafusion_err, Result};

pub fn parse_date(s: &str) -> Result<NaiveDate> {
    const DATE_ITEMS: &[Item<'static>] = &[
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Space(""),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
    ];

    let error = |e: ParseError| exec_datafusion_err!("invalid date: {e}: {s}");
    let mut parsed = Parsed::new();
    let _ = parse_and_remainder(&mut parsed, s, DATE_ITEMS.iter()).map_err(error)?;
    parsed.to_naive_date().map_err(error)
}
