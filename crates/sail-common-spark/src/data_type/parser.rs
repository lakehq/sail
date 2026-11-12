//! Parsers for the type strings embedded in Spark JSON schemas.

use chumsky::prelude::*;

use super::common::{DayTimeIntervalField, YearMonthIntervalField};

type ParserExtra<'a> = extra::Err<Rich<'a, char>>;

pub(crate) fn parse<'a, T>(
    input: &'a str,
    parser: impl Parser<'a, &'a str, T, ParserExtra<'a>>,
) -> Result<T, String> {
    parser
        .then_ignore(end())
        .parse(input)
        .into_result()
        .map_err(|errors| {
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        })
}

fn create_nonnegative_integer_parser<'a>() -> impl Parser<'a, &'a str, i32, ParserExtra<'a>> + Clone
{
    text::digits(10).to_slice().try_map(|input: &str, span| {
        input
            .parse()
            .map_err(|_| Rich::custom(span, "integer out of range"))
    })
}

fn create_signed_integer_parser<'a>() -> impl Parser<'a, &'a str, i32, ParserExtra<'a>> + Clone {
    just('-')
        .or_not()
        .then(text::digits(10))
        .to_slice()
        .try_map(|input: &str, span| {
            input
                .parse()
                .map_err(|_| Rich::custom(span, "integer out of range"))
        })
}

fn parenthesized<'a, T>(
    parser: impl Parser<'a, &'a str, T, ParserExtra<'a>> + Clone,
) -> impl Parser<'a, &'a str, T, ParserExtra<'a>> + Clone {
    parser.padded().delimited_by(just('('), just(')'))
}

pub(crate) fn create_time_parser<'a>() -> impl Parser<'a, &'a str, Option<i32>, ParserExtra<'a>> {
    just("time").ignore_then(parenthesized(create_nonnegative_integer_parser()).or_not())
}

pub(crate) fn create_character_parser<'a>(
    type_name: &'static str,
) -> impl Parser<'a, &'a str, i32, ParserExtra<'a>> {
    just(type_name).ignore_then(parenthesized(create_nonnegative_integer_parser()))
}

pub(crate) fn create_decimal_parser<'a>() -> impl Parser<'a, &'a str, (i32, i32), ParserExtra<'a>> {
    just("decimal").ignore_then(parenthesized(
        create_nonnegative_integer_parser()
            .then_ignore(just(',').padded())
            .then(create_signed_integer_parser()),
    ))
}

fn create_srid_parser<'a>() -> impl Parser<'a, &'a str, i32, ParserExtra<'a>> + Clone {
    just("ANY").to(-1).or(create_nonnegative_integer_parser())
}

pub(crate) fn create_geometry_parser<'a>() -> impl Parser<'a, &'a str, i32, ParserExtra<'a>> {
    just("geometry").ignore_then(parenthesized(create_srid_parser()))
}

pub(crate) fn create_geography_parser<'a>()
-> impl Parser<'a, &'a str, (i32, String), ParserExtra<'a>> {
    let algorithm = any()
        .filter(|character: &char| character.is_alphanumeric() || *character == '_')
        .repeated()
        .at_least(1)
        .collect::<String>();
    just("geography")
        .ignore_then(parenthesized(
            create_srid_parser().then(just(',').padded().ignore_then(algorithm).or_not()),
        ))
        .map(|(srid, algorithm)| (srid, algorithm.unwrap_or_else(|| "spherical".to_string())))
}

pub(crate) fn create_day_time_interval_field_parser<'a>()
-> impl Parser<'a, &'a str, DayTimeIntervalField, ParserExtra<'a>> + Clone {
    choice((
        just("day").to(DayTimeIntervalField::Day),
        just("hour").to(DayTimeIntervalField::Hour),
        just("minute").to(DayTimeIntervalField::Minute),
        just("second").to(DayTimeIntervalField::Second),
    ))
}

pub(crate) fn create_year_month_interval_field_parser<'a>()
-> impl Parser<'a, &'a str, YearMonthIntervalField, ParserExtra<'a>> + Clone {
    just("year")
        .to(YearMonthIntervalField::Year)
        .or(just("month").to(YearMonthIntervalField::Month))
}

pub(crate) fn create_interval_parser<'a, T>(
    field_parser: impl Parser<'a, &'a str, T, ParserExtra<'a>> + Clone,
) -> impl Parser<'a, &'a str, (Option<T>, Option<T>), ParserExtra<'a>> {
    let whitespace = text::whitespace().at_least(1);
    just("interval")
        .then_ignore(whitespace)
        .ignore_then(field_parser.clone())
        .then(
            whitespace
                .ignore_then(just("to"))
                .then_ignore(whitespace)
                .ignore_then(field_parser)
                .or_not(),
        )
        .map(|(start, end)| (Some(start), end))
}

#[cfg(test)]
mod tests {
    use crate::data_type::JsonDataType;

    #[test]
    fn parses_parameterized_types_and_intervals() -> Result<(), Box<dyn std::error::Error>> {
        for schema in [
            "time",
            "time( 9 )",
            "char(010)",
            "varchar( 20 )",
            "decimal( 10, -2 )",
            "geometry(ANY)",
            "geometry( 4326 )",
            "geography(ANY)",
            "geography( 4326 , spherical )",
            "interval day",
            "interval hour\tto\nsecond",
            "interval year to month",
        ] {
            let data_type: JsonDataType = serde_json::from_value(serde_json::json!(schema))?;
            let serialized = serde_json::to_string(&data_type)?;
            assert_eq!(data_type, serde_json::from_str(&serialized)?, "{schema}");
        }
        Ok(())
    }

    #[test]
    fn rejects_incomplete_or_malformed_types() {
        for schema in [
            "time()",
            "char(-1)",
            "varchar(20",
            "decimal(10,2,3)",
            "decimal(10,-)",
            "geometry(any)",
            "geography(4326,)",
            "interval day to month",
            "interval year to day",
            "interval day to",
            "char(2147483648)",
            "time(9) trailing",
            "CHAR(10)",
            " time(6)",
        ] {
            assert!(
                serde_json::from_value::<JsonDataType>(serde_json::json!(schema)).is_err(),
                "{schema}"
            );
        }
    }
}
