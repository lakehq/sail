use datafusion_common::Result;

use super::parser::parse_datetime_pattern;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFormat {
    pub(crate) items: Vec<DateTimeItem>,
    pub(crate) locale: LocaleSpec,
    pub(crate) resolver_style: ResolverStyle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum LocaleSpec {
    #[default]
    Default,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ResolverStyle {
    Strict,
    #[default]
    Smart,
    Lenient,
}

impl DateTimeFormat {
    pub fn for_parsing(pattern: &str) -> Result<Self> {
        parse_datetime_pattern(pattern, PatternUse::Parsing)
    }

    pub fn for_formatting(pattern: &str) -> Result<Self> {
        parse_datetime_pattern(pattern, PatternUse::Formatting)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PatternUse {
    Parsing,
    Formatting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DateTimeItem {
    Literal(String),
    Field(DateTimeFieldSpec),
    Fraction(FractionSpec),
    Zone(ZoneSpec),
    Optional(Vec<DateTimeItem>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DateTimeFieldSpec {
    pub(crate) kind: DateTimeField,
    pub(crate) width: usize,
    pub(crate) style: FieldStyle,
    pub(crate) sign_style: SignStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DateTimeField {
    Era,
    ProlepticYear,
    YearOfEra,
    WeekBasedYear,
    QuarterOfYear,
    MonthOfYear,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    WeekOfWeekBasedYear,
    WeekOfMonth,
    AlignedWeekOfMonth,
    AmPmOfDay,
    ClockHourOfAmPm,
    HourOfAmPm,
    ClockHourOfDay,
    HourOfDay,
    MinuteOfHour,
    SecondOfMinute,
    MilliOfDay,
    NanoOfSecond,
    NanoOfDay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[expect(dead_code)]
pub(crate) enum FieldStyle {
    Numeric,
    TextShort,
    TextFull,
    TextNarrow,
    StandaloneTextShort,
    StandaloneTextFull,
    LocalizedNumeric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[expect(dead_code)]
pub(crate) enum SignStyle {
    Normal,
    Never,
    NotNegative,
    ExceedsPad,
    Always,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FractionSpec {
    pub(crate) field: FractionField,
    pub(crate) min_width: usize,
    pub(crate) max_width: usize,
    pub(crate) decimal_point: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[expect(dead_code)]
pub(crate) enum FractionField {
    NanoOfSecond,
    NanoOfDay,
    MilliOfDay,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ZoneSpec {
    pub(crate) kind: ZoneField,
    pub(crate) width: usize,
    pub(crate) zero_as_z: bool,
    pub(crate) style: ZoneStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ZoneField {
    IsoOffset,
    Rfc822Offset,
    LocalizedOffset,
    ZoneId,
    ZoneName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ZoneStyle {
    Short,
    Full,
    Id,
}
