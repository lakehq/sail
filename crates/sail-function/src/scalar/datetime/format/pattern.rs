use datafusion_common::Result;

use super::parser::parse_datetime_pattern;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFormat {
    pub(crate) items: Vec<DateTimeItem>,
    pub(crate) predefined: Option<PredefinedFormatter>,
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
    pub fn parse(pattern: &str) -> Result<Self> {
        if let Some(predefined) = PredefinedFormatter::from_name(pattern) {
            let mut format = parse_datetime_pattern(predefined.base_pattern())?;
            if predefined.uses_variable_fraction() {
                make_fraction_width_variable(&mut format.items);
            }
            format.predefined = Some(predefined);
            format.locale = LocaleSpec::Default;
            return Ok(format);
        }
        let mut format = parse_datetime_pattern(pattern)?;
        format.locale = LocaleSpec::Default;
        Ok(format)
    }
}

fn make_fraction_width_variable(items: &mut [DateTimeItem]) {
    for item in items {
        match item {
            DateTimeItem::Fraction(spec) => {
                spec.min_width = 1;
            }
            DateTimeItem::Optional(items) => make_fraction_width_variable(items),
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PredefinedFormatter {
    BasicIsoDate,
    IsoLocalDate,
    IsoOffsetDate,
    IsoDate,
    IsoLocalTime,
    IsoOffsetTime,
    IsoTime,
    IsoLocalDateTime,
    IsoOffsetDateTime,
    IsoZonedDateTime,
    IsoDateTime,
    IsoOrdinalDate,
    IsoWeekDate,
    IsoInstant,
    Rfc1123DateTime,
}

impl PredefinedFormatter {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "BASIC_ISO_DATE" => Some(Self::BasicIsoDate),
            "ISO_LOCAL_DATE" => Some(Self::IsoLocalDate),
            "ISO_OFFSET_DATE" => Some(Self::IsoOffsetDate),
            "ISO_DATE" => Some(Self::IsoDate),
            "ISO_LOCAL_TIME" => Some(Self::IsoLocalTime),
            "ISO_OFFSET_TIME" => Some(Self::IsoOffsetTime),
            "ISO_TIME" => Some(Self::IsoTime),
            "ISO_LOCAL_DATE_TIME" => Some(Self::IsoLocalDateTime),
            "ISO_OFFSET_DATE_TIME" => Some(Self::IsoOffsetDateTime),
            "ISO_ZONED_DATE_TIME" => Some(Self::IsoZonedDateTime),
            "ISO_DATE_TIME" => Some(Self::IsoDateTime),
            "ISO_8601" => Some(Self::IsoDateTime),
            "ISO_ORDINAL_DATE" => Some(Self::IsoOrdinalDate),
            "ISO_WEEK_DATE" => Some(Self::IsoWeekDate),
            "ISO_INSTANT" => Some(Self::IsoInstant),
            "RFC_1123_DATE_TIME" => Some(Self::Rfc1123DateTime),
            _ => None,
        }
    }

    fn base_pattern(self) -> &'static str {
        match self {
            Self::BasicIsoDate => "yyyyMMdd",
            Self::IsoLocalDate => "yyyy-MM-dd",
            Self::IsoOffsetDate => "yyyy-MM-ddXXX",
            Self::IsoDate => "yyyy-MM-dd[XXX]",
            Self::IsoLocalTime => "HH:mm:ss[.SSSSSSSSS]",
            Self::IsoOffsetTime => "HH:mm:ss[.SSSSSSSSS]XXX",
            Self::IsoTime => "HH:mm:ss[.SSSSSSSSS][XXX]",
            Self::IsoLocalDateTime => "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]",
            Self::IsoOffsetDateTime | Self::IsoZonedDateTime => {
                "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"
            }
            Self::IsoDateTime => "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS][XXX]",
            Self::IsoOrdinalDate => "yyyy-DDD",
            Self::IsoWeekDate => "YYYY-'W'ww-e",
            Self::IsoInstant => "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX",
            Self::Rfc1123DateTime => "EEE, d MMM yyyy HH:mm:ss z",
        }
    }

    fn uses_variable_fraction(self) -> bool {
        matches!(
            self,
            Self::IsoLocalTime
                | Self::IsoOffsetTime
                | Self::IsoTime
                | Self::IsoLocalDateTime
                | Self::IsoOffsetDateTime
                | Self::IsoZonedDateTime
                | Self::IsoDateTime
                | Self::IsoInstant
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DateTimeItem {
    Literal(String),
    Field(DateTimeFieldSpec),
    Fraction(FractionSpec),
    Zone(ZoneSpec),
    Optional(Vec<DateTimeItem>),
    PadNext { width: usize, pad_char: char },
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
    Offset,
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
