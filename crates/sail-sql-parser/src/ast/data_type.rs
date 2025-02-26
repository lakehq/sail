use sail_sql_macro::TreeParser;

use crate::ast::identifier::Ident;
use crate::ast::keywords::{
    Array, Bigint, Binary, Bool, Boolean, Byte, Bytea, Char, Character, Comment, Date, Date32,
    Date64, Day, Dec, Decimal, Double, Float, Float32, Float64, Hour, Int, Int16, Int32, Int64,
    Int8, Integer, Interval, Local, Long, Map, Minute, Month, Not, Null, Numeric, Second, Short,
    Smallint, Struct, Text, Time, Timestamp, TimestampLtz, TimestampNtz, Tinyint, To, Uint16,
    Uint32, Uint64, Uint8, Unsigned, Varchar, Void, With, Without, Year, Zone,
};
use crate::ast::literal::{IntegerLiteral, StringLiteral};
use crate::ast::operator::{
    Colon, Comma, GreaterThan, LeftParenthesis, LessThan, RightParenthesis,
};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;
use crate::token::TokenLabel;

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType", label = TokenLabel::DataType)]
pub enum DataType {
    Null(Null),
    Void(Void),
    Boolean(Boolean),
    Bool(Bool),
    TinyInt(Option<Unsigned>, Tinyint),
    SmallInt(Option<Unsigned>, Smallint),
    Int(Option<Unsigned>, Int),
    BigInt(Option<Unsigned>, Bigint),
    Byte(Option<Unsigned>, Byte),
    Short(Option<Unsigned>, Short),
    Integer(Option<Unsigned>, Integer),
    Long(Option<Unsigned>, Long),
    Int8(Int8),
    Int16(Int16),
    Int32(Int32),
    Int64(Int64),
    UInt8(Uint8),
    UInt16(Uint16),
    UInt32(Uint32),
    UInt64(Uint64),
    Binary(Binary),
    Bytea(Bytea),
    Float(Float),
    Double(Double),
    Float32(Float32),
    Float64(Float64),
    #[allow(clippy::type_complexity)]
    Decimal(
        DecimalType,
        Option<(
            LeftParenthesis,
            IntegerLiteral,
            Option<(Comma, IntegerLiteral)>,
            RightParenthesis,
        )>,
    ),
    Char(
        Char,
        Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
    ),
    Character(
        Character,
        Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
    ),
    Varchar(Varchar, LeftParenthesis, IntegerLiteral, RightParenthesis),
    String(crate::ast::keywords::String),
    Text(Text),
    Timestamp(
        Timestamp,
        Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
        Option<TimezoneType>,
    ),
    TimestampNtz(
        TimestampNtz,
        Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
    ),
    TimestampLtz(
        TimestampLtz,
        Option<(LeftParenthesis, IntegerLiteral, RightParenthesis)>,
    ),
    Date(Date),
    Date32(Date32),
    Date64(Date64),
    Interval(IntervalType),
    Array(
        Array,
        LessThan,
        #[parser(function = |d, _| boxed(d))] Box<DataType>,
        GreaterThan,
    ),
    Struct(
        Struct,
        LessThan,
        #[parser(function = |d, o| sequence(compose(d, o), unit(o)).or_not())]
        Option<Sequence<StructField, Comma>>,
        GreaterThan,
    ),
    Map(
        Map,
        LessThan,
        #[parser(function = |d, _| boxed(d))] Box<DataType>,
        Comma,
        #[parser(function = |d, _| boxed(d))] Box<DataType>,
        GreaterThan,
    ),
}

#[derive(Debug, Clone, TreeParser)]
pub enum DecimalType {
    Decimal(Decimal),
    Dec(Dec),
    Numeric(Numeric),
}

#[derive(Debug, Clone, TreeParser)]
pub enum IntervalType {
    YearMonth(
        Interval,
        IntervalYearMonthUnit,
        Option<(To, IntervalYearMonthUnit)>,
    ),
    DayTime(
        Interval,
        IntervalDayTimeUnit,
        Option<(To, IntervalDayTimeUnit)>,
    ),
    Default(Interval),
}

#[derive(Debug, Clone, TreeParser)]
pub enum IntervalYearMonthUnit {
    Year(Year),
    Month(Month),
}

#[derive(Debug, Clone, TreeParser)]
pub enum IntervalDayTimeUnit {
    Day(Day),
    Hour(Hour),
    Minute(Minute),
    Second(Second),
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, TreeParser)]
pub enum TimezoneType {
    WithTimeZone(With, Time, Zone),
    WithoutTimeZone(Without, Time, Zone),
    WithLocalTimeZone(With, Local, Time, Zone),
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub struct StructField {
    pub identifier: Ident,
    pub colon: Option<Colon>,
    #[parser(function = |d, _| d)]
    pub data_type: DataType,
    pub not_null: Option<(Not, Null)>,
    pub comment: Option<(Comment, StringLiteral)>,
}
