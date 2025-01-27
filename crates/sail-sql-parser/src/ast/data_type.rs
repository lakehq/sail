use sail_sql_macro::TreeParser;

use crate::ast::identifier::Ident;
use crate::ast::keywords::{
    Array, Bigint, Binary, Bool, Boolean, Byte, Bytea, Char, Character, Comment, Date, Date32,
    Date64, Decimal, Double, Float, Float32, Float64, Hour, Int, Int16, Int32, Int64, Int8,
    Integer, Interval, Local, Long, Map, Minute, Month, Not, Null, Second, Short, Smallint, Struct,
    Text, Time, Timestamp, Tinyint, To, Uint16, Uint32, Uint64, Uint8, Unsigned, Varchar, Void,
    With, Without, Year, Zone,
};
use crate::ast::literal::{IntegerLiteral, StringLiteral};
use crate::ast::operator::{
    Colon, Comma, GreaterThan, LeftParenthesis, LessThan, RightParenthesis,
};
use crate::combinator::{boxed, compose, sequence, unit};
use crate::common::Sequence;

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
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
        Decimal,
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
    Date(Date),
    Date32(Date32),
    Date64(Date64),
    Interval(IntervalType),
    Array(
        Array,
        LessThan,
        #[parser(function = |x, _| boxed(x))] Box<DataType>,
        GreaterThan,
    ),
    Struct(
        Struct,
        LessThan,
        #[parser(function = |x, o| sequence(compose(x, o), unit(o)))] Sequence<StructField, Comma>,
        GreaterThan,
    ),
    Map(
        Map,
        LessThan,
        #[parser(function = |x, _| boxed(x))] Box<DataType>,
        Comma,
        #[parser(function = |x, _| boxed(x))] Box<DataType>,
        GreaterThan,
    ),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum IntervalType {
    Default(Interval),
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
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum IntervalYearMonthUnit {
    Year(Year),
    Month(Month),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
pub enum IntervalDayTimeUnit {
    Day(Year),
    Hour(Hour),
    Minute(Minute),
    Second(Second),
}

#[allow(unused)]
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, TreeParser)]
pub enum TimezoneType {
    WithTimeZone(With, Time, Zone),
    WithoutTimeZone(Without, Time, Zone),
    WithLocalTimeZone(With, Local, Time, Zone),
}

#[allow(unused)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub struct StructField {
    pub identifier: Ident,
    pub colon: Option<Colon>,
    #[parser(function = |x, _| x)]
    pub data_type: DataType,
    pub not_null: Option<(Not, Null)>,
    pub comment: Option<(Comment, StringLiteral)>,
}
