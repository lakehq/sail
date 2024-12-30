use sail_sql_macro::TreeParser;

use crate::ast::identifier::Ident;
use crate::ast::keywords::{
    Array, Bigint, Binary, Bool, Boolean, Byte, Bytea, Char, Character, Comment, Date, Date32,
    Date64, Decimal, Double, Float, Float32, Float64, Hour, Int, Int16, Int32, Int64, Int8,
    Integer, Interval, Local, Long, Map, Minute, Month, Not, Null, Second, Short, Smallint, Struct,
    Time, Timestamp, Tinyint, To, Unsigned, Varchar, Void, With, Without, Year, Zone,
};
use crate::ast::literal::StringLiteral;
use crate::ast::operator::{
    Colon, Comma, GreaterThan, LeftParenthesis, LessThan, RightParenthesis,
};
use crate::ast::value::IntegerValue;
use crate::container::{boxed, sequence, Sequence};

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
    Int8(Option<Unsigned>, Int8),
    Int16(Option<Unsigned>, Int16),
    Int32(Option<Unsigned>, Int32),
    Int64(Option<Unsigned>, Int64),
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
            IntegerValue,
            Option<(Comma, IntegerValue)>,
            RightParenthesis,
        )>,
    ),
    Char(
        Char,
        Option<(LeftParenthesis, IntegerValue, RightParenthesis)>,
    ),
    Character(
        Character,
        Option<(LeftParenthesis, IntegerValue, RightParenthesis)>,
    ),
    Varchar(Varchar, LeftParenthesis, IntegerValue, RightParenthesis),
    String(crate::ast::keywords::String),
    Timestamp(
        Timestamp,
        Option<(LeftParenthesis, IntegerValue, RightParenthesis)>,
        Option<TimezoneType>,
    ),
    Date(Date),
    Date32(Date32),
    Date64(Date64),
    Interval(IntervalType),
    Array(
        Array,
        LessThan,
        #[parser(function = |x| boxed(x))] Box<DataType>,
        GreaterThan,
    ),
    Struct(
        Struct,
        LessThan,
        #[parser(function = |x| boxed(sequence(StructField::parser(x), Comma::parser(()))))]
        Box<Sequence<StructField, Comma>>,
        GreaterThan,
    ),
    Map(
        Map,
        LessThan,
        #[parser(function = |x| boxed(x))] Box<DataType>,
        Comma,
        #[parser(function = |x| boxed(x))] Box<DataType>,
        GreaterThan,
    ),
}

#[allow(unused)]
#[allow(clippy::enum_variant_names)]
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
}

#[allow(unused)]
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, TreeParser)]
pub enum IntervalYearMonthUnit {
    Year(Year),
    Month(Month),
}

#[allow(unused)]
#[allow(clippy::enum_variant_names)]
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
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "DataType")]
pub struct StructField {
    pub identifier: Ident,
    pub colon: Option<Colon>,
    #[parser(function = |x| x)]
    pub data_type: DataType,
    pub not_null: Option<(Not, Null)>,
    pub comment: Option<(Comment, StringLiteral)>,
}
