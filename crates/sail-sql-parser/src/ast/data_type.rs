use chumsky::prelude::choice;
use chumsky::Parser;
use sail_sql_macro::TreeParser;

use crate::ast::keywords::{
    Array, Bigint, Binary, Bool, Boolean, Byte, Bytea, Char, Character, Date, Date32, Date64,
    Decimal, Double, Float, Float32, Float64, Int, Int16, Int32, Int64, Int8, Integer, Interval,
    Local, Long, Map, Null, Short, Smallint, Struct, Time, Timestamp, Tinyint, Unsigned, Varchar,
    Void, With, Without, Zone,
};
use crate::ast::operator::{Comma, GreaterThan, LeftParenthesis, LessThan, RightParenthesis};
use crate::ast::value::IntegerValue;
use crate::container::{boxed, sequence, Sequence};
use crate::token::Token;
use crate::tree::TreeParser;

#[allow(unused)]
#[derive(Debug, TreeParser)]
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
        Option<TimezoneConfiguration>,
    ),
    Date(Date),
    Date32(Date32),
    Date64(Date64),
    Interval(Interval),
    Array(
        Array,
        LessThan,
        #[parser(function = |x| boxed(x))] Box<DataType>,
        GreaterThan,
    ),
    Struct(
        Struct,
        LessThan,
        #[parser(function = |x| boxed(sequence(x, Comma::parser(()))))]
        Box<Sequence<DataType, Comma>>,
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
pub enum TimezoneConfiguration {
    WithTimeZone(With, Time, Zone),
    WithoutTimeZone(Without, Time, Zone),
    WithLocalTimeZone(With, Local, Time, Zone),
}
