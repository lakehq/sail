use crate::spec::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Binary(Vec<u8>),
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal {
        value: String,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    String(String),
    Date {
        days: i32,
    },
    Timestamp {
        microseconds: i64,
    },
    TimestampNtz {
        microseconds: i64,
    },
    CalendarInterval {
        months: i32,
        days: i32,
        microseconds: i64,
    },
    YearMonthInterval {
        months: i32,
    },
    DayTimeInterval {
        microseconds: i64,
    },
    Array {
        element_type: DataType,
        elements: Vec<Literal>,
    },
    Map {
        key_type: DataType,
        value_type: DataType,
        keys: Vec<Literal>,
        values: Vec<Literal>,
    },
    Struct {
        struct_type: DataType,
        elements: Vec<Literal>,
    },
}
