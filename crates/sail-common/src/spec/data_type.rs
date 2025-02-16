use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

pub const ARROW_DECIMAL128_MAX_PRECISION: u8 = arrow_schema::DECIMAL128_MAX_PRECISION;
pub const ARROW_DECIMAL128_MAX_SCALE: i8 = arrow_schema::DECIMAL128_MAX_SCALE;
pub const ARROW_DECIMAL256_MAX_PRECISION: u8 = arrow_schema::DECIMAL256_MAX_PRECISION;
pub const ARROW_DECIMAL256_MAX_SCALE: i8 = arrow_schema::DECIMAL256_MAX_SCALE;

/// Native Sail data types that convert to Arrow types.
/// Types directly match to [`arrow_schema::DataType`] variants when there is a corresponding type.
/// Additionally, custom data types are supported for cases not covered by Arrow.
///
/// The style of expressing the type may not always be exactly the same as Arrow's.
/// The spec is designed to have an easy-to-read JSON representation,
/// making language interoperability easier in the future.
/// This is achieved by eliminating the use of tuple structs and using named fields instead.
///
/// Some comments for the enum variants are copied from [`arrow_schema::DataType`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DataType {
    /// Null type.
    /// Corresponds to [`arrow_schema::DataType::Null`].
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    /// Corresponds to [`arrow_schema::DataType::Boolean`].
    Boolean,
    /// A signed 8-bit integer.
    /// Corresponds to [`arrow_schema::DataType::Int8`].
    Int8,
    /// A signed 16-bit integer.
    /// Corresponds to [`arrow_schema::DataType::Int16`].
    Int16,
    /// A signed 32-bit integer.
    /// Corresponds to [`arrow_schema::DataType::Int32`].
    Int32,
    /// A signed 64-bit integer.
    /// Corresponds to [`arrow_schema::DataType::Int64`].
    Int64,
    /// An unsigned 8-bit integer.
    /// Corresponds to [`arrow_schema::DataType::UInt8`].
    UInt8,
    /// An unsigned 16-bit integer.
    /// Corresponds to [`arrow_schema::DataType::UInt16`].
    UInt16,
    /// An unsigned 32-bit integer.
    /// Corresponds to [`arrow_schema::DataType::UInt32`].
    UInt32,
    /// An unsigned 64-bit integer.
    /// Corresponds to [`arrow_schema::DataType::UInt64`].
    UInt64,
    /// A 16-bit floating point number.
    /// Corresponds to [`arrow_schema::DataType::Float16`].
    Float16,
    /// A 32-bit floating point number.
    /// Corresponds to [`arrow_schema::DataType::Float32`].
    Float32,
    /// A 64-bit floating point number.
    /// Corresponds to [`arrow_schema::DataType::Float64`].
    Float64,
    /// A timestamp with an optional timezone.
    /// Corresponds to [`arrow_schema::DataType::Timestamp`].
    Timestamp {
        time_unit: TimeUnit,
        timezone_info: TimeZoneInfo,
    },
    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01) in days.
    /// Corresponds to [`arrow_schema::DataType::Date32`].
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01) in milliseconds.
    /// Corresponds to [`arrow_schema::DataType::Date64`].
    Date64,
    /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either seconds or milliseconds.
    /// Corresponds to [`arrow_schema::DataType::Time32`].
    Time32 {
        time_unit: TimeUnit,
    },
    /// A signed 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either microseconds or nanoseconds.
    /// Corresponds to [`arrow_schema::DataType::Time64`].
    Time64 {
        time_unit: TimeUnit,
    },
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    /// Corresponds to [`arrow_schema::DataType::Duration`].
    Duration {
        time_unit: TimeUnit,
    },
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during daylight savings time transitions).
    /// Corresponds to [`arrow_schema::DataType::Interval`].
    Interval {
        interval_unit: IntervalUnit,
        start_field: Option<IntervalFieldType>,
        end_field: Option<IntervalFieldType>,
    },
    /// Opaque binary data of variable length.
    /// A single Binary array can store up to [`i32::MAX`] bytes of binary data in total.
    /// Corresponds to [`arrow_schema::DataType::Binary`].
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    /// Corresponds to [`arrow_schema::DataType::FixedSizeBinary`].
    FixedSizeBinary {
        size: i32,
    },
    /// Opaque binary data of variable length and 64-bit offsets.
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes of binary data in total.
    /// Corresponds to [`arrow_schema::DataType::LargeBinary`].
    LargeBinary,
    /// Opaque binary data of variable length.
    /// Logically the same as [`DataType::Binary`].
    /// Corresponds to [`arrow_schema::DataType::BinaryView`].
    BinaryView,
    /// A variable-length string in Unicode with UTF-8 encoding.
    /// A single Utf8 array can store up to [`i32::MAX`] bytes of string data in total.
    /// Corresponds to [`arrow_schema::DataType::Utf8`].
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes of string data in total.
    /// Corresponds to [`arrow_schema::DataType::LargeUtf8`].
    LargeUtf8,
    /// A variable-length string in Unicode with UTF-8 encoding
    /// Logically the same as [`DataType::Utf8`].
    /// Corresponds to [`arrow_schema::DataType::Utf8View`].
    Utf8View,
    /// A list of some logical data type with variable length.
    /// A single List array can store up to [`i32::MAX`] elements in total.
    /// Corresponds to [`arrow_schema::DataType::List`].
    List {
        data_type: Box<DataType>,
        nullable: bool,
    },
    /// A list of some logical data type with fixed length.
    /// Corresponds to [`arrow_schema::DataType::FixedSizeList`].
    FixedSizeList {
        data_type: Box<DataType>,
        nullable: bool,
        length: i32,
    },
    /// A list of some logical data type with variable length and 64-bit offsets.
    /// A single LargeList array can store up to [`i64::MAX`] elements in total.
    /// Corresponds to [`arrow_schema::DataType::LargeList`].
    LargeList {
        data_type: Box<DataType>,
        nullable: bool,
    },
    /// A nested datatype that contains a number of sub-fields.
    /// Corresponds to [`arrow_schema::DataType::Struct`].
    Struct {
        fields: Fields,
    },
    /// A nested datatype that can represent slots of differing types.
    /// Corresponds to [`arrow_schema::DataType::Union`].
    Union {
        union_fields: UnionFields,
        union_mode: UnionMode,
    },
    /// A dictionary encoded array (`key_type`, `value_type`), where each array element is an index
    /// of `key_type` into an associated dictionary of `value_type`.
    /// Corresponds to [`arrow_schema::DataType::Dictionary`].
    Dictionary {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
    },
    /// Exact 128-bit width decimal value with precision and scale.
    /// Corresponds to [`arrow_schema::DataType::Decimal128`].
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    /// Exact 256-bit width decimal value with precision and scale.
    /// Corresponds to [`arrow_schema::DataType::Decimal256`].
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    /// A Map is a logical-nested type that is represented as
    /// `List<entries: Struct<key: K, value: V>>`.
    /// Corresponds to [`arrow_schema::DataType::Map`].
    Map {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        value_type_nullable: bool,
        keys_sorted: bool,
    },
    //
    // Everything below this line is not part of the Arrow specification.
    //
    UserDefined {
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
        sql_type: Box<DataType>,
    },
    /// Resolves to either [`DataType::Utf8`] or [`DataType::LargeUtf8`],
    /// based on `config.arrow_use_large_var_types`.
    ConfiguredUtf8 {
        utf8_type: Utf8Type,
    },
    ConfiguredBinary,
}

impl DataType {
    pub fn into_schema(self, default_field_name: &str, nullable: bool) -> Schema {
        let fields = match self {
            DataType::Struct { fields } => fields,
            x => Fields::from(vec![Field {
                name: default_field_name.to_string(),
                data_type: x,
                nullable,
                metadata: vec![],
            }]),
        };
        Schema { fields }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: Vec<(String, String)>,
}

/// A reference counted [`Field`].
/// The implementation is copied from [`arrow_schema::FieldRef`].
pub type FieldRef = Arc<Field>;

/// A cheaply cloneable, owned slice of [`FieldRef`].
/// The implementation is copied from [`arrow_schema::Fields`].
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Fields(Arc<[FieldRef]>);

impl Fields {
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<Field> for Fields {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<FieldRef> for Fields {
    fn from_iter<T: IntoIterator<Item = FieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for Fields {
    fn from(fields: Vec<Field>) -> Self {
        fields.into_iter().collect()
    }
}

impl From<Vec<FieldRef>> for Fields {
    fn from(fields: Vec<FieldRef>) -> Self {
        Self(fields.into())
    }
}

impl From<&[FieldRef]> for Fields {
    fn from(fields: &[FieldRef]) -> Self {
        Self(fields.into())
    }
}

impl<const N: usize> From<[FieldRef; N]> for Fields {
    fn from(fields: [FieldRef; N]) -> Self {
        Self(Arc::new(fields))
    }
}

impl Deref for Fields {
    type Target = [FieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a FieldRef;
    type IntoIter = std::slice::Iter<'a, FieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A cheaply cloneable, owned collection of [`FieldRef`] and their corresponding type IDs.
/// The implementation is copied from [`arrow_schema::UnionFields`].
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnionFields(Arc<[(i8, FieldRef)]>);

impl UnionFields {
    /// Create a new [`UnionFields`] with no fields.
    pub fn empty() -> Self {
        Self(Arc::from([]))
    }
}

impl Deref for UnionFields {
    type Target = [(i8, FieldRef)];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl FromIterator<(i8, FieldRef)> for UnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, FieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Sparse or dense union layouts.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = UnionMode::invalid))]
#[repr(i32)]
pub enum UnionMode {
    /// Sparse union layout.
    Sparse = 0,
    /// Dense union layout.
    Dense = 1,
}

impl UnionMode {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("interval union mode: {value}"))
    }
}

impl Display for UnionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnionMode::Sparse => write!(f, "Sparse"),
            UnionMode::Dense => write!(f, "Dense"),
        }
    }
}

/// YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO interval in SQL style.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = IntervalUnit::invalid))]
#[repr(i32)]
pub enum IntervalUnit {
    /// Indicates the number of elapsed whole months, stored as 4-byte integers.
    YearMonth = 0,
    /// Indicates the number of elapsed days and milliseconds,
    /// stored as 2 contiguous 32-bit integers (days, milliseconds) (8-bytes in total).
    DayTime = 1,
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// The values are stored contiguously in 16 byte blocks. Months and
    /// days are encoded as 32-bit integers and nanoseconds is encoded as a
    /// 64-bit integer. All integers are signed. Each field is independent
    /// (e.g. there is no constraint that nanoseconds have the same sign
    /// as days or that the quantity of nanoseconds represents less
    /// than a day's worth of time).
    MonthDayNano = 2,
}

impl IntervalUnit {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("interval unit field: {value}"))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = DayTimeIntervalField::invalid))]
#[repr(i32)]
pub enum DayTimeIntervalField {
    Day = 0,
    Hour = 1,
    Minute = 2,
    Second = 3,
}

impl DayTimeIntervalField {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("day time interval field: {value}"))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = YearMonthIntervalField::invalid))]
#[repr(i32)]
pub enum YearMonthIntervalField {
    Year = 0,
    Month = 1,
}

impl YearMonthIntervalField {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("year month interval field: {value}"))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromPrimitive,
)]
#[serde(rename_all = "camelCase")]
#[num_enum(error_type(name = CommonError, constructor = IntervalFieldType::invalid))]
#[repr(i32)]
pub enum IntervalFieldType {
    Year = 0,
    Month = 1,
    Day = 2,
    Hour = 3,
    Minute = 4,
    Second = 5,
}

impl IntervalFieldType {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("interval field type: {value}"))
    }
}

impl TryFrom<DayTimeIntervalField> for IntervalFieldType {
    type Error = CommonError;

    fn try_from(field_type: DayTimeIntervalField) -> CommonResult<IntervalFieldType> {
        match field_type {
            DayTimeIntervalField::Day => Ok(IntervalFieldType::Day),
            DayTimeIntervalField::Hour => Ok(IntervalFieldType::Hour),
            DayTimeIntervalField::Minute => Ok(IntervalFieldType::Minute),
            DayTimeIntervalField::Second => Ok(IntervalFieldType::Second),
        }
    }
}

impl TryFrom<YearMonthIntervalField> for IntervalFieldType {
    type Error = CommonError;

    fn try_from(field_type: YearMonthIntervalField) -> CommonResult<IntervalFieldType> {
        match field_type {
            YearMonthIntervalField::Year => Ok(IntervalFieldType::Year),
            YearMonthIntervalField::Month => Ok(IntervalFieldType::Month),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Fields,
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            fields: Fields::empty(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

// TODO: Currently the behavior for VarChar and Char is not implemented.
/// Reference: https://spark.apache.org/docs/3.5.3/sql-ref-datatypes.html#supported-data-types
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Utf8Type {
    Configured,
    /// String which has a length limitation.
    /// Data writing will fail if the input string exceeds the length limitation.
    /// Note: this type can only be used in table schema, not functions/operator
    VarChar {
        length: u32,
    },
    /// A variant of Varchar(length) which is fixed length.
    /// Reading column of type Char(n) always returns string values of length n.
    /// Char type column comparison will pad the short one to the longer length.
    Char {
        length: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeZoneInfo {
    SQLConfigured,
    LocalTimeZone,
    NoTimeZone,
    TimeZone { timezone: Option<Arc<str>> },
}
