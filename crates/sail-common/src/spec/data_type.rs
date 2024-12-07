use std::ops::Deref;
use std::sync::Arc;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

/// Native Sail data types that convert to Arrow types.
/// These types usually directly match to [arrow_schema::DataType] variants when there is a corresponding type.
/// Exceptions to this are: [`Interval`].
/// Additionally, custom data types are supported for cases not covered by Arrow (e.g. [`UserDefined`]).
/// [Credit]: Comments within the enum are copied from [`arrow_schema::DataType`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DataType {
    /// Null type
    Null,
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// A timestamp with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a signed 64-bit integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// Timestamps with a non-empty timezone
    /// ------------------------------------
    ///
    /// If a Timestamp column has a non-empty timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
    /// (the Unix epoch), regardless of the Timestamp's own timezone.
    ///
    /// Therefore, timestamp values with a non-empty timezone correspond to
    /// physical points in time together with some additional information about
    /// how the data was obtained and/or how to display it (the timezone).
    ///
    ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
    ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
    ///   application may prefer to display it as "January 1st 1970, 01h00" in
    ///   the Europe/Paris timezone (which is the same physical point in time).
    ///
    /// One consequence is that timestamp values with a non-empty timezone
    /// can be compared and ordered directly, since they all share the same
    /// well-known point of reference (the Unix epoch).
    ///
    /// Timestamps with an unset / empty timezone
    /// -----------------------------------------
    ///
    /// If a Timestamp column has no timezone value, its epoch is
    /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
    ///
    /// Therefore, timestamp values without a timezone cannot be meaningfully
    /// interpreted as physical points in time, but only as calendar / clock
    /// indications ("wall clock time") in an unspecified timezone.
    ///
    ///   For example, the timestamp value 0 with an empty timezone string
    ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
    ///   is not enough information to interpret it as a well-defined physical
    ///   point in time.
    ///
    /// One consequence is that timestamp values without a timezone cannot
    /// be reliably compared or ordered, since they may have different points of
    /// reference.  In particular, it is *not* possible to interpret an unset
    /// or empty timezone as the same as "UTC".
    ///
    /// Conversion between timezones
    /// ----------------------------
    ///
    /// If a Timestamp column has a non-empty timezone, changing the timezone
    /// to a different non-empty value is a metadata-only operation:
    /// the timestamp values need not change as their point of reference remains
    /// the same (the Unix epoch).
    ///
    /// However, if a Timestamp column has no timezone value, changing it to a
    /// non-empty value requires to think about the desired semantics.
    /// One possibility is to assume that the original timestamp values are
    /// relative to the epoch of the timezone being set; timestamp values should
    /// then adjusted to the Unix epoch (for example, changing the timezone from
    /// empty to "Europe/Paris" would require converting the timestamp values
    /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
    /// nevertheless correct).
    ///
    /// ```
    /// # use arrow_schema::{DataType, TimeUnit};
    /// DataType::Timestamp(TimeUnit::Second, None);
    /// DataType::Timestamp(TimeUnit::Second, Some("literal".into()));
    /// DataType::Timestamp(TimeUnit::Second, Some("string".to_string().into()));
    /// ```
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date32,
    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds.
    ///
    /// # Valid Ranges
    ///
    /// According to the Arrow specification ([Schema.fbs]), values of Date64
    /// are treated as the number of *days*, in milliseconds, since the UNIX
    /// epoch. Therefore, values of this type  must be evenly divisible by
    /// `86_400_000`, the number of milliseconds in a standard day.
    ///
    /// It is not valid to store milliseconds that do not represent an exact
    /// day. The reason for this restriction is compatibility with other
    /// language's native libraries (specifically Java), which historically
    /// lacked a dedicated date type and only supported timestamps.
    ///
    /// # Validation
    ///
    /// This library does not validate or enforce that Date64 values are evenly
    /// divisible by `86_400_000`  for performance and usability reasons. Date64
    /// values are treated similarly to `Timestamp(TimeUnit::Millisecond,
    /// None)`: values will be displayed with a time of day if the value does
    /// not represent an exact day, and arithmetic will be done at the
    /// millisecond granularity.
    ///
    /// # Recommendation
    ///
    /// Users should prefer [`DataType::Date32`] to cleanly represent the number
    /// of days, or one of the Timestamp variants to include time as part of the
    /// representation, depending on their use case.
    ///
    /// # Further Reading
    ///
    /// For more details, see [#5288](https://github.com/apache/arrow-rs/issues/5288).
    ///
    /// [Schema.fbs]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
    Date64,
    /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either seconds or milliseconds.
    Time32(TimeUnit),
    /// A signed 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    /// Must be either microseconds or nanoseconds.
    Time64(TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    Duration(TimeUnit),
    /// A "calendar" interval which models types that don't necessarily
    /// have a precise duration without the context of a base timestamp (e.g.
    /// days can differ in length during daylight savings time transitions).
    ///
    /// This differs from the Arrow specification.
    /// Sail's specification allows for an optional `start_field` and `end_field`.
    Interval(
        IntervalUnit,
        Option<IntervalFieldType>, // `start_field`
        Option<IntervalFieldType>, // `end_field`
    ),
    /// Opaque binary data of variable length.
    ///
    /// A single Binary array can store up to [`i32::MAX`] bytes
    /// of binary data in total.
    Binary,
    /// Opaque binary data of fixed size.
    /// Enum parameter specifies the number of bytes per value.
    FixedSizeBinary(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    ///
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes
    /// of binary data in total.
    LargeBinary,
    /// Opaque binary data of variable length.
    ///
    /// Logically the same as [`Self::Binary`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    BinaryView,
    /// A variable-length string in Unicode with UTF-8 encoding.
    ///
    /// A single Utf8 array can store up to [`i32::MAX`] bytes
    /// of string data in total.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    ///
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
    /// of string data in total.
    LargeUtf8,
    /// A variable-length string in Unicode with UTF-8 encoding
    ///
    /// Logically the same as [`Self::Utf8`], but the internal representation uses a view
    /// struct that contains the string length and either the string's entire data
    /// inline (for small strings) or an inlined prefix, an index of another buffer,
    /// and an offset pointing to a slice in that buffer (for non-small strings).
    Utf8View,
    /// A list of some logical data type with variable length.
    ///
    /// A single List array can store up to [`i32::MAX`] elements in total.
    List(FieldRef),
    /// A list of some logical data type with fixed length.
    FixedSizeList(FieldRef, i32),
    /// A list of some logical data type with variable length and 64-bit offsets.
    ///
    /// A single LargeList array can store up to [`i64::MAX`] elements in total.
    LargeList(FieldRef),
    /// A nested datatype that contains a number of sub-fields.
    Struct(Fields),
    /// A nested datatype that can represent slots of differing types. Components:
    ///
    /// 1. [`UnionFields`]
    /// 2. The type of union (Sparse or Dense)
    Union(UnionFields, UnionMode),
    /// A dictionary encoded array (`key_type`, `value_type`), where
    /// each array element is an index of `key_type` into an
    /// associated dictionary of `value_type`.
    ///
    /// Dictionary arrays are used to store columns of `value_type`
    /// that contain many repeated values using less memory, but with
    /// a higher CPU overhead for some operations.
    ///
    /// This type mostly used to represent low cardinality string
    /// arrays or a limited set of primitive types as integers.
    Dictionary(Box<DataType>, Box<DataType>),
    /// Exact 128-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    ///
    /// In certain situations, scale could be negative number. For
    /// negative scale, it is the number of padding 0 to the right
    /// of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal
    /// has precision 3 and scale -2.
    Decimal128(u8, i8),
    /// Exact 256-bit width decimal value with precision and scale
    ///
    /// * precision is the total number of digits
    /// * scale is the number of digits past the decimal
    ///
    /// For example the number 123.45 has precision 5 and scale 2.
    ///
    /// In certain situations, scale could be negative number. For
    /// negative scale, it is the number of padding 0 to the right
    /// of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal
    /// has precision 3 and scale -2.
    Decimal256(u8, i8),
    /// A Map is a logical-nested type that is represented as
    ///
    /// `List<entries: Struct<key: K, value: V>>`
    ///
    /// The keys and values are each respectively contiguous.
    /// The key and value types are not constrained, but keys should be
    /// hashable and unique.
    /// Whether the keys are sorted can be set in the `bool` after the `Field`.
    ///
    /// In a field with Map type, the field has a child Struct field, which then
    /// has two children: key type and the second the value type. The names of the
    /// child fields may be respectively "entries", "key", and "value", but this is
    /// not enforced.
    Map(FieldRef, bool),

    // Byte,
    // Short,
    // Integer,
    // Long,
    // Float,
    // Double,
    // String,
    // Char {
    //     length: u32,
    // },
    // VarChar {
    //     length: u32,
    // },
    // Date,
    // TimestampNtz,
    // CalendarInterval,
    // YearMonthInterval {
    //     start_field: Option<YearMonthIntervalField>,
    //     end_field: Option<YearMonthIntervalField>,
    // },
    // DayTimeInterval {
    //     start_field: Option<DayTimeIntervalField>,
    //     end_field: Option<DayTimeIntervalField>,
    // },
    // Array {
    //     element_type: Box<DataType>,
    //     contains_null: bool,
    // },
    // Map {
    //     key_type: Box<DataType>,
    //     value_type: Box<DataType>,
    //     value_contains_null: bool,
    // },
    UserDefined {
        jvm_class: Option<String>,
        python_class: Option<String>,
        serialized_python_class: Option<String>,
        sql_type: Box<DataType>,
    },
    // TODO: [CHECK HERE] Don't merge in till you revisit this to include optional "String type" information from Spark.
    /// Resolves to either [`Self::Utf8`] or [`Self::LargeUtf8`], based on `config.arrow_use_large_var_types`.
    /// Optional length parameter is currently unused but retained for potential future use.
    ///
    /// The [`sail-spark-connect`] crate uses `TryFrom` for mapping Spark types to Sail types,
    /// whereas [`PlanResolver`] resolves `DataType` without `TryFrom`. This makes
    /// [`PlanResolver::resolve_data_type`] more suitable here, as it has access to `config`
    /// and can determine the type accordingly.
    ///
    /// TODO: Refactor data type resolution in [`sail-spark-connect`] to avoid using `TryFrom`.
    ///     This would make it possible to remove this variant in the future.
    ConfiguredUtf8(Option<u32>),
}

impl DataType {
    pub fn into_schema(self, default_field_name: &str, nullable: bool) -> Schema {
        let fields = match self {
            DataType::Struct(fields) => fields,
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

/// [Credit]: The implementations for [`Field`], [`FieldRef`], [`Fields`], and [`UnionFields`] are
/// copied from [`arrow_schema::Field`], [`arrow_schema::FieldRef`], [`arrow_schema::Fields`], and [`arrow_schema::UnionFields`].

/// A reference counted [`Field`]
pub type FieldRef = Arc<Field>;

/// A cheaply cloneable, owned slice of [`FieldRef`]
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

/// A cheaply cloneable, owned collection of [`FieldRef`] and their corresponding type ids
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnionFields(Arc<[(i8, FieldRef)]>);

impl UnionFields {
    /// Create a new [`UnionFields`] with no fields
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

/// Sparse or Dense union layouts
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
    /// Sparse union layout
    Sparse = 0,
    /// Dense union layout
    Dense = 1,
}

impl UnionMode {
    fn invalid(value: i32) -> CommonError {
        CommonError::invalid(format!("interval union mode: {value}"))
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
