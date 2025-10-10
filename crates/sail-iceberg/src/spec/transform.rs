use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::datatypes::{PrimitiveType, Type};
use super::values::Literal;

/// Transform is used to transform predicates to partition predicates,
/// in addition to transforming data values.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Transform {
    /// Source value, unmodified
    Identity,
    /// Hash of value, mod `N`.
    Bucket(u32),
    /// Value truncated to width `W`
    Truncate(u32),
    /// Extract a date or timestamp year, as years from 1970
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    Hour,
    /// Always produces `null`
    Void,
    /// Used to represent some customized transform that can't be recognized or supported now.
    Unknown,
}

impl Transform {
    // TODO: Full value transformation support
    pub fn to_human_string(self, _field_type: &Type, value: Option<&Literal>) -> String {
        if let Some(_value) = value {
            match self {
                Self::Identity => "identity_value".to_string(),
                Self::Void => "null".to_string(),
                _ => "transformed_value".to_string(),
            }
        } else {
            "null".to_string()
        }
    }

    /// Get the return type of transform given the input type.
    /// Returns `None` if it can't be transformed.
    pub fn result_type(&self, input_type: &Type) -> Result<Type, String> {
        match self {
            Transform::Identity => {
                if matches!(input_type, Type::Primitive(_)) {
                    Ok(input_type.clone())
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of identity transform"
                    ))
                }
            }
            Transform::Void => Ok(input_type.clone()),
            Transform::Unknown => Ok(Type::Primitive(PrimitiveType::String)),
            Transform::Bucket(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::Date
                        | PrimitiveType::Time
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::String
                        | PrimitiveType::Uuid
                        | PrimitiveType::Fixed(_)
                        | PrimitiveType::Binary => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of bucket transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of bucket transform"
                    ))
                }
            }
            Transform::Truncate(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::String => Ok(input_type.clone()),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of truncate transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of truncate transform"
                    ))
                }
            }
            Transform::Year | Transform::Month | Transform::Day => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Date
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of date transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of date transform"
                    ))
                }
            }
            Transform::Hour => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(format!(
                            "{input_type} is not a valid input type of hour transform"
                        )),
                    }
                } else {
                    Err(format!(
                        "{input_type} is not a valid input type of hour transform"
                    ))
                }
            }
        }
    }

    /// Whether the transform preserves the order of values.
    pub fn preserves_order(&self) -> bool {
        !matches!(
            self,
            Transform::Void | Transform::Bucket(_) | Transform::Unknown
        )
    }

    /// Unique transform name to deduplicate equivalent transforms in a builder.
    pub fn dedup_name(&self) -> String {
        match self {
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                "time".to_string()
            }
            _ => format!("{self}"),
        }
    }

    /// Whether ordering by this transform satisfies the ordering of another transform.
    pub fn satisfies_order_of(&self, other: &Self) -> bool {
        match self {
            Transform::Identity => other.preserves_order(),
            Transform::Hour => matches!(
                other,
                Transform::Hour | Transform::Day | Transform::Month | Transform::Year
            ),
            Transform::Day => matches!(other, Transform::Day | Transform::Month | Transform::Year),
            Transform::Month => matches!(other, Transform::Month | Transform::Year),
            _ => self == other,
        }
    }
}

impl Display for Transform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Bucket(n) => write!(f, "bucket[{}]", n),
            Transform::Truncate(w) => write!(f, "truncate[{}]", w),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
            Transform::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for Transform {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "identity" => Ok(Transform::Identity),
            "year" => Ok(Transform::Year),
            "month" => Ok(Transform::Month),
            "day" => Ok(Transform::Day),
            "hour" => Ok(Transform::Hour),
            "void" => Ok(Transform::Void),
            _ => {
                if let Some(bucket_str) =
                    s.strip_prefix("bucket[").and_then(|s| s.strip_suffix(']'))
                {
                    let n: u32 = bucket_str
                        .parse()
                        .map_err(|_| format!("Invalid bucket parameter: {}", bucket_str))?;
                    Ok(Transform::Bucket(n))
                } else if let Some(truncate_str) = s
                    .strip_prefix("truncate[")
                    .and_then(|s| s.strip_suffix(']'))
                {
                    let w: u32 = truncate_str
                        .parse()
                        .map_err(|_| format!("Invalid truncate parameter: {}", truncate_str))?;
                    Ok(Transform::Truncate(w))
                } else {
                    Ok(Transform::Unknown)
                }
            }
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Transform::from_str(&s).map_err(serde::de::Error::custom)
    }
}
