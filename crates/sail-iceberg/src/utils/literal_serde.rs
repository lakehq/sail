use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::spec::PrimitiveLiteral;

#[derive(Serialize, Deserialize)]
enum LiteralBits {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Decimal(String),
    String(String),
    Uuid(String),
    Binary(Vec<u8>),
}
pub(crate) fn serialize<S: Serializer>(
    value: &PrimitiveLiteral,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let bits = match value {
        PrimitiveLiteral::Boolean(v) => LiteralBits::Boolean(*v),
        PrimitiveLiteral::Int(v) => LiteralBits::Int(*v),
        PrimitiveLiteral::Long(v) => LiteralBits::Long(*v),
        PrimitiveLiteral::Float(v) => LiteralBits::Float(v.0.to_bits()),
        PrimitiveLiteral::Double(v) => LiteralBits::Double(v.0.to_bits()),
        PrimitiveLiteral::Int128(v) => LiteralBits::Decimal(v.to_string()),
        PrimitiveLiteral::String(v) => LiteralBits::String(v.clone()),
        PrimitiveLiteral::UInt128(v) => LiteralBits::Uuid(v.to_string()),
        PrimitiveLiteral::Binary(v) => LiteralBits::Binary(v.clone()),
    };
    bits.serialize(serializer)
}
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<PrimitiveLiteral, D::Error> {
    Ok(match LiteralBits::deserialize(deserializer)? {
        LiteralBits::Boolean(v) => PrimitiveLiteral::Boolean(v),
        LiteralBits::Int(v) => PrimitiveLiteral::Int(v),
        LiteralBits::Long(v) => PrimitiveLiteral::Long(v),
        LiteralBits::Float(v) => PrimitiveLiteral::Float(f32::from_bits(v).into()),
        LiteralBits::Double(v) => PrimitiveLiteral::Double(f64::from_bits(v).into()),
        LiteralBits::Decimal(v) => {
            PrimitiveLiteral::Int128(v.parse().map_err(serde::de::Error::custom)?)
        }
        LiteralBits::String(v) => PrimitiveLiteral::String(v),
        LiteralBits::Uuid(v) => {
            PrimitiveLiteral::UInt128(v.parse().map_err(serde::de::Error::custom)?)
        }
        LiteralBits::Binary(v) => PrimitiveLiteral::Binary(v),
    })
}
