use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, plan_err};
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{
    VARIANT_VALUE_FIELD_NAME, is_variant_storage_field, variant_metadata_field,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JevKind {
    Noul,
    Choice,
    Score,
    SystemOne,
    Models,
}

impl JevKind {
    pub const ALL: [Self; 5] = [
        Self::Noul,
        Self::Choice,
        Self::Score,
        Self::SystemOne,
        Self::Models,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Noul => "jev_noul",
            Self::Choice => "jev_choice",
            Self::Score => "jev_score",
            Self::SystemOne => "jev_system_one",
            Self::Models => "jev_models",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }

    pub fn arity(self) -> (usize, usize) {
        match self {
            Self::Noul => (2, 4),
            Self::Choice | Self::Score => (3, 4),
            Self::SystemOne => (2, 3),
            Self::Models => (0, 1),
        }
    }

    pub fn options_index(self) -> usize {
        match self {
            Self::Noul | Self::Choice | Self::Score => 3,
            Self::SystemOne => 2,
            Self::Models => 0,
        }
    }
}

pub fn string_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}
pub fn string_map(ty: &DataType) -> bool {
    matches!(ty, DataType::Map(entry, _) if matches!(entry.data_type(), DataType::Struct(fields)
        if fields.len() == 2 && string_type(fields[0].data_type())
            && (string_type(fields[1].data_type()) || fields[1].data_type() == &DataType::Null)))
}
pub fn variant_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Struct(Fields::from(vec![
            Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
            variant_metadata_field(DataType::Binary, false),
        ])),
        nullable,
    )
    .with_extension_type(VariantType)
}
pub fn validate_fields(kind: JevKind, fields: &[FieldRef]) -> Result<()> {
    let (min, max) = kind.arity();
    if !(min..=max).contains(&fields.len()) {
        return plan_err!("{} requires {min} to {max} arguments", kind.name());
    }
    for (index, field) in fields.iter().enumerate() {
        let ty = field.data_type();
        let valid = if matches!(ty, DataType::Null) {
            true
        } else if index == kind.options_index() {
            string_map(ty)
        } else if kind == JevKind::SystemOne && index == 1 {
            is_variant_storage_field(field)
        } else if index == 2 {
            is_variant_storage_field(field)
                || match kind {
                    JevKind::Score => {
                        matches!(ty, DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) | DataType::ListView(item) | DataType::LargeListView(item) if string_type(item.data_type()))
                    }
                    _ => string_map(ty),
                }
        } else {
            string_type(ty) || is_variant_storage_field(field)
        };
        if !valid {
            return plan_err!(
                "{} argument {} has unsupported type {ty}; use STRING/VARIANT for content, the documented criteria type, and MAP<STRING,STRING> for options",
                kind.name(),
                index + 1
            );
        }
    }
    Ok(())
}
pub fn return_type(kind: JevKind, arg_types: &[DataType]) -> Result<DataType> {
    validate_fields(
        kind,
        &arg_types
            .iter()
            .map(|ty| Arc::new(Field::new("argument", ty.clone(), true)))
            .collect::<Vec<_>>(),
    )?;
    Ok(DataType::Struct(fields(kind)))
}
pub fn return_field_from_args(kind: JevKind, args: ReturnFieldArgs) -> Result<FieldRef> {
    validate_fields(kind, args.arg_fields)?;
    Ok(Arc::new(Field::new(
        kind.name(),
        DataType::Struct(fields(kind)),
        true,
    )))
}
pub fn map_field(name: &str, value: Field) -> Field {
    Field::new(
        name,
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    value,
                ])),
                false,
            )),
            false,
        ),
        false,
    )
}
pub fn fields(kind: JevKind) -> Fields {
    let string = |name, nullable| Field::new(name, DataType::Utf8, nullable);
    let double = |name| Field::new(name, DataType::Float64, false);
    let probabilities = || map_field("probabilities", double("value"));
    let mut fields = match kind {
        JevKind::Noul => vec![double("noul")],
        JevKind::Choice => vec![
            string("choice", false),
            probabilities(),
            double("confidence"),
        ],
        JevKind::Score => vec![
            double("score"),
            probabilities(),
            double("confidence"),
            map_field("legend", variant_field("value", false)),
        ],
        JevKind::SystemOne => vec![map_field("answers", variant_field("value", false))],
        JevKind::Models => {
            return Fields::from(vec![
                Field::new(
                    "models",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            string("name", false),
                            string("description", false),
                            string("release_date", false),
                        ])),
                        false,
                    ))),
                    false,
                ),
                string("request_id", true),
            ]);
        }
    };
    fields.extend([
        string("model", false),
        string("request_id", true),
        string("batch_id", false),
        Field::new(
            "usage",
            DataType::Struct(Fields::from(vec![
                Field::new("input_tokens", DataType::Int64, true),
                Field::new("output_tokens", DataType::Int64, true),
            ])),
            false,
        ),
    ]);
    Fields::from(fields)
}
