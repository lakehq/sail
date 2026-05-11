use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::{
    ArrayType, ColumnMetadataKey, DataType, DeltaError as DeltaTableError, DeltaResult, MapType,
    MetadataValue, PrimitiveType, Protocol, StructField, StructType, TableFeature,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeChange {
    pub from_type: DataType,
    pub to_type: DataType,
    pub field_path: Vec<String>,
    pub table_version: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawTypeChange {
    from_type: String,
    to_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_version: Option<i64>,
}

impl TypeChange {
    fn from_raw(raw: RawTypeChange) -> DeltaResult<Self> {
        Ok(Self {
            from_type: parse_atomic_delta_type(&raw.from_type)?,
            to_type: parse_atomic_delta_type(&raw.to_type)?,
            field_path: parse_field_path(raw.field_path.as_deref())?,
            table_version: raw.table_version,
        })
    }

    fn to_raw(&self) -> RawTypeChange {
        RawTypeChange {
            from_type: self.from_type.to_string(),
            to_type: self.to_type.to_string(),
            field_path: (!self.field_path.is_empty()).then(|| self.field_path.join(".")),
            table_version: self.table_version,
        }
    }
}

pub fn protocol_supports_type_widening(protocol: &Protocol) -> bool {
    protocol.min_reader_version() >= 3
        && (protocol.has_reader_feature(&TableFeature::TypeWidening)
            || protocol.has_reader_feature(&TableFeature::TypeWideningPreview))
}

pub fn protocol_can_write_type_widening(protocol: &Protocol) -> bool {
    protocol.min_reader_version() >= 3
        && protocol.min_writer_version() >= 7
        && ((protocol.has_reader_feature(&TableFeature::TypeWidening)
            && protocol.has_writer_feature(&TableFeature::TypeWidening))
            || (protocol.has_reader_feature(&TableFeature::TypeWideningPreview)
                && protocol.has_writer_feature(&TableFeature::TypeWideningPreview)))
}

pub fn schema_contains_type_widening_metadata(schema: &StructType) -> bool {
    schema.fields().any(field_contains_type_widening_metadata)
}

pub fn validate_type_widening_metadata(schema: &StructType) -> DeltaResult<()> {
    validate_struct_type(schema, &mut Vec::new())
}

pub fn type_changes_from_field(field: &StructField) -> DeltaResult<Vec<TypeChange>> {
    let Some(value) = field
        .metadata()
        .get(ColumnMetadataKey::TypeChanges.as_ref())
    else {
        return Ok(Vec::new());
    };

    let value = metadata_value_to_json(value)?;
    let Value::Array(items) = value else {
        return Err(DeltaTableError::schema(format!(
            "{} metadata for field '{}' must be a JSON array",
            ColumnMetadataKey::TypeChanges.as_ref(),
            field.name()
        )));
    };

    items
        .into_iter()
        .map(|item| {
            let raw: RawTypeChange =
                serde_json::from_value(item).map_err(DeltaTableError::generic_err)?;
            TypeChange::from_raw(raw)
        })
        .collect()
}

pub fn add_type_widening_metadata(
    existing: &StructType,
    candidate: &StructType,
) -> DeltaResult<StructType> {
    StructType::try_from_results(candidate.fields().map(|new_field| {
        if let Some(old_field) = existing.field(new_field.name()) {
            rewrite_field(old_field, new_field)
        } else {
            Ok(new_field.clone())
        }
    }))
}

pub fn collect_type_changes(
    existing: &StructType,
    candidate: &StructType,
) -> DeltaResult<Vec<(Vec<String>, TypeChange)>> {
    let mut out = Vec::new();
    collect_struct_type_changes(existing, candidate, &mut Vec::new(), &mut out)?;
    Ok(out)
}

pub(crate) fn alter_column_type(
    schema: &StructType,
    path: &[String],
    full_path: &[String],
    data_type: DataType,
) -> DeltaResult<StructType> {
    let Some((name, nested_path)) = path.split_first() else {
        return Err(DeltaTableError::schema(
            "ALTER COLUMN TYPE requires a column name",
        ));
    };

    let mut found = false;
    let schema = StructType::try_from_results(schema.fields().map(|field| {
        if field.name() == name {
            found = true;
            alter_field_type(field, nested_path, full_path, data_type.clone())
        } else {
            Ok(field.clone())
        }
    }))?;
    if !found {
        return Err(DeltaTableError::missing_column(full_path.join(".")));
    }
    Ok(schema)
}

pub fn is_supported_type_change(from_type: &DataType, to_type: &DataType) -> bool {
    if from_type == to_type {
        return true;
    }

    let (DataType::Primitive(from), DataType::Primitive(to)) = (from_type, to_type) else {
        return false;
    };

    match (from, to) {
        (from, to)
            if integral_rank(from)
                .zip(integral_rank(to))
                .is_some_and(|(a, b)| a < b) =>
        {
            true
        }
        (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Byte | PrimitiveType::Short | PrimitiveType::Integer,
            PrimitiveType::Double,
        ) => true,
        (PrimitiveType::Date, PrimitiveType::TimestampNtz) => true,
        (PrimitiveType::Decimal(from), PrimitiveType::Decimal(to)) => {
            let precision_diff = i16::from(to.precision()) - i16::from(from.precision());
            let scale_diff = i16::from(to.scale()) - i16::from(from.scale());
            precision_diff >= scale_diff && scale_diff >= 0
        }
        (
            PrimitiveType::Byte | PrimitiveType::Short | PrimitiveType::Integer,
            PrimitiveType::Decimal(to),
        ) => decimal_wider_than_integral(to.precision(), to.scale(), 10),
        (PrimitiveType::Long, PrimitiveType::Decimal(to)) => {
            decimal_wider_than_integral(to.precision(), to.scale(), 20)
        }
        _ => false,
    }
}

pub fn is_supported_type_change_for_write(
    protocol: &Protocol,
    from_type: &DataType,
    to_type: &DataType,
) -> bool {
    if from_type == to_type {
        return true;
    }
    if !is_supported_type_change(from_type, to_type) {
        return false;
    }
    if !protocol_has_iceberg_compat(protocol) {
        return true;
    }
    is_iceberg_v2_supported_type_change(from_type, to_type)
}

pub fn is_supported_type_change_for_schema_evolution(
    protocol: &Protocol,
    from_type: &DataType,
    to_type: &DataType,
) -> bool {
    if from_type == to_type {
        return true;
    }
    if !is_supported_type_change(from_type, to_type) {
        return false;
    }
    if !protocol_has_iceberg_compat(protocol) {
        return true;
    }
    is_iceberg_v2_supported_type_change(from_type, to_type)
}

fn protocol_has_iceberg_compat(protocol: &Protocol) -> bool {
    protocol.has_writer_feature(&TableFeature::IcebergCompatV1)
        || protocol.has_writer_feature(&TableFeature::IcebergCompatV2)
        || protocol.has_reader_feature(&TableFeature::IcebergCompatV1)
        || protocol.has_reader_feature(&TableFeature::IcebergCompatV2)
}

fn is_iceberg_v2_supported_type_change(from_type: &DataType, to_type: &DataType) -> bool {
    let (DataType::Primitive(from), DataType::Primitive(to)) = (from_type, to_type) else {
        return false;
    };

    match (from, to) {
        (
            PrimitiveType::Byte | PrimitiveType::Short | PrimitiveType::Integer,
            PrimitiveType::Double,
        ) => false,
        (PrimitiveType::Date, PrimitiveType::TimestampNtz) => false,
        (
            PrimitiveType::Byte
            | PrimitiveType::Short
            | PrimitiveType::Integer
            | PrimitiveType::Long,
            PrimitiveType::Decimal(_),
        ) => false,
        (PrimitiveType::Decimal(from), PrimitiveType::Decimal(to)) => to.scale() == from.scale(),
        _ => true,
    }
}

fn alter_field_type(
    field: &StructField,
    path: &[String],
    full_path: &[String],
    data_type: DataType,
) -> DeltaResult<StructField> {
    let data_type = if path.is_empty() {
        data_type
    } else {
        alter_nested_data_type(field.data_type(), path, full_path, data_type)?
    };
    Ok(StructField {
        name: field.name().clone(),
        data_type,
        nullable: field.is_nullable(),
        metadata: field.metadata().clone(),
    })
}

fn alter_nested_data_type(
    current: &DataType,
    path: &[String],
    full_path: &[String],
    data_type: DataType,
) -> DeltaResult<DataType> {
    let Some((name, nested_path)) = path.split_first() else {
        return Ok(data_type);
    };
    match current {
        DataType::Struct(struct_type) => Ok(DataType::Struct(Box::new(alter_column_type(
            struct_type,
            path,
            full_path,
            data_type,
        )?))),
        DataType::Array(array) if name == "element" => {
            Ok(DataType::Array(Box::new(ArrayType::new(
                alter_nested_data_type(array.element_type(), nested_path, full_path, data_type)?,
                array.contains_null(),
            ))))
        }
        DataType::Map(map) if name == "key" => Ok(DataType::Map(Box::new(MapType::new(
            alter_nested_data_type(map.key_type(), nested_path, full_path, data_type)?,
            map.value_type().clone(),
            map.value_contains_null(),
        )))),
        DataType::Map(map) if name == "value" => Ok(DataType::Map(Box::new(MapType::new(
            map.key_type().clone(),
            alter_nested_data_type(map.value_type(), nested_path, full_path, data_type)?,
            map.value_contains_null(),
        )))),
        DataType::Array(_) => Err(DeltaTableError::schema(format!(
            "expected 'element' for array column path, found '{name}'"
        ))),
        DataType::Map(_) => Err(DeltaTableError::schema(format!(
            "expected 'key' or 'value' for map column path, found '{name}'"
        ))),
        other => Err(DeltaTableError::schema(format!(
            "cannot resolve ALTER COLUMN TYPE path segment '{name}' through {other}"
        ))),
    }
}

fn field_contains_type_widening_metadata(field: &StructField) -> bool {
    field
        .metadata()
        .contains_key(ColumnMetadataKey::TypeChanges.as_ref())
        || match field.data_type() {
            DataType::Struct(st) => schema_contains_type_widening_metadata(st),
            DataType::Array(array) => type_contains_type_widening_metadata(array.element_type()),
            DataType::Map(map) => {
                type_contains_type_widening_metadata(map.key_type())
                    || type_contains_type_widening_metadata(map.value_type())
            }
            _ => false,
        }
}

fn type_contains_type_widening_metadata(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(st) => schema_contains_type_widening_metadata(st),
        DataType::Array(array) => type_contains_type_widening_metadata(array.element_type()),
        DataType::Map(map) => {
            type_contains_type_widening_metadata(map.key_type())
                || type_contains_type_widening_metadata(map.value_type())
        }
        _ => false,
    }
}

fn validate_struct_type(schema: &StructType, path: &mut Vec<String>) -> DeltaResult<()> {
    for field in schema.fields() {
        path.push(field.name().clone());
        validate_field(field, path)?;
        path.pop();
    }
    Ok(())
}

fn validate_field(field: &StructField, path: &mut Vec<String>) -> DeltaResult<()> {
    let mut latest_by_path: HashMap<Vec<String>, DataType> = HashMap::new();
    for change in type_changes_from_field(field)? {
        let full_path = format_type_change_path(path, &change.field_path);
        if !is_supported_type_change(&change.from_type, &change.to_type) {
            return Err(DeltaTableError::Unsupported(format!(
                "unsupported Delta type widening change at {full_path}: {} -> {}",
                change.from_type, change.to_type
            )));
        }

        let leaf_type = resolve_field_path(field.data_type(), &change.field_path).map_err(|e| {
            DeltaTableError::schema(format!(
                "invalid {} metadata at {full_path}: {e}",
                ColumnMetadataKey::TypeChanges.as_ref()
            ))
        })?;
        if !matches!(leaf_type, DataType::Primitive(_)) {
            return Err(DeltaTableError::schema(format!(
                "invalid {} metadata at {full_path}: fieldPath does not resolve to an atomic type",
                ColumnMetadataKey::TypeChanges.as_ref()
            )));
        }
        latest_by_path.insert(change.field_path.clone(), change.to_type);
    }

    for (field_path, latest_to_type) in latest_by_path {
        let leaf_type = resolve_field_path(field.data_type(), &field_path)?;
        if leaf_type != &latest_to_type {
            let full_path = format_type_change_path(path, &field_path);
            return Err(DeltaTableError::schema(format!(
                "invalid {} metadata at {full_path}: latest toType {} does not match schema type {}",
                ColumnMetadataKey::TypeChanges.as_ref(),
                latest_to_type,
                leaf_type
            )));
        }
    }

    match field.data_type() {
        DataType::Struct(st) => validate_struct_type(st, path)?,
        DataType::Array(array) => validate_type_for_nested_structs(array.element_type(), path)?,
        DataType::Map(map) => {
            validate_type_for_nested_structs(map.key_type(), path)?;
            validate_type_for_nested_structs(map.value_type(), path)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_type_for_nested_structs(
    data_type: &DataType,
    path: &mut Vec<String>,
) -> DeltaResult<()> {
    match data_type {
        DataType::Struct(st) => validate_struct_type(st, path),
        DataType::Array(array) => validate_type_for_nested_structs(array.element_type(), path),
        DataType::Map(map) => {
            validate_type_for_nested_structs(map.key_type(), path)?;
            validate_type_for_nested_structs(map.value_type(), path)
        }
        _ => Ok(()),
    }
}

fn rewrite_field(existing: &StructField, candidate: &StructField) -> DeltaResult<StructField> {
    let (data_type, changes) =
        rewrite_data_type(existing.data_type(), candidate.data_type(), Vec::new())?;

    let mut rewritten = StructField {
        name: candidate.name().clone(),
        data_type,
        nullable: candidate.is_nullable(),
        metadata: candidate.metadata().clone(),
    };

    for (key, existing_value) in existing.metadata() {
        rewritten
            .metadata
            .entry(key.clone())
            .or_insert_with(|| existing_value.clone());
    }

    append_type_changes_to_field(rewritten, changes)
}

fn rewrite_data_type(
    existing: &DataType,
    candidate: &DataType,
    field_path: Vec<String>,
) -> DeltaResult<(DataType, Vec<TypeChange>)> {
    match (existing, candidate) {
        (DataType::Struct(old), DataType::Struct(new)) => Ok((
            DataType::Struct(Box::new(add_type_widening_metadata(old, new)?)),
            Vec::new(),
        )),
        (DataType::Array(old), DataType::Array(new)) => {
            let mut child_path = field_path;
            child_path.push("element".to_string());
            let (element_type, changes) =
                rewrite_data_type(old.element_type(), new.element_type(), child_path)?;
            Ok((
                DataType::Array(Box::new(ArrayType::new(element_type, new.contains_null()))),
                changes,
            ))
        }
        (DataType::Map(old), DataType::Map(new)) => {
            let mut key_path = field_path.clone();
            key_path.push("key".to_string());
            let (key_type, mut changes) =
                rewrite_data_type(old.key_type(), new.key_type(), key_path)?;

            let mut value_path = field_path;
            value_path.push("value".to_string());
            let (value_type, value_changes) =
                rewrite_data_type(old.value_type(), new.value_type(), value_path)?;
            changes.extend(value_changes);

            Ok((
                DataType::Map(Box::new(MapType::new(
                    key_type,
                    value_type,
                    new.value_contains_null(),
                ))),
                changes,
            ))
        }
        (old, new) if old == new => Ok((new.clone(), Vec::new())),
        (old @ DataType::Primitive(_), new @ DataType::Primitive(_)) => {
            if !is_supported_type_change(old, new) {
                return Err(DeltaTableError::schema(format!(
                    "unsupported Delta type widening change: {old} -> {new}"
                )));
            }
            Ok((
                new.clone(),
                vec![TypeChange {
                    from_type: old.clone(),
                    to_type: new.clone(),
                    field_path,
                    table_version: None,
                }],
            ))
        }
        (old, new) => Err(DeltaTableError::schema(format!(
            "unsupported Delta schema type change: {old} -> {new}"
        ))),
    }
}

fn collect_struct_type_changes(
    existing: &StructType,
    candidate: &StructType,
    path: &mut Vec<String>,
    out: &mut Vec<(Vec<String>, TypeChange)>,
) -> DeltaResult<()> {
    for new_field in candidate.fields() {
        let Some(old_field) = existing.field(new_field.name()) else {
            continue;
        };
        path.push(new_field.name().clone());
        collect_data_type_changes(
            old_field.data_type(),
            new_field.data_type(),
            Vec::new(),
            path,
            out,
        )?;
        path.pop();
    }
    Ok(())
}

fn collect_data_type_changes(
    existing: &DataType,
    candidate: &DataType,
    field_path: Vec<String>,
    struct_path: &mut Vec<String>,
    out: &mut Vec<(Vec<String>, TypeChange)>,
) -> DeltaResult<()> {
    match (existing, candidate) {
        (DataType::Struct(old), DataType::Struct(new)) => {
            collect_struct_type_changes(old, new, struct_path, out)
        }
        (DataType::Array(old), DataType::Array(new)) => {
            let mut child_path = field_path;
            child_path.push("element".to_string());
            collect_data_type_changes(
                old.element_type(),
                new.element_type(),
                child_path,
                struct_path,
                out,
            )
        }
        (DataType::Map(old), DataType::Map(new)) => {
            let mut key_path = field_path.clone();
            key_path.push("key".to_string());
            collect_data_type_changes(old.key_type(), new.key_type(), key_path, struct_path, out)?;
            let mut value_path = field_path;
            value_path.push("value".to_string());
            collect_data_type_changes(
                old.value_type(),
                new.value_type(),
                value_path,
                struct_path,
                out,
            )
        }
        (old, new) if old == new => Ok(()),
        (old @ DataType::Primitive(_), new @ DataType::Primitive(_)) => {
            if !is_supported_type_change(old, new) {
                return Err(DeltaTableError::schema(format!(
                    "unsupported Delta type widening change at {}: {old} -> {new}",
                    format_type_change_path(struct_path, &field_path)
                )));
            }
            out.push((
                struct_path.clone(),
                TypeChange {
                    from_type: old.clone(),
                    to_type: new.clone(),
                    field_path,
                    table_version: None,
                },
            ));
            Ok(())
        }
        (old, new) => Err(DeltaTableError::schema(format!(
            "unsupported Delta schema type change at {}: {old} -> {new}",
            struct_path.join(".")
        ))),
    }
}

fn append_type_changes_to_field(
    mut field: StructField,
    changes: Vec<TypeChange>,
) -> DeltaResult<StructField> {
    if changes.is_empty() {
        return Ok(field);
    }

    let mut all_changes = type_changes_from_field(&field)?;
    all_changes.extend(changes);
    let json_changes = all_changes
        .iter()
        .map(|change| serde_json::to_value(change.to_raw()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(DeltaTableError::generic_err)?;
    field.metadata.insert(
        ColumnMetadataKey::TypeChanges.as_ref().to_string(),
        MetadataValue::Other(Value::Array(json_changes)),
    );
    Ok(field)
}

fn metadata_value_to_json(value: &MetadataValue) -> DeltaResult<Value> {
    match value {
        MetadataValue::Other(value) => Ok(value.clone()),
        MetadataValue::String(value) => {
            serde_json::from_str(value).map_err(DeltaTableError::generic_err)
        }
        other => Err(DeltaTableError::schema(format!(
            "{} metadata must be a JSON array, found {other}",
            ColumnMetadataKey::TypeChanges.as_ref()
        ))),
    }
}

fn parse_field_path(field_path: Option<&str>) -> DeltaResult<Vec<String>> {
    let Some(field_path) = field_path else {
        return Ok(Vec::new());
    };
    let field_path = field_path.trim();
    if field_path.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for part in field_path.split('.') {
        let part = part.trim();
        if part.is_empty() {
            return Err(DeltaTableError::schema(format!(
                "invalid empty segment in {} fieldPath '{field_path}'",
                ColumnMetadataKey::TypeChanges.as_ref()
            )));
        }
        out.push(part.to_string());
    }
    Ok(out)
}

fn parse_atomic_delta_type(value: &str) -> DeltaResult<DataType> {
    let normalized = value
        .trim()
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();

    match normalized.as_str() {
        "string" => Ok(DataType::STRING),
        "long" => Ok(DataType::LONG),
        "integer" | "int" => Ok(DataType::INTEGER),
        "short" => Ok(DataType::SHORT),
        "byte" => Ok(DataType::BYTE),
        "float" => Ok(DataType::FLOAT),
        "double" => Ok(DataType::DOUBLE),
        "boolean" => Ok(DataType::BOOLEAN),
        "binary" => Ok(DataType::BINARY),
        "date" => Ok(DataType::DATE),
        "timestamp" => Ok(DataType::TIMESTAMP),
        "timestamp_ntz" | "timestampntz" => Ok(DataType::TIMESTAMP_NTZ),
        value if value.starts_with("decimal(") && value.ends_with(')') => {
            let inner = &value["decimal(".len()..value.len() - 1];
            let (precision, scale) = inner.split_once(',').ok_or_else(|| {
                DeltaTableError::schema(format!(
                    "invalid decimal type in delta.typeChanges: {value}"
                ))
            })?;
            let precision = precision.parse::<u8>().map_err(|e| {
                DeltaTableError::schema(format!(
                    "invalid decimal precision in delta.typeChanges '{value}': {e}"
                ))
            })?;
            let scale = scale.parse::<u8>().map_err(|e| {
                DeltaTableError::schema(format!(
                    "invalid decimal scale in delta.typeChanges '{value}': {e}"
                ))
            })?;
            DataType::decimal(precision, scale)
        }
        other => Err(DeltaTableError::schema(format!(
            "unsupported type in delta.typeChanges: {other}"
        ))),
    }
}

fn resolve_field_path<'a>(
    mut data_type: &'a DataType,
    field_path: &[String],
) -> DeltaResult<&'a DataType> {
    for segment in field_path {
        match (data_type, segment.as_str()) {
            (DataType::Array(array), "element") => data_type = array.element_type(),
            (DataType::Map(map), "key") => data_type = map.key_type(),
            (DataType::Map(map), "value") => data_type = map.value_type(),
            (DataType::Array(_), other) => {
                return Err(DeltaTableError::schema(format!(
                    "expected 'element' for array type change path, found '{other}'"
                )))
            }
            (DataType::Map(_), other) => {
                return Err(DeltaTableError::schema(format!(
                    "expected 'key' or 'value' for map type change path, found '{other}'"
                )))
            }
            (other_type, other) => {
                return Err(DeltaTableError::schema(format!(
                    "cannot resolve type change path segment '{other}' through {other_type}"
                )))
            }
        }
    }
    Ok(data_type)
}

pub fn format_type_change_path(struct_path: &[String], field_path: &[String]) -> String {
    let mut path = struct_path.to_vec();
    path.extend(field_path.iter().cloned());
    path.join(".")
}

fn integral_rank(data_type: &PrimitiveType) -> Option<u8> {
    match data_type {
        PrimitiveType::Byte => Some(1),
        PrimitiveType::Short => Some(2),
        PrimitiveType::Integer => Some(3),
        PrimitiveType::Long => Some(4),
        _ => None,
    }
}

fn decimal_wider_than_integral(precision: u8, scale: u8, base_precision: u8) -> bool {
    precision >= base_precision && precision - base_precision >= scale
}
