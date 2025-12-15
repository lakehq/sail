// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::mem::discriminant;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema};
use datafusion_common::{DataFusionError, Result};

use crate::spec::schema::{Schema as IcebergSchema, SchemaBuilder};
use crate::spec::types::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use crate::spec::TableMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaMode {
    Merge,
    Overwrite,
}

#[derive(Debug)]
pub struct SchemaEvolutionOutcome {
    pub iceberg_schema: IcebergSchema,
    pub arrow_schema: Arc<ArrowSchema>,
    pub changed: bool,
}

const UTC_ALIASES: &[&str] = &["UTC", "+00:00", "Etc/UTC", "Z"];

pub struct SchemaEvolver;

impl SchemaEvolver {
    pub fn evolve(
        table_meta: &TableMetadata,
        input_schema: &ArrowSchema,
        mode: Option<SchemaMode>,
    ) -> Result<SchemaEvolutionOutcome> {
        use crate::datasource::type_converter::iceberg_schema_to_arrow;

        let current_schema = table_meta.current_schema().cloned().ok_or_else(|| {
            DataFusionError::Plan("No current schema in Iceberg table metadata".to_string())
        })?;

        match mode {
            Some(SchemaMode::Merge) => {
                Self::merge_schema(table_meta, &current_schema, input_schema)
            }
            Some(SchemaMode::Overwrite) => {
                Self::overwrite_schema(table_meta, &current_schema, input_schema)
            }
            None => {
                let arrow_schema = Arc::new(iceberg_schema_to_arrow(&current_schema)?);
                Self::validate_exact_schema(arrow_schema.as_ref(), &current_schema, input_schema)?;
                Ok(SchemaEvolutionOutcome {
                    iceberg_schema: current_schema,
                    arrow_schema,
                    changed: false,
                })
            }
        }
    }

    fn validate_exact_schema(
        table_schema: &ArrowSchema,
        iceberg_schema: &IcebergSchema,
        input_schema: &ArrowSchema,
    ) -> Result<()> {
        for field in input_schema.fields() {
            let table_field = table_schema.field_with_name(field.name()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in Iceberg table schema. Set mergeSchema=true to add columns or overwriteSchema=true to replace the schema.",
                    field.name()
                ))
            })?;
            if !Self::field_types_equivalent(table_field.data_type(), field.data_type())
                && !Self::is_safe_write_cast(table_field.data_type(), field.data_type())
            {
                return Err(DataFusionError::Plan(format!(
                    "Column '{}' has type {:?} in the table but {:?} in the input data. Set mergeSchema=true to allow schema evolution or overwriteSchema=true to replace the schema.",
                    field.name(),
                    table_field.data_type(),
                    field.data_type(),
                )));
            }
        }

        for field in table_schema.fields() {
            if input_schema.field_with_name(field.name()).is_ok() {
                continue;
            }

            let has_default = iceberg_schema
                .field_by_name(field.name())
                .and_then(|nested| {
                    nested
                        .write_default
                        .as_ref()
                        .or(nested.initial_default.as_ref())
                })
                .is_some();

            if !field.is_nullable() && !has_default {
                return Err(DataFusionError::Plan(format!(
                    "Column '{}' is required in the Iceberg table schema and must be present in the input data.",
                    field.name()
                )));
            }
        }

        Ok(())
    }

    fn field_types_compatible(table_field: &Field, input_field: &Field) -> bool {
        Self::field_types_equivalent(table_field.data_type(), input_field.data_type())
            || Self::is_allowed_type_promotion(table_field.data_type(), input_field.data_type())
            || Self::is_safe_write_cast(table_field.data_type(), input_field.data_type())
    }

    fn field_types_equivalent(table_type: &DataType, input_type: &DataType) -> bool {
        if table_type == input_type {
            return true;
        }

        matches!(
            (table_type, input_type),
            (
                DataType::Timestamp(table_unit, table_tz),
                DataType::Timestamp(input_unit, input_tz)
            ) if table_unit == input_unit
                && Self::timestamp_timezone_compatible(table_tz, input_tz)
        ) || Self::nested_types_equivalent(table_type, input_type)
    }

    fn field_has_default(field: &NestedField) -> bool {
        field.write_default.is_some() || field.initial_default.is_some()
    }

    fn types_share_shape(existing: &Type, candidate: &Type) -> bool {
        discriminant(existing) == discriminant(candidate)
    }

    fn is_allowed_type_promotion(table_type: &DataType, input_type: &DataType) -> bool {
        matches!(
            (table_type, input_type),
            (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64)
        ) || Self::decimal_precision_expands(table_type, input_type)
    }

    fn is_safe_write_cast(table_type: &DataType, input_type: &DataType) -> bool {
        matches!(
            (table_type, input_type),
            (DataType::Int64, DataType::Int32) | (DataType::Float64, DataType::Float32)
        ) || matches!(
            (table_type, input_type),
            (
                DataType::FixedSizeBinary(_),
                DataType::Binary | DataType::LargeBinary
            )
        ) || Self::decimal_precision_contracts(table_type, input_type)
    }

    fn decimal_precision_expands(table_type: &DataType, input_type: &DataType) -> bool {
        match (table_type, input_type) {
            (DataType::Decimal128(old_p, old_s), DataType::Decimal128(new_p, new_s))
                if new_s == old_s && new_p >= old_p =>
            {
                true
            }
            (DataType::Decimal128(old_p, old_s), DataType::Decimal256(new_p, new_s))
                if new_s == old_s && new_p >= old_p =>
            {
                true
            }
            (DataType::Decimal256(old_p, old_s), DataType::Decimal256(new_p, new_s))
                if new_s == old_s && new_p >= old_p =>
            {
                true
            }
            _ => false,
        }
    }

    fn decimal_precision_contracts(table_type: &DataType, input_type: &DataType) -> bool {
        match (table_type, input_type) {
            (DataType::Decimal128(table_p, table_s), DataType::Decimal128(input_p, input_s))
                if table_s == input_s && table_p >= input_p =>
            {
                true
            }
            (DataType::Decimal256(table_p, table_s), DataType::Decimal256(input_p, input_s))
                if table_s == input_s && table_p >= input_p =>
            {
                true
            }
            (DataType::Decimal256(table_p, table_s), DataType::Decimal128(input_p, input_s))
                if table_s == input_s && table_p >= input_p =>
            {
                true
            }
            _ => false,
        }
    }

    fn nested_types_equivalent(table_type: &DataType, input_type: &DataType) -> bool {
        if table_type == input_type {
            return true;
        }
        if let (
            DataType::Timestamp(table_unit, table_tz),
            DataType::Timestamp(input_unit, input_tz),
        ) = (table_type, input_type)
        {
            if table_unit == input_unit && Self::timestamp_timezone_compatible(table_tz, input_tz) {
                return true;
            }
        }
        match (table_type, input_type) {
            (DataType::Struct(table_fields), DataType::Struct(input_fields)) => {
                if table_fields.len() != input_fields.len() {
                    return false;
                }
                table_fields.iter().zip(input_fields.iter()).all(|(t, i)| {
                    t.name() == i.name()
                        && Self::nested_types_equivalent(t.data_type(), i.data_type())
                })
            }
            (
                DataType::List(table_child)
                | DataType::ListView(table_child)
                | DataType::LargeList(table_child)
                | DataType::LargeListView(table_child),
                DataType::List(input_child)
                | DataType::ListView(input_child)
                | DataType::LargeList(input_child)
                | DataType::LargeListView(input_child),
            ) => Self::nested_types_equivalent(table_child.data_type(), input_child.data_type()),
            (
                DataType::Map(table_entries, table_sorted),
                DataType::Map(input_entries, input_sorted),
            ) => {
                if table_sorted != input_sorted {
                    return false;
                }
                if let (DataType::Struct(table_fields), DataType::Struct(input_fields)) =
                    (table_entries.data_type(), input_entries.data_type())
                {
                    if table_fields.len() != input_fields.len() {
                        return false;
                    }
                    table_fields.iter().zip(input_fields.iter()).all(|(t, i)| {
                        t.name() == i.name()
                            && Self::nested_types_equivalent(t.data_type(), i.data_type())
                    })
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn timestamp_timezone_compatible(
        table_tz: &Option<std::sync::Arc<str>>,
        input_tz: &Option<std::sync::Arc<str>>,
    ) -> bool {
        match (table_tz.as_deref(), input_tz.as_deref()) {
            (None, None) => true,
            (Some(a), Some(b)) => Self::tz_alias_eq(a, b),
            (None, Some(tz)) | (Some(tz), None) => Self::is_utc_alias(tz),
        }
    }

    fn tz_alias_eq(lhs: &str, rhs: &str) -> bool {
        lhs == rhs || (Self::is_utc_alias(lhs) && Self::is_utc_alias(rhs))
    }

    fn is_utc_alias(tz: &str) -> bool {
        UTC_ALIASES
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(tz.trim()))
    }

    fn merge_schema(
        table_meta: &TableMetadata,
        current_schema: &IcebergSchema,
        input_schema: &ArrowSchema,
    ) -> Result<SchemaEvolutionOutcome> {
        use crate::datasource::type_converter::iceberg_schema_to_arrow;

        let current_arrow = Arc::new(iceberg_schema_to_arrow(current_schema)?);
        let mut next_field_id = table_meta.last_column_id + 1;
        let mut merged_fields: Vec<Arc<NestedField>> =
            Vec::with_capacity(current_schema.fields().len() + input_schema.fields().len());
        let mut changed = false;

        for existing in current_schema.fields() {
            // FIXME: Consider implementing case-insensitive matching like SchemaUpdate in Iceberg.
            match input_schema.field_with_name(&existing.name) {
                Ok(candidate) => {
                    let (merged_field, field_changed) =
                        Self::merge_field(existing.as_ref(), candidate, &mut next_field_id)?;
                    changed |= field_changed;
                    merged_fields.push(Arc::new(merged_field));
                }
                Err(_) => {
                    if existing.required && !Self::field_has_default(existing) {
                        return Err(DataFusionError::Plan(format!(
                            "Column '{}' is required in the Iceberg table schema and must be present in the input data.",
                            existing.name
                        )));
                    }
                    merged_fields.push(Arc::clone(existing));
                }
            }
        }

        for field in input_schema.fields() {
            if current_schema.field_by_name(field.name()).is_none() {
                // TODO: Support DDL RENAME COLUMN operation.
                let new_field = Self::build_field_from_arrow(field, &mut next_field_id)?;
                merged_fields.push(Arc::new(new_field));
                changed = true;
            }
        }

        if !changed {
            return Ok(SchemaEvolutionOutcome {
                iceberg_schema: current_schema.clone(),
                arrow_schema: current_arrow,
                changed: false,
            });
        }

        let mut builder = IcebergSchema::builder()
            .with_schema_id(Self::next_schema_id(table_meta))
            .with_fields(merged_fields);
        let identifiers: Vec<i32> = current_schema.identifier_field_ids().collect();
        if !identifiers.is_empty() {
            builder = builder.with_identifier_field_ids(identifiers);
        }
        let new_schema = builder
            .build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to build merged schema: {e}")))?;
        Self::validate_partition_spec_sources(table_meta, &new_schema)?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&new_schema)?);
        Ok(SchemaEvolutionOutcome {
            iceberg_schema: new_schema,
            arrow_schema,
            changed: true,
        })
    }

    fn merge_field(
        existing: &NestedField,
        input_field: &Field,
        next_field_id: &mut i32,
    ) -> Result<(NestedField, bool)> {
        use crate::datasource::type_converter::{arrow_type_to_iceberg, iceberg_field_to_arrow};

        let mut updated = existing.clone();
        let mut field_changed = false;

        if existing.required && input_field.is_nullable() {
            updated.required = false;
            field_changed = true;
        }

        match (&*existing.field_type, input_field.data_type()) {
            (Type::Struct(existing_struct), DataType::Struct(input_fields)) => {
                let (struct_type, changed) =
                    Self::merge_struct(existing_struct, input_fields, next_field_id)?;
                if changed {
                    updated.field_type = Box::new(Type::Struct(struct_type));
                }
                Ok((updated, field_changed || changed))
            }
            (
                Type::List(existing_list),
                DataType::List(child)
                | DataType::ListView(child)
                | DataType::LargeList(child)
                | DataType::LargeListView(child),
            ) => {
                let (element_field, element_changed) =
                    Self::merge_field(existing_list.element_field.as_ref(), child, next_field_id)?;
                if element_changed {
                    updated.field_type =
                        Box::new(Type::List(ListType::new(Arc::new(element_field))));
                }
                Ok((updated, field_changed || element_changed))
            }
            (Type::Map(existing_map), DataType::Map(entries, _)) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(DataFusionError::Plan(
                        "Iceberg map entries must be struct types".to_string(),
                    ));
                };
                if entry_fields.len() != 2 {
                    return Err(DataFusionError::Plan(format!(
                        "Iceberg map entries must have exactly 2 fields, found {}",
                        entry_fields.len()
                    )));
                }
                let (merged_key, key_changed) = Self::merge_field(
                    existing_map.key_field.as_ref(),
                    &entry_fields[0],
                    next_field_id,
                )?;
                if key_changed {
                    return Err(DataFusionError::Plan(
                        "Schema evolution for map keys is not allowed; use overwriteSchema=true to replace the schema."
                            .to_string(),
                    ));
                }
                let (merged_value, value_changed) = Self::merge_field(
                    existing_map.value_field.as_ref(),
                    &entry_fields[1],
                    next_field_id,
                )?;
                if value_changed {
                    updated.field_type = Box::new(Type::Map(MapType::new(
                        Arc::new(merged_key),
                        Arc::new(merged_value),
                    )));
                }
                Ok((updated, field_changed || value_changed))
            }
            _ => {
                let existing_arrow = iceberg_field_to_arrow(existing)?;
                if !Self::field_types_compatible(&existing_arrow, input_field) {
                    return Err(DataFusionError::Plan(format!(
                        "Column '{}' has type {:?} in the table but {:?} in the input data. Set mergeSchema=true to allow schema evolution or overwriteSchema=true to replace the schema.",
                        existing.name,
                        existing_arrow.data_type(),
                        input_field.data_type(),
                    )));
                }
                if Self::is_allowed_type_promotion(
                    existing_arrow.data_type(),
                    input_field.data_type(),
                ) {
                    updated.field_type = Box::new(arrow_type_to_iceberg(input_field.data_type())?);
                    field_changed = true;
                }
                Ok((updated, field_changed))
            }
        }
    }

    fn merge_struct(
        existing_struct: &StructType,
        input_fields: &[FieldRef],
        next_field_id: &mut i32,
    ) -> Result<(StructType, bool)> {
        // FIXME: This lookup map is case-sensitive.
        let mut existing_pool: HashMap<String, Arc<NestedField>> = HashMap::new();
        for field in existing_struct.fields() {
            existing_pool.insert(field.name.clone(), Arc::clone(field));
        }

        let mut changed = false;
        let mut merged_children =
            Vec::with_capacity(existing_struct.fields().len().max(input_fields.len()));

        // 1. Build fields in the order of Input Arrow Schema
        for input_field in input_fields {
            if let Some(existing_field) = existing_pool.remove(input_field.name()) {
                // Field exists: perform recursive merge_field, preserving original ID
                let (merged_field, field_changed) =
                    Self::merge_field(existing_field.as_ref(), input_field, next_field_id)?;
                changed |= field_changed;
                merged_children.push(Arc::new(merged_field));
            } else {
                // FIXME: - Missing Nested Column Rename detection.
                // Same as top-level fields: renaming a nested field is currently treated as adding a new field.
                // This breaks schema evolution for renamed nested fields.
                let new_field = Self::build_field_from_arrow(input_field, next_field_id)?;
                merged_children.push(Arc::new(new_field));
                changed = true;
            }
        }

        // 2. Handle fields that exist in Iceberg but not in Input (i.e., omitted old fields)
        // These fields should be preserved and placed at the end
        for remaining_field in existing_pool.values() {
            if remaining_field.required && !Self::field_has_default(remaining_field) {
                return Err(DataFusionError::Plan(format!(
                    "Column '{}' is required in the Iceberg schema and must be present in the input data.",
                    remaining_field.name
                )));
            }
            merged_children.push(Arc::clone(remaining_field));
        }

        Ok((StructType::new(merged_children), changed))
    }

    fn build_field_from_arrow(field: &Field, next_field_id: &mut i32) -> Result<NestedField> {
        use crate::datasource::type_converter::arrow_field_to_iceberg;

        let mut iceberg_field = arrow_field_to_iceberg(field)?;

        if iceberg_field.required && iceberg_field.initial_default.is_none() {
            iceberg_field.required = false;
        }

        iceberg_field.id = *next_field_id;
        *next_field_id += 1;
        Self::assign_nested_ids(&mut iceberg_field, next_field_id);
        Ok(iceberg_field)
    }

    fn assign_nested_ids(field: &mut NestedField, next_field_id: &mut i32) {
        Self::assign_type_ids(field.field_type.as_mut(), next_field_id);
    }

    pub fn assign_schema_field_ids(schema: &IcebergSchema) -> Result<IcebergSchema> {
        let mut next_field_id = 1;
        let mut new_fields = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let mut cloned = field.as_ref().clone();
            Self::reassign_ids_recursive(&mut cloned, &mut next_field_id);
            new_fields.push(Arc::new(cloned));
        }
        IcebergSchema::builder()
            .with_fields(new_fields)
            .build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to assign Iceberg field ids: {e}")))
    }

    fn reassign_ids_recursive(field: &mut NestedField, next_field_id: &mut i32) {
        field.id = *next_field_id;
        *next_field_id += 1;
        match field.field_type.as_ref() {
            Type::Struct(struct_type) => {
                let mut updated = Vec::with_capacity(struct_type.fields().len());
                for child in struct_type.fields() {
                    let mut nested = child.as_ref().clone();
                    Self::reassign_ids_recursive(&mut nested, next_field_id);
                    updated.push(Arc::new(nested));
                }
                *field.field_type = Type::Struct(StructType::new(updated));
            }
            Type::List(list_type) => {
                let mut element = list_type.element_field.as_ref().clone();
                Self::reassign_ids_recursive(&mut element, next_field_id);
                *field.field_type = Type::List(ListType::new(Arc::new(element)));
            }
            Type::Map(map_type) => {
                let mut key = map_type.key_field.as_ref().clone();
                Self::reassign_ids_recursive(&mut key, next_field_id);
                let mut value = map_type.value_field.as_ref().clone();
                Self::reassign_ids_recursive(&mut value, next_field_id);
                *field.field_type = Type::Map(MapType::new(Arc::new(key), Arc::new(value)));
            }
            _ => {}
        }
    }

    fn assign_type_ids(ty: &mut Type, next_field_id: &mut i32) {
        match ty {
            Type::Struct(struct_type) => {
                let mut updated_fields = Vec::with_capacity(struct_type.fields().len());
                for child in struct_type.fields() {
                    let mut nested = child.as_ref().clone();
                    nested.id = *next_field_id;
                    *next_field_id += 1;
                    Self::assign_nested_ids(&mut nested, next_field_id);
                    updated_fields.push(Arc::new(nested));
                }
                *ty = Type::Struct(StructType::new(updated_fields));
            }
            Type::List(list_type) => {
                let mut element = list_type.element_field.as_ref().clone();
                element.id = *next_field_id;
                *next_field_id += 1;
                Self::assign_nested_ids(&mut element, next_field_id);
                *ty = Type::List(ListType::new(Arc::new(element)));
            }
            Type::Map(map_type) => {
                let mut key = map_type.key_field.as_ref().clone();
                key.id = *next_field_id;
                *next_field_id += 1;
                Self::assign_nested_ids(&mut key, next_field_id);

                let mut value = map_type.value_field.as_ref().clone();
                value.id = *next_field_id;
                *next_field_id += 1;
                Self::assign_nested_ids(&mut value, next_field_id);

                *ty = Type::Map(MapType::new(Arc::new(key), Arc::new(value)));
            }
            Type::Primitive(_) => {}
        }
    }

    fn reuse_nested_ids_from_existing(
        existing: &NestedField,
        candidate: &mut NestedField,
        next_field_id: &mut i32,
    ) -> Result<()> {
        match (&*existing.field_type, candidate.field_type.as_mut()) {
            (Type::Struct(existing_struct), Type::Struct(candidate_struct)) => {
                let mut updated_fields = Vec::with_capacity(candidate_struct.fields().len());
                for child in candidate_struct.fields() {
                    let mut new_child = child.as_ref().clone();
                    if let Some(existing_child) = existing_struct.field_by_name(&new_child.name) {
                        new_child.id = existing_child.id;
                        Self::reuse_nested_ids_from_existing(
                            existing_child.as_ref(),
                            &mut new_child,
                            next_field_id,
                        )?;
                    } else {
                        new_child.id = *next_field_id;
                        *next_field_id += 1;
                        Self::assign_nested_ids(&mut new_child, next_field_id);
                    }
                    updated_fields.push(Arc::new(new_child));
                }
                *candidate.field_type = Type::Struct(StructType::new(updated_fields));
            }
            (Type::List(existing_list), Type::List(candidate_list)) => {
                let mut new_child = candidate_list.element_field.as_ref().clone();
                new_child.id = existing_list.element_field.id;
                Self::reuse_nested_ids_from_existing(
                    existing_list.element_field.as_ref(),
                    &mut new_child,
                    next_field_id,
                )?;
                *candidate.field_type = Type::List(ListType::new(Arc::new(new_child)));
            }
            (Type::Map(existing_map), Type::Map(candidate_map)) => {
                let mut new_key = candidate_map.key_field.as_ref().clone();
                if Self::types_share_shape(
                    existing_map.key_field.field_type.as_ref(),
                    new_key.field_type.as_ref(),
                ) {
                    new_key.id = existing_map.key_field.id;
                    Self::reuse_nested_ids_from_existing(
                        existing_map.key_field.as_ref(),
                        &mut new_key,
                        next_field_id,
                    )?;
                } else {
                    new_key.id = *next_field_id;
                    *next_field_id += 1;
                    Self::assign_nested_ids(&mut new_key, next_field_id);
                }

                let mut new_value = candidate_map.value_field.as_ref().clone();
                if Self::types_share_shape(
                    existing_map.value_field.field_type.as_ref(),
                    new_value.field_type.as_ref(),
                ) {
                    new_value.id = existing_map.value_field.id;
                    Self::reuse_nested_ids_from_existing(
                        existing_map.value_field.as_ref(),
                        &mut new_value,
                        next_field_id,
                    )?;
                } else {
                    new_value.id = *next_field_id;
                    *next_field_id += 1;
                    Self::assign_nested_ids(&mut new_value, next_field_id);
                }

                *candidate.field_type =
                    Type::Map(MapType::new(Arc::new(new_key), Arc::new(new_value)));
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_partition_spec_sources(
        table_meta: &TableMetadata,
        schema: &IcebergSchema,
    ) -> Result<()> {
        if let Some(spec) = table_meta.default_partition_spec() {
            for field in spec.fields() {
                if schema.field_by_id(field.source_id).is_none() {
                    return Err(DataFusionError::Plan(format!(
                        "Partition field '{}' references missing column id {} in the new schema. Include the column or update the partition spec before writing.",
                        field.name, field.source_id
                    )));
                }
            }
        }
        Ok(())
    }

    fn overwrite_schema(
        table_meta: &TableMetadata,
        current_schema: &IcebergSchema,
        input_schema: &ArrowSchema,
    ) -> Result<SchemaEvolutionOutcome> {
        use crate::datasource::type_converter::{arrow_type_to_iceberg, iceberg_schema_to_arrow};

        let mut identifier_names = HashSet::new();
        for id in current_schema.identifier_field_ids() {
            if let Some(name) = current_schema.name_by_field_id(id) {
                identifier_names.insert(name.to_string());
            }
        }

        let mut next_field_id = table_meta.last_column_id + 1;
        let mut new_fields = Vec::new();

        for field in input_schema.fields() {
            let iceberg_type = arrow_type_to_iceberg(field.data_type()).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to convert column '{}' to an Iceberg type: {e}",
                    field.name()
                ))
            })?;
            let existing_field = current_schema.field_by_name(field.name());
            let field_id = if let Some(existing) = existing_field {
                existing.id
            } else {
                let id = next_field_id;
                next_field_id += 1;
                id
            };
            let mut nested = NestedField::new(
                field_id,
                field.name().clone(),
                iceberg_type,
                !field.is_nullable(),
            );
            if let Some(existing) = existing_field {
                Self::reuse_nested_ids_from_existing(
                    existing.as_ref(),
                    &mut nested,
                    &mut next_field_id,
                )?;
            } else {
                Self::assign_nested_ids(&mut nested, &mut next_field_id);
            }
            new_fields.push(Arc::new(nested));
        }

        let struct_view = StructType::new(new_fields.clone());
        let (name_to_id, _) = SchemaBuilder::build_name_indexes(&struct_view);
        let id_to_field = SchemaBuilder::build_id_to_field_index(&struct_view);
        let mut identifier_ids = Vec::new();
        for identifier in &identifier_names {
            let Some(id) = name_to_id.get(identifier) else {
                return Err(DataFusionError::Plan(format!(
                    "Identifier field '{identifier}' is missing from the overwrite schema. Provide all identifier columns or drop the identifier before overwriting."
                )));
            };

            if let Some(field) = id_to_field.get(id) {
                if !field.required {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot overwrite table: Identifier field '{}' must be required, but input schema defines it as optional.",
                        identifier
                    )));
                }

                match field.field_type.as_ref() {
                    Type::Primitive(p) => {
                        if matches!(p, PrimitiveType::Float | PrimitiveType::Double) {
                            return Err(DataFusionError::Plan(format!(
                                "Cannot overwrite table: Identifier field '{}' cannot be type Float or Double.",
                                identifier
                            )));
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Cannot overwrite table: Identifier field '{}' must be a primitive type.",
                            identifier
                        )));
                    }
                }
            }

            identifier_ids.push(*id);
        }

        let mut builder = IcebergSchema::builder()
            .with_schema_id(Self::next_schema_id(table_meta))
            .with_fields(new_fields);
        if !identifier_ids.is_empty() {
            builder = builder.with_identifier_field_ids(identifier_ids);
        }
        let new_schema = builder.build().map_err(|e| {
            DataFusionError::Plan(format!("Failed to build overwritten schema: {e}"))
        })?;
        Self::validate_partition_spec_sources(table_meta, &new_schema)?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&new_schema)?);
        Ok(SchemaEvolutionOutcome {
            iceberg_schema: new_schema,
            arrow_schema,
            changed: true,
        })
    }

    fn next_schema_id(table_meta: &TableMetadata) -> i32 {
        table_meta
            .schemas
            .iter()
            .map(|s| s.schema_id())
            .max()
            .unwrap_or(0)
            + 1
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::metadata::format::FormatVersion;
    use crate::spec::partition::PartitionSpec;
    use crate::spec::transform::Transform;
    use crate::spec::types::values::{Literal, PrimitiveLiteral};
    use crate::spec::types::PrimitiveType;

    #[test]
    fn assign_schema_field_ids_assigns_nested_children() {
        let nested_struct = StructType::new(vec![
            Arc::new(NestedField::new(
                0,
                "inner_a",
                Type::Primitive(PrimitiveType::Int),
                true,
            )),
            Arc::new(NestedField::new(
                0,
                "inner_b",
                Type::Primitive(PrimitiveType::String),
                false,
            )),
        ]);

        let fields = vec![
            Arc::new(NestedField::new(
                0,
                "id",
                Type::Primitive(PrimitiveType::Long),
                true,
            )),
            Arc::new(NestedField::new(
                0,
                "payload",
                Type::Struct(nested_struct),
                false,
            )),
        ];

        let schema = IcebergSchema::builder()
            .with_fields(fields)
            .build()
            .expect("schema");

        let assigned = SchemaEvolver::assign_schema_field_ids(&schema).expect("assign ids");
        let id_field = assigned.field_by_name("id").expect("id field");
        assert_eq!(id_field.id, 1);

        let payload_field = assigned.field_by_name("payload").expect("payload field");
        assert!(payload_field.id > id_field.id);

        let Type::Struct(payload_struct) = payload_field.field_type.as_ref() else {
            panic!("payload not struct");
        };
        for child in payload_struct.fields() {
            assert!(
                child.id != 0,
                "expected nested field '{}' to have non-zero id",
                child.name
            );
        }
        assert_ne!(
            payload_struct.fields()[0].id,
            payload_struct.fields()[1].id,
            "nested field ids should be unique"
        );
    }

    #[test]
    fn validate_exact_schema_allows_int_to_long() {
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::new(
                1,
                "value",
                Type::Primitive(PrimitiveType::Long),
                true,
            ))])
            .build()
            .expect("schema");
        let table_schema =
            Arc::new(iceberg_schema_to_arrow(&iceberg_schema).expect("arrow schema"));
        let input_schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);

        SchemaEvolver::validate_exact_schema(table_schema.as_ref(), &iceberg_schema, &input_schema)
            .expect("int -> long promotion should be allowed");
    }

    #[test]
    fn validate_exact_schema_rejects_missing_required_column() {
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::new(
                1,
                "required_col",
                Type::Primitive(PrimitiveType::String),
                true,
            ))])
            .build()
            .expect("schema");
        let table_schema =
            Arc::new(iceberg_schema_to_arrow(&iceberg_schema).expect("arrow schema"));
        let input_schema = Schema::new(Vec::<Field>::new());

        let err = SchemaEvolver::validate_exact_schema(
            table_schema.as_ref(),
            &iceberg_schema,
            &input_schema,
        )
        .expect_err("missing required column should fail");
        match err {
            DataFusionError::Plan(msg) => assert!(
                msg.contains("required_col"),
                "unexpected plan error message: {msg}"
            ),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn validate_exact_schema_allows_missing_required_with_default() {
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::new(
                    1,
                    "generated_col",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .with_write_default(Literal::Primitive(PrimitiveLiteral::Int(42))),
            )])
            .build()
            .expect("schema");
        let table_schema =
            Arc::new(iceberg_schema_to_arrow(&iceberg_schema).expect("arrow schema"));
        let input_schema = Schema::new(Vec::<Field>::new());

        SchemaEvolver::validate_exact_schema(table_schema.as_ref(), &iceberg_schema, &input_schema)
            .expect("write defaults should allow omission");
    }

    #[test]
    fn merge_schema_allows_safe_int_to_long_writes() {
        let fields = vec![Arc::new(NestedField::new(
            1,
            "value",
            Type::Primitive(PrimitiveType::Long),
            true,
        ))];
        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("schema");
        let table_meta = test_table_metadata(schema.clone(), None);

        let input_schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        assert!(
            !input_schema
                .field_with_name("value")
                .expect("value field present")
                .is_nullable(),
            "value column should be non-nullable in input schema"
        );

        let outcome =
            SchemaEvolver::merge_schema(&table_meta, &schema, &input_schema).expect("merge");
        assert!(
            !outcome.changed,
            "Safe writes should not force schema evolution"
        );
        let merged_field = outcome
            .iceberg_schema
            .field_by_name("value")
            .expect("value field");
        assert!(merged_field.required, "existing field must remain required");
        assert!(
            matches!(
                merged_field.field_type.as_ref(),
                Type::Primitive(PrimitiveType::Long)
            ),
            "Field type should remain LONG"
        );
    }

    #[test]
    fn build_field_from_arrow_reassigns_nested_ids() {
        let inner_field = Field::new("child", DataType::Int32, false).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_string(), "777".to_string())]
                .into_iter()
                .collect(),
        );
        let arrow_struct = Field::new(
            "parent",
            DataType::Struct(Fields::from(vec![inner_field])),
            false,
        )
        .with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_string(), "555".to_string())]
                .into_iter()
                .collect(),
        );

        let mut next_id = 10;
        let field =
            SchemaEvolver::build_field_from_arrow(&arrow_struct, &mut next_id).expect("build");
        assert_eq!(field.id, 10);
        let Type::Struct(struct_ty) = field.field_type.as_ref() else {
            panic!("expected struct");
        };
        let child = &struct_ty.fields()[0];
        assert_eq!(
            child.id, 11,
            "child id should come from next_field_id, not metadata"
        );
        assert_eq!(next_id, 12);
    }

    #[test]
    fn overwrite_schema_preserves_nested_identifier_fields() {
        let customer_struct = StructType::new(vec![
            Arc::new(NestedField::new(
                2,
                "id",
                Type::Primitive(PrimitiveType::Long),
                true,
            )),
            Arc::new(NestedField::new(
                3,
                "name",
                Type::Primitive(PrimitiveType::String),
                true,
            )),
        ]);
        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::new(
                1,
                "customer",
                Type::Struct(customer_struct),
                true,
            ))])
            .with_identifier_field_ids([2])
            .build()
            .expect("schema");
        let table_meta = test_table_metadata(schema.clone(), None);

        let input_schema = Schema::new(vec![Field::new(
            "customer",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )]);

        let result = SchemaEvolver::overwrite_schema(&table_meta, &schema, &input_schema).unwrap();
        let identifiers: Vec<i32> = result.iceberg_schema.identifier_field_ids().collect();
        assert_eq!(
            identifiers,
            vec![2],
            "identifier should remain nested field id"
        );
    }

    #[test]
    fn overwrite_schema_rejects_partition_columns_removal() {
        let fields = vec![Arc::new(NestedField::new(
            1,
            "event_ts",
            Type::Primitive(PrimitiveType::Timestamp),
            false,
        ))];
        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("schema");

        let spec = PartitionSpec::builder()
            .with_spec_id(7)
            .add_field(1, "event_ts_bucket", Transform::Identity)
            .build();

        let table_meta = test_table_metadata(schema.clone(), Some(spec));

        let input_schema = Schema::new(vec![Field::new("other", DataType::Utf8, true)]);
        let err = SchemaEvolver::overwrite_schema(&table_meta, &schema, &input_schema).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Partition field"),
            "expected partition validation error, got {msg}"
        );
    }

    fn test_table_metadata(
        schema: IcebergSchema,
        partition_spec: Option<PartitionSpec>,
    ) -> TableMetadata {
        let last_column_id = schema.highest_field_id();
        let partition_specs: Vec<PartitionSpec> =
            partition_spec.clone().into_iter().collect::<Vec<_>>();
        let default_spec_id = partition_spec
            .as_ref()
            .map(|spec| spec.spec_id())
            .unwrap_or(0);
        let last_partition_id = partition_spec
            .as_ref()
            .and_then(|spec| spec.highest_field_id())
            .unwrap_or(0);
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "file:///tmp/iceberg".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            last_column_id,
            schemas: vec![schema.clone()],
            current_schema_id: schema.schema_id(),
            partition_specs,
            default_spec_id,
            last_partition_id,
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: HashMap::new(),
            statistics: vec![],
            partition_statistics: vec![],
        }
    }
}
