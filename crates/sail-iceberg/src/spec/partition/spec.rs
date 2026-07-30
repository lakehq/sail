// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/partition.rs

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::spec::metadata::FormatVersion;
use crate::spec::schema::Schema;
use crate::spec::transform::Transform;
use crate::spec::types::{NestedField, StructType};

pub(crate) const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;
pub(crate) const DEFAULT_PARTITION_SPEC_ID: i32 = 0;

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

/// Partition fields capture the transform from table data to partition values.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// A source column id from the table's schema
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub source_id: i32,
    /// Source column ids for Iceberg v3 multi-argument transforms.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_ids: Vec<i32>,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    pub field_id: i32,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

impl PartitionField {
    /// Create a new partition field.
    pub fn new(source_id: i32, field_id: i32, name: impl ToString, transform: Transform) -> Self {
        Self {
            source_id,
            source_ids: vec![],
            field_id,
            name: name.to_string(),
            transform,
        }
    }

    pub fn source_id(&self) -> Result<i32, String> {
        self.compatibility_source_id().ok_or_else(|| {
            "Iceberg v3 multi-argument partition transforms are not yet supported".to_string()
        })
    }

    fn compatibility_source_id(&self) -> Option<i32> {
        if self.source_id != 0 {
            Some(self.source_id)
        } else if self.source_ids.len() == 1 {
            Some(self.source_ids[0])
        } else {
            None
        }
    }
}

/// Reference to [`PartitionSpec`].
pub type PartitionSpecRef = Arc<PartitionSpec>;

/// Partition spec that defines how to produce a tuple of partition values from a record.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    /// Identifier for PartitionSpec
    spec_id: i32,
    /// Details of the partition spec
    fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create a new partition spec builder.
    pub fn builder() -> PartitionSpecBuilder {
        PartitionSpecBuilder::new()
    }

    /// Fields of the partition spec
    pub fn fields(&self) -> &[PartitionField] {
        &self.fields
    }

    /// Spec id of the partition spec
    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    /// Get a new unpartitioned partition spec
    pub fn unpartitioned_spec() -> Self {
        Self {
            spec_id: DEFAULT_PARTITION_SPEC_ID,
            fields: vec![],
        }
    }

    /// Returns if the partition spec is unpartitioned.
    ///
    /// A [`PartitionSpec`] is unpartitioned if it has no fields or all fields are [`Transform::Void`] transform.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty() || self.fields.iter().all(|f| f.transform == Transform::Void)
    }

    /// Returns the partition type of this partition spec.
    pub fn partition_type(&self, schema: &Schema) -> Result<StructType, String> {
        let mut partition_fields = Vec::new();

        for partition_field in self.fields.iter() {
            let source_id = partition_field.source_id()?;
            let result_type = if let Some(source_field) = schema.field_by_id(source_id) {
                // Prefer logical date type for Day transform to align with Iceberg writers
                if matches!(partition_field.transform, Transform::Day) {
                    crate::spec::types::Type::Primitive(crate::spec::types::PrimitiveType::Date)
                } else {
                    partition_field
                        .transform
                        .result_type(&source_field.field_type)?
                }
            } else {
                crate::spec::types::Type::Primitive(crate::spec::types::PrimitiveType::Unknown)
            };

            let nested_field = NestedField::new(
                partition_field.field_id,
                &partition_field.name,
                result_type,
                false, // Partition fields are typically optional
            );

            partition_fields.push(Arc::new(nested_field));
        }

        Ok(StructType::new(partition_fields))
    }

    /// Change the spec id of the partition spec
    pub fn with_spec_id(self, spec_id: i32) -> Self {
        Self { spec_id, ..self }
    }

    /// Get the highest field id in the partition spec.
    pub fn highest_field_id(&self) -> Option<i32> {
        self.fields.iter().map(|f| f.field_id).max()
    }

    /// Check if the partition spec has sequential field ids starting from 1000.
    /// Required for spec version 1 in the reference implementation.
    pub fn has_sequential_ids(&self) -> bool {
        for (expected, field) in (1000..).zip(self.fields.iter()) {
            if field.field_id != expected {
                return false;
            }
        }
        true
    }

    /// Check if this partition spec is compatible with another partition spec.
    ///
    /// Returns true if the partition spec is equal to the other spec with partition field ids ignored and
    /// spec_id ignored. The following must be identical:
    /// * The number of fields
    /// * Field order
    /// * Field names
    /// * Source column ids
    /// * Transforms
    pub fn is_compatible_with(&self, other: &PartitionSpec) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (this_field, other_field) in self.fields.iter().zip(other.fields.iter()) {
            let Some(this_source_id) = this_field.compatibility_source_id() else {
                return false;
            };
            let Some(other_source_id) = other_field.compatibility_source_id() else {
                return false;
            };

            if this_source_id != other_source_id
                || this_field.name != other_field.name
                || this_field.transform != other_field.transform
            {
                return false;
            }
        }

        true
    }
}

/// Assign a replacement spec without changing historical specs or reusing incompatible IDs.
pub fn assign_replacement_partition_spec(
    format_version: FormatVersion,
    partition_specs: &[PartitionSpec],
    default_spec_id: i32,
    last_partition_id: i32,
    requested: &PartitionSpec,
) -> Result<PartitionSpec, String> {
    if let Some(existing) = partition_specs
        .iter()
        .find(|spec| spec.is_compatible_with(requested))
    {
        return Ok(existing.clone());
    }

    let next_spec_id = partition_specs
        .iter()
        .map(PartitionSpec::spec_id)
        .max()
        .unwrap_or(-1)
        .checked_add(1)
        .ok_or_else(|| "Iceberg partition spec ID overflow".to_string())?;

    if format_version == FormatVersion::V1 {
        let current = partition_specs
            .iter()
            .find(|spec| spec.spec_id() == default_spec_id);
        let mut requested_used = vec![false; requested.fields().len()];
        let mut fields = Vec::new();

        if let Some(current) = current {
            for current_field in current.fields() {
                let requested_index =
                    requested
                        .fields()
                        .iter()
                        .enumerate()
                        .find_map(|(index, requested_field)| {
                            (!requested_used[index]
                                && current_field.compatibility_source_id()
                                    == requested_field.compatibility_source_id()
                                && current_field.name == requested_field.name
                                && current_field.transform == requested_field.transform)
                                .then_some(index)
                        });
                let transform = if let Some(index) = requested_index {
                    requested_used[index] = true;
                    current_field.transform
                } else {
                    Transform::Void
                };
                fields.push((
                    current_field.source_id()?,
                    current_field.name.clone(),
                    transform,
                ));
            }
        }

        for (index, requested_field) in requested.fields().iter().enumerate() {
            if !requested_used[index] {
                fields.push((
                    requested_field.source_id()?,
                    requested_field.name.clone(),
                    requested_field.transform,
                ));
            }
        }

        let mut builder = PartitionSpec::builder().with_spec_id(next_spec_id);
        for (index, (source_id, name, transform)) in fields.into_iter().enumerate() {
            let field_id = i32::try_from(index)
                .ok()
                .and_then(|index| 1000_i32.checked_add(index))
                .ok_or_else(|| "Iceberg v1 partition field ID overflow".to_string())?;
            builder = builder.add_field_with_id(source_id, field_id, name, transform);
        }
        let candidate = builder.build();
        if let Some(existing) = partition_specs
            .iter()
            .find(|spec| spec.is_compatible_with(&candidate))
        {
            Ok(existing.clone())
        } else {
            Ok(candidate)
        }
    } else {
        let historical_highest = partition_specs
            .iter()
            .filter_map(PartitionSpec::highest_field_id)
            .max()
            .unwrap_or(UNPARTITIONED_LAST_ASSIGNED_ID);
        let mut next_field_id = last_partition_id
            .max(historical_highest)
            .max(UNPARTITIONED_LAST_ASSIGNED_ID)
            .checked_add(1)
            .ok_or_else(|| "Iceberg partition field ID overflow".to_string())?;
        let mut builder = PartitionSpec::builder().with_spec_id(next_spec_id);

        for requested_field in requested.fields() {
            let source_id = requested_field.source_id()?;
            let historical_field_id = partition_specs
                .iter()
                .flat_map(|spec| spec.fields())
                .find(|historical_field| {
                    historical_field.compatibility_source_id() == Some(source_id)
                        && historical_field.name == requested_field.name
                        && historical_field.transform == requested_field.transform
                })
                .map(|field| field.field_id);
            let field_id = if let Some(field_id) = historical_field_id {
                field_id
            } else {
                let assigned = next_field_id;
                next_field_id = next_field_id
                    .checked_add(1)
                    .ok_or_else(|| "Iceberg partition field ID overflow".to_string())?;
                assigned
            };
            builder = builder.add_field_with_id(
                source_id,
                field_id,
                &requested_field.name,
                requested_field.transform,
            );
        }
        Ok(builder.build())
    }
}

/// Builder for partition spec.
#[derive(Debug)]
pub struct PartitionSpecBuilder {
    spec_id: i32,
    fields: Vec<PartitionField>,
    next_field_id: i32,
}

impl PartitionSpecBuilder {
    /// Create a new partition spec builder.
    pub fn new() -> Self {
        Self {
            spec_id: DEFAULT_PARTITION_SPEC_ID,
            fields: Vec::new(),
            next_field_id: 1000, // Partition field IDs typically start from 1000
        }
    }

    /// Set the spec id.
    pub fn with_spec_id(mut self, spec_id: i32) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Add a partition field.
    pub fn add_field(mut self, source_id: i32, name: impl ToString, transform: Transform) -> Self {
        let field = PartitionField::new(source_id, self.next_field_id, name, transform);
        self.fields.push(field);
        self.next_field_id += 1;
        self
    }

    /// Add a partition field with explicit field id.
    pub fn add_field_with_id(
        mut self,
        source_id: i32,
        field_id: i32,
        name: impl ToString,
        transform: Transform,
    ) -> Self {
        let field = PartitionField::new(source_id, field_id, name, transform);
        self.fields.push(field);
        self.next_field_id = self.next_field_id.max(field_id + 1);
        self
    }

    /// Build the partition spec.
    pub fn build(self) -> PartitionSpec {
        PartitionSpec {
            spec_id: self.spec_id,
            fields: self.fields,
        }
    }
}

impl Default for PartitionSpecBuilder {
    fn default() -> Self {
        Self::new()
    }
}
