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

use crate::spec::schema::Schema;
use crate::spec::transform::Transform;
use crate::spec::types::{NestedField, StructType};

#[allow(unused)]
pub(crate) const UNPARTITIONED_LAST_ASSIGNED_ID: i32 = 999;
pub(crate) const DEFAULT_PARTITION_SPEC_ID: i32 = 0;

/// Partition fields capture the transform from table data to partition values.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    /// A source column id from the table's schema
    pub source_id: i32,
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
            field_id,
            name: name.to_string(),
            transform,
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
            let source_field = schema
                .field_by_id(partition_field.source_id)
                .ok_or_else(|| {
                    format!(
                        "Cannot find source field with id {}",
                        partition_field.source_id
                    )
                })?;

            // Prefer logical date type for Day transform to align with Iceberg writers
            let result_type = if matches!(partition_field.transform, Transform::Day) {
                crate::spec::types::Type::Primitive(crate::spec::types::PrimitiveType::Date)
            } else {
                partition_field
                    .transform
                    .result_type(&source_field.field_type)?
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
        let mut expected = 1000;
        for field in &self.fields {
            if field.field_id != expected {
                return false;
            }
            expected += 1;
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
            if this_field.source_id != other_field.source_id
                || this_field.name != other_field.name
                || this_field.transform != other_field.transform
            {
                return false;
            }
        }

        true
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
