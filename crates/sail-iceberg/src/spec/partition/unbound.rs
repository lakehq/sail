// SPDX-License-Identifier: Apache-2.0
// SPDX-FileNotice: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// SPDX-FileComment: Credit: https://github.com/apache/iceberg-rust/blob/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/partition.rs
// SPDX-FileAttributionText: Notice: https://github.com/apache/iceberg-rust/blob/dc349284a4204c1a56af47fb3177ace6f9e899a0/NOTICE

use serde::{Deserialize, Serialize};

use crate::spec::transform::Transform;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionField {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_id: Option<i32>,
    pub source_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct UnboundPartitionSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_id: Option<i32>,
    pub fields: Vec<UnboundPartitionField>,
}

impl From<&super::PartitionSpec> for UnboundPartitionSpec {
    fn from(spec: &super::PartitionSpec) -> Self {
        Self {
            spec_id: Some(spec.spec_id()),
            fields: spec
                .fields()
                .iter()
                .map(|field| UnboundPartitionField {
                    field_id: Some(field.field_id),
                    source_id: field.source_id,
                    name: field.name.clone(),
                    transform: field.transform,
                })
                .collect(),
        }
    }
}
