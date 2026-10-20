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
